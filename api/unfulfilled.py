#!/usr/bin/env python3
import os
import json
import time
import math
import gspread
from http.server import BaseHTTPRequestHandler
from decimal import Decimal
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')
SHOP_NAME    = '05fd36-2'
API_VERSION  = '2024-07'

GRAPHQL_URL  = f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/graphql.json"
HEADERS      = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json"
}

# NEW: Behavior - Page size 250 is much faster
PAGE_SIZE                   = 250
INCLUDE_LINE_ITEMS          = False
INCLUDE_PARTIALS            = True

# NEW: Your updated filters
QUERY_EXTRA_FILTERS         = "-financial_status:voided -financial_status:refunded -status:cancelled"

# NEW: Google Sheet settings
GOOGLE_SHEET_URL = os.environ.get('UNFULFILLED_SHEET_URL')
SHEET_TAB_NAME = '_MasterOrderList'

# ──────────────────────────────────────────────────────────────

def build_query():
    status_clause = "fulfillment_status:unfulfilled" + (" OR fulfillment_status:partial" if INCLUDE_PARTIALS else "")
    search = status_clause + (f" {QUERY_EXTRA_FILTERS}" if QUERY_EXTRA_FILTERS.strip() else "")
    return f"""
      query($first:Int!, $after:String, $search:String!) {{
        orders(first: $first, after: $after, query: $search, sortKey: CREATED_AT, reverse: true) {{
          pageInfo {{ hasNextPage }}
          edges {{
            cursor
            node {{
              id
              legacyResourceId
              name
              createdAt
              email
              sourceName
              tags
              displayFinancialStatus
              displayFulfillmentStatus
              currentTotalPriceSet {{ shopMoney {{ amount currencyCode }} }}
              staffMember {{
                id
                firstName
                lastName
                email
              }}
              customer {{ displayName email }}
              fulfillmentOrders(first: 1) {{
                nodes {{
                  assignedLocation {{ location {{ name id }} }}
                }}
              }}
              lineItems(first: 50) {{
                edges {{ node {{ name sku quantity }} }}
              }}
            }}
          }}
        }}
      }}
    """

def make_session():
    s = requests.Session()
    retry = Retry(
        total=5, read=5, connect=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https", adapter)
    return s

def pace_from_cost(extensions):
    if not extensions or "cost" not in extensions: return
    cost = extensions["cost"]
    req_cost = cost.get("requestedQueryCost", 0)
    throttle = cost.get("throttleStatus", {})
    available = throttle.get("currentlyAvailable", 0)
    restore = throttle.get("restoreRate", 50)
    if available < req_cost:
        sleep_s = math.ceil((req_cost - available) / max(restore, 1)) + 1
        print(f"⏳ Throttled: sleeping {sleep_s}s")
        time.sleep(sleep_s)

def safe_get(d, path, default=None):
    cur = d
    for key in path:
        if isinstance(cur, dict): cur = cur.get(key)
        else: return default
        if cur is None: return default
    return cur

def extract_assigned_location(order_node):
    nodes = safe_get(order_node, ["fulfillmentOrders", "nodes"], [])
    if nodes: return safe_get(nodes[0], ["assignedLocation", "location", "name"], "") or ""
    return ""

def build_admin_url(legacy_id: str) -> str:
    return f"https://admin.shopify.com/store/{SHOP_NAME}/orders/{legacy_id}"

def flatten_line_items(order_node):
    edges = safe_get(order_node, ["lineItems", "edges"], []) or []
    parts = []
    for edge in edges:
        li = edge.get("node")
        if li:
            nm  = (li.get("name") or "").strip()
            sku = (li.get("sku") or "").strip()
            qty = li.get("quantity", 0) or 0
            parts.append(f"{nm} ({sku}) × {qty}" if sku else f"{nm} × {qty}")
    return "; ".join(parts)

def get_google_sheet():
    """Connects to Google Sheets using Vercel Env Vars."""
    creds_json_str = os.environ.get('GOOGLE_CREDENTIALS_JSON')
    if not creds_json_str:
        raise ValueError("GOOGLE_CREDENTIALS_JSON env var not set")
    if not GOOGLE_SHEET_URL:
        raise ValueError("GOOGLE_SHEET_URL env var not set")

    creds_dict = json.loads(creds_json_str)
    gc = gspread.service_account_from_dict(creds_dict)
    sheet = gc.open_by_url(GOOGLE_SHEET_URL)
    return sheet.worksheet(SHEET_TAB_NAME)

# This is the Vercel Serverless Function handler
class handler(BaseHTTPRequestHandler):
    def do_GET(self):

        print("▶ Fetching unfulfilled orders…")
        all_rows = []

        # Define header row
        header = [
            "Order Number", "Admin URL", "Order Date", "Customer Name",
            "Customer Email", "Payment Status", "Fulfillment Status",
            "Sales Channel", "Assigned Location", "Staff (API)",
            "Staff Email", "Order Tags", "Order Total"
        ]
        if INCLUDE_LINE_ITEMS: header.append("Order Items")

        query   = build_query()
        session = make_session()
        after   = None
        page    = 0
        total_fetched = 0

        try:
            # 1. FETCH FROM SHOPIFY
            while True:
                status_clause = "fulfillment_status:unfulfilled" + (" OR fulfillment_status:partial" if INCLUDE_PARTIALS else "")
                search = status_clause + (f" {QUERY_EXTRA_FILTERS}" if QUERY_EXTRA_FILTERS.strip() else "")
                variables = {"first": PAGE_SIZE, "after": after, "search": search}

                resp = session.post(GRAPHQL_URL, headers=HEADERS, json={"query": query, "variables": variables}, timeout=60)
                resp.raise_for_status()
                payload = resp.json()

                if payload.get("errors"):
                    print(f"GraphQL error: {payload['errors']}")
                    break

                pace_from_cost(payload.get("extensions"))
                data = payload.get("data", {})
                orders = (data.get("orders") or {})
                edges  = orders.get("edges") or []
                if not edges:
                    print("No matching orders found (edges empty).")
                    break

                # 2. PROCESS ROWS INTO MEMORY
                for edge in edges:
                    node = edge.get("node")
                    if not node: continue

                    legacy_id = str(node.get("legacyResourceId") or "").strip()
                    first = safe_get(node, ["staffMember", "firstName"], "") or ""
                    last  = safe_get(node, ["staffMember", "lastName"], "") or ""
                    staff_name = (" ".join([first, last])).strip() or first or last

                    row_data = [
                        node.get("name") or "",
                        build_admin_url(legacy_id) if legacy_id else "",
                        node.get("createdAt") or "",
                        safe_get(node, ["customer", "displayName"], "") or "",
                        safe_get(node, ["customer", "email"], "") or (node.get("email") or ""),
                        node.get("displayFinancialStatus") or "",
                        node.get("displayFulfillmentStatus") or "",
                        node.get("sourceName") or "",
                        extract_assigned_location(node),
                        staff_name,
                        safe_get(node, ["staffMember", "email"], "") or "",
                        ",".join(node.get("tags") or []),
                        safe_get(node, ["currentTotalPriceSet", "shopMoney", "amount"], "0") or "0",
                    ]
                    if INCLUDE_LINE_ITEMS:
                        row_data.append(flatten_line_items(node))

                    all_rows.append(row_data)
                    total_fetched += 1

                after = edges[-1].get("cursor")
                page += 1
                print(f"Fetched page {page}. Total orders so far: {total_fetched}")

                if not (orders.get("pageInfo") or {}).get("hasNextPage"):
                    print("hasNextPage = False; finished fetching.")
                    break

                time.sleep(0.3)

            # 3. WRITE TO GOOGLE SHEETS
            print(f"Connecting to Google Sheet '{SHEET_TAB_NAME}'...")
            worksheet = get_google_sheet()

            print("Clearing old data...")
            worksheet.clear()

            print(f"Writing {len(all_rows)} new rows in one batch...")
            # Write header + all data rows at once
            worksheet.update('A1', [header] + all_rows, value_input_option='USER_ENTERED')

            print("✅ Done.")

            # 4. SEND VERCEL RESPONSE
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_body = json.dumps({"status": "success", "orders_written": len(all_rows)})
            self.wfile.write(response_body.encode('utf-8'))

        except Exception as e:
            print(f"Unhandled exception: {repr(e)}")
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_body = json.dumps({"status": "error", "message": str(e)})
            self.wfile.write(response_body.encode('utf-8'))

        return