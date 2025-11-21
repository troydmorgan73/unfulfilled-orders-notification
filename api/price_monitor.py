#!/usr/bin/env python3
import os
import json
import time
import math
import gspread
from http.server import BaseHTTPRequestHandler
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURATION ---
ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')
SHOP_NAME    = '05fd36-2' # Replace if dynamic
API_VERSION  = '2024-07'

GRAPHQL_URL  = f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/graphql.json"
HEADERS      = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json"
}

GOOGLE_SHEET_URL = os.environ.get('PRICE_SHEET_URL') # New Env Var
SHEET_TAB_NAME   = 'Price_Change_Log' # Make sure this tab exists

# ──────────────────────────────────────────────────────────────

def make_session():
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https", adapter)
    return s

def pace_from_cost(extensions):
    if not extensions or "cost" not in extensions: return
    cost = extensions["cost"]
    throttle = cost.get("throttleStatus", {})
    available = throttle.get("currentlyAvailable", 0)
    restore = throttle.get("restoreRate", 50)
    # Aggressive throttling buffer
    if available < 200:
        time.sleep(2)

def get_google_sheet():
    creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
    creds = json.loads(creds_json)
    gc = gspread.service_account_from_dict(creds)
    sheet = gc.open_by_url(GOOGLE_SHEET_URL)
    return sheet.worksheet(SHEET_TAB_NAME)

def build_admin_url(product_id, variant_id):
    # Extracts numeric ID from gid://shopify/Product/12345
    p_id = product_id.split('/')[-1]
    v_id = variant_id.split('/')[-1]
    return f"https://admin.shopify.com/store/{SHOP_NAME}/products/{p_id}/variants/{v_id}"

def build_storefront_url(handle):
    return f"https://{SHOP_NAME}.myshopify.com/products/{handle}"

# 1. FETCH PRODUCTS & METAFIELDS
def fetch_all_products(session):
    query = """
    query($cursor: String) {
      products(first: 150, after: $cursor) {
        pageInfo { hasNextPage }
        edges {
          cursor
          node {
            id
            title
            handle
            variants(first: 50) {
              nodes {
                id
                title
                sku
                price
                metafield(namespace: "custom", key: "last_known_price") {
                  id
                  value
                }
              }
            }
          }
        }
      }
    }
    """
    products = []
    cursor = None
    has_next = True

    while has_next:
        resp = session.post(GRAPHQL_URL, headers=HEADERS, json={
            "query": query, "variables": {"cursor": cursor}
        })
        data = resp.json()
        pace_from_cost(data.get("extensions"))
        
        for edge in data['data']['products']['edges']:
            products.append(edge['node'])
            cursor = edge['cursor']
            
        has_next = data['data']['products']['pageInfo']['hasNextPage']
    
    return products

# 2. UPDATE SHOPIFY METAFIELDS (BATCHED)
def batch_update_shopify(session, updates):
    # Updates is a list of objects for metafieldsSet
    # We must batch this because GraphQL has limits (approx 25 per call is safe)
    
    mutation = """
    mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        userErrors {
          field
          message
        }
        metafields {
          id
        }
      }
    }
    """
    
    chunk_size = 25
    for i in range(0, len(updates), chunk_size):
        chunk = updates[i:i + chunk_size]
        payload = {
            "query": mutation,
            "variables": {"metafields": chunk}
        }
        resp = session.post(GRAPHQL_URL, headers=HEADERS, json=payload)
        res_json = resp.json()
        
        # Check for errors
        if 'data' in res_json and res_json['data']['metafieldsSet']['userErrors']:
            print(f"⚠️ Error updating metafields: {res_json['data']['metafieldsSet']['userErrors']}")
        
        pace_from_cost(res_json.get("extensions"))
        time.sleep(0.5) # Be gentle

# --- MAIN HANDLER ---
class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        print("▶ Starting Price Check...")
        session = make_session()
        
        try:
            products = fetch_all_products(session)
            
            sheet_rows = []
            shopify_updates = []
            
            for product in products:
                for variant in product['variants']['nodes']:
                    current_price = float(variant['price'])
                    
                    # Parse Metafield (Handle "Money" type JSON or None)
                    last_price = 0.0
                    has_history = False
                    
                    if variant['metafield'] and variant['metafield']['value']:
                        try:
                            # Metafield is JSON string: {"amount": "100.00", "currency_code": "USD"}
                            mf_data = json.loads(variant['metafield']['value'])
                            last_price = float(mf_data['amount'])
                            has_history = True
                        except:
                            # Fallback if data is malformed
                            last_price = 0.0
                    
                    # COMPARE
                    if not has_history:
                        # First time seeing product? Just sync it, don't alert (optional)
                        # Or treat as $0 -> $Price change
                        pass 
                        
                    elif current_price != last_price:
                        # PRICE CHANGED!
                        
                        diff = current_price - last_price
                        direction = "INCREASE 📈" if diff > 0 else "DROP 📉"
                        
                        # 1. Prepare Sheet Row
                        sheet_rows.append([
                            datetime.now().strftime("%Y-%m-%d %H:%M"), # Date
                            product['title'],                          # Product Name
                            variant['title'],                          # Variant Name
                            variant['sku'],                            # SKU
                            last_price,                                # Old Price
                            current_price,                             # New Price
                            direction,                                 # Type
                            build_admin_url(product['id'], variant['id']), # Admin Link
                            build_storefront_url(product['handle'])        # Site Link
                        ])
                        
                        # 2. Prepare Shopify Update
                        # Use the "Money" JSON format
                        new_value_json = json.dumps({
                            "amount": f"{current_price:.2f}",
                            "currency_code": "USD"
                        })
                        
                        shopify_updates.append({
                            "ownerId": variant['id'],
                            "namespace": "custom",
                            "key": "last_known_price",
                            "type": "money",
                            "value": new_value_json
                        })

            # ACTION: WRITE TO SHEETS
            if sheet_rows:
                print(f"Writing {len(sheet_rows)} changes to Google Sheets...")
                ws = get_google_sheet()
                # APPEND rows to the bottom (keep history)
                ws.append_rows(sheet_rows, value_input_option='USER_ENTERED')
            else:
                print("No price changes detected.")

            # ACTION: UPDATE SHOPIFY
            if shopify_updates:
                print(f"Updating {len(shopify_updates)} variants in Shopify...")
                batch_update_shopify(session, shopify_updates)

            # FINISH
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "success", 
                "changes_detected": len(sheet_rows)
            }).encode('utf-8'))

        except Exception as e:
            print(f"Error: {str(e)}")
            self.send_response(500)
            self.wfile.write(json.dumps({"error": str(e)}).encode('utf-8'))