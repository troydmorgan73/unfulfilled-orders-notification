# ==========================================
# This looks at the price and the variant metafield custom.last_known_price to see if a price changed. if a price did change
# it will write that to the google sheet
# ==========================================

#!/usr/bin/env python3
import os
import json
import time
import gspread
from http.server import BaseHTTPRequestHandler
from datetime import datetime, timedelta
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

GOOGLE_SHEET_URL  = os.environ.get('PRICE_SHEET_URL') 
SHEET_TAB_NAME    = 'Price_Change_Log' 
SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK_URL') # New Env Var

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
    # Aggressive throttling buffer
    if available < 200:
        time.sleep(2)

def get_google_sheet():
    creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
    creds = json.loads(creds_json)
    gc = gspread.service_account_from_dict(creds)
    sheet = gc.open_by_url(GOOGLE_SHEET_URL)
    return sheet.worksheet(SHEET_TAB_NAME)

def send_slack_alert(change_count, sheet_url):
    """Sends a notification to Slack if the webhook env var is set."""
    if not SLACK_WEBHOOK_URL:
        print("⚠️ No Slack Webhook URL found. Skipping notification.")
        return

    message = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"Product prices have changed. Please check this <{sheet_url}|Google Sheet> to see latest prices"
                }
            }
        ]
    }
    
    try:
        requests.post(SLACK_WEBHOOK_URL, json=message, timeout=10)
        print("✅ Slack notification sent.")
    except Exception as e:
        print(f"❌ Failed to send Slack alert: {e}")

def build_admin_url(product_id, variant_id):
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
        
        if 'data' in data and 'products' in data['data']:
            for edge in data['data']['products']['edges']:
                products.append(edge['node'])
            cursor = data['data']['products']['edges'][-1]['cursor']
            has_next = data['data']['products']['pageInfo']['hasNextPage']
        else:
            has_next = False
    
    return products

# 2. UPDATE SHOPIFY METAFIELDS (BATCHED)
def batch_update_shopify(session, updates):
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
        pace_from_cost(res_json.get("extensions"))
        time.sleep(0.5) 

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
                    
                    last_price = 0.0
                    has_history = False
                    
                    if variant['metafield'] and variant['metafield']['value']:
                        try:
                            mf_data = json.loads(variant['metafield']['value'])
                            last_price = float(mf_data['amount'])
                            has_history = True
                        except:
                            last_price = 0.0
                    
                    # LOGIC
                    if not has_history:
                        # Initialize new product (silent)
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
                        
                    elif current_price != last_price:
                        # PRICE CHANGED!
                        diff = current_price - last_price
                        direction = "INCREASE 📈" if diff > 0 else "DROP 📉"
                        
                        admin_link = build_admin_url(product['id'], variant['id'])
                        site_link = build_storefront_url(product['handle'])

                        # Add row with Hyperlinks
                        sheet_rows.append([
                            datetime.now().strftime("%Y-%m-%d %H:%M"), 
                            product['title'],                          
                            variant['title'],                          
                            variant['sku'],                            
                            last_price,                                
                            current_price,                             
                            direction,                                 
                            f'=HYPERLINK("{admin_link}", "View in Admin")',
                            f'=HYPERLINK("{site_link}", "View on Website")'
                        ])
                        
                        # Queue Shopify Update
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
                print(f"Processing {len(sheet_rows)} changes...")
                ws = get_google_sheet()
                
                # 1. Read existing
                try:
                    all_values = ws.get_all_values()
                    if all_values:
                        header = all_values[0]
                        existing_data = all_values[1:]
                    else:
                        header = ["Date", "Product", "Variant", "SKU", "Old Price", "New Price", "Direction", "Admin Link", "Website Link"]
                        existing_data = []
                except:
                    header = ["Date", "Product", "Variant", "SKU", "Old Price", "New Price", "Direction", "Admin Link", "Website Link"]
                    existing_data = []

                # 2. Sort: Newest on Top
                combined_rows = sheet_rows + existing_data
                
                # 3. Prune: Keep only last 7 days
                cutoff_date = datetime.now() - timedelta(days=7)
                final_rows = []
                
                for row in combined_rows:
                    try:
                        row_date = datetime.strptime(row[0], "%Y-%m-%d %H:%M")
                        if row_date >= cutoff_date:
                            final_rows.append(row)
                    except:
                        # Keep invalid/header rows to be safe
                        final_rows.append(row)

                # 4. Cap total size
                if len(final_rows) > 1000:
                    final_rows = final_rows[:1000]

                # 5. Write
                ws.clear()
                ws.update('A1', [header] + final_rows, value_input_option='USER_ENTERED')
                print(f"Updated sheet with {len(final_rows)} rows.")
                
                # 6. SEND SLACK ALERT (New Feature)
                send_slack_alert(len(sheet_rows), GOOGLE_SHEET_URL)

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