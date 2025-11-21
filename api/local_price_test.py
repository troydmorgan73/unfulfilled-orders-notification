# ==========================================
# This is the same as the price_monitor.py file except i can run this locally
# ==========================================

import os
import json
import time
import gspread
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timedelta

# ==========================================
# 🔧 MANUAL CONFIGURATION (PLUG VALUES HERE)
# ==========================================



API_VERSION = '2024-07'
GRAPHQL_URL = f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/graphql.json"
HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
    "Content-Type": "application/json"
}

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
    # Simple throttle print for local debugging
    if available < 300:
        print(f"   ⏳ API Throttling (Available: {available}). Sleeping...")
        time.sleep(2)

def get_google_sheet():
    print(f"📂 Connecting to Google Sheet...")
    # Load credentials from local file
    gc = gspread.service_account(filename=GOOGLE_CREDS_FILE)
    sheet = gc.open_by_url(GOOGLE_SHEET_URL)
    return sheet.worksheet(SHEET_TAB_NAME)

def send_slack_alert(change_count, sheet_url):
    """Sends a notification to Slack if the webhook URL is set."""
    if not SLACK_WEBHOOK_URL:
        print("   ⚠️ No Slack Webhook URL configured. Skipping notification.")
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
        print("   ✅ Slack notification sent.")
    except Exception as e:
        print(f"   ❌ Failed to send Slack alert: {e}")

def build_admin_url(product_id, variant_id):
    p_id = product_id.split('/')[-1]
    v_id = variant_id.split('/')[-1]
    return f"https://admin.shopify.com/store/{SHOP_NAME}/products/{p_id}/variants/{v_id}"

def build_storefront_url(handle):
    return f"https://{SHOP_NAME}.myshopify.com/products/{handle}"

def fetch_all_products(session):
    print("🔍 Fetching all products from Shopify...")
    query = """
    query($cursor: String) {
      products(first: 100, after: $cursor) {
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
    page_count = 1

    while has_next:
        resp = session.post(GRAPHQL_URL, headers=HEADERS, json={
            "query": query, "variables": {"cursor": cursor}
        })
        data = resp.json()
        
        if 'errors' in data:
            print(f"❌ API Error: {data['errors']}")
            break

        pace_from_cost(data.get("extensions"))
        
        edges = data['data']['products']['edges']
        for edge in edges:
            products.append(edge['node'])
            cursor = edge['cursor']
            
        print(f"   → Page {page_count} fetched ({len(edges)} products)")
        has_next = data['data']['products']['pageInfo']['hasNextPage']
        page_count += 1
    
    return products

def batch_update_shopify(session, updates):
    print(f"💾 Syncing {len(updates)} new prices back to Shopify...")
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
        
        if 'data' in res_json and res_json['data']['metafieldsSet']['userErrors']:
            errs = res_json['data']['metafieldsSet']['userErrors']
            if errs:
                print(f"   ⚠️ Shopify Update Error: {errs}")
        
        pace_from_cost(res_json.get("extensions"))
        print(f"   ✓ Updated batch {i // chunk_size + 1}")
        time.sleep(0.5)

def main():
    print("▶ STARTING LOCAL PRICE MONITOR TEST")
    session = make_session()
    
    try:
        # 1. Get Data
        products = fetch_all_products(session)
        
        sheet_rows = []
        shopify_updates = []
        
        print(f"🧐 Comparing prices for {len(products)} products...")

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
                    new_value_json = json.dumps({"amount": f"{current_price:.2f}", "currency_code": "USD"})
                    shopify_updates.append({
                        "ownerId": variant['id'],
                        "namespace": "custom",
                        "key": "last_known_price",
                        "type": "money",
                        "value": new_value_json
                    })

                elif current_price != last_price:
                    diff = current_price - last_price
                    direction = "INCREASE 📈" if diff > 0 else "DROP 📉"
                    
                    print(f"   🚨 CHANGE DETECTED: {product['title']} ({variant['title']}) | {last_price} -> {current_price}")

                    # Generate URLs first
                    admin_link = build_admin_url(product['id'], variant['id'])
                    site_link = build_storefront_url(product['handle'])

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
                    
                    new_value_json = json.dumps({"amount": f"{current_price:.2f}", "currency_code": "USD"})
                    shopify_updates.append({
                        "ownerId": variant['id'],
                        "namespace": "custom",
                        "key": "last_known_price",
                        "type": "money",
                        "value": new_value_json
                    })

        # 2. Update Sheet (Prepend + Prune Strategy)
        if sheet_rows:
            ws = get_google_sheet()
            
            print("   Existing sheet data...")
            try:
                # Fetch all existing data
                all_values = ws.get_all_values()
                
                # Separate Header from Data
                if all_values:
                    header = all_values[0]
                    existing_data = all_values[1:]
                else:
                    # Fallback if sheet is totally empty
                    header = ["Date", "Product", "Variant", "SKU", "Old Price", "New Price", "Direction", "Admin Link", "Website Link"]
                    existing_data = []
            except Exception as e:
                print(f"   ⚠️ Could not read existing sheet: {e}")
                header = ["Date", "Product", "Variant", "SKU", "Old Price", "New Price", "Direction", "Admin Link", "Website Link"]
                existing_data = []

            # COMBINE: New rows go FIRST
            combined_rows = sheet_rows + existing_data
            
            # FILTER: Keep only last 7 days
            cutoff_date = datetime.now() - timedelta(days=7)
            final_rows = []
            
            print(f"   🧹 Pruning data older than {cutoff_date.strftime('%Y-%m-%d')}...")

            for row in combined_rows:
                try:
                    # Column 0 is the Date string "YYYY-MM-DD HH:MM"
                    row_date = datetime.strptime(row[0], "%Y-%m-%d %H:%M")
                    if row_date >= cutoff_date:
                        final_rows.append(row)
                except:
                    # If date parsing fails (bad data), keep it just in case so we don't lose it
                    final_rows.append(row)

            # SAFETY: If the list is still massive (e.g., huge sale event), cap it at 1000 rows
            if len(final_rows) > 1000:
                final_rows = final_rows[:1000]

            # WRITE: Clear and Overwrite
            print(f"   💾 Writing {len(final_rows)} rows to top of sheet...")
            ws.clear()
            ws.update('A1', [header] + final_rows, value_input_option='USER_ENTERED')
            print(f"✅ Successfully updated sheet.")
            
            # 3. SEND SLACK ALERT (New Feature)
            send_slack_alert(len(sheet_rows), GOOGLE_SHEET_URL)

        else:
            print("✅ No price changes found.")

        # 4. Update Shopify
        if shopify_updates:
            batch_update_shopify(session, shopify_updates)
            print("✅ Shopify Metafields updated.")
        
        print("🏁 SCRIPT COMPLETE")

    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {e}")

if __name__ == "__main__":
    main()