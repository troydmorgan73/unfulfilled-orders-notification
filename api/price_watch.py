import os
import json
import gspread
from http.server import BaseHTTPRequestHandler
from serpapi import GoogleSearch
from datetime import datetime
import traceback # Import traceback for detailed error logging

# --- Config ---
PRICE_WATCH_SHEET_URL = os.environ.get('PRICE_WATCH_SHEET_URL')
SHEET_TAB_NAME = 'Price_Watch'
SERPAPI_KEY = os.environ.get('SERPAPI_KEY')
GOOGLE_CREDENTIALS_JSON = os.environ.get('GOOGLE_CREDENTIALS_JSON')

def get_google_sheet():
    """Connects to Google Sheets using Vercel Env Vars."""
    print("--- [LOG] Inside get_google_sheet() ---")
    if not GOOGLE_CREDENTIALS_JSON:
        print("[ERROR] GOOGLE_CREDENTIALS_JSON env var not set")
        raise ValueError("GOOGLE_CREDENTIALS_JSON env var not set")
    if not PRICE_WATCH_SHEET_URL:
        print("[ERROR] PRICE_WATCH_SHEET_URL env var not set")
        raise ValueError("PRICE_WATCH_SHEET_URL env var not set")
        
    print("[LOG] Env vars found. Loading JSON credentials...")
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    
    print("[LOG] Authenticating with Google Service Account...")
    gc = gspread.service_account_from_dict(creds_dict)
    
    print(f"[LOG] Opening Google Sheet by URL: {PRICE_WATCH_SHEET_URL}")
    sheet = gc.open_by_url(PRICE_WATCH_SHEET_URL)
    
    print(f"[LOG] Accessing worksheet: {SHEET_TAB_NAME}")
    worksheet = sheet.worksheet(SHEET_TAB_NAME)
    
    print("--- [LOG] Successfully connected to Google Sheet ---")
    return worksheet

def search_google_shopping(product_name, mpn):
    """Searches Google Shopping using a combined Product Name + "MPN" query."""
    
    search_query = f"{product_name} \"{mpn}\""
    
    print(f"--- [LOG] Inside search_google_shopping() ---")
    print(f"[LOG] Golden Query (with quotes): {search_query}")
    
    if not SERPAPI_KEY:
        print("[ERROR] SERPAPI_KEY env var not set")
        raise ValueError("SERPAPI_KEY env var not set")
        
    params = {
        "engine": "google_shopping",
        "api_key": SERPAPI_KEY,
        "q": search_query
    }
    
    print(f"[LOG] Sending request to SerpApi with query: {search_query}")
    search = GoogleSearch(params)
    results = search.get_dict()
    
    if "error" in results:
        print(f"[ERROR] SerpApi returned an error: {results['error']}")
        return []
        
    shopping_results = results.get("shopping_results", [])
    product_results = results.get("product_results", {}).get("offers", [])
    sellers_results = results.get("sellers_results", {}).get("online_sellers", [])
    
    all_offers = shopping_results + product_results + sellers_results
    
    print(f"[LOG] SerpApi returned {len(all_offers)} total offers.")
    return all_offers

def find_competitor_offer(results, competitor_name, brand):
    """
    Finds a competitor's offer by matching BOTH the source and the EXPLICIT brand.
    """
    if not competitor_name or not brand:
        print("[WARN] Skipping, missing competitor name or brand.")
        return None
        
    brand_keyword = brand.lower()
    print(f"[LOG] Matching for: store='{competitor_name}', brand='{brand_keyword}'")

    for item in results:
        source = item.get("source", "").lower()
        title_str = item.get("title", "")
        
        if competitor_name.lower() in source:
            if brand_keyword in title_str.lower():
                price_str = item.get("price", "0")
                print(f"[LOG] Found match! Title: {title_str}, Price: {price_str}")
                price_cleaned = price_str.replace('$', '').replace(',', '')
                try:
                    float(price_cleaned) 
                    return {
                        "title": title_str,
                        "price": price_cleaned
                    }
                except ValueError:
                    print(f"[WARN] Found match, but price '{price_str}' is not a valid number. Skipping.")
                    return None
            
    print(f"[LOG] No item found that matches BOTH store and brand.")
    return None # Not found

# This is the Vercel Serverless Function handler
class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        print("\n" + "="*50)
        print(f"▶ Vercel function handler started at {datetime.now()}")
        print("="*50)
        
        try:
            print("▶ [STEP 1] Connecting to Google Sheet...")
            worksheet = get_google_sheet()
            
            print("▶ [STEP 2] Fetching all records from sheet...")
            rows_to_check = worksheet.get_all_records()
            print(f"[LOG] Found {len(rows_to_check)} rows to process.")
            
            cell_updates = []
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

            for index, row in enumerate(rows_to_check):
                sheet_row_index = index + 2
                print(f"\n--- Processing Sheet Row {sheet_row_index} ---")
                
                product_name = row.get('Product_Name')
                mpn = row.get('MPN') 
                my_price = row.get('My_Price')
                brand = row.get('Brand')
                compA_name = row.get('CompetitorA_Name')
                compB_name = row.get('CompetitorB_Name')

                if not mpn or not product_name or not brand:
                    print(f"[LOG] Skipping row {sheet_row_index}: missing MPN, Product_Name, or Brand")
                    continue
                if not my_price:
                    print(f"[LOG] Skipping row {sheet_row_index}: no 'My_Price' for comparison")
                    continue

                print(f"[LOG] Row Data: Name={product_name}, MPN={mpn}, Brand={brand}")
                
                shopping_results = search_google_shopping(product_name, str(mpn))
                
                offer_A = find_competitor_offer(shopping_results, compA_name, brand)
                offer_B = find_competitor_offer(shopping_results, compB_name, brand)
                
                # --- THIS IS THE FIX ---
                # We will now update every cell, every time, to clear stale data.
                
                price_A_float = None
                price_B_float = None

                # Competitor A
                if offer_A:
                    cell_updates.append(gspread.Cell(sheet_row_index, 6, offer_A['title'])) # Col F (Title)
                    cell_updates.append(gspread.Cell(sheet_row_index, 7, offer_A['price'])) # Col G (Price)
                    try: price_A_float = float(offer_A['price'])
                    except ValueError: pass # Keep it None if price is weird
                else:
                    cell_updates.append(gspread.Cell(sheet_row_index, 6, "")) # Clear Title
                    cell_updates.append(gspread.Cell(sheet_row_index, 7, "")) # Clear Price

                # Competitor B
                if offer_B:
                    cell_updates.append(gspread.Cell(sheet_row_index, 9, offer_B['title'])) # Col I (Title)
                    cell_updates.append(gspread.Cell(sheet_row_index, 10, offer_B['price'])) # Col J (Price)
                    try: price_B_float = float(offer_B['price'])
                    except ValueError: pass # Keep it None if price is weird
                else:
                    cell_updates.append(gspread.Cell(sheet_row_index, 9, "")) # Clear Title
                    cell_updates.append(gspread.Cell(sheet_row_index, 10, "")) # Clear Price
                
                # Update status logic
                status = "Match" # Default status
                try:
                    my_price_float = float(str(my_price).replace(',', ''))
                    
                    # Check for alerts
                    a_low = price_A_float is not None and price_A_float < my_price_float
                    b_low = price_B_float is not None and price_B_float < my_price_float

                    if a_low and b_low:
                        status = "ALERT - Both Low"
                    elif a_low:
                        status = "ALERT - A Low"
                    elif b_low:
                        status = "ALERT - B Low"
                    
                    # Check if at least one was found and not low
                    elif (price_A_float is not None or price_B_float is not None):
                        status = "Match"
                    else:
                        # Neither was found
                        status = "Not Found"
                        
                except ValueError as e:
                    print(f"[WARN] Could not compare prices for row {sheet_row_index}. MyPrice '{my_price}' is not a valid number. Error: {e}")
                    status = "ERROR - Check My_Price"

                cell_updates.append(gspread.Cell(sheet_row_index, 11, status)) # Col K (Status)
                cell_updates.append(gspread.Cell(sheet_row_index, 12, now_str)) # Col L (Last_Checked)
                print(f"--- Finished Processing Row {sheet_row_index} ---")

            if cell_updates:
                print(f"\n▶ [STEP 3] Batch updating {len(cell_updates)} cells in Google Sheet...")
                worksheet.update_cells(cell_updates, value_input_option='USER_ENTERED')
                print("[LOG] Batch update complete.")
            else:
                print("\n▶ [STEP 3] No cell updates to perform.")
            
            print("✅ Price Watch script finished successfully.")
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_body = json.dumps({"status": "success", "cells_updated": len(cell_updates)})
            self.wfile.write(response_body.encode('utf-8'))

        except Exception as e:
            print("="*50)
            print(f"🔥🔥🔥 UNHANDLED EXCEPTION! SCRIPT CRASHED. 🔥🔥🔥")
            print(f"Error Type: {type(e).__name__}")
            print(f"Error Message: {e}")
            print("--- Full Traceback ---")
            traceback.print_exc()
            print("="*50) 
            
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_body = json.dumps({"status": "error", "message": str(e), "traceback": traceback.format_exc()})
            self.wfile.write(response_body.encode('utf-8'))
        
        return