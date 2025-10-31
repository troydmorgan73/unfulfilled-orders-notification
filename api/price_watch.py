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
# --- NEW: Add your store name to exclude it from "Best Market Offer" ---
MY_STORE_NAME = "R&A Cycles" # The script will ignore any results from this source

def get_google_sheet():
    """Connects to Google Sheets using Vercel Env Vars."""
    print("--- [LOG] Inside get_google_sheet() ---")
    if not (GOOGLE_CREDENTIALS_JSON and PRICE_WATCH_SHEET_URL):
        raise ValueError("Missing Google credentials or Sheet URL env var")
        
    print("[LOG] Authenticating with Google...")
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    gc = gspread.service_account_from_dict(creds_dict)
    
    print(f"[LOG] Opening Google Sheet and '{SHEET_TAB_NAME}' tab...")
    sheet = gc.open_by_url(PRICE_WATCH_SHEET_URL)
    worksheet = sheet.worksheet(SHEET_TAB_NAME)
    
    print("--- [LOG] Successfully connected to Google Sheet ---")
    return worksheet

def search_google_shopping(product_name, brand, mpn, gtin, variant):
    """Searches Google Shopping using the ultimate hyper-specific query."""
    
    query_parts = [
        product_name,
        brand,
        (variant or ''),
        f"\"{mpn}\"",
        f"\"{gtin}\""
    ]
    search_query = " ".join(part for part in query_parts if part) 
    
    print(f"--- [LOG] Inside search_google_shopping() ---")
    print(f"[LOG] Ultimate Query: {search_query}")
    
    if not SERPAPI_KEY:
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
        
    all_offers = results.get("shopping_results", []) + \
                 results.get("product_results", {}).get("offers", []) + \
                 results.get("sellers_results", {}).get("online_sellers", [])
    
    print(f"[LOG] SerpApi returned {len(all_offers)} total offers.")
    return all_offers

def find_competitor_offer(results, competitor_name, brand):
    """Finds a specific competitor's offer."""
    if not competitor_name or not brand:
        return None
        
    brand_keyword = brand.lower()
    print(f"[LOG] Matching for SPECIFIC store: '{competitor_name}', brand: '{brand_keyword}'")

    for item in results:
        source = item.get("source", "").lower()
        title_str = item.get("title", "")
        
        if competitor_name.lower() in source and brand_keyword in title_str.lower():
            price_str = item.get("price", "0")
            print(f"[LOG] Found specific match! Title: {title_str}, Price: {price_str}")
            price_cleaned = price_str.replace('$', '').replace(',', '')
            try:
                float(price_cleaned) 
                return {"title": title_str, "price": price_cleaned}
            except ValueError:
                continue # Price was invalid
            
    print(f"[LOG] No specific match found for {competitor_name}.")
    return None

# --- NEW FUNCTION ---
def find_best_market_offer(results, brand, exclude_stores):
    """
    Finds the LOWEST price offer from ANY store that is not in the exclude_list.
    """
    if not brand:
        return None
        
    brand_keyword = brand.lower()
    # Create a list of lowercase store names to exclude
    exclude_list = [store.lower() for store in exclude_stores if store]

    print(f"[LOG] Matching for BEST MARKET offer, brand: '{brand_keyword}', excluding: {exclude_list}")
    
    best_offer = None
    lowest_price = float('inf')

    for item in results:
        source_str = item.get("source", "")
        title_str = item.get("title", "")
        
        # Check if the source is in our exclusion list
        is_excluded = False
        for ex_store in exclude_list:
            if ex_store in source_str.lower():
                is_excluded = True
                break
        
        # If it's not excluded AND the brand matches...
        if not is_excluded and brand_keyword in title_str.lower():
            price_str = item.get("price", "0")
            price_cleaned = price_str.replace('$', '').replace(',', '')
            try:
                price_float = float(price_cleaned)
                # If this is the new lowest price, save it
                if price_float < lowest_price:
                    lowest_price = price_float
                    best_offer = {
                        "title": title_str,
                        "price": price_cleaned,
                        "source": source_str # We need to save the source name!
                    }
            except ValueError:
                continue # Price was invalid

    if best_offer:
        print(f"[LOG] Found best market offer! Store: {best_offer['source']}, Price: {best_offer['price']}")
    else:
        print(f"[LOG] No other market offers found.")
        
    return best_offer

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
                
                # Read all data from the sheet
                product_name = row.get('Product_Name')
                gtin = row.get('GTIN')
                mpn = row.get('MPN')
                brand = row.get('Brand')
                variant = row.get('Variant_Options')
                my_price = row.get('My_Price')
                compA_name = row.get('CompetitorA_Name') # Specific competitor
                
                if not (gtin and mpn and product_name and brand and my_price):
                    print(f"[LOG] Skipping row {sheet_row_index}: missing required data.")
                    continue

                print(f"[LOG] Row Data: Name={product_name}, GTIN={gtin}, MPN={mpn}, Brand={brand}")
                
                # --- THIS IS THE CORE LOGIC ---
                shopping_results = search_google_shopping(product_name, brand, str(mpn), str(gtin), variant)
                
                # 1. Find the specific Competitor A
                offer_A = find_competitor_offer(shopping_results, compA_name, brand)
                
                # 2. Find the "wildcard" best market offer
                best_market_offer = find_best_market_offer(
                    shopping_results, 
                    brand, 
                    exclude_stores=[compA_name, MY_STORE_NAME] # Exclude Comp A and ourselves
                )
                
                # --- This logic clears stale data ---
                
                price_A_float = None
                price_B_float = None # This now represents the "best market" price

                # Competitor A (Title: H, Price: I)
                if offer_A:
                    cell_updates.append(gspread.Cell(sheet_row_index, 8, offer_A['title'])) # Col H
                    cell_updates.append(gspread.Cell(sheet_row_index, 9, offer_A['price'])) # Col I
                    try: price_A_float = float(offer_A['price'])
                    except ValueError: pass
                else:
                    cell_updates.append(gspread.Cell(sheet_row_index, 8, "")) # Clear Title
                    cell_updates.append(gspread.Cell(sheet_row_index, 9, "")) # Clear Price

                # Best Market Offer (Source: J, Title: K, Price: L)
                if best_market_offer:
                    cell_updates.append(gspread.Cell(sheet_row_index, 10, best_market_offer['source'])) # Col J
                    cell_updates.append(gspread.Cell(sheet_row_index, 11, best_market_offer['title']))  # Col K
                    cell_updates.append(gspread.Cell(sheet_row_index, 12, best_market_offer['price']))  # Col L
                    try: price_B_float = float(best_market_offer['price'])
                    except ValueError: pass
                else:
                    cell_updates.append(gspread.Cell(sheet_row_index, 10, "")) # Clear Source
                    cell_updates.append(gspread.Cell(sheet_row_index, 11, "")) # Clear Title
                    cell_updates.append(gspread.Cell(sheet_row_index, 12, "")) # Clear Price
                
                # Update status logic (Col M)
                status = "Match"
                try:
                    my_price_float = float(str(my_price).replace(',', ''))
                    
                    a_low = price_A_float is not None and price_A_float < my_price_float
                    b_low = price_B_float is not None and price_B_float < my_price_float

                    if a_low and b_low:
                        status = "ALERT - Both Low"
                    elif a_low:
                        status = "ALERT - A Low"
                    elif b_low:
                        status = "ALERT - Market Low" # Renamed for clarity
                    elif (price_A_float is not None or price_B_float is not None):
                        status = "Match"
                    else:
                        status = "Not Found"
                        
                except ValueError as e:
                    print(f"[WARN] Could not compare prices... Error: {e}")
                    status = "ERROR - Check My_Price"

                cell_updates.append(gspread.Cell(sheet_row_index, 13, status)) # Col M (Status)
                cell_updates.append(gspread.Cell(sheet_row_index, 14, now_str)) # Col N (Last_Checked)
                print(f"--- Finished Processing Row {sheet_row_index} ---")

            if cell_updates:
                print(f"\n▶ [STEP 3] Batch updating {len(cell_updates)} cells...")
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