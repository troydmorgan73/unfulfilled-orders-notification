import os
import json
import gspread
import re
from http.server import BaseHTTPRequestHandler
from serpapi import GoogleSearch
from datetime import datetime
import traceback # Import traceback for detailed error logging

# --- Config ---
PRICE_WATCH_SHEET_URL = os.environ.get('PRICE_WATCH_SHEET_URL')
SHEET_TAB_NAME = 'Price_Watch'
SERPAPI_KEY = os.environ.get('SERPAPI_KEY')
GOOGLE_CREDENTIALS_JSON = os.environ.get('GOOGLE_CREDENTIALS_JSON')
MY_STORE_NAME = "R&A Cycles"

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

def search_google_shopping(gtin, product_name, brand, mpn):
    """
    Tries a GTIN search first. If it fails, falls back to a query search.
    """
    print(f"--- [LOG] Inside search_google_shopping() ---")
    print(f"[LOG] Pass 1: Searching for exact GTIN: {gtin}")
    
    if not SERPAPI_KEY:
        raise ValueError("SERPAPI_KEY env var not set")
        
    params_gtin = {
        "engine": "google_shopping",
        "api_key": SERPAPI_KEY,
        "tbs": f"gts:1,gtin:{gtin}"
    }
    
    search = GoogleSearch(params_gtin)
    results_dict = search.get_dict()
    
    all_offers = results_dict.get("shopping_results", []) + \
                 results_dict.get("product_results", {}).get("offers", []) + \
                 results_dict.get("sellers_results", {}).get("online_sellers", [])
    
    # --- FALLBACK LOGIC ---
    if not all_offers:
        print(f"[LOG] GTIN search returned 0 results. Falling back to query search.")
        query = f"{product_name} {brand} \"{mpn}\""
        print(f"[LOG] Pass 2: Searching for query: {query}")
        
        params_query = {
            "engine": "google_shopping",
            "api_key": SERPAPI_KEY,
            "q": query
        }
        
        search = GoogleSearch(params_query)
        results_dict = search.get_dict()
        
        all_offers = results_dict.get("shopping_results", []) + \
                     results_dict.get("product_results", {}).get("offers", []) + \
                     results_dict.get("sellers_results", {}).get("online_sellers", [])

    if "error" in results_dict:
        print(f"[ERROR] SerpApi returned an error: {results_dict['error']}")
        return []
        
    print(f"[LOG] SerpApi returned {len(all_offers)} total offers.")
    return all_offers

# --- THIS FUNCTION IS THE FIX ---
def find_offer(results, store_name, brand, mpn):
    """
    Finds an offer by matching Store, Brand, AND MPN.
    If store_name is None, it finds the best market offer.
    """
    if not brand or not mpn:
        print(f"[WARN] Skipping find, missing brand or MPN.")
        return None
        
    brand_keyword = brand.lower()
    mpn_keyword = mpn.lower() # Get the MPN
    is_wildcard = store_name is None
    
    if is_wildcard:
        print(f"[LOG] Matching for BEST MARKET, brand: '{brand_keyword}', MPN: '{mpn_keyword}'")
    else:
        print(f"[LOG] Matching for SPECIFIC store: '{store_name}', brand: '{brand_keyword}', MPN: '{mpn_keyword}'")

    best_offer = None
    lowest_price = float('inf')

    for item in results:
        source_str = item.get("source", "")
        title_str = item.get("title", "")
        title_lower = title_str.lower()
        
        # 1. Check Store
        store_match = False
        if is_wildcard:
            if MY_STORE_NAME.lower() not in source_str.lower():
                 store_match = True
        elif store_name.lower() in source_str.lower():
            store_match = True

        # 2. Check Brand and MPN (MUST match)
        brand_match = brand_keyword in title_lower
        mpn_match = mpn_keyword in title_lower # Check if MPN is in the title

        if store_match and brand_match and mpn_match:
            price_str = item.get("price", "0")
            price_cleaned = price_str.replace('$', '').replace(',', '')
            try:
                price_float = float(price_cleaned)
                
                if is_wildcard:
                    if price_float < lowest_price:
                        lowest_price = price_float
                        best_offer = { "title": title_str, "price": price_cleaned, "source": source_str }
                else:
                    print(f"[LOG] Found specific match! Title: {title_str}, Price: {price_str}")
                    return { "title": title_str, "price": price_cleaned, "source": source_str }
            except ValueError:
                continue
    
    if is_wildcard:
        if best_offer:
            print(f"[LOG] Found best market offer! Store: {best_offer['source']}, Price: {best_offer['price']}")
        else:
            print(f"[LOG] No other market offers found.")
        return best_offer
    else:
        print(f"[LOG] No specific match found.")
        return None

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
            all_data = worksheet.get_all_values()
            if not all_data:
                print("[LOG] Sheet is empty.")
                return

            headers = all_data[0]
            rows_to_check = all_data[1:]
            print(f"[LOG] Found {len(rows_to_check)} rows to process.")
            
            header_map = {name: i for i, name in enumerate(headers)}
            
            cell_updates = []
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

            for index, row in enumerate(rows_to_check):
                sheet_row_index = index + 2
                print(f"\n--- Processing Sheet Row {sheet_row_index} ---")
                
                def get_row_data(col_name):
                    try: return row[header_map[col_name]]
                    except (KeyError, IndexError): return None
                
                # --- THIS SECTION MATCHES YOUR CSV ---
                product_name = get_row_data('Product_Name')
                gtin = get_row_data('GTIN')
                mpn = get_row_data('MPN') # We need this for the filter
                brand = get_row_data('Brand')
                my_price = get_row_data('My_Price')
                compA_name = get_row_data('CompetitorA_Name')
                
                if not (gtin and product_name and brand and my_price and mpn):
                    print(f"[LOG] Skipping row {sheet_row_index}: missing required data (GTIN, MPN, Name, Brand, or Price).")
                    continue

                print(f"[LOG] Row Data: GTIN={gtin}, Brand={brand}, MPN={mpn}")
                
                # --- THIS CALL IS UPDATED ---
                shopping_results = search_google_shopping(str(gtin), product_name, brand, str(mpn))
                
                offer_A = find_offer(shopping_results, compA_name, brand, str(mpn))
                
                filtered_results = [
                    item for item in shopping_results 
                    if compA_name.lower() not in item.get("source", "").lower()
                ]
                best_market_offer = find_offer(filtered_results, None, brand, str(mpn)) # None = Wildcard
                
                # --- This logic clears stale data ---
                
                price_A_float = None
                price_B_float = None

                # Col J, K
                col_A_Title = header_map['CompetitorA_Title'] + 1
                col_A_Price = header_map['CompetitorA_Price'] + 1
                if offer_A:
                    cell_updates.append(gspread.Cell(sheet_row_index, col_A_Title, offer_A['title']))
                    cell_updates.append(gspread.Cell(sheet_row_index, col_A_Price, offer_A['price']))
                    try: price_A_float = float(offer_A['price'])
                    except ValueError: pass
                else:
                    cell_updates.append(gspread.Cell(sheet_row_index, col_A_Title, ""))
                    cell_updates.append(gspread.Cell(sheet_row_index, col_A_Price, ""))

                # Col L, M, N
                col_B_Source = header_map['Best_Offer_Source'] + 1
                col_B_Title = header_map['Best_Offer_Title'] + 1
                col_B_Price = header_map['Best_Offer_Price'] + 1
                if best_market_offer:
                    cell_updates.append(gspread.Cell(sheet_row_index, col_B_Source, best_market_offer['source']))
                    cell_updates.append(gspread.Cell(sheet_row_index, col_B_Title, best_market_offer['title']))
                    cell_updates.append(gspread.Cell(sheet_row_index, col_B_Price, best_market_offer['price']))
                    try: price_B_float = float(best_market_offer['price'])
                    except ValueError: pass
                else:
                    cell_updates.append(gspread.Cell(sheet_row_index, col_B_Source, ""))
                    cell_updates.append(gspread.Cell(sheet_row_index, col_B_Title, ""))
                    cell_updates.append(gspread.Cell(sheet_row_index, col_B_Price, ""))
                
                # Col O, P
                status = "Match"
                col_Status = header_map['Status'] + 1
                col_Last_Check = header_map['Last_Checked'] + 1
                
                try:
                    my_price_float = float(str(my_price).replace(',', ''))
                    
                    a_low = price_A_float is not None and price_A_float < my_price_float
                    b_low = price_B_float is not None and price_B_float < my_price_float

                    if a_low and b_low: status = "ALERT - Both Low"
                    elif a_low: status = "ALERT - A Low"
                    elif b_low: status = "ALERT - Market Low"
                    elif (price_A_float is not None or price_B_float is not None): status = "Match"
                    else: status = "Not Found"
                        
                except ValueError as e:
                    print(f"[WARN] Could not compare prices... Error: {e}")
                    status = "ERROR - Check My_Price"

                cell_updates.append(gspread.Cell(sheet_row_index, col_Status, status))
                cell_updates.append(gspread.Cell(sheet_row_index, col_Last_Check, now_str))
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