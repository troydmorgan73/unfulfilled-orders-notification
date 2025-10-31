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

# --- THIS FUNCTION IS UPDATED ---
def search_google_shopping(gtin):
    """Searches Google Shopping using a direct GTIN filter."""
    
    print(f"--- [LOG] Inside search_google_shopping() ---")
    print(f"[LOG] Searching for exact GTIN: {gtin}")
    
    if not SERPAPI_KEY:
        raise ValueError("SERPAPI_KEY env var not set")
        
    params = {
        "engine": "google_shopping",
        "api_key": SERPAPI_KEY,
        # This is the direct GTIN lookup. This is the most accurate search.
        # We are no longer using 'q='
        "tbs": f"gts:1,gtin:{gtin}"
    }
    
    print(f"[LOG] Sending request to SerpApi with GTIN filter: {gtin}")
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

# --- THIS FUNCTION IS UPDATED ---
def find_offer(results, store_name, brand, model_attrs):
    """
    Finds an offer by matching Store, Brand, AND Model Attributes.
    If store_name is None, it finds the best market offer.
    """
    if not brand or not model_attrs:
        print(f"[WARN] Skipping find, missing brand or model attributes.")
        return None
        
    brand_keyword = brand.lower()
    # We will check for the first attribute in the model list
    # e.g., "Wahoo" from "Wahoo, Smart Trainers, Bike Trainers / Rollers"
    try:
        model_keyword = model_attrs.split(',')[0].strip().lower()
    except Exception:
        model_keyword = model_attrs.lower()

    is_wildcard = store_name is None
    
    if is_wildcard:
        print(f"[LOG] Matching for BEST MARKET, brand: '{brand_keyword}', model: '{model_keyword}'")
    else:
        print(f"[LOG] Matching for SPECIFIC store: '{store_name}', brand: '{brand_keyword}', model: '{model_keyword}'")

    best_offer = None
    lowest_price = float('inf')

    for item in results:
        source_str = item.get("source", "")
        title_str = item.get("title", "")
        
        # 1. Check Store
        store_match = False
        if is_wildcard:
            # For wildcard, just make sure it's not our own store
            if MY_STORE_NAME.lower() not in source_str.lower():
                 store_match = True
        elif store_name.lower() in source_str.lower():
            store_match = True

        # 2. Check Brand and Model
        brand_match = brand_keyword in title_str.lower()
        model_match = model_keyword in title_str.lower()

        if store_match and brand_match and model_match:
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
                brand = get_row_data('Brand')
                model_attrs = get_row_data('Model') # Your compiled Column F
                my_price = get_row_data('My_Price')
                compA_name = get_row_data('CompetitorA_Name')
                
                if not (gtin and product_name and brand and my_price and model_attrs):
                    print(f"[LOG] Skipping row {sheet_row_index}: missing required data.")
                    continue

                print(f"[LOG] Row Data: GTIN={gtin}, Brand={brand}, Model={model_attrs.split(',')[0]}")
                
                # --- THIS CALL IS UPDATED ---
                shopping_results = search_google_shopping(str(gtin))
                
                offer_A = find_offer(shopping_results, compA_name, brand, model_attrs)
                
                filtered_results = [
                    item for item in shopping_results 
                    if compA_name.lower() not in item.get("source", "").lower()
                ]
                best_market_offer = find_offer(filtered_results, None, brand, model_attrs) # None = Wildcard
                
                # --- This logic clears stale data ---
                
                price_A_float = None
                price_B_float = None

                # Competitor A (Title: J, Price: K in your CSV)
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

                # Best Market Offer (Source: L, Title: M, Price: N in your CSV)
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
                
                # Update status logic (Col O)
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