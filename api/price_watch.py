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

def search_google_shopping(mpn):
    """Searches Google Shopping for a specific MPN."""
    print(f"--- [LOG] Inside search_google_shopping() for MPN: {mpn} ---")
    if not SERPAPI_KEY:
        print("[ERROR] SERPAPI_KEY env var not set")
        raise ValueError("SERPAPI_KEY env var not set")
        
    # --- THIS IS THE FIX ---
    # We are now using 'q' (query) with the MPN.
    # This is a strong search signal and more reliable than a GTIN filter
    # if the number isn't a valid GTIN.
    params = {
        "engine": "google_shopping",
        "api_key": SERPAPI_KEY,
        "q": mpn
    }
    
    print(f"[LOG] Sending request to SerpApi with query: {mpn}")
    search = GoogleSearch(params)
    results = search.get_dict()
    
    if "error" in results:
        print(f"[ERROR] SerpApi returned an error: {results['error']}")
        return []
        
    # Combine all possible result types to find offers
    shopping_results = results.get("shopping_results", [])
    product_results = results.get("product_results", {}).get("offers", [])
    sellers_results = results.get("sellers_results", {}).get("online_sellers", [])
    
    all_offers = shopping_results + product_results + sellers_results
    
    print(f"[LOG] SerpApi returned {len(all_offers)} total offers.")
    return all_offers

def find_competitor_price(results, competitor_name):
    """Finds a specific competitor's price from the search results."""
    if not competitor_name:
        return None # Skip if no competitor name is in the sheet
        
    for item in results:
        source = item.get("source", "").lower()
        if competitor_name.lower() in source:
            price_str = item.get("price", "0") # Get price as string
            print(f"[LOG] Found match for '{competitor_name}'. Price: {price_str}")
            
            # Clean the price string (remove $, commas)
            price_cleaned = price_str.replace('$', '').replace(',', '')
            
            try:
                # Convert to float to ensure it's a valid number
                float_price = float(price_cleaned)
                return price_cleaned # Return the cleaned string for the sheet
            except ValueError:
                print(f"[WARN] Could not convert price '{price_str}' to a number. Skipping.")
                return None
                
    print(f"[LOG] No match found for '{competitor_name}' in results.")
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

            # Loop over each row from the Google Sheet
            for index, row in enumerate(rows_to_check):
                sheet_row_index = index + 2 # +1 for header, +1 for 0-index
                print(f"\n--- Processing Sheet Row {sheet_row_index} ---")
                
                # --- THIS IS THE CHANGE ---
                # Look for 'MPN' column instead of 'UPC'
                mpn = row.get('MPN') 
                my_price = row.get('My_Price')
                compA_name = row.get('CompetitorA_Name')
                compB_name = row.get('CompetitorB_Name')

                if not mpn:
                    print(f"[LOG] Skipping row {sheet_row_index}: no MPN")
                    continue
                
                if not my_price:
                    print(f"[LOG] Skipping row {sheet_row_index}: no 'My_Price' for comparison")
                    continue

                print(f"[LOG] Row Data: MPN={mpn}, MyPrice={my_price}, CompA={compA_name}, CompB={compB_name}")
                
                # --- This is the core logic ---
                shopping_results = search_google_shopping(str(mpn))
                
                price_A_str = find_competitor_price(shopping_results, compA_name)
                price_B_str = find_competitor_price(shopping_results, compB_name)
                
                # Prep cell updates for this row
                if price_A_str:
                    cell_updates.append(gspread.Cell(sheet_row_index, 5, price_A_str)) # Col E
                
                if price_B_str:
                    cell_updates.append(gspread.Cell(sheet_row_index, 7, price_B_str)) # Col G
                
                # Update status
                status = "Match"
                try:
                    my_price_float = float(str(my_price).replace(',', '')) # Clean My_Price
                    if price_A_str and float(price_A_str) < my_price_float:
                        status = "ALERT - A Low"
                    elif price_B_str and float(price_B_str) < my_price_float:
                        status = "ALERT - B Low"
                except ValueError as e:
                    print(f"[WARN] Could not compare prices for row {sheet_row_index}. MyPrice '{my_price}' is not a valid number. Error: {e}")
                    status = "ERROR - Check My_Price"

                cell_updates.append(gspread.Cell(sheet_row_index, 8, status)) # Col H
                cell_updates.append(gspread.Cell(sheet_row_index, 9, now_str)) # Col I
                print(f"--- Finished Processing Row {sheet_row_index} ---")

            # Write all updates to the Google Sheet in one batch
            if cell_updates:
                print(f"\n▶ [STEP 3] Batch updating {len(cell_updates)} cells in Google Sheet...")
                worksheet.update_cells(cell_updates, value_input_option='USER_ENTERED')
                print("[LOG] Batch update complete.")
            else:
                print("\n▶ [STEP 3] No cell updates to perform.")
            
            print("✅ Price Watch script finished successfully.")
            
            # Send Vercel Response
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_body = json.dumps({"status": "success", "cells_updated": len(cell_updates)})
            self.wfile.write(response_body.encode('utf-8'))

        except Exception as e:
            # THIS IS THE MOST IMPORTANT PART
            print("="*50)
            print(f"🔥🔥🔥 UNHANDLED EXCEPTION! SCRIPT CRASHED. 🔥🔥🔥")
            print(f"Error Type: {type(e).__name__}")
            print(f"Error Message: {e}")
            print("--- Full Traceback ---")
            # Print the full traceback to the Vercel log
            traceback.print_exc()
            print("="*50)
            
            # Send Vercel Response
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_body = json.dumps({"status": "error", "message": str(e), "traceback": traceback.format_exc()})
            self.wfile.write(response_body.encode('utf-8'))
        
        return