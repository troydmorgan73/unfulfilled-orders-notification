import os
import json
import gspread
from http.server import BaseHTTPRequestHandler
from serpapi import GoogleSearch
from datetime import datetime

# --- Config ---
GOOGLE_SHEET_URL = os.environ.get('PRICE_WATCH_SHEET_URL')
SHEET_TAB_NAME = 'Price_Watch'
SERPAPI_KEY = os.environ.get('SERPAPI_KEY')

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

def search_google_shopping(upc):
    """Searches Google Shopping for a specific UPC and returns results."""
    print(f"Searching for UPC: {upc}")
    search = GoogleSearch({
        "engine": "google_shopping",
        "q": upc,
        "api_key": SERPAPI_KEY
    })
    results = search.get_dict()
    return results.get("shopping_results", [])

def find_competitor_price(results, competitor_name):
    """Finds a specific competitor's price from the search results."""
    if not competitor_name:
        return None # Skip if no competitor name is in the sheet

    for item in results:
        source = item.get("source", "").lower()
        # Check if competitor name is in the 'source' (e.g., "Competitive Cyclist")
        if competitor_name.lower() in source:
            return item.get("price")
    return None # Not found

# This is the Vercel Serverless Function handler
class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            print("▶ Starting Price Watch script...")
            worksheet = get_google_sheet()

            # Get all rows from sheet as a list of dictionaries
            rows_to_check = worksheet.get_all_records()

            # This list will hold all the updates we need to make
            cell_updates = []

            # Get the current time
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

            # Loop over each row from the Google Sheet
            for index, row in enumerate(rows_to_check):
                sheet_row_index = index + 2 # +1 for header, +1 for 0-index

                upc = row.get('UPC')
                my_price = row.get('My_Price')
                compA_name = row.get('CompetitorA_Name')
                compB_name = row.get('CompetitorB_Name')

                if not upc:
                    print(f"Skipping row {sheet_row_index}: no UPC")
                    continue

                # --- This is the core logic ---
                shopping_results = search_google_shopping(upc)

                price_A = find_competitor_price(shopping_results, compA_name)
                price_B = find_competitor_price(shopping_results, compB_name)

                # Prep cell updates for this row
                if price_A:
                    cell_updates.append(gspread.Cell(sheet_row_index, 5, price_A)) # Col E

                if price_B:
                    cell_updates.append(gspread.Cell(sheet_row_index, 7, price_B)) # Col G

                # Update status
                status = "Match"
                if price_A and float(price_A.replace('$', '').replace(',', '')) < float(my_price):
                    status = "ALERT - A Low"
                elif price_B and float(price_B.replace('$', '').replace(',', '')) < float(my_price):
                    status = "ALERT - B Low"

                cell_updates.append(gspread.Cell(sheet_row_index, 8, status)) # Col H
                cell_updates.append(gspread.Cell(sheet_row_index, 9, now_str)) # Col I

            # Write all updates to the Google Sheet in one batch
            if cell_updates:
                print(f"Batch updating {len(cell_updates)} cells...")
                worksheet.update_cells(cell_updates, value_input_option='USER_ENTERED')

            print("✅ Price Watch script finished.")

            # Send Vercel Response
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_body = json.dumps({"status": "success", "cells_updated": len(cell_updates)})
            self.wfile.write(response_body.encode('utf-8'))

        except Exception as e:
            print(f"Unhandled exception: {repr(e)}")
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_body = json.dumps({"status": "error", "message": str(e)})
            self.wfile.write(response_body.encode('utf-8'))

        return