#!/usr/bin/env python3
import os, sys, requests, json

SERPAPI_KEY = "1e73ea3571ab5f756bda09aebf01f1737184a3fa147b0eb121945a33751ffcb5"
GTIN = "753759336288"  # Garmin Edge 1050 UPC-A

def get(url, params):
    r = requests.get(url, params=params, timeout=40)
    try:
        obj = r.json()
    except Exception:
        obj = {"raw": r.text}
    return r.status_code, obj

def main():
    if not SERPAPI_KEY or SERPAPI_KEY.startswith("PUT_"):
        print("❌ Set SERPAPI_KEY (env or in file)."); sys.exit(1)

    print("→ shopping search…")
    code, resp = get("https://serpapi.com/search.json", {
        "engine":"google_shopping","q":f"\"{GTIN}\"","tbm":"shop","gl":"us","hl":"en",
        "api_key":SERPAPI_KEY
    })
    print("status:", code)
    if "error" in resp:
        print("ERROR:", resp["error"]); return
    print("top-level keys:", list(resp.keys())[:12])
    # peek 1st item if present
    items = (resp.get("product_results") or resp.get("shopping_results") or []) or []
    print("items_count:", len(items))
    if items:
        first = items[0]
        print("first item keys:", list(first.keys())[:20])
        print("serpapi_product_api:", first.get("serpapi_product_api"))
        print("product_id:", first.get("product_id"))
        print("page_token fields:",
              first.get("immersive_product_page_token") or first.get("product_page_token") or first.get("page_token"))

if __name__ == "__main__":
    main()