#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import math
import os
import re
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
SERPAPI_KEY = "1e73ea3571ab5f756bda09aebf01f1737184a3fa147b0eb121945a33751ffcb5"   # ← add your key
INPUT_CSV   = "Price-Watch.csv"     # your attached CSV name
OUTPUT_CSV  = "price_watch_enriched.csv"
SLEEP_SECS  = 0.8                    # polite pacing; SerpAPI can handle more, tune as you like

# Only consider offers from these domains (normalized host match).
ALLOWED_DOMAINS = {
    # backcountry group
    "backcountry.com", "www.backcountry.com",
    "competitivecyclist.com", "www.competitivecyclist.com",   # optional if you want to include CC
    # bike retailers
    "excelsports.com", "www.excelsports.com",
    "mikesbikes.com", "www.mikesbikes.com",
    "performancebike.com", "www.performancebike.com",
    "sportsbasement.com", "shop.sportsbasement.com", "www.sportsbasement.com",
    "jensonusa.com", "www.jensonusa.com",
    # add more as needed…
}

# Normalize merchant display names to domains (SerpAPI returns merchant names, sometimes URLs)
MERCHANT_TO_DOMAIN_HINTS: Dict[str, str] = {
    "Backcountry.com": "www.backcountry.com",
    "Backcountry": "www.backcountry.com",
    "Competitive Cyclist": "www.competitivecyclist.com",
    "Excel Sports": "www.excelsports.com",
    "Mike's Bikes": "mikesbikes.com",
    "Performance Bicycle": "www.performancebike.com",
    "Sports Basement": "www.sportsbasement.com",
    "Jenson USA": "www.jensonusa.com",
    # extend as needed
}

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "price-watch/1.0 (R&A Cycles)"})

def norm_price(x) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(str(x).replace("$", "").replace(",", "").strip())
    except Exception:
        return None

def host_of(url: str) -> str:
    try:
        h = urlparse(url).netloc.lower()
        return h
    except Exception:
        return ""

def merchant_to_domain(merchant_name: Optional[str], offer_url: Optional[str]) -> str:
    """
    Convert a SerpAPI merchant name and/or offer URL to a normalized domain string.
    """
    # Prefer explicit mapping
    if merchant_name and merchant_name in MERCHANT_TO_DOMAIN_HINTS:
        return MERCHANT_TO_DOMAIN_HINTS[merchant_name].lower()

    # Try URL host
    if offer_url:
        h = host_of(offer_url)
        if h:
            return h

    # Fallback: simple normalization
    if merchant_name:
        key = merchant_name.lower().replace(" ", "").replace("'", "")
        # heuristic
        for d in ALLOWED_DOMAINS:
            if key in d.replace(".", ""):
                return d
    return ""

def allowed_domain(domain: str) -> bool:
    d = domain.lower()
    return d in ALLOWED_DOMAINS

def price_diff(my_price: Optional[float], their_price: Optional[float]) -> Optional[float]:
    if my_price is None or their_price is None:
        return None
    return round(my_price - their_price, 2)

# ─────────────────────────────────────────────────────────────
# SerpAPI: Google Shopping
# Docs: https://serpapi.com/search-api
# We’ll do two phases:
#   1) Find product_id from query (GTIN preferred; else Brand + MPN)
#   2) Fetch product offers by product_id
# ─────────────────────────────────────────────────────────────
BASE = "https://serpapi.com/search.json"
PRODUCT_BASE = "https://serpapi.com/search.json"  # same endpoint, different engine params

def serpapi_query(params: Dict) -> Dict:
    params = dict(params)
    params["api_key"] = SERPAPI_KEY
    # default engine
    params.setdefault("engine", "google_shopping")
    # shopping location/currency bias (US)
    params.setdefault("gl", "us")
    params.setdefault("hl", "en")
    resp = SESSION.get(BASE, params=params, timeout=40)
    resp.raise_for_status()
    return resp.json()

def find_product_id_by_gtin(gtin: str) -> Optional[str]:
    if not gtin:
        return None
    q = f'"{gtin}"'  # exact match
    data = serpapi_query({
        "q": q,
        "engine": "google_shopping",
        "google_domain": "google.com",
        "tbm": "shop"
    })
    # Prefer a “product” entity with product_id; else top result that has product_id
    for key in ("product_results", "shopping_results"):
        for item in data.get(key, []) or []:
            pid = item.get("product_id")
            # Some results inline their product_id only in 'serpapi_product_api' URL — extract if missing
            if not pid and item.get("serpapi_product_api"):
                m = re.search(r"product_id=([^&]+)", item["serpapi_product_api"])
                if m:
                    pid = m.group(1)
            if pid:
                return pid
    return None

def find_product_id_by_brand_mpn(brand: str, mpn: str) -> Optional[str]:
    if not brand and not mpn:
        return None
    q = " ".join([s for s in [brand, mpn] if s]).strip()
    if not q:
        return None
    data = serpapi_query({
        "q": q,
        "engine": "google_shopping",
        "google_domain": "google.com",
        "tbm": "shop"
    })
    # Look for exact(ish) title match containing brand and mpn tokens
    tokens = [t.lower() for t in re.split(r"[\s\-_/]+", (brand or "") + " " + (mpn or "")) if t]
    for key in ("product_results", "shopping_results"):
        for item in data.get(key, []) or []:
            pid = item.get("product_id")
            title = (item.get("title") or "").lower()
            if tokens and all(t in title for t in tokens[:2]):  # require at least first two tokens
                if pid:
                    return pid
            if not pid and item.get("serpapi_product_api"):
                m = re.search(r"product_id=([^&]+)", item["serpapi_product_api"])
                if m:
                    # accept if title looks reasonable
                    if tokens and all(t in title for t in tokens[:2]):
                        return m.group(1)
    # Fallback: first result with product_id
    for key in ("product_results", "shopping_results"):
        for item in data.get(key, []) or []:
            pid = item.get("product_id")
            if pid:
                return pid
    return None

def get_offers_for_product_id(product_id: str) -> List[Dict]:
    data = serpapi_query({
        "engine": "google_product",
        "product_id": product_id,
        "google_domain": "google.com",
        "gl": "us",
        "hl": "en"
    })
    # Offers appear under these keys depending on result type
    offers = []
    for key in ("sellers_results", "shopping_results", "product_results"):
        val = data.get(key)
        if isinstance(val, list):
            offers.extend(val)
        elif isinstance(val, dict) and "sellers" in val:
            offers.extend(val["sellers"])
    # Normalize a common shape
    normalized = []
    for o in offers:
        merchant = o.get("seller_name") or o.get("source") or o.get("merchant")
        price = o.get("extracted_price") or o.get("price")
        link = o.get("link") or o.get("product_link") or o.get("source_link")
        normalized.append({
            "merchant": merchant,
            "price": norm_price(price),
            "link": link
        })
    return normalized

# ─────────────────────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────────────────────
def find_best_competitor_offer(gtin: str, brand: str, mpn: str) -> Tuple[str, Optional[float], str, str]:
    """
    Returns: (match_source, best_price, best_url, matched_by)
      match_source: the competitor domain we matched (or "")
      best_price:   lowest price among allowed domains (or None)
      best_url:     link to offer
      matched_by:   "GTIN" or "BRAND+MPN" or ""
    """
    product_id = None
    matched_by = ""

    # 1) Try GTIN first (strongest)
    if gtin:
        try:
            product_id = find_product_id_by_gtin(gtin)
            matched_by = "GTIN" if product_id else ""
        except Exception:
            product_id = None
        time.sleep(SLEEP_SECS)

    # 2) Try Brand+MPN
    if not product_id:
        try:
            product_id = find_product_id_by_brand_mpn(brand or "", mpn or "")
            matched_by = "BRAND+MPN" if product_id else ""
        except Exception:
            product_id = None
        time.sleep(SLEEP_SECS)

    if not product_id:
        return "", None, "", ""

    # 3) Get offers and filter to allowed domains
    try:
        offers = get_offers_for_product_id(product_id)
    except Exception:
        return "", None, "", matched_by

    best_price = None
    best_url = ""
    best_domain = ""

    for o in offers:
        dom = merchant_to_domain(o.get("merchant"), o.get("link"))
        if not dom or not allowed_domain(dom):
            continue
        p = o.get("price")
        if p is None:
            continue
        if best_price is None or p < best_price:
            best_price = p
            best_url = o.get("link") or ""
            best_domain = dom

    return best_domain, best_price, best_url, matched_by

def run():
    rows_out = []

    # Read your CSV
    with open(INPUT_CSV, "r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f, delimiter=",")
        input_rows = list(rdr)

    for row in input_rows:
        product_name = row.get("Product_Name") or row.get("Name") or ""
        gtin = (row.get("GTIN") or "").strip()
        mpn  = (row.get("MPN") or "").strip()
        brand = (row.get("Brand") or "").strip()
        my_price = norm_price(row.get("My_Price") or row.get("Price"))

        # Query SerpAPI once per row
        domain, comp_price, comp_url, matched_by = find_best_competitor_offer(gtin, brand, mpn)

        diff = price_diff(my_price, comp_price)

        # Assemble output row (preserve originals + new fields)
        new_row = dict(row)
        new_row.update({
            "Match_Source": domain or "NO_MATCH",
            "Match_URL": comp_url or "",
            "Match_Price": f"{comp_price:.2f}" if comp_price is not None else "",
            "Match_By": matched_by or "NO_MATCH",
            "Price_Diff": f"{diff:.2f}" if diff is not None else "",
        })
        rows_out.append(new_row)

        # Optional: print progress
        pn = product_name or (brand + " " + mpn).strip()
        print(f"• {pn[:60]:60s} → {domain or 'NO_MATCH'} @ {comp_price if comp_price is not None else '-'} ({matched_by or '—'})")

    # Write enriched CSV
    fieldnames = list(rows_out[0].keys()) if rows_out else []
    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows_out)

    print(f"\n✅ Wrote {len(rows_out)} rows → {OUTPUT_CSV}")

if __name__ == "__main__":
    # Simple sanity check
    if not SERPAPI_KEY or SERPAPI_KEY.startswith("PUT_"):
        raise SystemExit("❌ Please set SERPAPI_KEY at the top of the file.")
    run()