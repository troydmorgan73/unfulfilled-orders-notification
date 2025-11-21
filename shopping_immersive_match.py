
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
shopping_immersive_match.py
---------------------------------
Enrich a CSV/TSV of products with competitor pricing using SerpApi:
1) Google Shopping search (by GTIN, by Brand+MPN, by Product Name)
2) Extract product_id + page_token from Shopping results
3) Fetch Google Immersive Product (by page_token) for richer details
4) Parse sellers/offers (robust to schema variations)
5) Fallback to Shopping results when Immersive has no offers
6) Save debug JSON and URLs for each query for troubleshooting

Environment:
    SERPAPI_KEY = your SerpApi key

Usage:
    python shopping_immersive_match.py [INPUT_CSV] [OUTPUT_CSV]

If omitted:
    INPUT  defaults to ./price_watch.csv
    OUTPUT defaults to ./price_watch_enriched.csv
"""

import os
import sys
import csv
import json
import time
import math
import traceback
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from urllib.parse import urlparse

import requests

# ──────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────
SERPAPI_KEY = "1e73ea3571ab5f756bda09aebf01f1737184a3fa147b0eb121945a33751ffcb5"
DEFAULT_INPUT  = "Price_Watch.csv"
DEFAULT_OUTPUT = "price_watch_enriched.csv"
DEBUG_DIR      = "debug"
TIMEOUT        = 30
RETRIES        = 2
SLEEP_BETWEEN  = 0.6  # polite pacing

SERPAPI_BASE   = "https://serpapi.com/search.json"

# Domains we don't want to use as price benchmarks (optional; can expand)
DENY_DOMAINS = {
    "ebay.com", "ebay.ca", "ebay.co.uk", "aliexpress.com", "amazon.com",
    "walmart.com", "target.com", "facebook.com", "google.com",
}

# ──────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────

def _domain(url: str) -> str:
    try:
        netloc = urlparse(url).netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc
    except Exception:
        return ""

def _mkdir(path: str) -> None:
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)

def _write_json(path: str, data: Any) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"    ⚠ failed to write debug json: {e}")

def _get(params: Dict[str, Any]) -> Dict[str, Any]:
    api_key = os.environ.get("SERPAPI_KEY")
    if not api_key:
        raise RuntimeError("SERPAPI_KEY env var is required")
    params = dict(params)
    params["api_key"] = api_key

    last_err = None
    for attempt in range(1, RETRIES + 2):
        try:
            r = requests.get(SERPAPI_BASE, params=params, timeout=TIMEOUT)
            if r.status_code == 429:
                # back-off
                time.sleep(1.0 + attempt * 0.5)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            if attempt <= RETRIES:
                time.sleep(0.8 * attempt)
                continue
            raise last_err

def price_to_float(p: Union[str, float, int, None]) -> Optional[float]:
    if p is None or p == "":
        return None
    if isinstance(p, (float, int)):
        return float(p)
    s = str(p)
    # Remove currency symbols and commas
    s = s.replace("$", "").replace(",", "").strip()
    # Extract leading float
    num = ""
    dot_seen = False
    for ch in s:
        if ch.isdigit():
            num += ch
        elif ch == "." and not dot_seen:
            dot_seen = True
            num += "."
        else:
            break
    try:
        return float(num) if num else None
    except:
        return None

def csv_sniff_delimiter(path: str) -> str:
    with open(path, "r", encoding="utf-8", newline="") as f:
        sample = f.read(4096)
    if "\t" in sample and sample.count("\t") >= sample.count(","):
        return "\t"
    return ","

def safe_get(d: Dict[str, Any], *keys) -> Any:
    cur = d
    for k in keys:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return None
    return cur

# ──────────────────────────────────────────────────────────────
# SerpApi calls
# ──────────────────────────────────────────────────────────────

def serpapi_shopping_search(query: str) -> Dict[str, Any]:
    return _get({
        "engine": "google_shopping",
        "q": query,
        "gl": "us",
        "hl": "en",
        "google_domain": "google.com",
        "num": 40,
    })

def serpapi_immersive_product(page_token: str) -> Dict[str, Any]:
    return _get({
        "engine": "google_immersive_product",
        "page_token": page_token,
    })

def serpapi_google_product(product_id: str) -> Dict[str, Any]:
    # This endpoint is often deprecated/broken but we keep as an optional fallback attempt.
    return _get({
        "engine": "google_product",
        "product_id": product_id,
        "gl": "us",
        "hl": "en",
        "google_domain": "google.com",
    })

# ──────────────────────────────────────────────────────────────
# Extractors
# ──────────────────────────────────────────────────────────────

def extract_handles_from_shopping(payload: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (product_id, immersive_page_token) from a Shopping results payload.
    """
    items = payload.get("shopping_results") or []
    if not isinstance(items, list) or not items:
        return None, None
    # Prefer the first item
    top = items[0]
    product_id = top.get("product_id")
    token = top.get("immersive_product_page_token") or top.get("page_token")
    return product_id, token

def collect_offers_from_obj(obj: Any, offers: List[Dict[str, Any]]) -> None:
    """
    Recursively collect plausible offers from any dict/list structure.
    An "offer" is any object that appears to carry price + link + seller-ish info.
    """
    if isinstance(obj, dict):
        # Several possible shapes:
        price = obj.get("extracted_price") or obj.get("price")
        link  = obj.get("link") or obj.get("product_link") or obj.get("canonical_link") or obj.get("shopping_link")
        seller = obj.get("seller") or obj.get("source") or obj.get("merchant") or obj.get("store") or obj.get("site")
        title  = obj.get("title") or obj.get("name") or obj.get("product_title")

        # Normalize nested 'price' if it's a dict like {"extracted_price": 123}
        if isinstance(price, dict):
            price = price.get("extracted_price") or price.get("value")

        if link and (price is not None or isinstance(price, (int, float, str))):
            offers.append({
                "title": title,
                "seller": seller,
                "link": link,
                "extracted_price": price_to_float(price),
            })

        # Recurse into dictionaries and lists
        for v in obj.values():
            collect_offers_from_obj(v, offers)

    elif isinstance(obj, list):
        for it in obj:
            collect_offers_from_obj(it, offers)

def offers_from_immersive(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    offers: List[Dict[str, Any]] = []
    # Most data is inside "product_results", but we'll walk entire payload.
    collect_offers_from_obj(payload, offers)

    # Filter junk/no-price & deny list
    cleaned = []
    for off in offers:
        price = off.get("extracted_price")
        link  = off.get("link") or ""
        if price is None:
            continue
        dom = _domain(link)
        if not dom or any(dom.endswith(d) for d in DENY_DOMAINS):
            continue
        cleaned.append({
            "title": off.get("title"),
            "seller": off.get("seller"),
            "link": link,
            "domain": dom,
            "price": price,
        })
    # Deduplicate by domain+price+link
    uniq = []
    seen = set()
    for o in cleaned:
        key = (o["domain"], o["price"], o["link"])
        if key in seen:
            continue
        seen.add(key)
        uniq.append(o)
    return uniq

def best_offer(offers: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not offers:
        return None
    # choose lowest price
    offers_sorted = sorted(offers, key=lambda o: (o.get("price") if o.get("price") is not None else math.inf))
    return offers_sorted[0] if offers_sorted else None

def fallback_offer_from_shopping(shopping: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    If Immersive yields no sellers, try Shopping top item.
    """
    items = shopping.get("shopping_results") or []
    if not items:
        return None
    top = items[0]
    link = top.get("product_link") or top.get("link")
    price = top.get("extracted_price") or top.get("price")
    price_f = price_to_float(price)
    if not link or price_f is None:
        return None
    dom = _domain(link)
    if not dom or any(dom.endswith(d) for d in DENY_DOMAINS):
        return None
    return {
        "title": top.get("title"),
        "seller": top.get("source") or top.get("merchant") or top.get("store"),
        "link": link,
        "domain": dom,
        "price": price_f,
        "source": "google_shopping_top",
    }

# ──────────────────────────────────────────────────────────────
# Core matching
# ──────────────────────────────────────────────────────────────

def find_best_offer_via_google(shopping_payload: Dict[str, Any], row_tag: str, dbg_prefix: str) -> Tuple[str, Optional[float], str, str]:
    """
    Given a Shopping payload, attempt to resolve to an Immersive Product and return
    (source, price, url, matched_by)
    """
    product_id, page_token = extract_handles_from_shopping(shopping_payload)
    imm_payload = None
    sellers: List[Dict[str, Any]] = []

    # Save the computed Immersive URL we intend to hit (for auditing)
    if page_token:
        imm_url = f"{SERPAPI_BASE}?engine=google_immersive_product&page_token={page_token}"
        _write_json(os.path.join(DEBUG_DIR, f"{dbg_prefix}__imm_url.json"), {"imm_url": imm_url})

        try:
            imm_payload = serpapi_immersive_product(page_token)
            _write_json(os.path.join(DEBUG_DIR, f"{dbg_prefix}__imm__imm.json"), imm_payload)
            sellers = offers_from_immersive(imm_payload)
        except Exception as e:
            # Save the exception detail
            _write_json(os.path.join(DEBUG_DIR, f"{dbg_prefix}__imm__err.json"), {"error": str(e)})

    if sellers:
        bo = best_offer(sellers)
        if bo:
            return ("immersive", bo["price"], bo["link"], "immersive_offers")

    # Fallback to Shopping top item
    fall = fallback_offer_from_shopping(shopping_payload)
    if fall:
        return (fall.get("source") or "shopping_top", fall["price"], fall["link"], "shopping_top")

    # Last-ditch: if we have a product_id, *attempt* deprecated google_product to see if SerpApi returns anything useful
    if product_id:
        try:
            gp = serpapi_google_product(product_id)
            _write_json(os.path.join(DEBUG_DIR, f"{dbg_prefix}__prod__prod.json"), gp)
            # try to harvest offers from this payload too
            gp_offers = offers_from_immersive(gp)
            if gp_offers:
                bo = best_offer(gp_offers)
                if bo:
                    return ("google_product", bo["price"], bo["link"], "google_product_offers")
        except Exception as e:
            _write_json(os.path.join(DEBUG_DIR, f"{dbg_prefix}__prod__err.json"), {"error": str(e)})

    return ("NO_MATCH", None, "-", "NO_MATCH")

# ──────────────────────────────────────────────────────────────
# Runner
# ──────────────────────────────────────────────────────────────

def main():
    _mkdir(DEBUG_DIR)

    in_path  = sys.argv[1] if len(sys.argv) >= 2 else DEFAULT_INPUT
    out_path = sys.argv[2] if len(sys.argv) >= 3 else DEFAULT_OUTPUT

    if not os.path.isfile(in_path):
        print(f"❌ Input file not found: {in_path}")
        sys.exit(2)

    delim = csv_sniff_delimiter(in_path)
    with open(in_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=delim)
        fieldnames = list(reader.fieldnames) if reader.fieldnames else []

        # Ensure output columns exist
        extra_cols = ["Match_Source", "Match_URL", "Match_Price", "Match_By", "Price_Diff"]
        for c in extra_cols:
            if c not in fieldnames:
                fieldnames.append(c)

        rows = list(reader)

    enriched_rows: List[Dict[str, Any]] = []

    for idx, row in enumerate(rows, start=1):
        name   = (row.get("Product_Name") or row.get("Name") or "").strip()
        gtin   = (row.get("GTIN") or "").strip()
        mpn    = (row.get("MPN") or "").strip()
        brand  = (row.get("Brand") or "").strip()
        myp    = price_to_float(row.get("My_Price"))
        tag    = f"row{idx:03d}"
        print(f"🔎 {name or '(no name)'} — starting")

        # Build queries: GTIN (quoted), Brand + MPN, Name (if present)
        queries: List[Tuple[str, str]] = []  # (query, tag-suffix)
        if gtin:
            queries.append((f"\"{gtin}\"", "q1"))
        if brand and mpn:
            queries.append((f"{brand} {mpn}", "q2"))
        if name:
            queries.append((name, "q3"))
        if not queries and mpn:
            queries.append((mpn, "q4"))

        match_source, match_url, match_price, match_by = ("NO_MATCH", "-", None, "NO_MATCH")

        for q, qtag in queries:
            # Shopping search
            try:
                print(f"   • Shopping for: {q}")
                sh = serpapi_shopping_search(q)
                dbg_prefix = f"{tag}_{qtag}"
                _write_json(os.path.join(DEBUG_DIR, f"{dbg_prefix}__shopping.json"), sh)

                items = sh.get("shopping_results") or []
                print(f"     shopping items: {len(items)}")

                if not items:
                    time.sleep(SLEEP_BETWEEN)
                    continue

                # Hand-off to best-offer discovery
                src, price, url, by = find_best_offer_via_google(sh, tag, dbg_prefix)

                # If we got anything but NO_MATCH, accept and stop trying more queries for this row
                if src != "NO_MATCH":
                    match_source, match_url, match_price, match_by = src, url, price, by
                    break

            except Exception as e:
                # Dump the error and continue to next query
                _write_json(os.path.join(DEBUG_DIR, f"{tag}_{qtag}__shopping_err.json"),
                            {"error": str(e), "trace": traceback.format_exc()})
            finally:
                time.sleep(SLEEP_BETWEEN)

        # Compute Price_Diff if we have both prices
        price_diff = None
        if match_price is not None and myp is not None:
            try:
                price_diff = round(float(match_price) - float(myp), 2)
            except Exception:
                price_diff = None

        # Write enriched fields
        row["Match_Source"] = match_source
        row["Match_URL"]    = match_url
        row["Match_Price"]  = f"{match_price:.2f}" if isinstance(match_price, (int, float)) else ""
        row["Match_By"]     = match_by
        row["Price_Diff"]   = "" if price_diff is None else f"{price_diff:.2f}"

        enriched_rows.append(row)

        # Console summary
        if match_source == "NO_MATCH":
            print("   → NO_MATCH @ - (NO_MATCH)")
        else:
            print(f"   → {match_source} @ {match_url} (${row['Match_Price']}) [{match_by}]")

    # Write output
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=enriched_rows[0].keys() if enriched_rows else fieldnames, delimiter=delim)
        writer.writeheader()
        for r in enriched_rows:
            writer.writerow(r)

    print(f"\n✅ Wrote {len(enriched_rows)} rows → {out_path}")
    print("   Debug artifacts in ./debug/")

if __name__ == "__main__":
    main()
