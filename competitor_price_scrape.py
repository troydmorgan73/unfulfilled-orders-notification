#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Competitor price scraper (GTIN/MPN-first) using:
 • SerpApi (engine=google) to find the correct product URL per competitor via site:domain
 • requests + extruct to parse JSON-LD/Microdata/OpenGraph for price/currency
 • Shopify fallback: product .json endpoint
 • Last-ditch regex price extraction

Usage:
  python competitor_price_scrape.py [INPUT_CSV] [OUTPUT_CSV]

If INPUT_CSV is omitted, will try these in order:
  Price-Watch.csv, Price_Watch.csv, price_watch.csv

Output defaults to: price_watch_enriched.csv
Requires env var: SERPAPI_API_KEY
"""

import csv
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup
import extruct
from w3lib.html import get_base_url

# ──────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────

SERPAPI_KEY = "1e73ea3571ab5f756bda09aebf01f1737184a3fa147b0eb121945a33751ffcb5"

# Competitor domains you care about (edit to taste)
COMPETITOR_DOMAINS = [
    "www.backcountry.com",
    "www.excelsports.com",
    "mikesbikes.com",
    "www.performancebike.com",
]

# How many Google results to consider per query (we take the first matching domain)
MAX_SERP_RESULTS = 10

# Simple UA and timeouts/backoff
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    )
}
REQ_TIMEOUT = 20
BACKOFF_SECONDS = 1.2

# Input discovery
CANDIDATE_INPUTS = ["Price-Watch.csv", "Price_Watch.csv", "price_watch.csv"]

OUTFILE_DEFAULT = "price_watch_enriched.csv"
DEBUG_DIR = "debug"


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def debug_save(name: str, payload) -> None:
    os.makedirs(DEBUG_DIR, exist_ok=True)
    path = os.path.join(DEBUG_DIR, name)
    try:
        with open(path, "w", encoding="utf-8") as f:
            if isinstance(payload, (dict, list)):
                json.dump(payload, f, ensure_ascii=False, indent=2)
            else:
                f.write(str(payload))
    except Exception:
        pass


def domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def money_to_float(val) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    s = re.sub(r"[^\d\.,-]", "", s)
    # normalize commas like 1,299.00
    if s.count(",") > 0 and s.count(".") == 0:
        s = s.replace(",", "")
    try:
        return float(Decimal(s))
    except (InvalidOperation, ValueError):
        return None


def find_input_path(cli_arg: Optional[str]) -> str:
    if cli_arg and os.path.exists(cli_arg):
        return cli_arg
    for cand in CANDIDATE_INPUTS:
        if os.path.exists(cand):
            return cand
    raise FileNotFoundError(
        f"❌ Input file not found. Tried: {([cli_arg] if cli_arg else []) + CANDIDATE_INPUTS}"
    )


# ──────────────────────────────────────────────────────────────
# SerpApi (engine=google) to find product URL by site:domain
# ──────────────────────────────────────────────────────────────

def serpapi_google_search(query: str) -> Dict:
    """Use SerpApi engine=google (web) to search."""
    if not SERPAPI_KEY:
        raise RuntimeError("Missing SERPAPI_API_KEY environment variable.")
    params = {
        "engine": "google",
        "q": query,
        "hl": "en",
        "gl": "us",
        "num": MAX_SERP_RESULTS,
        "api_key": SERPAPI_KEY,
    }
    url = "https://serpapi.com/search.json"
    r = requests.get(url, params=params, headers=HEADERS, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return r.json()


def pick_result_for_domain(serp_json: Dict, domain: str) -> Optional[str]:
    """Return the first result URL that belongs to the target domain."""
    # Try "organic_results"
    results = serp_json.get("organic_results") or []
    for item in results[:MAX_SERP_RESULTS]:
        link = item.get("link")
        if link and domain_of(link).endswith(domain.replace("www.", "")):
            return link
    # Also try "shopping_results" if present (some queries route there)
    shop = serp_json.get("shopping_results") or []
    for item in shop[:MAX_SERP_RESULTS]:
        link = item.get("link") or item.get("product_link")
        if link and domain_of(link).endswith(domain.replace("www.", "")):
            return link
    return None


# ──────────────────────────────────────────────────────────────
# Page Fetch + Structured Data Extraction
# ──────────────────────────────────────────────────────────────

def fetch(url: str) -> requests.Response:
    return requests.get(url, headers=HEADERS, timeout=REQ_TIMEOUT, allow_redirects=True)


def extract_structured(html: str, url: str) -> Dict:
    base = get_base_url(html, url)
    data = extruct.extract(
        html,
        base_url=base,
        syntaxes=["json-ld", "microdata", "opengraph"],
        errors="log",
    )
    return data


def is_shopify(resp: requests.Response, url: str) -> bool:
    # Heuristics: URL pattern or headers revealing Shopify
    u = url.lower()
    if "/products/" in u:
        return True
    if "x-shopify-stage" in resp.headers or "x-shopify-request-trackable" in resp.headers:
        return True
    # theme asset references
    if "Shopify.theme" in resp.text or "window.Shopify" in resp.text:
        return True
    return False


def try_shopify_json(url: str) -> Tuple[Optional[float], Optional[str]]:
    """
    Try Shopify product JSON endpoint for price.
    Returns (price, currency).
    """
    # canonical product URL → append .json
    probe = url
    if "?" in probe:
        probe = probe.split("?", 1)[0]
    if not probe.endswith(".json"):
        probe = probe.rstrip("/") + ".json"
    try:
        r = fetch(probe)
        if r.status_code >= 400:
            return None, None
        j = r.json()
        # Themes differ: could be {"product": {...}} or just {...}
        prod = j.get("product") if isinstance(j, dict) else None
        if prod is None and isinstance(j, dict) and "variants" in j:
            prod = j
        if not prod:
            return None, None
        # Pick minimum variant price (compare price or price / 100 in cents)
        prices = []
        for v in prod.get("variants", []):
            p = v.get("price")
            if p is None:
                continue
            # Could be string "699.99" or integer cents like 69999
            fv = money_to_float(p)
            if fv is None and isinstance(p, int):
                fv = float(p) / 100.0
            if fv is not None:
                prices.append(fv)
        if not prices:
            # some themes put "price" at top level
            top = prod.get("price")
            fv = money_to_float(top)
            if fv:
                prices.append(fv)
        if prices:
            return min(prices), None  # currency is often not present here
        return None, None
    except Exception:
        return None, None


def extract_price_from_structured(struct: Dict) -> Tuple[Optional[float], Optional[str]]:
    """
    Look through JSON-LD + Microdata for a Product with offers/price.
    Returns (price, currency)
    """
    # JSON-LD
    for item in struct.get("json-ld", []):
        # item can be dict or list
        blocks = item if isinstance(item, list) else [item]
        for b in blocks:
            t = b.get("@type") or b.get("@graph") or []
            if isinstance(t, list):
                # @graph array → search inside
                for g in t:
                    p, c = _price_from_ld_block(g)
                    if p is not None:
                        return p, c
            else:
                p, c = _price_from_ld_block(b)
                if p is not None:
                    return p, c
    # Microdata (itemscope items)
    for md in struct.get("microdata", []):
        if md.get("type") and any("Product" in x for x in md.get("type", [])):
            props = md.get("properties", {})
            # offers can be object or list
            offers = props.get("offers")
            price, cur = _price_from_offers(offers)
            if price is not None:
                return price, cur
            # sometimes price is direct
            price = money_to_float(props.get("price"))
            if price is not None:
                return price, props.get("priceCurrency")
    return None, None


def _price_from_ld_block(block: Dict) -> Tuple[Optional[float], Optional[str]]:
    if not isinstance(block, dict):
        return None, None
    btype = block.get("@type")
    # Accept Product or Offer directly
    if isinstance(btype, list):
        is_product = any(x.lower() == "product" for x in [str(t).lower() for t in btype])
    else:
        is_product = (str(btype).lower() == "product")

    if is_product:
        offers = block.get("offers")
        price, cur = _price_from_offers(offers)
        if price is not None:
            return price, cur
        # some sites put price at product level
        price = money_to_float(block.get("price"))
        if price is not None:
            return price, block.get("priceCurrency")
    elif str(btype).lower() == "offer":
        price = money_to_float(block.get("price"))
        if price is not None:
            return price, block.get("priceCurrency")
    return None, None


def _price_from_offers(offers) -> Tuple[Optional[float], Optional[str]]:
    # offers might be dict or list of dicts
    def read_offer(o: Dict) -> Tuple[Optional[float], Optional[str]]:
        p = money_to_float(o.get("price"))
        c = o.get("priceCurrency") or o.get("price_currency")
        return p, c

    if isinstance(offers, dict):
        return read_offer(offers)
    if isinstance(offers, list):
        best = None
        cur = None
        for o in offers:
            p, c = read_offer(o)
            if p is not None:
                if best is None or p < best:
                    best, cur = p, c
        return best, cur
    return None, None


PRICE_REGEX = re.compile(
    r'(?i)(?:price|sale)\D{0,10}\$?\s*([0-9]{1,3}(?:[,][0-9]{3})*(?:[.][0-9]{2})|[0-9]+(?:[.][0-9]{2})?)'
)

def regex_price_guess(html: str) -> Optional[float]:
    # Try to find $699.99-like strings near "price/sale"
    m = PRICE_REGEX.search(html)
    if not m:
        return None
    return money_to_float(m.group(1))


# ──────────────────────────────────────────────────────────────
# Core flow per row
# ──────────────────────────────────────────────────────────────

@dataclass
class RowIn:
    Product_Name: str
    GTIN: str
    MPN: str
    Brand: str
    My_Price: Optional[float]


def best_attempt_for_domain(
    row: RowIn,
    domain: str,
    row_index: int,
) -> Tuple[str, Optional[float], str, str]:
    """
    Try GTIN → MPN → Product_Name with site:domain to find a URL and extract price.
    Returns (domain, price, url, matched_by)
    """
    queries = []
    if row.GTIN:
        queries.append((f'"{row.GTIN}" site:{domain}', "gtin"))
    if row.MPN:
        # sometimes MPN needs brand join
        queries.append((f'"{row.MPN}" site:{domain}', "mpn"))
        if row.Brand:
            queries.append((f'"{row.Brand} {row.MPN}" site:{domain}', "brand+mpn"))
    if row.Product_Name:
        # help Google a bit with brand if known
        if row.Brand:
            queries.append((f'"{row.Brand} {row.Product_Name}" site:{domain}', "brand+name"))
        queries.append((f'"{row.Product_Name}" site:{domain}', "name"))

    for q, tag in queries:
        try:
            serp = serpapi_google_search(q)
            debug_save(f"row{row_index:03d}__{domain}__{tag}__serp.json", serp)
            url = pick_result_for_domain(serp, domain)
            if not url:
                time.sleep(BACKOFF_SECONDS)
                continue

            # Fetch product page
            resp = fetch(url)
            if resp.status_code >= 400 or not resp.text:
                time.sleep(BACKOFF_SECONDS)
                continue

            # Shopify shortcut if present
            if is_shopify(resp, url):
                price, currency = try_shopify_json(url)
                if price is not None:
                    return domain, price, url, tag

            # Structured data
            struct = extract_structured(resp.text, resp.url)
            price, currency = extract_price_from_structured(struct)
            if price is not None:
                return domain, price, url, tag

            # Fallback: regex
            price = regex_price_guess(resp.text)
            if price is not None:
                return domain, price, url, tag

            time.sleep(BACKOFF_SECONDS)
        except Exception as e:
            debug_save(f"row{row_index:03d}__{domain}__{tag}__error.txt", str(e))
            time.sleep(BACKOFF_SECONDS)

    return domain, None, "-", "NO_MATCH"


def process(input_csv: str, output_csv: str) -> None:
    rows_in: List[RowIn] = []
    with open(input_csv, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            rows_in.append(
                RowIn(
                    Product_Name=(r.get("Product_Name") or "").strip(),
                    GTIN=(r.get("GTIN") or "").strip(),
                    MPN=(r.get("MPN") or "").strip(),
                    Brand=(r.get("Brand") or "").strip(),
                    My_Price=money_to_float(r.get("My_Price")),
                )
            )

    out_fields = [
        "Product_Name",
        "GTIN",
        "MPN",
        "Brand",
        "My_Price",
        "Match_Source",
        "Match_URL",
        "Match_Price",
        "Match_By",
        "Price_Diff",
    ]

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=out_fields)
        w.writeheader()

        for idx, row in enumerate(rows_in, start=1):
            print(f'🔎 {row.Product_Name or row.GTIN or row.MPN} — starting')
            best_domain = None
            best_price = None
            best_url = "-"
            best_tag = "NO_MATCH"

            for dom in COMPETITOR_DOMAINS:
                print(f'   • searching on: {dom}')
                dom, price, url, tag = best_attempt_for_domain(row, dom, idx)
                if price is not None:
                    best_domain, best_price, best_url, best_tag = dom, price, url, tag
                    break  # first match wins
                # keep looking on next domain

            price_diff = None
            if row.My_Price is not None and best_price is not None:
                price_diff = round(best_price - row.My_Price, 2)

            w.writerow({
                "Product_Name": row.Product_Name,
                "GTIN": row.GTIN,
                "MPN": row.MPN,
                "Brand": row.Brand,
                "My_Price": row.My_Price if row.My_Price is not None else "",
                "Match_Source": best_domain if best_price is not None else "NO_MATCH",
                "Match_URL": best_url,
                "Match_Price": f"{best_price:.2f}" if best_price is not None else "",
                "Match_By": best_tag,
                "Price_Diff": price_diff if price_diff is not None else "",
            })

    print(f"\n✅ Wrote → {output_csv}")
    print(f"   Debug artifacts in ./{DEBUG_DIR}/")


# ──────────────────────────────────────────────────────────────
# main
# ──────────────────────────────────────────────────────────────

def main():
    try:
        in_arg = sys.argv[1] if len(sys.argv) > 1 else None
        out_arg = sys.argv[2] if len(sys.argv) > 2 else OUTFILE_DEFAULT
        input_csv = find_input_path(in_arg)
        process(input_csv, out_arg)
    except Exception as e:
        print(str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()