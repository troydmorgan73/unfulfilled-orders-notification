#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
competitor_price_via_serpapi.py
--------------------------------
Exact-match price scraping via SerpAPI Google Shopping, constrained to
competitivecyclist.com and jensonusa.com.

Env:
  SERPAPI_KEY     = your SerpAPI key

Input CSV columns (case-insensitive accepted):
  GTIN | Barcode
  MPN  | PartNumber
  Brand
  My_Price | Price   (optional; used to compute Price_Diff)

Output columns added:
  Match_Source, Match_URL, Match_Price, Match_By, Price_Diff, Status, Last_Checked
"""

import os, csv, time, json, sys
from datetime import datetime
from urllib.parse import quote_plus
import requests
import re

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
SERPAPI_KEY = "1e73ea3571ab5f756bda09aebf01f1737184a3fa147b0eb121945a33751ffcb5"
INPUT_CSV   = "Price-Watch.csv"
OUTPUT_CSV  = "Price_Watch_compared.csv"

# Only these two competitors (domain + seller)
ALLOWED_DOMAINS = {
    "backcountry.com", "www.backcountry.com",
    "sportsbasement.com", "www.sportsbasement.com", "shop.sportsbasement.com",
}
ALLOWED_SELLERS = {
    "backcountry", "backcountry.com",
    "sports basement", "sportsbasement.com", "shop.sportsbasement.com",
}

REQUESTS_PER_MINUTE = 60
SLEEP_BETWEEN_CALLS = max(60.0 / max(1, REQUESTS_PER_MINUTE), 0.5)

BIG_TICKET_THRESHOLD     = 300.0
MIN_FRACTION_OF_MY_PRICE = 0.50
DEBUG_TOP_HITS = False

SERPAPI_ENDPOINT = "https://serpapi.com/search.json"

# ── HELPERS ────────────────────────────────────────────────────────────────────
def log(msg): print(msg, flush=True)

def norm_price(p):
    try: return float(str(p).replace("$","").replace(",","").strip())
    except Exception: return None

def read_rows(path):
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        return list(r), (r.fieldnames or [])

def domain_of(url):
    try: return urlparse(url).netloc.lower()
    except Exception: return ""

def normalize_mpn(m):
    if not m: return ""
    return re.sub(r"[\s\.\-_/]", "", m).upper()

def serpapi(q, gl="us", hl="en"):
    if not SERPAPI_KEY:
        raise RuntimeError("Missing SERPAPI_KEY")
    resp = requests.get(
        SERPAPI_ENDPOINT,
        params={"engine":"google_shopping","q":q,"gl":gl,"hl":hl,"num":100,"api_key":SERPAPI_KEY},
        timeout=30
    )
    if resp.status_code != 200:
        raise RuntimeError(f"SerpAPI error {resp.status_code}: {resp.text[:200]}")
    return resp.json()

def extract_hits(data):
    items = data.get("shopping_results") or data.get("organic_results") or []
    hits = []
    for it in items:
        link   = it.get("link") or it.get("product_link") or ""
        title  = it.get("title") or ""
        seller = (it.get("source") or it.get("seller") or "").strip()
        price  = it.get("extracted_price")
        if price is None and isinstance(it.get("price"), str):
            price = norm_price(it["price"])
        hits.append({"link": link, "title": title, "seller": seller, "price": price})
    return hits

def site_filter_clause():
    return " OR ".join([
        "site:backcountry.com",
        "site:sportsbasement.com",
        "site:shop.sportsbasement.com",
    ])

def reasonable_price(row, price):
    myp = norm_price(row.get("My_Price") or row.get("Price") or "")
    if price is None: return False
    if myp is None or myp < BIG_TICKET_THRESHOLD: return True
    return price >= (myp * MIN_FRACTION_OF_MY_PRICE)

def title_matches(row, title):
    brand = (row.get("Brand") or "").strip().lower()
    mpn   = (row.get("MPN") or row.get("PartNumber") or "").strip()
    pname = (row.get("Product_Name") or "").strip().lower()
    t     = (title or "").lower()

    has_brand = bool(brand) and brand in t

    has_mpn = False
    if mpn:
        tok = normalize_mpn(mpn).lower()
        variants = {mpn.lower(), mpn.replace(" ","").lower(), mpn.replace("-","").lower(), tok}
        has_mpn = any(v and v in t for v in variants)

    # fallback keyword (from product name) if no exact MPN in title
    has_kw = False
    if pname:
        tokens = re.findall(r"[a-z0-9]+", pname)
        tokens = [x for x in tokens if x not in {
            "rear","front","tubeless","disc","bike","bikes","wheel","wheels","tire","tires",
            "black","white","mens","women","womens","the","and","with"
        }]
        has_kw = any(x in t for x in tokens[:3])

    # Require brand + (mpn OR strong keyword), or mpn alone if no brand
    return (has_brand and (has_mpn or has_kw)) or (has_mpn and not brand)

def host_allowed(link):
    host = domain_of(link)
    return host in ALLOWED_DOMAINS  # strictly merchant domain only

def best_valid_hit(row, hits):
    # strict: merchant HOST must match; seller name optional
    candidates = []
    for h in hits:
        if not host_allowed(h["link"]): 
            continue
        if not title_matches(row, h["title"]):
            continue
        if not reasonable_price(row, h["price"]):
            continue
        candidates.append(h)

    if DEBUG_TOP_HITS and not candidates:
        log("  • No valid candidates. Top 5 raw hits:")
        for h in hits[:5]:
            log(f"    - host={domain_of(h['link'])} seller={h['seller'] or '?'} "
                f"price={h['price']} title={h['title'][:80]}")

    best, best_price = None, float("inf")
    for h in candidates:
        p = h.get("price")
        if p is None: continue
        if p < best_price:
            best, best_price = h, p
    return best

def try_queries(row):
    gtin = (row.get("GTIN") or row.get("Barcode") or "").strip()
    mpn  = (row.get("MPN")  or row.get("PartNumber") or "").strip()
    brand= (row.get("Brand") or "").strip()
    pname= (row.get("Product_Name") or "").strip()

    # 1) GTIN + site filters (strict)
    if gtin:
        q = f"\"{gtin}\" ({site_filter_clause()})"
        hit = best_valid_hit(row, extract_hits(serpapi(q)))
        if hit: return ("GTIN", hit)

    # 2) Brand + MPN (normalized variants) + site filters
    if brand and mpn:
        mpn_norm = normalize_mpn(mpn)
        variants = [mpn, mpn.replace(" ",""), mpn.replace("-",""), mpn_norm]
        seen = set()
        for v in variants:
            v = v.strip()
            if not v or v in seen: continue
            seen.add(v)
            q = f"\"{brand} {v}\" ({site_filter_clause()})"
            hit = best_valid_hit(row, extract_hits(serpapi(q)))
            if hit: return ("Brand+MPN", hit)

    # 3) MPN only + site filters
    if mpn:
        mpn_norm = normalize_mpn(mpn)
        variants = [mpn, mpn.replace(" ",""), mpn.replace("-",""), mpn_norm]
        seen = set()
        for v in variants:
            v = v.strip()
            if not v or v in seen: continue
            seen.add(v)
            q = f"\"{v}\" ({site_filter_clause()})"
            hit = best_valid_hit(row, extract_hits(serpapi(q)))
            if hit: return ("MPN", hit)

    # 4) Brand + cleaned Product_Name + site filters
    if brand and pname:
        kw = re.sub(r"\b(black|white|tubeless|disc|rear|front|700x\d+\w*|700x\d+|700c|men's|womens|women|mens|bike|bicycle|wheels?|tires?)\b", " ", pname, flags=re.I)
        kw = re.sub(r"\s+", " ", kw).strip()
        if kw:
            q = f"\"{brand} {kw}\" ({site_filter_clause()})"
            hit = best_valid_hit(row, extract_hits(serpapi(q)))
            if hit: return ("Brand+Name", hit)

    return ("NO_MATCH", None)

# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    rows, headers = read_rows(INPUT_CSV)
    extra = ["Match_Source","Match_URL","Match_Price","Match_By","Price_Diff","Status","Last_Checked"]
    out_headers = headers[:] + [c for c in extra if c not in headers]

    out = []
    for i, row in enumerate(rows, 1):
        ident = row.get("GTIN") or row.get("MPN") or row.get("Brand") or row.get("Product_Name") or ""
        log(f"[{i}/{len(rows)}] {ident or '(no ident)'}")

        my_price = norm_price(row.get("My_Price") or row.get("Price") or "")
        match_by, hit = "NO_MATCH", None
        status = "NO_MATCH"; ms = mu = mp = ""; price_diff = ""

        try:
            match_by, hit = try_queries(row)
            if hit:
                ms = hit.get("seller") or ""
                mu = hit.get("link") or ""
                pv = hit.get("price")
                mp = f"{pv:.2f}" if isinstance(pv, (int,float)) else ""
                status = "MATCH"
                if my_price is not None and pv is not None:
                    price_diff = f"{(my_price - pv):.2f}"
        except Exception as e:
            status = f"ERROR: {str(e)[:120]}"

        r = dict(row)
        r["Match_Source"] = ms
        r["Match_URL"]    = mu
        r["Match_Price"]  = mp
        r["Match_By"]     = match_by
        r["Price_Diff"]   = price_diff
        r["Status"]       = status
        r["Last_Checked"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        out.append(r)

        time.sleep(SLEEP_BETWEEN_CALLS)

    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=out_headers)
        w.writeheader(); w.writerows(out)

    log(f"✅ Done. Wrote {len(out)} rows to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()