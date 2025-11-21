#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, csv, time, re, json, gzip
from datetime import datetime
from urllib.parse import urlparse, urljoin
import requests

# ── CONFIG ─────────────────────────────────────────────────────────────────────
SERPAPI_KEY = "1e73ea3571ab5f756bda09aebf01f1737184a3fa147b0eb121945a33751ffcb5"   # <-- put your key here
INPUT_CSV   = "Price-Watch.csv"
OUTPUT_CSV  = "Price_Watch_compared.csv"

ALLOWED_DOMAINS = {
    "backcountry.com", "www.backcountry.com",
    "sportsbasement.com", "www.sportsbasement.com", "shop.sportsbasement.com",
}

REQUESTS_PER_MINUTE = 40
SLEEP_BETWEEN_CALLS = max(60.0 / max(1, REQUESTS_PER_MINUTE), 0.5)

BIG_TICKET_THRESHOLD     = 300.0
MIN_FRACTION_OF_MY_PRICE = 0.50

UA = "RA-CompetitorPriceBot/1.0 (+https://racycles.com)"

SERPAPI_ENDPOINT = "https://serpapi.com/search.json"

JSONLD_RE = re.compile(rb'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', re.I|re.S)
MICRODATA_PRICE_RE = re.compile(r'itemprop=["\']price["\'][^>]*content=["\']([^"\']+)["\']', re.I)
META_PRICE_RE = re.compile(r'<meta[^>]+(?:property|name)=["\'](?:product:price:amount|og:price:amount)["\'][^>]*content=["\'](?P<price>[\d.,]+)["\']', re.I)

GTIN_KEYS = {"gtin","gtin13","gtin14","gtin12","gtin8","barcode"}
MPN_KEYS  = {"mpn","sku","model","partNumber","itemModel"}

def log(msg): print(msg, flush=True)

def norm_price(p):
    try: return float(str(p).replace("$","").replace(",","").strip())
    except Exception: return None

def normalize_mpn(m):
    if not m: return ""
    return re.sub(r"[\s\.\-_/]", "", m).upper()

def read_rows(path):
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        return list(r), (r.fieldnames or [])

def host(url):
    try: return urlparse(url).netloc.lower()
    except Exception: return ""

def http_get(url, timeout=25):
    resp = requests.get(url, headers={"User-Agent": UA, "Accept-Encoding": "gzip"}, timeout=timeout)
    resp.raise_for_status()
    data = resp.content
    if resp.headers.get("Content-Encoding") == "gzip":
        data = gzip.decompress(data)
    return data, resp.headers.get("Content-Type",""), resp.url

def serpapi_google_web(q, gl="us", hl="en"):
    if not SERPAPI_KEY: raise RuntimeError("Missing SERPAPI_KEY")
    params = {"engine":"google", "q":q, "gl":gl, "hl":hl, "num":10, "api_key":SERPAPI_KEY}
    r = requests.get(SERPAPI_ENDPOINT, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"SerpAPI error {r.status_code}: {r.text[:200]}")
    return r.json()

def extract_jsonld(html_bytes):
    out = []
    for m in JSONLD_RE.finditer(html_bytes):
        chunk = m.group(1).strip()
        for candidate in (chunk, b"["+chunk+b"]"):
            try:
                data = json.loads(candidate)
            except Exception:
                continue
            nodes = [data] if isinstance(data, dict) else (data if isinstance(data, list) else [])
            stack = nodes[:]
            while stack:
                cur = stack.pop()
                if isinstance(cur, dict):
                    t = cur.get("@type") or cur.get("type") or ""
                    if isinstance(t, list): t = ",".join(map(str, t))
                    if "product" in str(t).lower():
                        gt = next((str(cur.get(k)).strip() for k in GTIN_KEYS if cur.get(k)), "")
                        mp = next((str(cur.get(k)).strip() for k in MPN_KEYS  if cur.get(k)), "")
                        price = None
                        offers = cur.get("offers")
                        if isinstance(offers, dict):
                            price = offers.get("price") or offers.get("lowPrice")
                        elif isinstance(offers, list) and offers:
                            price = offers[0].get("price") or offers[0].get("lowPrice")
                        out.append({"gtin": gt, "mpn": mp, "price": (str(price) if price is not None else "")})
                    for v in cur.values():
                        if isinstance(v, (dict,list)): stack.append(v)
                elif isinstance(cur, list):
                    stack.extend(cur)
    return out

def extract_micro_meta(html_text):
    price = ""
    m = MICRODATA_PRICE_RE.search(html_text)
    if m: price = m.group(1).strip()
    if not price:
        m = META_PRICE_RE.search(html_text)
        if m: price = m.group("price").strip()
    return {"price": price}

def fetch_sportsbasement_shopify_js(url):
    # If this is a Shopify PDP, we can fetch .js
    if "/products/" not in urlparse(url).path: return []
    js_url = urljoin(url, ".js")
    try:
        b, _, _ = http_get(js_url)
        data = json.loads(b.decode("utf-8", errors="ignore"))
        rows = []
        for v in data.get("variants", []):
            gt = (v.get("barcode") or "").strip()
            mp = (v.get("sku")     or "").strip()
            price = v.get("price")
            if price is not None:
                price = f"{int(price)/100:.2f}"
            rows.append({"gtin": gt, "mpn": mp, "price": price or ""})
        return rows
    except Exception:
        return []

def fetch_and_extract(url):
    """Return a list of dicts with gtin/mpn/price from a PDP page."""
    try:
        body, ctype, final_url = http_get(url)
    except Exception:
        return []

    h = host(final_url)
    out = []

    # Site-specific quick path for Sports Basement (Shopify)
    if "sportsbasement.com" in h:
        out = fetch_sportsbasement_shopify_js(final_url)
        if out: return out

    # Generic JSON-LD
    jld = extract_jsonld(body)
    if jld:
        out.extend(jld)

    # Fallback price from microdata/meta if no JSON-LD price
    if not out:
        price = extract_micro_meta(body.decode("utf-8", errors="ignore")).get("price") or ""
        if price:
            out.append({"gtin":"", "mpn":"", "price":price})
    return out

def matches_identifiers(row, found):
    """Accept if GTIN equals or MPN normalized equals; otherwise require brand+keyword title match upstream."""
    want_gtin = (row.get("GTIN") or row.get("Barcode") or "").strip()
    want_mpn  = normalize_mpn(row.get("MPN") or row.get("PartNumber") or "")
    for f in found:
        f_gtin = (f.get("gtin") or "").strip()
        f_mpn  = normalize_mpn(f.get("mpn") or "")
        if want_gtin and f_gtin and want_gtin == f_gtin:
            return True
        if want_mpn and f_mpn and want_mpn == f_mpn:
            return True
    return False

def reasonable_price(row, found_price):
    myp = norm_price(row.get("My_Price") or row.get("Price") or "")
    fp  = norm_price(found_price)
    if fp is None: return False
    if myp is None or myp < BIG_TICKET_THRESHOLD: return True
    return fp >= (myp * MIN_FRACTION_OF_MY_PRICE)

def title_ok(row, title):
    brand = (row.get("Brand") or "").strip().lower()
    mpn   = (row.get("MPN") or row.get("PartNumber") or "").strip()
    pname = (row.get("Product_Name") or "").strip().lower()
    t = (title or "").lower()

    has_brand = bool(brand) and brand in t
    has_mpn = False
    if mpn:
        tok = normalize_mpn(mpn).lower()
        variants = {mpn.lower(), mpn.replace(" ","").lower(), mpn.replace("-","").lower(), tok}
        has_mpn = any(v and v in t for v in variants)

    # allow brand+mpn or mpn-only if brand missing; otherwise require brand+keyword
    if has_brand and has_mpn: return True
    if has_mpn and not brand: return True
    if has_brand and pname:
        toks = [x for x in re.findall(r"[a-z0-9]+", pname) if x not in
                {"rear","front","tubeless","disc","bike","bikes","wheel","wheels","tire","tires","black","white","mens","women","womens","the","and","with"}]
        return any(x in t for x in toks[:3])
    return False

def search_and_validate(row):
    # Build queries in order of precision
    gtin = (row.get("GTIN") or row.get("Barcode") or "").strip()
    mpn  = (row.get("MPN")  or row.get("PartNumber") or "").strip()
    brand= (row.get("Brand") or "").strip()
    pname= (row.get("Product_Name") or "").strip()

    site_clause = " (site:backcountry.com OR site:sportsbasement.com OR site:shop.sportsbasement.com)"
    queries = []

    if gtin: queries.append(("GTIN",       f"\"{gtin}\"{site_clause}"))
    if brand and mpn:
        mpn_norm = normalize_mpn(mpn)
        for v in [mpn, mpn.replace(" ",""), mpn.replace("-",""), mpn_norm]:
            v = v.strip()
            if v: queries.append(("Brand+MPN", f"\"{brand} {v}\"{site_clause}"))
    if mpn:
        mpn_norm = normalize_mpn(mpn)
        for v in [mpn, mpn.replace(" ",""), mpn.replace("-",""), mpn_norm]:
            v = v.strip()
            if v: queries.append(("MPN", f"\"{v}\"{site_clause}"))
    if brand and pname:
        kw = re.sub(r"\b(black|white|tubeless|disc|rear|front|700x\d+\w*|700x\d+|700c|men's|womens|women|mens|bike|bicycle|wheels?|tires?)\b", " ", pname, flags=re.I)
        kw = re.sub(r"\s+", " ", kw).strip()
        if kw:
            queries.append(("Brand+Name", f"\"{brand} {kw}\"{site_clause}"))

    for qtype, q in queries:
        data = serpapi_google_web(q)
        results = data.get("organic_results", []) or data.get("shopping_results", []) or []
        for item in results:
            link = item.get("link") or item.get("product_link") or ""
            title = item.get("title") or ""
            if not link or host(link) not in ALLOWED_DOMAINS: 
                continue
            if not title_ok(row, title):
                continue
            # fetch PDP and validate identifiers
            found = fetch_and_extract(link)
            if not found:
                continue
            # pick a price from any of the records
            candidate_price = None
            for f in found:
                candidate_price = f.get("price") or candidate_price
            # identifier match gate
            if not matches_identifiers(row, found):
                continue
            # final price sanity
            if candidate_price and reasonable_price(row, candidate_price):
                return qtype, link, candidate_price
        time.sleep(SLEEP_BETWEEN_CALLS)
    return None, None, None

def main():
    rows, headers = read_rows(INPUT_CSV)
    extra = ["Match_Source","Match_URL","Match_Price","Match_By","Price_Diff","Status","Last_Checked"]
    out_headers = headers[:] + [c for c in extra if c not in headers]

    out = []
    for i, row in enumerate(rows, 1):
        ident = row.get("GTIN") or row.get("MPN") or row.get("Brand") or row.get("Product_Name") or ""
        log(f"[{i}/{len(rows)}] {ident or '(no ident)'}")

        my_price = norm_price(row.get("My_Price") or row.get("Price") or "")
        status = "NO_MATCH"; ms = mu = mp = ""; match_by = ""; price_diff = ""

        try:
            match_by, url, price = search_and_validate(row)
            if url and price:
                mu = url
                mp = f"{norm_price(price):.2f}" if norm_price(price) is not None else str(price)
                ms = host(url)
                status = "MATCH"
                if my_price is not None and norm_price(price) is not None:
                    price_diff = f"{(my_price - norm_price(price)):.2f}"
        except Exception as e:
            status = f"ERROR: {str(e)[:120]}"

        r = dict(row)
        r["Match_Source"] = ms
        r["Match_URL"]    = mu
        r["Match_Price"]  = mp
        r["Match_By"]     = match_by or ""
        r["Price_Diff"]   = price_diff
        r["Status"]       = status
        r["Last_Checked"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        out.append(r)

    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=out_headers)
        w.writeheader(); w.writerows(out)

    log(f"✅ Done. Wrote {len(out)} rows to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()