#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Multi-engine competitor price scraper:
 - Candidate gen via SerpApi Google Web + Google Shopping
 - Merge candidates, validate with schema.org/Shopify JSON, compute confidence
 - Skip category/collection URLs (prevents $20 and $0 bogus prices)
 - Output price_watch_enriched.csv

Usage:
  python competitor_price_scrape_multi.py [INPUT_CSV] [OUTPUT_CSV]

If INPUT_CSV omitted, tries: Price-Watch.csv, Price_Watch.csv, price_watch.csv
"""

import csv, json, os, re, sys, time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
import extruct
from w3lib.html import get_base_url

SERPAPI_KEY = "1e73ea3571ab5f756bda09aebf01f1737184a3fa147b0eb121945a33751ffcb5"
if not SERPAPI_KEY:
    print("❌ SERPAPI_API_KEY not set")
    sys.exit(1)

# Edit this:
COMPETITOR_DOMAINS = [
    "www.backcountry.com",
    "www.excelsports.com",
    "mikesbikes.com",
    "www.performancebike.com",
    "www.competitivecyclist.com",
]

MAX_SERP_RESULTS = 10
REQ_TIMEOUT = 20
BACKOFF = 1.1
DEBUG_DIR = "debug"
CANDIDATE_INPUTS = ["Price-Watch.csv","Price_Watch.csv","price_watch.csv"]
OUTFILE_DEFAULT = "price_watch_enriched.csv"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/126.0.0.0 Safari/537.36")
HEADERS = {"User-Agent": UA}

# ---------- utils ----------

def dsave(name: str, payload):
    os.makedirs(DEBUG_DIR, exist_ok=True)
    path = os.path.join(DEBUG_DIR, name)
    try:
        with open(path,"w",encoding="utf-8") as f:
            if isinstance(payload,(dict,list)):
                json.dump(payload,f,ensure_ascii=False,indent=2)
            else:
                f.write(str(payload))
    except Exception:
        pass

def domain_of(url: str) -> str:
    try: return urlparse(url).netloc.lower()
    except: return ""

def money_to_float(val) -> Optional[float]:
    if val is None: return None
    if isinstance(val,(int,float)): return float(val)
    s = str(val).strip()
    s = re.sub(r"[^\d\.,-]","", s)
    if s.count(",")>0 and s.count(".")==0:
        s = s.replace(",","")
    try:
        return float(Decimal(s))
    except (InvalidOperation, ValueError):
        return None

def find_input_path(cli: Optional[str]) -> str:
    if cli and os.path.exists(cli): return cli
    for c in CANDIDATE_INPUTS:
        if os.path.exists(c): return c
    raise FileNotFoundError(f"❌ Input file not found. Tried: {([cli] if cli else []) + CANDIDATE_INPUTS}")

def fetch(url: str) -> requests.Response:
    return requests.get(url, headers=HEADERS, timeout=REQ_TIMEOUT, allow_redirects=True)

# ---------- SerpApi: google web + shopping ----------

def serpapi_call(params: Dict) -> Dict:
    url = "https://serpapi.com/search.json"
    p = {**params, "api_key": SERPAPI_KEY}
    r = requests.get(url, params=p, headers=HEADERS, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return r.json()

def serp_google_web(query: str) -> Dict:
    return serpapi_call({"engine":"google","q":query,"hl":"en","gl":"us","num":MAX_SERP_RESULTS})

def serp_google_shopping(query: str) -> Dict:
    return serpapi_call({"engine":"google_shopping","q":query,"hl":"en","gl":"us"})

def pick_web_for_domain(serp_json: Dict, want_domain: str) -> List[str]:
    hits = []
    for item in (serp_json.get("organic_results") or [])[:MAX_SERP_RESULTS]:
        link = item.get("link")
        if link and domain_of(link).endswith(want_domain.replace("www.","")):
            hits.append(link)
    # some queries return shopping_results within web
    for item in (serp_json.get("shopping_results") or [])[:MAX_SERP_RESULTS]:
        link = item.get("link") or item.get("product_link")
        if link and domain_of(link).endswith(want_domain.replace("www.","")):
            hits.append(link)
    # prefer PDP-ish paths over category (/b/, /brand/, /c/)
    hits.sort(key=lambda u: 0 if is_likely_pdp(u) else 1)
    return dedupe(hits)

def pick_shopping_candidates(serp_json: Dict, want_domain: Optional[str]=None) -> List[Tuple[str, Optional[float]]]:
    out = []
    for item in serp_json.get("shopping_results",[])[:25]:
        link = item.get("link") or item.get("product_link")
        price = item.get("extracted_price")
        if not link: continue
        if want_domain:
            if not domain_of(link).endswith(want_domain.replace("www.","")):
                continue
        out.append((link, money_to_float(price)))
    # prefer PDP-ish
    out.sort(key=lambda p: (0 if is_likely_pdp(p[0]) else 1))
    # de-dupe urls
    seen = set(); dedup=[]
    for url,pr in out:
        if url not in seen:
            seen.add(url); dedup.append((url,pr))
    return dedup[:10]

# ---------- page analysis ----------

def is_shopify(resp: requests.Response, url: str) -> bool:
    u = url.lower()
    if "/products/" in u: return True
    if "x-shopify-" in (";".join(k.lower() for k in resp.headers.keys())): return True
    if "window.Shopify" in resp.text: return True
    return False

def is_likely_category(url: str) -> bool:
    u = url.lower()
    bad_bits = ["/b/","/category","/categories","/collections/","/c/","/brand/","/brands/","/search"]
    return any(x in u for x in bad_bits)

def is_likely_pdp(url: str) -> bool:
    if is_likely_category(url): return False
    u = url.lower()
    good_bits = ["/product","/products/","/p/","/sku/","/item/"]
    return any(x in u for x in good_bits) or (u.count("-")>=2 and len(u.split("/")[-1])>10)

def try_shopify_json(url: str) -> Tuple[Optional[float], Optional[str]]:
    probe = url.split("?",1)[0].rstrip("/")
    if not probe.endswith(".json"): probe += ".json"
    try:
        r = fetch(probe)
        if r.status_code>=400: return None,None
        j = r.json()
        prod = j.get("product") if isinstance(j,dict) else None
        if prod is None and isinstance(j,dict) and "variants" in j: prod = j
        if not prod: return None,None
        prices=[]
        for v in prod.get("variants",[]):
            p = v.get("price")
            fv = money_to_float(p if p is not None else None)
            if fv is None and isinstance(p,int): fv = p/100.0
            if fv is not None: prices.append(fv)
        if not prices:
            fv = money_to_float(prod.get("price"))
            if fv is not None: prices=[fv]
        return (min(prices), None) if prices else (None,None)
    except Exception:
        return None,None

def extract_structured(html: str, url: str) -> Dict:
    base = get_base_url(html, url)
    return extruct.extract(html, base_url=base,
                           syntaxes=["json-ld","microdata","opengraph"],
                           errors="log")

def _price_from_offers(offers) -> Tuple[Optional[float], Optional[str]]:
    def read(o: Dict): return money_to_float(o.get("price")), (o.get("priceCurrency") or o.get("price_currency"))
    if isinstance(offers,dict): return read(offers)
    if isinstance(offers,list):
        best,cur=None,None
        for o in offers:
            p,c = read(o)
            if p is not None and (best is None or p<best):
                best,cur=p,c
        return best,cur
    return None,None

def _price_from_ld_block(b: Dict) -> Tuple[Optional[float], Optional[str]]:
    if not isinstance(b,dict): return None,None
    t = b.get("@type")
    is_product = (isinstance(t,list) and any(str(x).lower()=="product" for x in t)) or (str(t).lower()=="product")
    if is_product:
        price,cur = _price_from_offers(b.get("offers"))
        if price is not None: return price,cur
        price = money_to_float(b.get("price"))
        if price is not None: return price, b.get("priceCurrency")
    if str(t).lower()=="offer":
        price = money_to_float(b.get("price"))
        if price is not None: return price, b.get("priceCurrency")
    return None,None

def extract_price_structured(struct: Dict) -> Tuple[Optional[float], Optional[str]]:
    for item in struct.get("json-ld", []):
        blocks = item if isinstance(item,list) else [item]
        for b in blocks:
            if isinstance(b,dict) and "@graph" in b:
                for g in b["@graph"]:
                    p,c = _price_from_ld_block(g)
                    if p is not None: return p,c
            p,c = _price_from_ld_block(b)
            if p is not None: return p,c
    for md in struct.get("microdata", []):
        if any("Product" in x for x in md.get("type",[])):
            props = md.get("properties",{})
            p,c = _price_from_offers(props.get("offers"))
            if p is not None: return p,c
            p = money_to_float(props.get("price"))
            if p is not None: return p, props.get("priceCurrency")
    return None,None

PRICE_NEAR_REGEX = re.compile(
    r'(?is)(?:price|sale|our price)[^$0-9]{0,25}\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})|[0-9]+(?:\.[0-9]{2}))'
)

def regex_price(html: str) -> Optional[float]:
    m = PRICE_NEAR_REGEX.search(html)
    return money_to_float(m.group(1)) if m else None

def find_gtin_like(text: str, gtin: str) -> bool:
    gtin = (gtin or "").strip()
    if not gtin: return False
    return gtin in text

def find_mpn_like(text: str, mpn: str) -> bool:
    mpn = (mpn or "").strip()
    if not mpn: return False
    return re.search(re.escape(mpn), text, re.I) is not None

def title_similarity(a: str, b: str) -> float:
    ax = re.sub(r'[^a-z0-9]+',' ', a.lower()).strip().split()
    bx = re.sub(r'[^a-z0-9]+',' ', b.lower()).strip().split()
    if not ax or not bx: return 0.0
    inter = len(set(ax)&set(bx))
    return inter / max(len(set(ax)),1)

@dataclass
class RowIn:
    Product_Name: str
    GTIN: str
    MPN: str
    Brand: str
    My_Price: Optional[float]

@dataclass
class Candidate:
    domain: str
    url: str
    hinted_price: Optional[float]
    engine: str
    tag: str  # gtin/mpn/brand+name/name

def dedupe(urls: List[str]) -> List[str]:
    seen=set(); out=[]
    for u in urls:
        if u not in seen:
            seen.add(u); out.append(u)
    return out

def candidates_for_domain(row: RowIn, domain: str, idx: int) -> List[Candidate]:
    cands: List[Candidate] = []

    queries = []
    if row.GTIN: queries.append((f'"{row.GTIN}" site:{domain}', "gtin"))
    if row.MPN:
        queries.append((f'"{row.MPN}" site:{domain}', "mpn"))
        if row.Brand: queries.append((f'"{row.Brand} {row.MPN}" site:{domain}', "brand+mpn"))
    if row.Product_Name:
        if row.Brand: queries.append((f'"{row.Brand} {row.Product_Name}" site:{domain}', "brand+name"))
        queries.append((f'"{row.Product_Name}" site:{domain}', "name"))

    # Google Web
    for q, tag in queries:
        try:
            js = serp_google_web(q); dsave(f"row{idx:03d}__{domain}__{tag}__web.json", js)
            for url in pick_web_for_domain(js, domain):
                cands.append(Candidate(domain, url, None, "google_web", tag))
        except Exception as e:
            dsave(f"row{idx:03d}__{domain}__{tag}__web.err.txt", str(e))
        time.sleep(BACKOFF)

    # Google Shopping (domain scoped)
    for q, tag in [(row.GTIN or "", "gtin"), (row.MPN or "", "mpn"), (f"{row.Brand} {row.Product_Name}".strip(), "brand+name")]:
        if not q: continue
        try:
            js = serp_google_shopping(q); dsave(f"row{idx:03d}__{domain}__{tag}__gshop.json", js)
            for url, hinted in pick_shopping_candidates(js, want_domain=domain):
                cands.append(Candidate(domain, url, hinted, "google_shopping", tag))
        except Exception as e:
            dsave(f"row{idx:03d}__{domain}__{tag}__gshop.err.txt", str(e))
        time.sleep(BACKOFF)

    # Merge de-dupe: prefer PDP first
    cands.sort(key=lambda c: (0 if is_likely_pdp(c.url) else 1, c.engine!="google_web"))
    # Keep at most two per domain to limit fetches
    out: List[Candidate] = []
    seen=set()
    for c in cands:
        if c.url in seen: continue
        seen.add(c.url); out.append(c)
        if len(out)>=2: break
    return out

def validate_and_price(row: RowIn, cand: Candidate) -> Tuple[Optional[float], str, int]:
    """
    Return (price, matched_by, confidence)
    matched_by: schema|shopify_json|regex
    confidence: integer score
    """
    try:
        resp = fetch(cand.url)
        if resp.status_code>=400 or not resp.text:
            return None, "bad_fetch", -5

        # hard filter: category-like pages are rarely reliable
        if is_likely_category(resp.url):
            return None, "category_page", -3

        # Shopify fast path
        if is_shopify(resp, resp.url):
            p,_ = try_shopify_json(resp.url)
            conf = 2 if p is not None else 0
            # identity checks from page HTML too
            text = resp.text[:250000]  # cap
            if find_gtin_like(text, row.GTIN): conf += 2
            if find_mpn_like(text, row.MPN):  conf += 1
            ts = title_similarity(row.Product_Name, BeautifulSoup(resp.text,"html.parser").title.get_text() if resp.text else "")
            if ts>0.35: conf += 1
            # sanity: if price absurdly low (e.g., 20 for $1300 item), downrank
            if p is not None and row.My_Price and p < min(0.25*row.My_Price, 40):
                conf -= 3
            return p, "shopify_json", conf

        # Structured data
        struct = extract_structured(resp.text, resp.url)
        p, cur = extract_price_structured(struct)
        conf = 0
        text = resp.text[:250000]
        if find_gtin_like(text, row.GTIN): conf += 2
        if find_mpn_like(text, row.MPN):  conf += 1
        ts = title_similarity(row.Product_Name, BeautifulSoup(resp.text,"html.parser").title.get_text() if resp.text else "")
        if ts>0.35: conf += 1
        if p is not None: conf += 2

        # sanity guard on weird 0/20 values from grids
        if p is not None and row.My_Price and p < min(0.25*row.My_Price, 40):
            conf -= 3

        if p is not None:
            return p, "schema", conf

        # Last-ditch regex (very conservative)
        p = regex_price(resp.text)
        if p is not None:
            if row.My_Price and p < min(0.25*row.My_Price, 40):
                return None, "regex_too_low", -2
            return p, "regex", 0

        return None, "no_price", -1
    except Exception as e:
        dsave(f"validate_err__{domain_of(cand.url)}.txt", str(e))
        return None, "exception", -4

def process(input_csv: str, output_csv: str) -> None:
    rows: List[RowIn] = []
    with open(input_csv, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            rows.append(RowIn(
                Product_Name=(r.get("Product_Name") or "").strip(),
                GTIN=(r.get("GTIN") or "").strip(),
                MPN=(r.get("MPN") or "").strip(),
                Brand=(r.get("Brand") or "").strip(),
                My_Price=money_to_float(r.get("My_Price"))
            ))

    out_fields = ["Product_Name","GTIN","MPN","Brand","My_Price",
                  "Match_Source","Match_URL","Match_Price","Match_By","Confidence","Price_Diff"]
    with open(output_csv,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=out_fields)
        w.writeheader()

        for i,row in enumerate(rows, start=1):
            label = row.Product_Name or row.GTIN or row.MPN or f"row{i}"
            print(f"🔎 {label} — starting")

            overall_best = (None, None, None, None, -9999)  # domain, url, price, by, conf

            for dom in COMPETITOR_DOMAINS:
                cands = candidates_for_domain(row, dom, i)
                if not cands: continue
                best_for_dom = (None, None, None, None, -9999)
                for c in cands:
                    price, by, conf = validate_and_price(row, c)
                    if price is not None:
                        # prefer higher confidence; tie-break by lower price
                        if conf > best_for_dom[4] or (conf==best_for_dom[4] and (best_for_dom[2] is None or price < best_for_dom[2])):
                            best_for_dom = (c.domain, c.url, price, by, conf)

                # fold domain winner into overall
                if best_for_dom[2] is not None:
                    if best_for_dom[4] > overall_best[4] or (
                        best_for_dom[4]==overall_best[4] and (overall_best[2] is None or best_for_dom[2] < overall_best[2])
                    ):
                        overall_best = best_for_dom

                time.sleep(BACKOFF)

            dom,url,price,by,conf = overall_best
            price_diff = ""
            if row.My_Price is not None and price is not None:
                price_diff = round(price - row.My_Price, 2)

            w.writerow({
                "Product_Name": row.Product_Name,
                "GTIN": row.GTIN,
                "MPN": row.MPN,
                "Brand": row.Brand,
                "My_Price": f"{row.My_Price:.2f}" if row.My_Price is not None else "",
                "Match_Source": dom if price is not None else "NO_MATCH",
                "Match_URL": url or "-",
                "Match_Price": f"{price:.2f}" if price is not None else "",
                "Match_By": by or "NO_MATCH",
                "Confidence": conf if price is not None else "",
                "Price_Diff": price_diff
            })

    print(f"\n✅ Wrote → {output_csv}")
    print(f"   Debug artifacts in ./{DEBUG_DIR}/")

def main():
    in_arg  = sys.argv[1] if len(sys.argv)>1 else None
    out_arg = sys.argv[2] if len(sys.argv)>2 else OUTFILE_DEFAULT
    input_csv = find_input_path(in_arg)
    process(input_csv, out_arg)

if __name__ == "__main__":
    main()