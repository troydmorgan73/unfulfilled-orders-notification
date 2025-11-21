#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Platform-aware competitor product probe (REI removed)

Supported:
- Backcountry (SFCC)
- Excel Sports (custom HTML)
- Mike’s Bikes (Shopify)
- Performance Bike (Magento/Custom)

Extraction layers:
- Platform-specific adapters
- JSON-LD (incl. @graph)
- microdata, RDFa, OpenGraph (extruct)
- window.dataLayer
- Theme-specific script/meta/data-* fallbacks
- Debug snapshots to ./probe_debug/

Install:
  python3 -m pip install -U requests urllib3 beautifulsoup4 extruct w3lib lxml
Run:
  python platform_aware_product_probe.py
"""

import csv
import json
import os
import random
import re
import subprocess
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ───────────────────────────────────────────────
# CONFIG
# ───────────────────────────────────────────────
TIMEOUT = 25
SLEEP_BETWEEN = 0.6
OUTPUT_CSV = "competitor_probe_results.csv"

TEST_URLS = [
    "https://www.backcountry.com/garmin-edge-1050-gps-bike-computer",
    "https://www.excelsports.com/garmin-edge-1050-gps-computer",
    "https://mikesbikes.com/products/garmin-edge-1050?_pos=1&_sid=74051528e&_ss=r",
    "https://www.performancebike.com/garmin-edge-1050-gps-cycling-computer-black-010-02890-00/p1565043",
]

DEBUG_DUMP = True
DEBUG_DIR = "probe_debug"

# ───────────────────────────────────────────────
# Optional structured-data libs
# ───────────────────────────────────────────────
try:
    import extruct
    from w3lib.html import get_base_url
    EXSTRUCT_AVAILABLE = True
except Exception:
    EXSTRUCT_AVAILABLE = False

# ───────────────────────────────────────────────
# HTTP
# ───────────────────────────────────────────────
def host(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""

def build_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s

SESSION = build_session()

UA_POOL = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36",
]

def make_headers() -> dict:
    return {
        "User-Agent": random.choice(UA_POOL),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.google.com/",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
    }

def dump_snapshot(domain: str, url: str, html: bytes, tag: str):
    if not DEBUG_DUMP:
        return
    os.makedirs(DEBUG_DIR, exist_ok=True)
    safe = domain.replace(":", "_").replace("/", "_")
    path = os.path.join(DEBUG_DIR, f"{safe}__{tag}.html")
    with open(path, "wb") as f:
        f.write(b"<!-- URL: " + url.encode("utf-8") + b" -->\n" + html)

def http_get(url: str) -> Tuple[bytes, str, str]:
    REQ_TIMEOUT = max(35, TIMEOUT)
    CURL_TIMEOUT = max(45, TIMEOUT + 10)

    time.sleep(0.2 + random.random() * 0.4)
    headers = make_headers()

    try:
        resp = SESSION.get(url, headers=headers, timeout=REQ_TIMEOUT, allow_redirects=True)
        if resp.status_code in (403, 406):
            headers2 = make_headers()
            alt = url + ("&" if "?" in url else "?") + f"probe={int(time.time())}"
            time.sleep(0.4 + random.random() * 0.6)
            resp = SESSION.get(alt, headers=headers2, timeout=REQ_TIMEOUT, allow_redirects=True)
        resp.raise_for_status()
        return resp.content, resp.headers.get("Content-Type", ""), resp.url
    except Exception as e_requests:
        ua = headers["User-Agent"]
        curl_cmd = [
            "curl", "-LsS", "--compressed",
            "-A", ua,
            "-e", "https://www.google.com/",
            "-H", "Accept: " + headers["Accept"],
            "-H", "Accept-Language: " + headers["Accept-Language"],
            "--max-time", str(CURL_TIMEOUT),
            url,
        ]
        p = subprocess.run(curl_cmd, capture_output=True, timeout=CURL_TIMEOUT, check=False)
        if p.returncode != 0 or not p.stdout:
            raise RuntimeError(f"requests failed: {e_requests}; curl rc={p.returncode} err={p.stderr[:200]!r}")
        return p.stdout, "", url

# ───────────────────────────────────────────────
# Utils
# ───────────────────────────────────────────────
JSONLD_RE = re.compile(
    rb'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', re.I | re.S
)

META_PRICE_RE = re.compile(
    r'<meta[^>]+(?:property|name)=["\'](?:product:price:amount|og:price:amount)["\'][^>]*content=["\'](?P<price>[\d.,]+)["\']',
    re.I,
)
META_CURR_RE = re.compile(
    r'<meta[^>]+(?:property|name)=["\'](?:product:price:currency|og:price:currency)["\'][^>]*content=["\'](?P<curr>[A-Z]{3})["\']',
    re.I,
)
TWITTER_DATA1_RE = re.compile(
    r'<meta[^>]+name=["\']twitter:data1["\'][^>]*content=["\']\$?\s*([\d\.,]+)["\']', re.I
)  # often "Price $699.99" or just number

GTIN_KEYS = {"gtin", "gtin13", "gtin14", "gtin12", "gtin8", "barcode"}
MPN_KEYS = {"mpn", "sku", "model", "partNumber", "itemModel"}

def norm_price(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    try:
        return float(str(s).replace("$", "").replace(",", "").strip())
    except Exception:
        return None

def first_nonempty(*vals) -> str:
    for v in vals:
        if v and str(v).strip():
            return str(v).strip()
    return ""

@dataclass
class ProductInfo:
    name: str = ""
    brand: str = ""
    gtin: str = ""
    mpn: str = ""
    price: Optional[float] = None
    currency: str = ""
    availability: str = ""
    source: str = ""

# ───────────────────────────────────────────────
# JSON-LD (incl. @graph)
# ───────────────────────────────────────────────
def extract_jsonld_products(html_bytes: bytes) -> List[Dict]:
    out: List[Dict] = []

    def collect(node):
        if isinstance(node, dict):
            t = node.get("@type")
            if t:
                if isinstance(t, list):
                    if any(str(x).lower() == "product" for x in t):
                        out.append(node)
                elif str(t).lower() == "product":
                    out.append(node)
            if "@graph" in node and isinstance(node["@graph"], list):
                for g in node["@graph"]:
                    collect(g)
            for v in node.values():
                collect(v)
        elif isinstance(node, list):
            for el in node:
                collect(el)

    for m in JSONLD_RE.finditer(html_bytes):
        try:
            data = json.loads(m.group(1))
        except Exception:
            continue
        collect(data)
    return out

def parse_product_from_jsonld(obj: Dict) -> ProductInfo:
    name = first_nonempty(obj.get("name"))
    brand_val = obj.get("brand")
    brand = brand_val.get("name") if isinstance(brand_val, dict) else (brand_val or "")
    gtin, mpn = "", ""
    for k in GTIN_KEYS:
        if obj.get(k):
            gtin = str(obj[k]).strip(); break
    for k in MPN_KEYS:
        if obj.get(k):
            mpn = str(obj[k]).strip(); break
    offers = obj.get("offers") or {}
    if isinstance(offers, list):
        offers = offers[0]
    price = norm_price(first_nonempty(offers.get("price"), offers.get("lowPrice"), offers.get("highPrice")))
    curr = first_nonempty(offers.get("priceCurrency"))
    avail = first_nonempty(offers.get("availability"))
    return ProductInfo(name, brand, gtin, mpn, price, curr, avail, "jsonld")

def meta_price_currency(html: str) -> Tuple[Optional[float], str]:
    p = META_PRICE_RE.search(html)
    c = META_CURR_RE.search(html)
    if not p:
        td = TWITTER_DATA1_RE.search(html)
        if td:
            return norm_price(td.group(1)), "USD"
    return (norm_price(p.group("price")) if p else None, c.group("curr") if c else "")

# ───────────────────────────────────────────────
# SFCC / Backcountry
# ───────────────────────────────────────────────
MPN_PATTERN = re.compile(r'\b\d{3}-\d{5}-\d{2}\b')  # e.g., 010-02890-00

def fetch_sfcc_jsonld(url: str) -> Optional[ProductInfo]:
    body, _, _ = http_get(url)
    prods = extract_jsonld_products(body)
    pi = None
    if prods:
        parsed = [parse_product_from_jsonld(p) for p in prods]
        parsed.sort(key=lambda x: (x.price is None, x.gtin == "", x.mpn == ""))
        pi = parsed[0]
        if not pi.currency:
            pi.currency = "USD"
        pi.source = "sfcc_jsonld"

    html = body.decode("utf-8", errors="ignore")
    if not pi or not pi.mpn:
        m = MPN_PATTERN.search(html)
        if m:
            pi = pi or ProductInfo()
            pi.mpn = m.group(0)
            pi.source = "sfcc_html"
    if pi and pi.price is None:
        p, c = meta_price_currency(html)
        if p:
            pi.price, pi.currency = p, c or "USD"
    return pi

# ───────────────────────────────────────────────
# Shopify (Mike's Bikes) + fallbacks
# ───────────────────────────────────────────────
HANDLE_RE = re.compile(r'(?:"|&)handle["\']?\s*[:=]\s*["\']([^"\']+)["\']', re.I)
DATA_HANDLE_RE = re.compile(r'data-product-handle=["\']([^"\']+)["\']', re.I)
ANALYTICS_HANDLE_RE = re.compile(r'ShopifyAnalytics\.meta\.product\.handle\s*=\s*["\']([^"\']+)["\']', re.I)
SHOPIFY_ANALYTICS_RE = re.compile(r'ShopifyAnalytics\.meta\s*=\s*(\{.*?\});', re.I | re.S)
PRICE_META_ITEMPROP_RE = re.compile(r'<meta[^>]+itemprop=["\']price["\'][^>]*content=["\']([^"\']+)["\']', re.I)
CURRENCY_META_ITEMPROP_RE = re.compile(r'<meta[^>]+itemprop=["\']priceCurrency["\'][^>]*content=["\']([A-Z]{3})["\']', re.I)
# theme JSON blocks
PRODUCTJSON_SCRIPT_RE = re.compile(r'<script[^>]+type=["\']application/json["\'][^>]*id=["\']ProductJson[^"\']*["\'][^>]*>(.*?)</script>', re.I | re.S)
DATA_PRODUCT_RE = re.compile(r'data-product=["\'](.*?)["\']', re.I | re.S)

def extract_shopify_handle_from_html(html: str) -> Optional[str]:
    for rx in (ANALYTICS_HANDLE_RE, DATA_HANDLE_RE, HANDLE_RE):
        m = rx.search(html)
        if m:
            return m.group(1).strip()
    return None

def canonical_from_html(html: str) -> str:
    m = re.search(r'<link[^>]+rel=["\']canonical["\'][^>]*href=["\']([^"\']+)["\']', html, re.I)
    return m.group(1) if m else ""

def ensure_products_path_for_shopify(url: str, html_bytes: Optional[bytes]) -> str:
    path = urlparse(url).path
    if "/products/" in path:
        return url
    html = html_bytes.decode("utf-8", errors="ignore") if html_bytes else ""
    can = canonical_from_html(html)
    if can and "/products/" in urlparse(can).path:
        return can
    handle = extract_shopify_handle_from_html(html)
    if handle:
        base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
        return urljoin(base, f"/products/{handle}")
    seg = path.strip("/").split("/")[-1]
    if seg and "." not in seg:
        base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
        return urljoin(base, f"/products/{seg}")
    return url

def _shopify_best_from_variants(vlist) -> Tuple[Optional[float], str, str]:
    price = None; gtin = ""; mpn = ""
    for v in (vlist or []):
        cents = v.get("price")
        p = float(cents) / 100.0 if isinstance(cents, (int, float)) else None
        if p is not None and (price is None or p < price):
            price = p
        if not gtin and v.get("barcode"):
            gtin = str(v.get("barcode")).strip()
        if not mpn and v.get("sku"):
            mpn = str(v.get("sku")).strip()
    return price, gtin, mpn

def fetch_shopify_js(url: str) -> Optional[ProductInfo]:
    body, _, final = http_get(url)
    dump_snapshot(host(final), final, body, "shopify_fetched")
    prod_url = ensure_products_path_for_shopify(final, body)

    # Try the .js endpoints first
    for js_url in [urljoin(prod_url, ".js"), urljoin(prod_url.rstrip('/') + "/", ".js"), urljoin(prod_url, "/.js")]:
        try:
            jb, _, _ = http_get(js_url)
            data = json.loads(jb.decode("utf-8", errors="ignore"))
            title = data.get("title", "")
            brand = data.get("vendor", "")
            price, gtin, mpn = _shopify_best_from_variants(data.get("variants", []))
            return ProductInfo(title, brand, gtin, mpn, price, "USD", "", "shopify_js")
        except Exception:
            continue

    # Fallback 1: theme JSON block <script id="ProductJson…">
    html = body.decode("utf-8", errors="ignore")
    m = PRODUCTJSON_SCRIPT_RE.search(html)
    if m:
        try:
            pj = json.loads(m.group(1))
            title = pj.get("title") or pj.get("product", {}).get("title") or ""
            brand = pj.get("vendor") or pj.get("product", {}).get("vendor") or ""
            variants = pj.get("variants") or pj.get("product", {}).get("variants") or []
            price, gtin, mpn = _shopify_best_from_variants(variants)
            return ProductInfo(title, brand, gtin, mpn, price, "USD", "", "shopify_productjson")
        except Exception:
            pass

    # Fallback 1b: data-product="…json…"
    m2 = DATA_PRODUCT_RE.search(html)
    if m2:
        try:
            pj = json.loads(m2.group(1).replace("&quot;", '"'))
            title = pj.get("title", "")
            brand = pj.get("vendor", "")
            price, gtin, mpn = _shopify_best_from_variants(pj.get("variants", []))
            return ProductInfo(title, brand, gtin, mpn, price, "USD", "", "shopify_dataproduct")
        except Exception:
            pass

    # Fallback 2: JSON-LD in HTML
    prods = extract_jsonld_products(body)
    if prods:
        jp = parse_product_from_jsonld(prods[0])
        if not jp.currency:
            jp.currency = "USD"
        jp.source = "shopify_jsonld_fallback"
        return jp

    # Fallback 3: ShopifyAnalytics.meta in HTML
    m3 = SHOPIFY_ANALYTICS_RE.search(html)
    if m3:
        try:
            meta = json.loads(m3.group(1))
            prod = meta.get("product", {})
            title = prod.get("title", "")
            brand = prod.get("vendor", "")
            price, gtin, mpn = _shopify_best_from_variants(prod.get("variants", []))
            return ProductInfo(title, brand, gtin, mpn, price, "USD", "", "shopify_analytics_meta")
        except Exception:
            pass

    # Fallback 4: meta itemprop price (themes) + OG/Twitter price hints
    m_price = PRICE_META_ITEMPROP_RE.search(html)
    p = norm_price(m_price.group(1)) if m_price else None
    curr = "USD"
    m_curr = CURRENCY_META_ITEMPROP_RE.search(html)
    if m_curr:
        curr = m_curr.group(1)
    if p is None:
        p2, c2 = meta_price_currency(html)  # OG & twitter:data1
        if p2 is not None:
            p, curr = p2, c2 or curr

    if p is not None:
        title = extract_shopify_handle_from_html(html) or (canonical_from_html(html).split("/")[-1].replace("-", " "))
        brand = ""
        if prods:
            jp = parse_product_from_jsonld(prods[0])
            title = jp.name or title
            brand = jp.brand or brand
        return ProductInfo(title or "", brand or "", "", "", p, curr, "", "shopify_meta_price")

    return None

# ───────────────────────────────────────────────
# Excel Sports (Custom)
# ───────────────────────────────────────────────
EXCEL_PRICE_RE = re.compile(r'<span[^>]*class=["\']retail-pdp-price[^"\']*["\'][^>]*>(?P<p>[^<]+)</span>', re.I)
EXCEL_PRICE_ALT_RE = re.compile(r'data-price\s*=\s*["\']([\d\.,]+)["\']', re.I)
EXCEL_PRICE_ANY_RE = re.compile(r'(?:Price|Your Price|Our Price)\s*[:\-]?\s*\$?\s*([\d\.,]+)', re.I)
EXCEL_ITEMPROP_PRICE_RE = re.compile(r'itemprop=["\']price["\'][^>]*content=["\']([\d\.,]+)["\']', re.I)
EXCEL_MODEL_RE = re.compile(r'<p[^>]*id=["\']product-model["\'][^>]*data-mansku=["\'](?P<mpn>[^"\']*)["\'][^>]*data-upc=["\'](?P<gtin>[^"\']*)["\']', re.I)
EXCEL_BRAND_RE = re.compile(r'<div[^>]*class=["\']vendor-pdp-link["\'][^>]*>\s*<a[^>]*>(?P<brand>[^<]+)</a>', re.I)
EXCEL_NAME_RE  = re.compile(r'<h1[^>]*class=["\'][^"\']*product-title[^"\']*["\'][^>]*>(?P<name>[^<]+)</h1>', re.I)

def fetch_excel_custom(url: str) -> Optional[ProductInfo]:
    body, _, _ = http_get(url)
    html = body.decode("utf-8", errors="ignore")
    dump_snapshot(host(url), url, body, "excel_fetched")

    name_m  = EXCEL_NAME_RE.search(html)
    brand_m = EXCEL_BRAND_RE.search(html)
    mdl_m   = EXCEL_MODEL_RE.search(html)

    name  = name_m.group("name").strip() if name_m else ""
    brand = brand_m.group("brand").strip() if brand_m else ""
    mpn   = mdl_m.group("mpn").strip() if mdl_m else ""
    gtin  = mdl_m.group("gtin").strip() if mdl_m else ""

    # price probes
    price = None
    m = EXCEL_PRICE_RE.search(html)
    if m: price = norm_price(m.group("p"))
    if price is None:
        m = EXCEL_ITEMPROP_PRICE_RE.search(html)
        if m: price = norm_price(m.group(1))
    if price is None:
        m = EXCEL_PRICE_ALT_RE.search(html)
        if m: price = norm_price(m.group(1))
    if price is None:
        m = EXCEL_PRICE_ANY_RE.search(html)
        if m: price = norm_price(m.group(1))

    # JSON-LD fallback
    if price is None or not (mpn or gtin or brand or name):
        prods = extract_jsonld_products(body)
        if prods:
            jp = parse_product_from_jsonld(prods[0])
            name  = name  or jp.name
            brand = brand or jp.brand
            gtin  = gtin  or jp.gtin
            mpn   = mpn   or jp.mpn
            price = price or jp.price

    return ProductInfo(name, brand, gtin, mpn, price, "USD", "", "excel_custom_html")

# ───────────────────────────────────────────────
# Magento / Performance Bike
# ───────────────────────────────────────────────
MAGENTO_PRICE_RE = re.compile(r'data-price-amount=["\']([\d\.,]+)["\']', re.I)
MAGENTO_PRICE_SPAN_RE = re.compile(r'class=["\'][^"\']*price[^"\']*["\'][^>]*>\s*\$?\s*([\d\.,]+)\s*<', re.I)
MAGENTO_INIT_RE = re.compile(r'<script[^>]+type=["\']text/x-magento-init["\'][^>]*>(.*?)</script>', re.I | re.S)

def fetch_magento_jsonld(url: str) -> Optional[ProductInfo]:
    body, _, _ = http_get(url)
    dump_snapshot(host(url), url, body, "magento_fetched")
    html = body.decode("utf-8", errors="ignore")

    # JSON-LD first
    prods = extract_jsonld_products(body)
    if prods:
        pi = parse_product_from_jsonld(prods[0])
        if not pi.currency:
            pi.currency = "USD"
        pi.source = "magento_jsonld"
        return pi

    # meta & price attribute fallbacks
    p, c = meta_price_currency(html)
    if not p:
        m = MAGENTO_PRICE_RE.search(html) or MAGENTO_PRICE_SPAN_RE.search(html)
        if m:
            p = norm_price(m.group(1))
    if not p:
        # price inside text/x-magento-init (priceBox config)
        for m in MAGENTO_INIT_RE.finditer(html):
            blob = m.group(1)
            try:
                # The blob may contain multiple JSON objects; try to find "finalPrice"
                if '"finalPrice"' in blob:
                    num = re.search(r'"finalPrice"\s*:\s*{\s*"amount"\s*:\s*([\d\.,]+)', blob)
                    if num:
                        p = norm_price(num.group(1))
                        break
            except Exception:
                pass
    if p:
        return ProductInfo("", "", "", "", p, c or "USD", "", "magento_html_price")
    return None

# ───────────────────────────────────────────────
# Schema harvester (JSON-LD + microdata + RDFa + OG + dataLayer)
# ───────────────────────────────────────────────
def _normalize_offer_price(offer: dict) -> Tuple[Optional[float], str, str]:
    if not isinstance(offer, dict):
        return None, "", ""
    price = offer.get("price") or offer.get("lowPrice") or offer.get("highPrice") or offer.get("priceAmount")
    curr  = offer.get("priceCurrency") or offer.get("currency") or ""
    avail = offer.get("availability") or offer.get("itemAvailability") or ""
    return norm_price(price), curr, avail

def _brand_name(node_brand) -> str:
    if isinstance(node_brand, dict):
        return first_nonempty(node_brand.get("name"), node_brand.get("brand"), node_brand.get("@id"))
    return first_nonempty(node_brand)

def _extract_gtin_mpn_generic(node: dict) -> Tuple[str, str]:
    gtin, mpn = "", ""
    for k in GTIN_KEYS:
        if node.get(k):
            gtin = str(node[k]).strip(); break
    for k in MPN_KEYS:
        if node.get(k):
            mpn = str(node[k]).strip(); break
    return gtin, mpn

def parse_product_like_node(node: dict) -> Optional[ProductInfo]:
    if not isinstance(node, dict):
        return None
    types = node.get("@type")
    if types:
        if isinstance(types, list):
            is_product = any(str(t).lower() == "product" for t in types)
        else:
            is_product = str(types).lower() == "product"
    else:
        is_product = any(k in node for k in ["sku","mpn","gtin","brand","offers","name"])
    if not is_product:
        return None

    name  = first_nonempty(node.get("name"), node.get("title"))
    brand = _brand_name(node.get("brand"))
    gtin, mpn = _extract_gtin_mpn_generic(node)

    offers = node.get("offers")
    price = curr = avail = None
    if isinstance(offers, list) and offers:
        price, curr, avail = _normalize_offer_price(offers[0])
    elif isinstance(offers, dict):
        price, curr, avail = _normalize_offer_price(offers)

    if not name and node.get("_og:title"):
        name = node["_og:title"]
    if not curr and node.get("_og:price:currency"):
        curr = node["_og:price:currency"]
    if (price is None) and node.get("_og:price:amount"):
        price = norm_price(node["_og:price:amount"])

    return ProductInfo(
        name=name or "",
        brand=brand or "",
        gtin=gtin,
        mpn=mpn,
        price=price,
        currency=curr or "USD",
        availability=avail or "",
        source=node.get("_source", "schema_mix"),
    )

def _og_to_dict(og_raw) -> dict:
    if isinstance(og_raw, dict):
        return {str(k).lower(): v for k, v in og_raw.items()}
    if isinstance(og_raw, list):
        out = {}
        for item in og_raw:
            if not isinstance(item, dict):
                continue
            key = (item.get("property") or item.get("name") or item.get("property_attr") or item.get("name_attr") or "")
            val = (item.get("content") or item.get("value") or item.get("content_attr") or item.get("value_attr") or "")
            key = str(key).lower().strip()
            if key:
                out[key] = val
        return out
    return {}

def harvest_structured_products(html_bytes: bytes, url: str) -> List[ProductInfo]:
    results: List[ProductInfo] = []

    # JSON-LD
    jsonld_nodes = extract_jsonld_products(html_bytes)
    for n in jsonld_nodes:
        pi = parse_product_from_jsonld(n)
        if pi: results.append(pi)

    # extruct paths
    if EXSTRUCT_AVAILABLE:
        html_str = html_bytes.decode("utf-8", errors="ignore")
        data = extruct.extract(
            html_str,
            base_url=get_base_url(html_str, url),
            syntaxes=["json-ld", "microdata", "rdfa", "opengraph"],
            errors="ignore",
        )

        # microdata
        for item in data.get("microdata", []):
            node = item.get("properties") or {}
            if item.get("type"): node["@type"] = item["type"]
            node["_source"] = "microdata"
            pi = parse_product_like_node(node)
            if pi: results.append(pi)

        # RDFa
        for item in data.get("rdfa", []):
            node = dict(item); node["_source"] = "rdfa"
            pi = parse_product_like_node(node)
            if pi: results.append(pi)

        # OpenGraph
        og = _og_to_dict(data.get("opengraph") or {})
        if og:
            node = {
                "name": og.get("og:title") or og.get("title"),
                "_og:title": og.get("og:title") or og.get("title"),
                "_og:price:amount": (
                    og.get("product:price:amount")
                    or og.get("og:price:amount")
                    or og.get("product:price:standard_amount")
                ),
                "_og:price:currency": (
                    og.get("product:price:currency")
                    or og.get("og:price:currency")
                ),
                "@type": "Product",
                "_source": "opengraph",
            }
            pi = parse_product_like_node(node)
            if pi: results.append(pi)

    # window.dataLayer
    html = html_bytes.decode("utf-8", errors="ignore")
    dl = re.findall(r'window\.dataLayer\s*=\s*\[(.*?)\];', html, flags=re.I|re.S)
    if not dl:
        dl = re.findall(r'dataLayer\.push\(\s*(\{.*?\})\s*\)', html, flags=re.I|re.S)

    for blob in dl:
        try:
            blob_json = json.loads(blob)
        except Exception:
            try:
                blob_json = json.loads(re.sub(r"(?<!\\)'", '"', blob))
            except Exception:
                continue
        node = {}
        for k, v in blob_json.items():
            lk = str(k).lower()
            if lk in ("productname","name","title"): node["name"] = v
            if lk in ("brand","manufacturer","vendor"): node["brand"] = v
            if lk in ("mpn","sku","model"): node["mpn"] = v
            if lk in ("gtin","upc","ean","barcode"): node["gtin"] = v
            if lk in ("price","saleprice","listprice"): node["offers"] = {"price": v}
            if lk in ("currency","pricecurrency"): node.setdefault("offers", {}).update({"priceCurrency": v})
            if lk in ("availability","stock","instock"): node.setdefault("offers", {}).update({"availability": v})
        if node:
            node["@type"] = "Product"; node["_source"] = "datalayer"
            pi = parse_product_like_node(node)
            if pi: results.append(pi)

    # de-dup: prefer GTIN/MPN, then price
    uniq: Dict[Tuple[str,str,str], ProductInfo] = {}
    for pi in results:
        key = (pi.name or "", pi.gtin or "", pi.mpn or "")
        prev = uniq.get(key)
        if not prev:
            uniq[key] = pi; continue
        score_prev = (prev.gtin != "", prev.mpn != "", prev.price is not None)
        score_new  = (pi.gtin   != "", pi.mpn   != "", pi.price  is not None)
        if score_new > score_prev:
            uniq[key] = pi
    return list(uniq.values())

# ───────────────────────────────────────────────
# Router + merge
# ───────────────────────────────────────────────
def detect_platform(url: str, html: Optional[bytes]) -> str:
    h = host(url)
    if "excelsports.com" in h:
        return "Excel Custom"
    if "backcountry.com" in h:
        return "SFCC"
    if "mikesbikes.com" in h:
        return "Shopify"
    if "performancebike.com" in h:
        return "Magento/Custom"
    if html and re.search(r"cdn\.shopify|ShopifyAnalytics|/cart\.js", html.decode(errors="ignore"), re.I):
        return "Shopify"
    return "Unknown"

def _best_fill(primary: Optional[ProductInfo], harvested: List[ProductInfo]) -> Optional[ProductInfo]:
    cands: List[ProductInfo] = []
    if primary: cands.append(primary)
    cands.extend(harvested)
    if not cands: return primary

    def score(pi: ProductInfo):
        return (
            1 if (pi.gtin) else 0,
            1 if (pi.mpn) else 0,
            1 if (pi.price is not None) else 0,
            1 if (pi.brand) else 0,
            1 if (pi.name) else 0,
        )
    cands.sort(key=score, reverse=True)
    return cands[0]

def get_product_data(url: str) -> Tuple[str, Optional[ProductInfo]]:
    hst = host(url)
    try:
        body, _, final = http_get(url)
    except Exception:
        return "ERROR_FETCH", None

    dump_snapshot(hst, final, body, "fetched")
    platform = detect_platform(final, body)

    primary = None
    try:
        if platform == "Shopify":
            primary = fetch_shopify_js(final)
        elif platform == "SFCC":
            primary = fetch_sfcc_jsonld(final)
        elif platform == "Excel Custom":
            primary = fetch_excel_custom(final)
        elif platform == "Magento/Custom":
            primary = fetch_magento_jsonld(final)
        else:
            prods = extract_jsonld_products(body)
            primary = parse_product_from_jsonld(prods[0]) if prods else None
            if primary:
                if not primary.currency: primary.currency = "USD"
                primary.source = "generic_jsonld"
    except Exception:
        primary = None

    harvested = harvest_structured_products(body, final)
    best = _best_fill(primary, harvested)
    return platform, best

# ───────────────────────────────────────────────
# Main
# ───────────────────────────────────────────────
def main():
    rows = []
    for url in TEST_URLS:
        print(f"🔎 {host(url)} — probing")
        platform, info = get_product_data(url)
        rows.append({
            "Domain": host(url),
            "URL": url,
            "Platform": platform,
            "Name": info.name if info else "",
            "Brand": info.brand if info else "",
            "GTIN": info.gtin if info else "",
            "MPN": info.mpn if info else "",
            "Price": f"{info.price:.2f}" if info and (info.price is not None) else "",
            "Currency": info.currency if info else "",
            "Availability": info.availability if info else "",
            "Source": info.source if info else "",
        })
        time.sleep(SLEEP_BETWEEN)

    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "Domain","URL","Platform","Name","Brand","GTIN","MPN",
                "Price","Currency","Availability","Source"
            ],
        )
        w.writeheader()
        w.writerows(rows)

    print(f"✅ Wrote {len(rows)} rows → {OUTPUT_CSV}")

if __name__ == "__main__":
    main()