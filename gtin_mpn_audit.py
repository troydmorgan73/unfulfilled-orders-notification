#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import re

INPUT = "Price-Watch.csv"
OUTPUT = "gtin_mpn_audit.csv"

def clean_digits(s):
    return "".join(ch for ch in (s or "") if ch.isdigit())

def luhn_gs1_checksum(body: str) -> int:
    # GS1 mod10: right-to-left, odd positions ×3, even ×1 (excluding check digit)
    total = 0
    rev = body[::-1]
    for i, ch in enumerate(rev, start=1):
        n = int(ch)
        total += n * (3 if i % 2 == 1 else 1)
    return (10 - (total % 10)) % 10

def gtin_type_and_valid(raw: str):
    d = clean_digits(raw)
    if not d:
        return "", "", False, ""
    # Try 12, 13, 14
    for length, label in ((12, "GTIN-12/UPC-A"), (13, "GTIN-13/EAN-13"), (14, "GTIN-14")):
        if len(d) == length:
            body, chk = d[:-1], int(d[-1])
            calc = luhn_gs1_checksum(body)
            return d, label, (calc == chk), f"calc={calc}"
    # Sometimes UPC is provided without leading zero as 11; add a leading 0 to test
    if len(d) == 11:
        body = "0" + d
        calc = luhn_gs1_checksum(body[:-1])
        ok = (calc == int(body[-1]))
        return body, "GTIN-12/UPC-A (prefixed 0)", ok, f"calc={calc}"
    # 8-digit EAN?
    if len(d) == 8:
        body, chk = d[:-1], int(d[-1])
        calc = luhn_gs1_checksum(body)
        return d, "GTIN-8/EAN-8", (calc == chk), f"calc={calc}"
    return d, f"Unknown length {len(d)}", False, ""

def looks_like_mpn(s: str) -> bool:
    if not s:
        return False
    # Heuristics: mpn often alphanum with dashes/letters; avoid pure digits 12-14 (those are GTINs)
    s2 = s.strip()
    if re.fullmatch(r"\d{12,14}", s2):
        return False
    # Garmin-style: 010-02890-00; general: letters+digits+dashes/underscores
    return bool(re.search(r"[A-Za-z]", s2)) or bool(re.search(r"-|_", s2))

def main():
    with open(INPUT, "r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        rows = list(rdr)

    out = []
    for r in rows:
        prod = r.get("Product_Name") or r.get("Name") or ""
        brand = r.get("Brand","")
        gtin_raw = (r.get("GTIN") or "").strip()
        mpn_raw  = (r.get("MPN") or "").strip()

        gtin_norm, gtin_type, gtin_valid, note = gtin_type_and_valid(gtin_raw)
        mpn_looks = looks_like_mpn(mpn_raw)

        # Guess if swapped:
        mpn_is_digits_12_14 = bool(re.fullmatch(r"\d{12,14}", mpn_raw.strip()))
        gtin_has_letters = bool(re.search(r"[A-Za-z]", gtin_raw))
        swapped_hint = ""
        if mpn_is_digits_12_14 and (not gtin_valid):
            swapped_hint = "MPN_looks_like_GTIN"
        if gtin_has_letters and mpn_looks and not gtin_valid:
            swapped_hint = swapped_hint or "GTIN_has_letters"
        if not gtin_valid and mpn_is_digits_12_14:
            swapped_hint = swapped_hint or "Try swapping GTIN/MPN"

        out.append({
            **r,
            "GTIN_Normalized": gtin_norm,
            "GTIN_Type": gtin_type,
            "GTIN_Check_OK": "YES" if gtin_valid else "NO",
            "GTIN_Note": note,
            "MPN_LooksLike": "YES" if mpn_looks else "NO",
            "Swap_Suspected": swapped_hint or "",
        })

    with open(OUTPUT, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=out[0].keys())
        w.writeheader()
        w.writerows(out)

    print(f"✅ Wrote {len(out)} rows → {OUTPUT}")

if __name__ == "__main__":
    main()