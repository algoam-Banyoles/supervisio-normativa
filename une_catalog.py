"""
une_catalog.py — Scrapes une.org for ICS codes relevant to civil engineering
and saves normativa_une/_catalogo/catalogo_une.json.

Real search mechanism (discovered via JS analysis):
  - The site uses a KQL (Keyword Query Language) engine, NOT ASP.NET form postback.
  - Button `idButton` calls JS Search() which posts `form1` to `encuentra-tu-norma`
    with a KQL query: e.g. (g:UNE) AND (e:VI) AND (i:91*)
  - Pagination: repost same params to `form2` with n=<page_number>

Key parameters:
  k   = KQL query string
  n   = page number (1 = first, 2 = second, …)
  m   = total results (from <span id="totalElementos"> in first response)
  p1  = "UNE@@" (norm type filter)
  p4  = "VI" (Vigente) | "AN" (Anulada)
  p7  = "<ics_code>@@<ics_display_name>"
  ptit = "" (free-text title, unused here)

Usage:
    python une_catalog.py [output_dir]   (default: normativa_une)

Dependencies: curl_cffi + beautifulsoup4 + stdlib.
"""

from __future__ import annotations

import io
import json
import sys

# Ensure UTF-8 output on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import os
import re
import time

from curl_cffi import requests
from bs4 import BeautifulSoup

# ─── Constants ────────────────────────────────────────────────────────────────

BASE_URL   = "https://www.une.org"
SEARCH_URL = f"{BASE_URL}/encuentra-tu-norma"

OUTPUT_DIR   = "normativa_une"
CATALOG_PATH = os.path.join(OUTPUT_DIR, "_catalogo", "catalogo_une.json")
DELAY        = 1.5
IMPERSONATE  = "chrome120"

# ICS targets: (dropdown_value, ics_display_name_suffix_for_p7)
# The p7 parameter is "{value}@@{display_text from dropdown option}"
ICS_TARGETS = [
    ("91",    "EDIFICACION Y MATERIALES DE CONSTRUCCION"),
    ("93",    "INGENIERIA CIVIL"),
    ("13080", "Calidad del suelo. Pedolog\u00eda"),
    ("13060", "Calidad del agua"),
    ("45",    "INGENIERIA FERROVIARIA"),
    ("77140", "Productos de acero"),
]

# UNE reference regex (for result parsing)
UNE_REF_PAT = re.compile(
    r"(UNE(?:-EN)?(?:-ISO)?(?:/IEC)?\s+[\d][\w\s\-/:\.]+?:\d{4}(?:/\w+:\d{4})?)",
    re.IGNORECASE,
)

RESULTS_DIV_PATTERN = re.compile(r"divResultados")


# ─── Session ──────────────────────────────────────────────────────────────────

def make_session() -> requests.Session:
    """Create a curl_cffi session with browser fingerprint."""
    session = requests.Session(impersonate=IMPERSONATE)
    session.headers.update({"Accept-Language": "es-ES,es;q=0.9,ca;q=0.8"})
    # Warm up: visit homepage to collect initial cookies
    try:
        session.get(BASE_URL, timeout=30)
        time.sleep(1)
    except Exception:
        pass
    return session


def get_ics_display_names(session: requests.Session) -> dict[str, str]:
    """
    GET the search page and extract the drpClasificacion dropdown option texts.
    Returns {value: display_text} for all ICS targets we care about.
    """
    resp = session.get(SEARCH_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    sel  = soup.find("select", id=re.compile("drpClasificacion", re.I))
    if not sel:
        return {}
    names = {}
    for opt in sel.find_all("option"):
        val  = opt.get("value", "")
        text = opt.get_text(strip=True)
        if val:
            names[val] = text
    return names


# ─── Result parser ────────────────────────────────────────────────────────────

def parse_results_from_html(html: str) -> list[dict]:
    """
    Extract norm metadata from the divResultados div.
    Falls back to regex scanning if the div is not found.
    """
    soup    = BeautifulSoup(html, "html.parser")
    div_res = next(
        (d for d in soup.find_all("div", id=RESULTS_DIV_PATTERN)),
        None,
    )

    if not div_res:
        refs = UNE_REF_PAT.findall(html)
        seen: set[str] = set()
        results = []
        for ref in refs:
            ref = ref.strip()
            if ref and ref not in seen:
                seen.add(ref)
                results.append({
                    "referencia":      ref,
                    "estat":           "",
                    "data_publicacio": "",
                    "descripcio":      "",
                    "ctn":             "",
                    "font":            "UNE scraping (regex fallback)",
                })
        return results

    text   = div_res.get_text("\n")
    blocks = UNE_REF_PAT.split(text)
    # blocks: [preamble, ref1, body1, ref2, body2, …]

    results = []
    i = 1
    while i < len(blocks) - 1:
        ref     = blocks[i].strip()
        content = blocks[i + 1] if i + 1 < len(blocks) else ""

        m_estat = re.search(r"Estado:\s*(Vigente|Anulada)", content)
        m_data  = re.search(
            r"Estado:\s*(?:Vigente|Anulada)\s*/\s*(\d{4}-\d{2}-\d{2})", content
        )

        estat = ""
        if m_estat:
            estat = "VIGENT" if m_estat.group(1) == "Vigente" else "ANULADA"
        data = m_data.group(1) if m_data else ""

        lines      = [l.strip() for l in content.split("\n") if l.strip()]
        descripcio = ""
        ctn        = ""
        for j, line in enumerate(lines):
            if "Estado:" in line:
                if j + 1 < len(lines) and not lines[j + 1].startswith("CTN"):
                    descripcio = lines[j + 1]
            if line.startswith("CTN"):
                ctn = line
                break

        if ref:
            results.append({
                "referencia":      ref,
                "estat":           estat,
                "data_publicacio": data,
                "descripcio":      descripcio[:200],
                "ctn":             ctn,
                "font":            "UNE scraping",
            })

        i += 2

    return results


# ─── Core search ──────────────────────────────────────────────────────────────

def _build_kql(ics_val: str, estat_filter: str) -> str:
    """Build KQL query: always UNE type + optionally estado + ICS."""
    parts = ["(g:UNE)"]
    if estat_filter == "V":
        parts.append("(e:VI)")
    elif estat_filter == "A":
        parts.append("(e:AN)")
    parts.append(f"(i:{ics_val}*)")
    return " AND ".join(parts)


def search_ics(
    session:      requests.Session,
    ics_val:      str,
    ics_name:     str,
    estat_filter: str,   # "V" | "A" | ""
) -> list[dict]:
    """
    Fetch all pages of results for one ICS code + estado.
    Uses KQL query posted to form1 / form2.
    Returns deduplicated list of norm dicts.
    """
    kql      = _build_kql(ics_val, estat_filter)
    p4_val   = {"V": "VI", "A": "AN"}.get(estat_filter, "")
    p7_val   = f"{ics_val}@@{ics_name}"

    all_results: list[dict] = []
    seen_refs:   set[str]   = set()
    total_items  = 0
    page         = 1

    while True:
        data = {
            "k":    kql,
            "n":    str(page),
            "m":    str(total_items),
            "v":    "",
            "p1":   "UNE@@",
            "p4":   p4_val,
            "p7":   p7_val,
            "ptit": "",
        }

        try:
            resp = session.post(SEARCH_URL, data=data, timeout=30)
            resp.raise_for_status()
        except Exception as exc:
            print(f"\n      Error p{page}: {exc}")
            break

        # On first page, read total item count
        if page == 1:
            soup = BeautifulSoup(resp.text, "html.parser")
            te = soup.find(id="totalElementos")
            if te:
                raw = te.get_text(strip=True).replace(".", "").replace(",", "")
                try:
                    total_items = int(raw)
                    print(f"\n      total web: {total_items:,}", end="")
                except ValueError:
                    pass

        results = parse_results_from_html(resp.text)
        if not results:
            break

        new = [r for r in results if r["referencia"] not in seen_refs]
        seen_refs.update(r["referencia"] for r in new)
        all_results.extend(new)
        print(f"  p{page}:{len(new)}", end="", flush=True)

        # Check for next page: presence of a link with id=pag{page+1}
        soup     = BeautifulSoup(resp.text, "html.parser")
        next_pag = f"pag{page + 1}"
        if not soup.find("a", id=next_pag):
            break

        page += 1
        time.sleep(DELAY)

    print()
    return all_results


# ─── Main ─────────────────────────────────────────────────────────────────────

def main(output_dir: str = OUTPUT_DIR) -> None:
    global CATALOG_PATH
    if output_dir != OUTPUT_DIR:
        CATALOG_PATH = os.path.join(output_dir, "_catalogo", "catalogo_une.json")

    print("=== UNE Catalog Builder (KQL) ===")
    print("Iniciant sessio...")

    session = make_session()

    print("Llegint noms ICS del formulari...")
    ics_names = get_ics_display_names(session)
    print(f"  {len(ics_names)} opcions ICS trobades")
    time.sleep(1)

    catalog:   list[dict] = []
    seen_refs: set[str]   = set()

    for ics_val, ics_name_fallback in ICS_TARGETS:
        # Use dropdown display text if found, otherwise use our hardcoded fallback
        ics_name = ics_names.get(ics_val, ics_name_fallback)
        print(f"\n  ICS {ics_val} ({ics_name_fallback}):")

        for estat_filter, estat_label in [("V", "vigentes"), ("A", "anuladas")]:
            print(f"    [{estat_label}]...", end="", flush=True)

            try:
                results = search_ics(session, ics_val, ics_name, estat_filter)
            except Exception as exc:
                print(f" error: {exc}")
                time.sleep(DELAY)
                continue

            new = [r for r in results if r["referencia"] not in seen_refs]
            seen_refs.update(r["referencia"] for r in new)
            catalog.extend(new)
            print(f" -> {len(new)} noves")
            time.sleep(DELAY)

    # Save
    os.makedirs(os.path.dirname(CATALOG_PATH), exist_ok=True)
    with open(CATALOG_PATH, "w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)

    vigents  = sum(1 for d in catalog if d["estat"] == "VIGENT")
    anulades = sum(1 for d in catalog if d["estat"] == "ANULADA")

    print(f"\n  Total UNE (ICS seleccionats): {len(catalog):,}")
    print(f"  Vigents:                      {vigents:,}")
    print(f"  Anulades:                     {anulades:,}")
    print(f"  Cataleg guardat:              {CATALOG_PATH}")


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else OUTPUT_DIR
    main(out)
