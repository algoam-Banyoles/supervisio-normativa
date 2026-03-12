"""
une_catalog.py — Scrapes une.org (ASP.NET WebForms / SharePoint) for ICS
codes relevant to civil engineering and saves
normativa_une/_catalogo/catalogo_une.json.

Flow:
  1. GET search page  → session cookies + __VIEWSTATE + field names
  2. POST with ICS + checkbox estado filter (Vigente / Anulada)
  3. Parse divResultados, paginate via __doPostBack links
  4. Update __VIEWSTATE from each response before the next POST

Usage:
    python une_catalog.py [output_dir]   (default: normativa_une)

Dependencies: curl_cffi + beautifulsoup4 + stdlib.
"""

from __future__ import annotations

import io
import json
import sys

# Ensure UTF-8 output on Windows consoles (avoids cp1252 UnicodeEncodeError)
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
DELAY = 2.0

RESULTS_DIV_PATTERN = re.compile(r"divResultados$")

# UNE reference regex (used as fallback parser)
UNE_REF_PAT = re.compile(
    r"(UNE(?:-EN)?(?:-ISO)?(?:/IEC)?\s+[\d][\w\s\-/:\.]+?:\d{4}(?:/\w+:\d{4})?)",
    re.IGNORECASE,
)

ICS_TARGETS = [
    ("91",    "Edificació i materials construcció"),
    ("93",    "Enginyeria civil"),
    ("13080", "Qualitat del sòl"),
    ("13060", "Qualitat de l'aigua"),
    ("45",    "Enginyeria ferroviària"),
    ("77140", "Productes d'acer"),
]

IMPERSONATE = "chrome120"   # curl_cffi browser fingerprint


# ─── Session / form-state helpers ─────────────────────────────────────────────

def _extract_hidden_fields(soup: BeautifulSoup) -> dict:
    """Return all hidden <input> fields as a name→value dict."""
    return {
        inp["name"]: inp.get("value", "")
        for inp in soup.find_all("input", {"type": "hidden"})
        if inp.get("name")
    }


def _find_select_name(soup: BeautifulSoup, id_fragment: str) -> str:
    """Find a <select> whose id contains id_fragment and return its name."""
    el = soup.find("select", id=re.compile(re.escape(id_fragment), re.I))
    return el["name"] if el and el.get("name") else ""


def _find_checkbox_name(soup: BeautifulSoup, id_fragment: str) -> str:
    """Find a checkbox whose id contains id_fragment and return its name."""
    el = soup.find(
        "input",
        {"type": "checkbox", "id": lambda x: x and id_fragment.lower() in x.lower()},
    )
    return el.get("name", "") if el else ""


def get_session_and_form_data() -> tuple:
    """
    GET the search page, establish a session, and extract all ASP.NET
    form-field names.

    Returns:
        (session, hidden_dict, name_clas,
         chk_vig_name, chk_anul_name, btn_name, btn_value)
    """
    session = requests.Session(impersonate=IMPERSONATE)
    session.headers.update({"Accept-Language": "es-ES,es;q=0.9,ca;q=0.8"})

    # Warm up: visit homepage to collect initial cookies
    try:
        session.get(BASE_URL, timeout=30)
        time.sleep(1)
    except Exception:
        pass

    resp = session.get(SEARCH_URL, timeout=30)
    resp.raise_for_status()

    soup   = BeautifulSoup(resp.text, "html.parser")
    hidden = _extract_hidden_fields(soup)

    name_clas    = _find_select_name(soup, "drpClasificacion")
    chk_vig_name = _find_checkbox_name(soup, "vigent")
    chk_anul_name = _find_checkbox_name(soup, "anulad")

    # Button: try id="idButton" first, then any submit
    btn = (
        soup.find("input", {"id": "idButton"})
        or soup.find("button", {"id": "idButton"})
        or soup.find("input", {"type": "submit"})
    )
    btn_name  = btn.get("name", "")  if btn else ""
    btn_value = btn.get("value", "Submit") if btn else "Submit"

    print(f"  __VIEWSTATE length : {len(hidden.get('__VIEWSTATE', ''))}")
    print(f"  drpClasificacion   : {name_clas or '(not found)'}")
    print(f"  chk_vigentes       : {chk_vig_name or '(not found)'}")
    print(f"  chk_anuladas       : {chk_anul_name or '(not found)'}")
    print(f"  button             : {btn_name or '(not found)'} = {btn_value!r}")

    # Optional SharePoint team-settings header
    m = re.search(r'"teamSettings"\s*:\s*(\{[^}]+\})', resp.text)
    if m:
        session.headers["X-MicrosoftSharePoint-TeamSettings"] = m.group(1)

    return session, hidden, name_clas, chk_vig_name, chk_anul_name, btn_name, btn_value


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
        # Fallback: scan full page text for UNE references
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

    # Split text into per-norm blocks using UNE reference as delimiter
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

def search_ics(
    session:       requests.Session,
    hidden:        dict,
    name_clas:     str,
    chk_vig_name:  str,
    chk_anul_name: str,
    btn_name:      str,
    btn_value:     str,
    ics_val:       str,
    estat_filter:  str,  # "V" | "A" | ""
) -> list[dict]:
    """
    POST search form for one ICS code + status, paginating via __doPostBack.
    Returns deduplicated list of norm dicts.
    """
    all_results: list[dict] = []
    seen_refs:   set[str]   = set()
    page = 1

    while True:
        # Rebuild payload fresh each page from current hidden state
        data: dict = dict(hidden)

        if name_clas:
            data[name_clas] = ics_val

        # Estado via checkboxes — include only the relevant one(s)
        if estat_filter == "V":
            if chk_vig_name:
                data[chk_vig_name] = "on"
            # chk_anuladas omitted → unchecked
        elif estat_filter == "A":
            if chk_anul_name:
                data[chk_anul_name] = "on"
            # chk_vigentes omitted → unchecked
        else:
            # Both
            if chk_vig_name:
                data[chk_vig_name] = "on"
            if chk_anul_name:
                data[chk_anul_name] = "on"

        if page == 1:
            # First page: submit button triggers the search
            data.pop("__EVENTTARGET", None)
            data.pop("__EVENTARGUMENT", None)
            if btn_name:
                data[btn_name] = btn_value
        # Subsequent pages: __EVENTTARGET already set in hidden, button omitted

        # ── DEBUG: log exactly what we are sending ─────────────────────────
        print(f"\n  [DBG] POST URL    : {SEARCH_URL}")
        print(f"  [DBG] Page        : {page}")
        vs = data.get("__VIEWSTATE", "")
        print(f"  [DBG] __VIEWSTATE : {vs[:500]!r}  (len={len(vs)})")
        print(f"  [DBG] ICS field   : {name_clas!r} = {data.get(name_clas, '(absent)')!r}")
        print(f"  [DBG] chk_vig     : {chk_vig_name!r} = {data.get(chk_vig_name, '(absent)')!r}")
        print(f"  [DBG] chk_anul    : {chk_anul_name!r} = {data.get(chk_anul_name, '(absent)')!r}")
        print(f"  [DBG] button      : {btn_name!r} = {data.get(btn_name, '(absent)')!r}")
        non_hidden_keys = [k for k in data if not k.startswith("__")]
        print(f"  [DBG] other fields: {non_hidden_keys}")
        # ──────────────────────────────────────────────────────────────────

        try:
            resp = session.post(SEARCH_URL, data=data, timeout=30)
            resp.raise_for_status()
        except Exception as exc:
            print(f"\n      ✗ Error p{page}: {exc}")
            break

        # ── DEBUG: log response ────────────────────────────────────────────
        print(f"  [DBG] Response status : {resp.status_code}")
        print(f"  [DBG] Response HTML (first 1000 chars):")
        print(f"        {resp.text[:1000]!r}")
        soup = BeautifulSoup(resp.text, "html.parser")
        all_trs = soup.find_all("tr")
        print(f"  [DBG] <tr> count in page : {len(all_trs)}")
        div_res = next((d for d in soup.find_all("div", id=RESULTS_DIV_PATTERN)), None)
        print(f"  [DBG] divResultados found : {div_res is not None}")
        if div_res:
            trs_in_div = div_res.find_all("tr")
            print(f"  [DBG] <tr> inside divResultados : {len(trs_in_div)}")
            print(f"  [DBG] divResultados text (first 500):")
            print(f"        {div_res.get_text()[:500]!r}")
        # Look for any counter/total field
        for pat in [r"N[uú]mero de resultados[:\s]*([\d.,]+)",
                    r"resultado[s]?\s*encontrado[s]?\s*[:\s]*([\d.,]+)",
                    r"Total[:\s]*([\d.,]+)"]:
            m_total = re.search(pat, resp.text, re.IGNORECASE)
            if m_total:
                print(f"  [DBG] Counter match ({pat[:30]}): {m_total.group(0)!r}")
                break
        # ──────────────────────────────────────────────────────────────────

        results = parse_results_from_html(resp.text)
        if not results:
            break

        new = [r for r in results if r["referencia"] not in seen_refs]
        seen_refs.update(r["referencia"] for r in new)
        all_results.extend(new)
        print(f"  p{page}:{len(new)}", end="", flush=True)

        # Update VIEWSTATE fields only (not the whole form — would overwrite ICS etc.)
        for inp in soup.find_all("input", {"type": "hidden"}):
            n = inp.get("name", "")
            if n in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"):
                hidden[n] = inp.get("value", "")

        # Check for next page via __doPostBack
        next_a = soup.find(
            "a",
            string=re.compile(r"^(Siguiente|Next|[›»>])$"),
        )
        if not next_a:
            break
        href = next_a.get("href", "")
        m2 = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", href)
        if not m2:
            break

        hidden["__EVENTTARGET"]   = m2.group(1)
        hidden["__EVENTARGUMENT"] = m2.group(2)
        page += 1
        time.sleep(DELAY)

    print()
    return all_results


# ─── Main ─────────────────────────────────────────────────────────────────────

def main(output_dir: str = OUTPUT_DIR) -> None:
    global CATALOG_PATH
    if output_dir != OUTPUT_DIR:
        CATALOG_PATH = os.path.join(output_dir, "_catalogo", "catalogo_une.json")

    print("=== UNE Catalog Builder (ASP.NET) ===")
    print("Carregant sessió i formulari…")

    try:
        session, hidden, name_clas, chk_vig_name, chk_anul_name, btn_name, btn_value = (
            get_session_and_form_data()
        )
    except Exception as exc:
        print(f"  ✗ No s'ha pogut carregar la pagina de cerca: {exc}")
        sys.exit(1)

    catalog:   list[dict] = []
    seen_refs: set[str]   = set()

    # DEBUG: only ICS 91 vigentes
    _debug_targets = [("91", "Edificació i materials construcció")]

    for ics_val, ics_desc in _debug_targets:
        print(f"\n  ICS {ics_val} ({ics_desc}):")

        for estat_filter, estat_label in [("V", "vigentes")]:
            print(f"    [{estat_label}]…", end="", flush=True)

            try:
                results = search_ics(
                    session, hidden,
                    name_clas, chk_vig_name, chk_anul_name,
                    btn_name, btn_value,
                    ics_val, estat_filter,
                )
            except Exception as exc:
                print(f" ✗ error — {exc}")
                time.sleep(DELAY)
                continue

            new = [r for r in results if r["referencia"] not in seen_refs]
            seen_refs.update(r["referencia"] for r in new)
            catalog.extend(new)
            print(f" → {len(new)} noves")
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
