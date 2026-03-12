"""
pjcat_scraper.py — Fetches normativa from the Portal Juridic de Catalunya
(portaljuridic.gencat.cat) for construction/infrastructure projects.

Uses:
  - JSON API (tried first): GET /api/search?q=...&estat=...&pagina=N
  - HTML fallback: BeautifulSoup on /ca/pjur_ocults/pjur_resultats_recerca/

Output:
    normativa_pjcat/_catalogo/catalogo_pjcat.json

Optional integration:
    Adds DEROGADA entries to normativa_annexes.json (idempotent).

Usage:
    python pjcat_scraper.py [output_dir]   (default: normativa_pjcat)
"""

from __future__ import annotations

import io
import json
import os
import re
import shutil
import sys
import time
from datetime import datetime
from urllib.parse import urljoin, urlencode, quote

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import requests
from bs4 import BeautifulSoup

# ─── Constants ────────────────────────────────────────────────────────────────

SITE_BASE   = "https://portaljuridic.gencat.cat"
OUTPUT_DIR  = "normativa_pjcat"
CATALOG_DIR = os.path.join(OUTPUT_DIR, "_catalogo")
CATALOG_PATH = os.path.join(CATALOG_DIR, "catalogo_pjcat.json")
ANNEXES_PATH = "normativa_annexes.json"

DELAY       = 2.0   # seconds between requests (site can be slow)
PAGE_SIZE   = 20

HEADERS = {
    "User-Agent":      (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/json,*/*;q=0.8",
    "Accept-Language": "ca-ES,ca;q=0.9,es;q=0.7",
    "Referer":         SITE_BASE,
}

# Search terms to fetch: (query_string, categoria, fetch_derogades_too)
SEARCH_CATEGORIES = [
    ("contractació pública",  "contractes",       True),
    ("llei de contractes",    "contractes",       True),
    ("carreteres",            "carreteres_cat",   True),
    ("urbanisme",             "urbanisme",        True),
    ("medi ambient projectes","medi_ambient",     True),
    ("residus construcció",   "medi_ambient",     False),
    ("accessibilitat",        "accessibilitat",   True),
]

# If True, also fetches 'derogat' estat for each category
FETCH_DEROGADES = True

# ─── HTML search endpoint ─────────────────────────────────────────────────────
# URL format confirmed from portaljuridic.gencat.cat source (as of 2025).
SEARCH_URL  = f"{SITE_BASE}/ca/pjur_ocults/pjur_resultats_recerca/"
DETAIL_BASE = f"{SITE_BASE}/ca/pjur_ocults/pjur_fitxa_document/"

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _safe_get(
    session:  requests.Session,
    url:      str,
    params:   dict | None = None,
    is_json:  bool        = False,
) -> requests.Response | None:
    """GET with graceful 404/503 handling."""
    try:
        r = session.get(url, params=params, headers=HEADERS, timeout=30)
        if r.status_code in (404, 410):
            print(f"    [SKIP] 404/410: {url}")
            return None
        if r.status_code in (429, 503, 502):
            print(f"    [WARN] {r.status_code}, waiting 10s: {url}")
            time.sleep(10)
            r = session.get(url, params=params, headers=HEADERS, timeout=30)
        r.raise_for_status()
        return r
    except requests.exceptions.Timeout:
        print(f"    [WARN] Timeout: {url}")
        return None
    except Exception as exc:
        print(f"    [WARN] GET error ({exc}): {url}")
        return None


def _clean(text: str | None) -> str:
    if not text:
        return ""
    return " ".join(text.split()).strip()


def _parse_estat(estat_text: str) -> str:
    """Normalize raw estat string to VIGENT / DEROGADA / PENDENT."""
    s = estat_text.upper().strip()
    if any(v in s for v in ("VIGENT", "VIGENTE", "VIGENTI")):
        return "VIGENT"
    if any(v in s for v in ("DEROGAT", "DEROGADA", "DEROGA", "ANNULLAT")):
        return "DEROGADA"
    if any(v in s for v in ("PENDENT", "SUSPÈS", "SUSPENS")):
        return "PENDENT"
    return "PENDENT"


def _parse_date(raw: str) -> str:
    """Try common date formats, return ISO or original."""
    raw = _clean(raw)
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw


def _make_doc_url(doc_id: str) -> str:
    return f"{DETAIL_BASE}?documentId={doc_id}"


# ─── HTML parser ──────────────────────────────────────────────────────────────

def _parse_results_page(html: str, categoria: str) -> list[dict]:
    """
    Parse one results HTML page from portaljuridic.gencat.cat search.
    Returns list of partial catalog entries (may lack some fields).
    """
    soup = BeautifulSoup(html, "html.parser")
    entries: list[dict] = []

    # Results are typically in a <table class="taula-resultats"> or <ul class="resultats">
    # Try table rows first
    rows = soup.select("table.taula-resultats tr") or soup.select("table tr")
    if rows:
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue
            entry = _parse_table_row(cells, categoria)
            if entry:
                entries.append(entry)

    # Fallback: list-based results (newer portal layout)
    if not entries:
        items = (
            soup.select("ul.resultats li") or
            soup.select("div.resultat") or
            soup.select("article.document-item")
        )
        for item in items:
            entry = _parse_list_item(item, categoria)
            if entry:
                entries.append(entry)

    return entries


def _parse_table_row(cells: list, categoria: str) -> dict | None:
    """Parse a <tr> result row from the search table."""
    # Column order varies; detect by content
    # Typical: [rang/tipus, numero, titol, data, estat, organisme]
    texts = [_clean(c.get_text()) for c in cells]
    if not any(texts):
        return None

    # Find the link to the document
    link_tag = None
    for cell in cells:
        a = cell.find("a", href=True)
        if a:
            link_tag = a
            break

    href = ""
    if link_tag:
        href = link_tag.get("href", "")
        if href and not href.startswith("http"):
            href = urljoin(SITE_BASE, href)

    # Extract doc_id from href (?documentId=... or /document/XXXX)
    doc_id = ""
    m = re.search(r"documentId=([^&]+)", href)
    if m:
        doc_id = m.group(1)
    else:
        m = re.search(r"/document/([^/?]+)", href)
        if m:
            doc_id = m.group(1)

    # Try to find title (longest text or link text)
    title = _clean(link_tag.get_text()) if link_tag else ""
    if not title:
        title = max(texts, key=len, default="")

    # Estat (usually last or second-to-last col)
    estat_raw = texts[-1] if texts else ""
    estat = _parse_estat(estat_raw)
    if estat == "PENDENT":
        # Try second-to-last
        estat = _parse_estat(texts[-2]) if len(texts) >= 2 else "PENDENT"

    # Date — look for dd/mm/yyyy pattern
    data = ""
    for t in texts:
        m = re.search(r"\d{2}/\d{2}/\d{4}", t)
        if m:
            data = _parse_date(m.group(0))
            break

    # Codi — first column or from title
    codi = texts[0] if texts else doc_id

    # Departament — last column not matching estat/date
    departament = ""
    for t in reversed(texts):
        if t and not re.search(r"\d{2}/\d{2}/\d{4}", t) and _parse_estat(t) == "PENDENT":
            departament = t[:100]
            break

    if not title and not doc_id:
        return None

    return {
        "id":                 doc_id or codi,
        "codi":               codi[:80],
        "text":               title[:300],
        "categoria":          categoria,
        "estat":              estat,
        "data_actualizacion": data,
        "url_pjcat":          href,
        "departament":        departament,
        "observacions":       "",
        "derogada_per":       "",
        "materias":           [],
        "font":               "Portal Juridic Catalunya",
    }


def _parse_list_item(item, categoria: str) -> dict | None:
    """Parse a <li> or <article> result item from the search list."""
    a = item.find("a", href=True)
    if not a:
        return None

    href = a.get("href", "")
    if href and not href.startswith("http"):
        href = urljoin(SITE_BASE, href)

    doc_id = ""
    m = re.search(r"documentId=([^&]+)", href)
    if m:
        doc_id = m.group(1)
    else:
        m = re.search(r"/document/([^/?]+)", href)
        if m:
            doc_id = m.group(1)

    title = _clean(a.get_text())
    full_text = _clean(item.get_text())

    # Estat
    estat_tag = item.find(class_=re.compile(r"estat|status|vigent|derogat", re.I))
    estat_raw = _clean(estat_tag.get_text()) if estat_tag else ""
    estat = _parse_estat(estat_raw) if estat_raw else "VIGENT"

    # Date
    data = ""
    m = re.search(r"\d{2}/\d{2}/\d{4}", full_text)
    if m:
        data = _parse_date(m.group(0))

    # Derogada per — look for "derogat per" pattern
    derogada_per = ""
    m = re.search(r"[Dd]erogat\s+per\s+(.{5,80})", full_text)
    if m:
        derogada_per = _clean(m.group(1))[:150]

    # Codi — look for pattern like "Llei X/XXXX", "Decret XX/XXXX", etc.
    codi = ""
    m = re.search(
        r"((?:Llei|Decret|Ordre|Resolució|Reglament)\s+[\w\-/]+/\d{4})",
        full_text, re.I
    )
    if m:
        codi = m.group(1)[:80]
    if not codi:
        codi = doc_id or title[:60]

    if not title and not doc_id:
        return None

    return {
        "id":                 doc_id or codi,
        "codi":               codi,
        "text":               title[:300],
        "categoria":          categoria,
        "estat":              estat,
        "data_actualizacion": data,
        "url_pjcat":          href,
        "departament":        "",
        "observacions":       "",
        "derogada_per":       derogada_per,
        "materias":           [],
        "font":               "Portal Juridic Catalunya",
    }


def _has_next_page(html: str, current_page: int) -> bool:
    """Returns True if there's a 'next page' link."""
    soup = BeautifulSoup(html, "html.parser")
    # Look for next-page link or page (current+1) link
    next_n = str(current_page + 1)
    for a in soup.find_all("a", href=True):
        text = _clean(a.get_text())
        href = a.get("href", "")
        if text == next_n or f"pagina={next_n}" in href or text.lower() in ("seg", ">>", "seguent", ">"):
            return True
    return False


# ─── Main search function ─────────────────────────────────────────────────────

def search_pjcat(
    session:   requests.Session,
    query:     str,
    categoria: str,
    estat_filter: str = "vigent",  # "vigent" | "derogat" | ""
) -> list[dict]:
    """
    Paginate through Portal Juridic search results for a query.
    Returns list of catalog entries.
    """
    results: list[dict] = []
    seen:    set[str]   = set()
    page = 1

    estat_display = f"[{estat_filter or 'tot'}]"
    print(f"    '{query}' {estat_display} ...", end="", flush=True)

    while True:
        params: dict = {
            "action":  "fitxa",
            "modo":    "simple",
            "texto":   query,
            "pagina":  page,
        }
        if estat_filter:
            params["estat"] = estat_filter

        resp = _safe_get(session, SEARCH_URL, params=params)
        if not resp:
            break

        html = resp.text
        page_entries = _parse_results_page(html, categoria)

        if not page_entries:
            # Also try with "tot" estant if we got nothing
            break

        new_count = 0
        for entry in page_entries:
            key = entry["id"] or entry["text"]
            if key and key not in seen:
                seen.add(key)
                results.append(entry)
                new_count += 1

        print(f" {page}", end="", flush=True)

        if not _has_next_page(html, page) or new_count == 0:
            break
        page += 1
        time.sleep(DELAY)

    print(f"  -> {len(results)}")
    return results


# ─── Detail enrichment ────────────────────────────────────────────────────────

def _enrich_from_detail(session: requests.Session, entry: dict) -> None:
    """
    Optionally fetch detail page for a DEROGADA entry to find 'derogada_per'.
    Modifies entry in-place; does nothing if already populated.
    """
    if entry.get("derogada_per") or not entry.get("url_pjcat"):
        return
    resp = _safe_get(session, entry["url_pjcat"])
    if not resp:
        return
    soup = BeautifulSoup(resp.text, "html.parser")
    full = _clean(soup.get_text(" "))
    m = re.search(
        r"[Dd]erogat\s+per\s+(.{5,120}?)(?:\.|,|\n|$)",
        full
    )
    if m:
        entry["derogada_per"] = _clean(m.group(1))[:150]
    # Also try departament if missing
    if not entry.get("departament"):
        dept_tag = soup.find(class_=re.compile(r"organisme|departament|organ", re.I))
        if dept_tag:
            entry["departament"] = _clean(dept_tag.get_text())[:150]


# ─── Merge into normativa_annexes.json ────────────────────────────────────────

def merge_into_annexes(catalog: list[dict], annexes_path: str = ANNEXES_PATH) -> None:
    """
    Idempotent merge of DEROGADA catalog entries into normativa_annexes.json.
    Only adds entries to 'normativa_derogada'; never touches 'annexes' (VIGENT).
    Creates a .bak backup before writing.
    """
    if not os.path.exists(annexes_path):
        print(f"  [SKIP] {annexes_path} not found — skipping merge")
        return

    with open(annexes_path, encoding="utf-8") as f:
        data = json.load(f)

    derogades: list[dict] = data.get("normativa_derogada", [])

    # Build set of existing codis (case-insensitive) for idempotency check
    existing = {e.get("codi", "").upper() for e in derogades}

    added = 0
    for entry in catalog:
        if entry.get("estat") != "DEROGADA":
            continue
        codi = entry.get("codi", "") or entry.get("id", "")
        if not codi:
            continue
        if codi.upper() in existing:
            continue

        new_entry: dict = {
            "codi":        codi,
            "text":        entry.get("text", "")[:250],
            "derogada_per": entry.get("derogada_per", ""),
            "observacions": f"Font: Portal Juridic Catalunya. {entry.get('observacions', '')}".strip().rstrip(".") + ".",
        }
        # Include estat only if non-standard (PARCIALMENT_VIGENT etc.)
        estat = entry.get("estat", "DEROGADA")
        if estat != "DEROGADA":
            new_entry["estat"] = estat

        derogades.append(new_entry)
        existing.add(codi.upper())
        added += 1

    if added == 0:
        print("  normativa_annexes.json: cap nova entrada derogada")
        return

    data["normativa_derogada"] = derogades

    # Backup
    bak_path = annexes_path + ".bak"
    shutil.copy2(annexes_path, bak_path)

    with open(annexes_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"  Actualitzat normativa_annexes.json: +{added} entrades derogades  (backup: {bak_path})")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main(output_dir: str = OUTPUT_DIR) -> None:
    global CATALOG_DIR, CATALOG_PATH
    if output_dir != OUTPUT_DIR:
        CATALOG_DIR  = os.path.join(output_dir, "_catalogo")
        CATALOG_PATH = os.path.join(CATALOG_DIR, "catalogo_pjcat.json")

    print("=== Portal Juridic Catalunya — Catalog Builder ===")
    print(f"  Site: {SITE_BASE}")
    print(f"  Output: {CATALOG_PATH}\n")

    os.makedirs(CATALOG_DIR, exist_ok=True)
    # Ensure init.py exists
    init_py = os.path.join(output_dir, "init.py")
    if not os.path.exists(init_py):
        open(init_py, "w").close()

    session = requests.Session()

    catalog:  list[dict] = []
    seen_ids: set[str]   = set()

    for query, categoria, also_derogades in SEARCH_CATEGORIES:
        print(f"\n  [{categoria}] {query}")

        # Vigents
        vigents = search_pjcat(session, query, categoria, estat_filter="vigent")
        for e in vigents:
            key = e["id"] or e["text"]
            if key not in seen_ids:
                seen_ids.add(key)
                catalog.append(e)
        time.sleep(DELAY)

        # Derogades (optional per category)
        if FETCH_DEROGADES and also_derogades:
            derogades = search_pjcat(session, query, categoria, estat_filter="derogat")
            for e in derogades:
                key = e["id"] or e["text"]
                if key not in seen_ids:
                    seen_ids.add(key)
                    catalog.append(e)
            time.sleep(DELAY)

    # Enrich DEROGADA entries with detail page info if needed
    derogades_to_enrich = [e for e in catalog if e["estat"] == "DEROGADA" and not e.get("derogada_per")]
    if derogades_to_enrich:
        print(f"\n  Enriquint {len(derogades_to_enrich)} entrades derogades amb detail page...")
        for i, entry in enumerate(derogades_to_enrich, 1):
            print(f"    {i}/{len(derogades_to_enrich)} {entry['codi']} ...", end="", flush=True)
            _enrich_from_detail(session, entry)
            print(" ok")
            time.sleep(DELAY)

    # Save catalog
    with open(CATALOG_PATH, "w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)

    vigents  = sum(1 for e in catalog if e["estat"] == "VIGENT")
    derog    = sum(1 for e in catalog if e["estat"] == "DEROGADA")
    pendent  = sum(1 for e in catalog if e["estat"] == "PENDENT")

    cats: dict[str, int] = {}
    for e in catalog:
        cats[e["categoria"]] = cats.get(e["categoria"], 0) + 1

    print(f"\n  Total entrades PJCAT:  {len(catalog):,}")
    print(f"  Vigents:               {vigents:,}")
    print(f"  Derogades:             {derog:,}")
    print(f"  Pendent/altre:         {pendent:,}")
    for cat, n in sorted(cats.items()):
        print(f"    [{cat}]  {n:,}")
    print(f"  Cataleg guardat:       {CATALOG_PATH}")

    # Merge derogades into normativa_annexes.json
    print()
    merge_into_annexes(catalog)


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else OUTPUT_DIR
    main(out)
