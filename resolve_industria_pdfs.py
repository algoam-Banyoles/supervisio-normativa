"""
resolve_industria_pdfs.py
Resol les URL de PDF per a les 44 entrades de normativa_industria sense url_pdf.

Estratègia A: visita la pàgina BOE (act.php o doc.php) i cerca link al PDF.
Estratègia B: extreu la data de publicació del HTML i construeix la URL del PDF.

Ús:
    python resolve_industria_pdfs.py
"""

import json
import os
import re
import sys
import time
from datetime import date

import requests
from bs4 import BeautifulSoup

# ── Constants ──────────────────────────────────────────────────────────────────

CATALOG_PATH = os.path.join("normativa_industria", "_catalogo", "catalogo_industria.json")
PDF_DIR      = os.path.join("normativa_industria", "pdfs")
BASE_BOE     = "https://www.boe.es"
DELAY        = 2.0

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":     "text/html,application/pdf,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9",
}

_PDF_INLINE_RE = re.compile(
    r"/boe/dias/(\d{4})/(\d{2})/(\d{2})/pdfs/(BOE-[A-Za-z0-9\-]+\.pdf)",
    re.IGNORECASE,
)
_FECHA_RE = re.compile(
    r"Publicado en BOE.*?(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})",
    re.IGNORECASE | re.DOTALL,
)
_FECHA2_RE = re.compile(
    r"Fecha de publicaci[oó]n[:\s]+(\d{2})[/\-](\d{2})[/\-](\d{4})",
    re.IGNORECASE,
)
_MESOS = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
    "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
}


# ── Session ────────────────────────────────────────────────────────────────────

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    s.mount("https://", requests.adapters.HTTPAdapter(max_retries=3))
    return s


# ── Helpers ────────────────────────────────────────────────────────────────────

def _get(session, url, timeout=20):
    try:
        r = session.get(url, timeout=timeout, allow_redirects=True)
        r.raise_for_status()
        return r
    except requests.RequestException as exc:
        print(f"    [GET] {url} → {exc}")
        return None


def _head_ok(session, url) -> bool:
    """Returns True if HEAD returns 200 with Content-Type: application/pdf."""
    try:
        r = session.head(url, timeout=15, allow_redirects=True)
        if r.status_code != 200:
            return False
        ct = r.headers.get("Content-Type", "")
        return "pdf" in ct.lower() or url.lower().endswith(".pdf")
    except requests.RequestException:
        return False


def _extract_pdf_url_from_html(html: str, boe_id: str) -> str | None:
    """Estratègia A: cerca link directe al PDF al contingut HTML."""
    # Manera 1: patró /boe/dias/YYYY/MM/DD/pdfs/BOE-*.pdf
    m = _PDF_INLINE_RE.search(html)
    if m:
        yyyy, mm, dd, fname = m.groups()
        return f"{BASE_BOE}/boe/dias/{yyyy}/{mm}/{dd}/pdfs/{fname}"

    # Manera 2: BeautifulSoup cerca <a href="...pdf..."> que contingui el boe_id
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if boe_id.lower() in href.lower() and ".pdf" in href.lower():
            return href if href.startswith("http") else BASE_BOE + href

    # Manera 3: qualsevol link .pdf visible al HTML
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if _PDF_INLINE_RE.search(href):
            return BASE_BOE + href if href.startswith("/") else href

    return None


def _extract_pdf_url_from_date(html: str, boe_id: str, session) -> str | None:
    """Estratègia B: extreu la data de publicació i construeix la URL del PDF."""
    yyyy = mm = dd = None

    # Opció 1: "Publicado en BOE num. XXX de DD de mes de YYYY"
    m = _FECHA_RE.search(html)
    if m:
        dd_s, mes_s, yyyy = m.group(1).zfill(2), m.group(2).lower(), m.group(3)
        mm = _MESOS.get(mes_s)
        dd = dd_s

    # Opció 2: "Fecha de publicación: DD/MM/YYYY"
    if not yyyy:
        m = _FECHA2_RE.search(html)
        if m:
            dd, mm, yyyy = m.group(1), m.group(2), m.group(3)

    if not (yyyy and mm and dd):
        return None

    candidate = f"{BASE_BOE}/boe/dias/{yyyy}/{mm}/{dd}/pdfs/{boe_id}.pdf"
    if _head_ok(session, candidate):
        return candidate

    # Algunes normes antigues usen majúscules/minúscules diferent al nom del fitxer
    boe_lower = boe_id.lower()
    candidate2 = f"{BASE_BOE}/boe/dias/{yyyy}/{mm}/{dd}/pdfs/{boe_lower}.pdf"
    if candidate2 != candidate and _head_ok(session, candidate2):
        return candidate2

    return None


def _download_pdf(url: str, dest: str, session) -> bool:
    try:
        r = session.get(url, timeout=60, stream=True)
        r.raise_for_status()
        content = r.content
        if not content[:4] == b"%PDF":
            return False
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(dest, "wb") as fh:
            fh.write(content)
        return True
    except requests.RequestException as exc:
        print(f"    [DL] {url} → {exc}")
        return False


# ── Main ───────────────────────────────────────────────────────────────────────

def resolve_industria_pdfs() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    if not os.path.exists(CATALOG_PATH):
        print(f"ERROR: {CATALOG_PATH} not found")
        sys.exit(1)

    with open(CATALOG_PATH, encoding="utf-8") as fh:
        catalog = json.load(fh)

    session = _make_session()
    os.makedirs(PDF_DIR, exist_ok=True)

    missing = [e for e in catalog if not e.get("url_pdf")]
    print(f"Entrades sense url_pdf: {len(missing)}")

    resolts    = 0
    descarregats = 0
    no_resolts = 0

    for i, entry in enumerate(missing, 1):
        boe_id    = entry["boe_id"]
        tipus_url = entry.get("tipus_url", "act")
        boe_url   = f"{BASE_BOE}/buscar/{tipus_url}.php?id={boe_id}"

        print(f"  [{i}/{len(missing)}] {boe_id} ({tipus_url})")
        time.sleep(DELAY)

        resp = _get(session, boe_url)
        pdf_url = None

        if resp:
            html = resp.text
            # Estratègia A
            pdf_url = _extract_pdf_url_from_html(html, boe_id)

            # Estratègia B (si A falla)
            if not pdf_url:
                time.sleep(0.5)
                pdf_url = _extract_pdf_url_from_date(html, boe_id, session)

        # Si era doc.php, reintenta amb act.php i viceversa
        if not pdf_url:
            alt = "act" if tipus_url == "doc" else "doc"
            alt_url = f"{BASE_BOE}/buscar/{alt}.php?id={boe_id}"
            time.sleep(DELAY)
            resp2 = _get(session, alt_url)
            if resp2:
                html2 = resp2.text
                pdf_url = _extract_pdf_url_from_html(html2, boe_id)
                if not pdf_url:
                    time.sleep(0.5)
                    pdf_url = _extract_pdf_url_from_date(html2, boe_id, session)

        if not pdf_url:
            print(f"    → NO PDF RESOLT")
            entry["pdf_descarregat"] = False
            no_resolts += 1
            continue

        resolts += 1
        entry["url_pdf"] = pdf_url

        # Descarrega
        dest = os.path.join(PDF_DIR, boe_id + ".pdf")
        if os.path.exists(dest) and os.path.getsize(dest) > 1000:
            print(f"    → Ja existeix: {pdf_url}")
            entry["path_local"]      = dest.replace("\\", "/")
            entry["pdf_descarregat"] = True
            descarregats += 1
            continue

        time.sleep(DELAY)
        ok = _download_pdf(pdf_url, dest, session)
        if ok:
            entry["path_local"]      = dest.replace("\\", "/")
            entry["pdf_descarregat"] = True
            descarregats += 1
            print(f"    → Descarregat: {pdf_url}")
        else:
            entry["pdf_descarregat"] = False
            print(f"    → Error en descàrrega: {pdf_url}")

    # Desa el catàleg actualitzat
    with open(CATALOG_PATH, "w", encoding="utf-8") as fh:
        json.dump(catalog, fh, ensure_ascii=False, indent=2)

    print()
    print(f"Resolts: {resolts}/{len(missing)} | Descarregats: {descarregats} | No resolts: {no_resolts}")


if __name__ == "__main__":
    resolve_industria_pdfs()
