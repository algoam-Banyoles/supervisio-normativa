"""
territori_scraper.py — Downloads technical instructions and circulars from
the Departament de Territori de la Generalitat de Catalunya (DGIM/DGIMT).

PDFs are fetched directly from territori.gencat.cat and validated.
Metadata is extracted from the first 2 pages using PyMuPDF.

Output:
    normativa_territori/_catalogo/catalogo_territori.json
    normativa_territori/pdfs/{id}.pdf

Usage:
    python territori_scraper.py [output_dir]   (default: normativa_territori)
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

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import requests

try:
    import fitz  # PyMuPDF
    PYMUPDF_OK = True
except ImportError:
    PYMUPDF_OK = False
    print("[WARN] PyMuPDF not installed — PDF metadata extraction disabled")

# ─── Constants ────────────────────────────────────────────────────────────────

OUTPUT_DIR   = "normativa_territori"
CATALOG_DIR  = os.path.join(OUTPUT_DIR, "_catalogo")
CATALOG_PATH = os.path.join(CATALOG_DIR, "catalogo_territori.json")
PDF_DIR      = os.path.join(OUTPUT_DIR, "pdfs")
ANNEXES_PATH = "normativa_annexes.json"

DELAY = 1.5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/pdf,*/*;q=0.8",
    "Accept-Language": "ca-ES,ca;q=0.9,es;q=0.7",
    "Referer": "https://territori.gencat.cat/",
}

TERRITORI_BASE = (
    "https://territori.gencat.cat/web/.content/home/serveis/normativa/"
    "procediments_i_actuacions_juridiques/instruccions_i_circulars/"
    "infraestructures_mobilitat/"
)

# ─── Priority documents ───────────────────────────────────────────────────────

PRIORITY_DOCS: list[dict] = [
    # === DECRETS ===
    {
        "id": "D-168/2025",
        "codi": "D-168/2025",
        "text": (
            "Decret 168/2025, de 29 de juliol, de gestio de la seguretat viaria "
            "en les infraestructures viaries de la Generalitat de Catalunya"
        ),
        "categoria": "seguretat_viaria",
        "estat": "VIGENT",
        "dogc": "9467",
        "data": "2025-07-31",
        "deroga": "D-190/2016",
        "derogada_per": None,
        "url_pdf": (
            "https://www.icab.es/export/sites/icab/.galleries/documents-noticies/"
            "Decret-168_2025-de-29-de-juliol-de-gestio-de-la-seguretat-viaria-en-les-"
            "infraestructures-viaries-de-la-Generalitat-de-Catalunya.pdf"
        ),
        "observacions": "Transposa Directiva (UE) 2019/1936. Deroga D-190/2016",
    },
    {
        "id": "D-190/2016",
        "codi": "D-190/2016",
        "text": (
            "Decret 190/2016, de 16 de febrer, de gestio de la seguretat viaria "
            "en les infraestructures viaries de la Generalitat de Catalunya"
        ),
        "categoria": "seguretat_viaria",
        "estat": "DEROGADA",
        "dogc": "7064",
        "data": "2016-02-16",
        "deroga": None,
        "derogada_per": "D-168/2025",
        "url_pdf": "",
        "observacions": "Derogat pel Decret 168/2025 des del 31.07.2025",
    },

    # === INSTRUCCIONS DGIM/DGIMT ===
    {
        "id": "DGIM-1-2025",
        "codi": "DGIM/1/2025",
        "text": (
            "Instruccio DGIM/1/2025 sobre la cartografia de detall de Catalunya "
            "en format estandard per a actuacions BIM d'infraestructures"
        ),
        "categoria": "instruccio_tecnica",
        "estat": "VIGENT",
        "dogc": None,
        "data": "2025",
        "deroga": None,
        "derogada_per": None,
        "url_pdf": f"{TERRITORI_BASE}instruccions/2025-01-instruccio-DGIM-cartografia-BIM.pdf",
        "observacions": "Cartografia IFC/BIM per projectes infraestructures DGIM",
    },
    {
        "id": "DGIM-1-2023",
        "codi": "DGIM/1/2023",
        "text": (
            "Instruccio DGIM/1/2023 sobre la codificacio dels estudis i projectes "
            "d'infraestructures impulsats per la Direccio General d'Infraestructures de Mobilitat"
        ),
        "categoria": "instruccio_tecnica",
        "estat": "VIGENT",
        "dogc": None,
        "data": "2023",
        "deroga": None,
        "derogada_per": None,
        "url_pdf": f"{TERRITORI_BASE}instruccions/2023_01_instruccio_DGIM.pdf",
        "url_pdf_alternatius": [
            f"{TERRITORI_BASE}instruccions/instruccio-DGIM-1-2023-codificacio-estudis-projectes.pdf",
            f"{TERRITORI_BASE}instruccions/2023-01-instruccio-DGIM-codificacio.pdf",
            f"{TERRITORI_BASE}instruccions/DGIM_1_2023_codificacio_projectes.pdf",
            f"{TERRITORI_BASE}instruccions/2023_instruccio_DGIM_1_codificacio.pdf",
        ],
        "observacions": "Clau per verificar codificacio dels projectes supervisats per DGIM",
    },
    {
        "id": "DGIM-3-2018",
        "codi": "DGIM/3/2018",
        "text": (
            "Instruccio DGIM/3/2018 sobre la gestio d'incidencies "
            "per despreniments i esllavissades"
        ),
        "categoria": "instruccio_tecnica",
        "estat": "VIGENT",
        "dogc": None,
        "data": "2018",
        "deroga": None,
        "derogada_per": None,
        "url_pdf": f"{TERRITORI_BASE}instruccions/2018_03_instruccio_DGIM.pdf",
        "url_pdf_alternatius": [
            f"{TERRITORI_BASE}instruccions/2018-03-instruccio-DGIM-despreniments.pdf",
        ],
        "observacions": "",
    },
    {
        "id": "DGIM-01-2018",
        "codi": "DGIM/01/2018",
        "text": (
            "Instruccio DGIM/01/2018 sobre la senyalitzacio d'orientacio "
            "en rutes cicloturistiques i vies ciclistes"
        ),
        "categoria": "instruccio_tecnica",
        "estat": "VIGENT",
        "dogc": None,
        "data": "2018",
        "deroga": None,
        "derogada_per": None,
        # URL confirmada per cerca web
        "url_pdf": f"{TERRITORI_BASE}instruccions/2018_01_instruccio_DGIM.pdf",
        "observacions": "",
    },
    {
        "id": "DGIM-3-2017",
        "codi": "DGIM/03/2017",
        "text": (
            "Instruccio DGIM/03/2017 sobre criteris d'abalisament de les obres "
            "a la xarxa de carreteres de la Generalitat de Catalunya"
        ),
        "categoria": "instruccio_tecnica",
        "estat": "VIGENT",
        "dogc": None,
        "data": "2017",
        "deroga": None,
        "derogada_per": None,
        # URL confirmada per cerca web
        "url_pdf": f"{TERRITORI_BASE}instruccions/2017_03_instruccio_DGIM.pdf",
        "observacions": "Nova — no estava al cataleg anterior",
    },
    {
        "id": "DGIM-2-2017",
        "codi": "DGIM/2/2017",
        "text": (
            "Instruccio DGIM/2/2017 sobre l'inventari digital general d'elements "
            "funcionals de la xarxa de carreteres"
        ),
        "categoria": "instruccio_tecnica",
        "estat": "VIGENT",
        "dogc": None,
        "data": "2017",
        "deroga": None,
        "derogada_per": None,
        "url_pdf": f"{TERRITORI_BASE}instruccions/2017_02_instruccio_DGIM.pdf",
        "url_pdf_alternatius": [
            f"{TERRITORI_BASE}instruccions/2017-02-instruccio-DGIM-inventari.pdf",
        ],
        "observacions": "Elements funcionals: senyalitzacio, barreres, etc.",
    },
    {
        "id": "DGIMT-1-2016",
        "codi": "DGIMT/1/2016",
        "text": (
            "Instruccio DGIMT/1/2016 sobre el disseny i la implantacio de separadors "
            "de fluxos de transit en carreteres convencionals de calcada unica"
        ),
        "categoria": "instruccio_tecnica",
        "estat": "VIGENT",
        "dogc": None,
        "data": "2016",
        "deroga": None,
        "derogada_per": None,
        "url_pdf": f"{TERRITORI_BASE}instruccions/2016_01_instruccio_DGIMT.pdf",
        "url_pdf_alternatius": [
            f"{TERRITORI_BASE}instruccions/2016-01-instruccio-DGIMT-separadors.pdf",
            f"{TERRITORI_BASE}instruccions/instruccio_dgimt_1-2016_separadors.pdf",
        ],
        "observacions": "Separadors 2+1 i similars",
    },
    {
        "id": "DGIM-1-2019",
        "codi": "DGIM/1/2019",
        "text": (
            "Instruccio DGIM/1/2019 sobre l'establiment de carreteres convencionals "
            "de doble sentit de circulacio en calcada unica"
        ),
        "categoria": "instruccio_tecnica",
        "estat": "VIGENT",
        "dogc": None,
        "data": "2019",
        "deroga": None,
        "derogada_per": None,
        # URL ja funcionava — no canviar
        "url_pdf": f"{TERRITORI_BASE}instruccions/instruccio_dgim_1-2019_carreteres_2_1.pdf",
        "observacions": "Carreteres 2+1. URL confirmada (ja descarregava OK)",
    },

    # === CIRCULARS ===
    {
        "id": "CIRCULAR-1-2013",
        "codi": "Circular 1/2013 DGIMT",
        "text": (
            "Circular 1/2013 sobre les condicions tecniques i criteris d'implantacio "
            "de la senyalitzacio d'accessos i d'activitats amb acces "
            "a la xarxa de carreteres convencionals"
        ),
        "categoria": "circular_tecnica",
        "estat": "VIGENT",
        "dogc": None,
        "data": "2013",
        "deroga": None,
        "derogada_per": None,
        "url_pdf": f"{TERRITORI_BASE}circulars/2013_01_circular_carreteres.pdf",
        "url_pdf_alternatius": [
            f"{TERRITORI_BASE}circulars/2013_01_circular_DGIMT.pdf",
            f"{TERRITORI_BASE}circulars/2013-01-circular-carreteres-senyalitzacio.pdf",
        ],
        "observacions": "",
    },
    {
        "id": "CIRCULAR-1-2012",
        "codi": "Circular 1/12 DGC",
        "text": (
            "Circular 1/12 de la Direccio General de Carreteres per a l'aplicacio "
            "de l'Estudi basic de seguretat i salut en contractes "
            "d'obres o serveis de conservacio"
        ),
        "categoria": "circular_tecnica",
        "estat": "VIGENT",
        "dogc": None,
        "data": "2012",
        "deroga": None,
        "derogada_per": None,
        "url_pdf": f"{TERRITORI_BASE}circulars/2012_01_circular_carreteres.pdf",
        "url_pdf_alternatius": [
            f"{TERRITORI_BASE}circulars/2012_01_circular_DGC.pdf",
            f"{TERRITORI_BASE}circulars/2012-01-circular-carreteres-ESS.pdf",
        ],
        "observacions": "Rellevant per ESS en contractes de conservacio",
    },
    {
        "id": "CIRCULAR-1-2010",
        "codi": "Circular 1/2010",
        "text": (
            "Circular 1/2010, de 16 de novembre de 2010, sobre criteris d'aplicacio "
            "de barreres de seguretat metalliques en la xarxa de carreteres "
            "de la Generalitat de Catalunya"
        ),
        "categoria": "circular_tecnica",
        "estat": "VIGENT",
        "dogc": None,
        "data": "2010",
        "deroga": None,
        "derogada_per": None,
        "url_pdf": f"{TERRITORI_BASE}circulars/2010_01_circular_carreteres.pdf",
        "url_pdf_alternatius": [
            f"{TERRITORI_BASE}circulars/2010_01_circular_DGC.pdf",
            f"{TERRITORI_BASE}circulars/2010-01-circular-carreteres-barreres.pdf",
        ],
        "observacions": "Nova — no estava al cataleg anterior. Barreres de seguretat metalliques",
    },
    {
        "id": "CIRCULAR-1-2009",
        "codi": "Circular 1/2009",
        "text": (
            "Circular 1/2009, de 15 de maig de 2009, sobre l'adaptacio a les normes "
            "europees harmonitzades en materia de mescles bituminoses en calent"
        ),
        "categoria": "circular_tecnica",
        "estat": "VIGENT",
        "dogc": None,
        "data": "2009",
        "deroga": None,
        "derogada_per": None,
        "url_pdf": f"{TERRITORI_BASE}circulars/2009_01_circular_carreteres.pdf",
        "url_pdf_alternatius": [
            f"{TERRITORI_BASE}circulars/2009_01_circular_DGC.pdf",
            f"{TERRITORI_BASE}circulars/2009-01-circular-carreteres-mescles.pdf",
        ],
        "observacions": "Mescles bituminoses en calent. Normes europees harmonitzades",
    },
    {
        "id": "CIRCULAR-3-2005",
        "codi": "Circular 3/2005",
        "text": (
            "Circular 3/2005 sobre les especificacions tecniques "
            "per l'equipament de tunels d'obres de carretera"
        ),
        "categoria": "circular_tecnica",
        "estat": "VIGENT",
        "dogc": None,
        "data": "2005",
        "deroga": None,
        "derogada_per": None,
        "url_pdf": f"{TERRITORI_BASE}circulars/2005_03_circular_carreteres.pdf",
        "url_pdf_alternatius": [
            f"{TERRITORI_BASE}circulars/2005_03_circular_DGC.pdf",
        ],
        "observacions": "",
    },
    {
        "id": "CIRCULAR-2-2005",
        "codi": "Circular 2/2005",
        "text": (
            "Circular 2/2005 sobre les condicions d'implantacio d'elements "
            "reductors de la velocitat en travesseres urbanes"
        ),
        "categoria": "circular_tecnica",
        "estat": "VIGENT",
        "dogc": None,
        "data": "2005",
        "deroga": None,
        "derogada_per": None,
        "url_pdf": f"{TERRITORI_BASE}circulars/2005_02_circular_carreteres.pdf",
        "url_pdf_alternatius": [
            f"{TERRITORI_BASE}circulars/2005_02_circular_DGC.pdf",
        ],
        "observacions": "",
    },
]


# ─── PDF download helpers ──────────────────────────────────────────────────────

def _safe_filename(doc_id: str) -> str:
    """Convert doc id to a safe filename."""
    return re.sub(r"[/\\:*?\"<>|]", "-", doc_id) + ".pdf"


def _alternate_urls(doc: dict) -> list[str]:
    """Generate fallback URL patterns for a document."""
    doc_id   = doc["id"].lower()
    slug     = doc_id.replace("/", "_").replace("-", "_")
    slug_sl  = doc_id.replace("-", "/")
    codi     = doc.get("codi", "").lower()
    codi_sl  = re.sub(r"[^a-z0-9/]", "_", codi)
    cat      = doc.get("categoria", "")
    subdir   = "circulars" if "circular" in cat else "instruccions"
    other    = "instruccions" if subdir == "circulars" else "circulars"

    candidates = [
        f"{TERRITORI_BASE}{subdir}/{slug}.pdf",
        f"{TERRITORI_BASE}{subdir}/{codi_sl}.pdf",
        f"{TERRITORI_BASE}{other}/{slug}.pdf",
        f"{TERRITORI_BASE}{subdir}/{slug_sl}.pdf",
    ]
    # Remove duplicates while preserving order
    seen:  set[str]  = set()
    out:   list[str] = []
    for u in candidates:
        if u and u not in seen and u != doc.get("url_pdf", ""):
            seen.add(u)
            out.append(u)
    return out


def _try_download(session: requests.Session, url: str) -> bytes | None:
    """
    Try to GET a URL. Returns raw bytes if response looks like a PDF,
    None otherwise. Handles 404/503 gracefully.
    """
    try:
        r = session.get(url, headers=HEADERS, timeout=30, allow_redirects=True)
        if r.status_code == 404:
            return None
        if r.status_code in (429, 503, 502):
            print(f"      [{r.status_code}] waiting 10s...", end="", flush=True)
            time.sleep(10)
            r = session.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            return None
        ct = r.headers.get("content-type", "").lower()
        if "html" in ct and b"%PDF" not in r.content[:200]:
            return None
        return r.content
    except Exception as exc:
        print(f"      [ERR] {exc}")
        return None


def _is_valid_pdf(content: bytes) -> bool:
    return content[:4] == b"%PDF"


# ─── PDF metadata extraction ──────────────────────────────────────────────────

_DATE_RE = re.compile(
    r"\b(\d{1,2})\s+de\s+(gener|febrer|marc|abril|maig|juny|juliol|agost|"
    r"setembre|octubre|novembre|desembre|enero|febrero|marzo|abril|mayo|"
    r"junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+de\s+(\d{4})\b",
    re.IGNORECASE,
)
_DOGC_RE = re.compile(r"DOGC\s+(?:num\.?\s*)?(\d{4,5})", re.IGNORECASE)


def _extract_pdf_meta(pdf_bytes: bytes) -> dict:
    """Extract title, date, DOGC number from first 2 pages via PyMuPDF."""
    meta: dict = {"pdf_title": "", "pdf_date_found": "", "pdf_dogc": ""}
    if not PYMUPDF_OK:
        return meta
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        text_pages = ""
        for i in range(min(2, len(doc))):
            text_pages += doc[i].get_text()
        doc.close()

        # Title: first non-empty line longer than 20 chars
        for line in text_pages.splitlines():
            line = line.strip()
            if len(line) > 20:
                meta["pdf_title"] = line[:200]
                break

        # Date
        m = _DATE_RE.search(text_pages)
        if m:
            meta["pdf_date_found"] = f"{m.group(1)} de {m.group(2)} de {m.group(3)}"

        # DOGC number
        m = _DOGC_RE.search(text_pages)
        if m:
            meta["pdf_dogc"] = m.group(1)
    except Exception as exc:
        print(f"      [WARN] PyMuPDF error: {exc}")
    return meta


# ─── Per-document processing ──────────────────────────────────────────────────

def process_doc(session: requests.Session, doc: dict) -> dict:
    """
    Try to download and validate the PDF for one document.
    Returns an enriched copy of doc with pdf_ok, pdf_local, pdf_title, etc.
    """
    entry = dict(doc)
    entry.setdefault("pdf_ok",        False)
    entry.setdefault("pdf_local",     "")
    entry.setdefault("pdf_title",     "")
    entry.setdefault("pdf_date_found","")
    entry.setdefault("pdf_dogc",      "")

    safe_name   = _safe_filename(doc["id"])
    local_path  = os.path.join(PDF_DIR, safe_name)
    rel_path    = os.path.join(OUTPUT_DIR, "pdfs", safe_name).replace("\\", "/")

    # If already downloaded and valid, skip network
    if os.path.exists(local_path) and os.path.getsize(local_path) > 500:
        with open(local_path, "rb") as f:
            first = f.read(4)
        if first == b"%PDF":
            print(f"  [CACHE] {doc['id']}")
            entry["pdf_ok"]    = True
            entry["pdf_local"] = rel_path
            if PYMUPDF_OK:
                with open(local_path, "rb") as f:
                    entry.update(_extract_pdf_meta(f.read()))
            return entry

    # No URL — skip download
    if not doc.get("url_pdf"):
        print(f"  [SKIP]  {doc['id']} (no url_pdf)")
        return entry

    # Build URL list: primary + explicit alternates + auto-generated fallbacks
    urls_to_try = []
    if doc.get("url_pdf"):
        urls_to_try.append(doc["url_pdf"])
    urls_to_try.extend(doc.get("url_pdf_alternatius") or [])
    urls_to_try.extend(_alternate_urls(doc))

    content: bytes | None = None
    used_url = ""
    for url in urls_to_try:
        print(f"  [TRY]   {doc['id']} <- {url[:80]}", flush=True)
        content = _try_download(session, url)
        if content and _is_valid_pdf(content):
            used_url = url
            break
        if content:
            print(f"      [WARN] Response is not a valid PDF (magic bytes check failed)")
            content = None
        time.sleep(0.4)

    if not content:
        print(f"  [FAIL]  {doc['id']} — PDF not found after {len(urls_to_try)} attempts")
        return entry

    # Save PDF
    os.makedirs(PDF_DIR, exist_ok=True)
    with open(local_path, "wb") as f:
        f.write(content)

    entry["pdf_ok"]    = True
    entry["pdf_local"] = rel_path
    if used_url != doc["url_pdf"]:
        entry["url_pdf"] = used_url  # update to working URL

    # Extract PDF metadata
    pdf_meta = _extract_pdf_meta(content)
    entry.update(pdf_meta)

    print(f"  [OK]    {doc['id']}  ({len(content)//1024} KB)"
          + (f"  title={entry['pdf_title'][:50]}" if entry.get("pdf_title") else ""))
    return entry


# ─── Merge into normativa_annexes.json ────────────────────────────────────────

def merge_into_annexes(catalog: list[dict], annexes_path: str = ANNEXES_PATH) -> None:
    """
    Idempotent: adds DEROGADA entries from catalog to normativa_annexes.json.
    Never modifies VIGENT entries. Makes a .bak backup before writing.
    """
    if not os.path.exists(annexes_path):
        print(f"  [SKIP] {annexes_path} not found")
        return

    with open(annexes_path, encoding="utf-8") as f:
        data = json.load(f)

    derogades: list[dict] = data.get("normativa_derogada", [])
    existing = {e.get("codi", "").upper() for e in derogades}

    added = 0
    for entry in catalog:
        if entry.get("estat") != "DEROGADA":
            continue
        codi = entry.get("codi") or entry.get("id", "")
        if not codi or codi.upper() in existing:
            continue

        derog_per = entry.get("derogada_per") or ""
        if derog_per:
            data_entry = entry.get("data", "")
            # Try to add date context to derogada_per
            # e.g. "D-168/2025" -> find the deroga entry for date
            derog_entry = next(
                (d for d in catalog if d.get("id") == derog_per or d.get("codi") == derog_per),
                None,
            )
            if derog_entry and derog_entry.get("data"):
                derog_per = f"{derog_per} ({derog_entry['data']})"

        new_entry: dict = {
            "codi":        codi,
            "text":        (entry.get("text") or "")[:200],
            "derogada_per": derog_per,
            "observacions": (
                f"{entry.get('observacions', '')} "
                f"Font: Departament de Territori (DGIM/DGIMT)."
            ).strip(),
        }
        derogades.append(new_entry)
        existing.add(codi.upper())
        added += 1

    if added == 0:
        print("  normativa_annexes.json: cap nova entrada derogada")
        return

    data["normativa_derogada"] = derogades
    bak = annexes_path + ".bak"
    shutil.copy2(annexes_path, bak)

    with open(annexes_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"  normativa_annexes.json actualitzat: +{added} derogades  (backup: {bak})")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main(output_dir: str = OUTPUT_DIR) -> None:
    global OUTPUT_DIR, CATALOG_DIR, CATALOG_PATH, PDF_DIR
    if output_dir != OUTPUT_DIR:
        OUTPUT_DIR   = output_dir
        CATALOG_DIR  = os.path.join(output_dir, "_catalogo")
        CATALOG_PATH = os.path.join(CATALOG_DIR, "catalogo_territori.json")
        PDF_DIR      = os.path.join(output_dir, "pdfs")

    print("=== Territori Gencat — Catalog Builder ===")
    print(f"  PyMuPDF: {'disponible' if PYMUPDF_OK else 'NO (pip install pymupdf)'}")
    print(f"  Output:  {CATALOG_PATH}\n")

    os.makedirs(CATALOG_DIR, exist_ok=True)
    os.makedirs(PDF_DIR, exist_ok=True)

    session = requests.Session()

    catalog: list[dict] = []
    ok_count   = 0
    fail_count = 0

    for doc in PRIORITY_DOCS:
        entry = process_doc(session, doc)
        catalog.append(entry)
        if entry["pdf_ok"]:
            ok_count += 1
        elif doc.get("url_pdf"):   # had a URL but failed
            fail_count += 1
        time.sleep(DELAY)

    # Save catalog
    with open(CATALOG_PATH, "w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)

    vigents  = sum(1 for e in catalog if e.get("estat") == "VIGENT")
    derog    = sum(1 for e in catalog if e.get("estat") == "DEROGADA")

    cats: dict[str, int] = {}
    for e in catalog:
        cats[e.get("categoria", "altre")] = cats.get(e.get("categoria", "altre"), 0) + 1

    print(f"\n{'='*55}")
    print(f"  Total documents:  {len(catalog)}")
    print(f"  Vigents:          {vigents}")
    print(f"  Derogades:        {derog}")
    print(f"  PDFs descarregats:{ok_count}")
    print(f"  PDFs fallits:     {fail_count}")
    for cat, n in sorted(cats.items()):
        print(f"    [{cat}]  {n}")
    print(f"  Cataleg:          {CATALOG_PATH}")
    print(f"{'='*55}\n")

    merge_into_annexes(catalog)


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else OUTPUT_DIR
    main(out)
