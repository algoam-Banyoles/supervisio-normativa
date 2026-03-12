"""
boe_scraper.py — Fetches normativa from BOE OpenData REST API
for public contracts (CONTRATACION PUBLICA) and state roads (CARRETERAS).

API base: https://boe.es/datosabiertos/api
No authentication required.

Endpoints used:
  GET /legislacion-consolidada/id/{id}/metadatos
  GET /legislacion-consolidada?materia={mat}&estado=1&offset={n}&limite=50

Output: normativa_boe/_catalogo/catalogo_boe.json

Usage:
    python boe_scraper.py [output_dir]   (default: normativa_boe)
"""

from __future__ import annotations

import io
import json
import os
import sys
import time

# UTF-8 on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─── Constants ────────────────────────────────────────────────────────────────

API_BASE     = "https://boe.es/datosabiertos/api"
BOE_BASE     = "https://www.boe.es"
OUTPUT_DIR   = "normativa_boe"
CATALOG_DIR  = os.path.join(OUTPUT_DIR, "_catalogo")
CATALOG_PATH = os.path.join(CATALOG_DIR, "catalogo_boe.json")

DELAY        = 1.0      # seconds between requests
PAGE_SIZE    = 50
MAX_RETRIES  = 3

# Priority document IDs — always fetched individually
PRIORITY_IDS = [
    "BOE-A-2017-12902",   # Ley 9/2017 LCSP
    "BOE-A-2011-17887",   # RDL 3/2011 TRLCSP (derogada)
    "BOE-A-2001-19995",   # RD 1098/2001 Reglament LCAP (parcialment vigent)
    "BOE-A-2019-15790",   # RDL 14/2019 mesures urgents contractacio
    "BOE-A-1988-18937",   # Ley 25/1988 de Carreteras
    "BOE-A-1994-28285",   # RD 1812/1994 Reglamento General de Carreteras
]

# Thematic searches: (materia_param, categoria_label)
THEMATIC_SEARCHES = [
    ("CONTRATACION+PUBLICA", "contractes"),
    ("CARRETERAS",           "carreteres_estat"),
]

HEADERS = {
    "Accept":          "application/json",
    "Accept-Language": "es-ES,es;q=0.9",
    "User-Agent":      (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}


# ─── Session with retry ────────────────────────────────────────────────────────

def make_session() -> requests.Session:
    """Create a requests session with exponential backoff retry."""
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=2,           # 2, 4, 8 seconds
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    return session


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _get(session: requests.Session, url: str) -> dict | None:
    """GET JSON from API with retry; return None on 404 or error."""
    try:
        r = session.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 404:
            print(f"  [WARNING] 404 -> {url}")
            return None
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        print(f"  [WARNING] GET {url} -> {exc}")
        return None


def _classify_categoria(materias: list[str]) -> str:
    s = " ".join(m.upper() for m in materias)
    if "CONTRATACION" in s or "CONTRATOS" in s or "LICITACION" in s:
        return "contractes"
    if "CARRETERA" in s or "AUTOPISTA" in s or "AUTOVIA" in s:
        return "carreteres_estat"
    return "altres"


def _classify_estat(meta: dict) -> str:
    if meta.get("fecha_anulacion") or meta.get("derogada"):
        return "DEROGADA"
    estado = str(meta.get("estado", "")).upper()
    if estado in ("VI", "VIGENTE", "1", "FINALIZADO"):   # "Finalizado" = consolidation complete
        return "VIGENT"
    if estado in ("AN", "ANULADA", "0"):
        return "DEROGADA"
    return "PENDENT"


def _extract_derogada_per(refs: list[dict]) -> str:
    for r in refs:
        rel = str(r.get("relacion", "")).upper()
        if "DEROGA" in rel or "ANULA" in rel:
            return r.get("id", "") or r.get("titulo", "")
    return ""


def _extract_pdf_url(meta: dict) -> str:
    doc_id = meta.get("id", "")
    # Try explicit pdf URL from API
    if meta.get("url_pdf"):
        url = meta["url_pdf"]
        return url if url.startswith("http") else f"{BOE_BASE}{url}"
    # Build consolidated PDF URL from document ID pattern BOE-A-{year}-{num}
    if doc_id and doc_id.startswith("BOE-A-"):
        parts = doc_id.split("-")
        if len(parts) >= 3:
            year = parts[2]
            return f"{BOE_BASE}/buscar/pdf/{year}/{doc_id}-consolidado.pdf"
    # Fallback: HTML viewer page
    if doc_id:
        return f"{BOE_BASE}/buscar/act.php?id={doc_id}"
    return ""


def _build_entry(meta: dict, categoria: str) -> dict:
    doc_id = (
        meta.get("id") or
        meta.get("identificador") or
        meta.get("idLeg") or ""
    )
    titulo = (
        meta.get("titulo") or
        meta.get("denominacion") or ""
    )
    if isinstance(titulo, dict):
        titulo = titulo.get("#text", "") or titulo.get("$", "") or ""

    departament = (
        meta.get("departamento") or
        meta.get("emisor") or
        meta.get("organo") or ""
    )
    if isinstance(departament, dict):
        departament = departament.get("nombre", "") or departament.get("#text", "")

    materias_raw = meta.get("materias") or meta.get("materia") or []
    if isinstance(materias_raw, str):
        materias_raw = [materias_raw]
    if isinstance(materias_raw, dict):
        materias_raw = list(materias_raw.values())
    materias = [str(m).strip() for m in materias_raw if m]

    fecha = (
        meta.get("fecha_actualizacion") or
        meta.get("fecha_publicacion") or
        meta.get("fecha_disposicion") or ""
    )
    if isinstance(fecha, dict):
        fecha = fecha.get("$", "") or fecha.get("#text", "") or ""

    url_boe = meta.get("url_boe") or meta.get("url") or ""
    if url_boe and not url_boe.startswith("http"):
        url_boe = f"{BOE_BASE}{url_boe}"
    if not url_boe and doc_id:
        url_boe = f"{BOE_BASE}/buscar/act.php?id={doc_id}"

    url_pdf = _extract_pdf_url(meta)
    estat   = _classify_estat(meta)

    refs = meta.get("referencias") or []
    if isinstance(refs, dict):
        refs = refs.get("referencia", [])
    if isinstance(refs, dict):
        refs = [refs]
    derogada_per = _extract_derogada_per(refs) if estat == "DEROGADA" else ""

    if not categoria:
        categoria = _classify_categoria(materias)

    return {
        "id":                 doc_id,
        "codi":               meta.get("codi") or meta.get("numero") or doc_id,
        "text":               str(titulo).strip()[:300],
        "categoria":          categoria,
        "estat":              estat,
        "data_actualizacion": str(fecha).strip(),
        "url_boe":            url_boe,
        "url_pdf":            url_pdf,
        "departament":        str(departament).strip()[:200],
        "materias":           materias,
        "derogada_per":       derogada_per,
        "observacions":       str(meta.get("observaciones") or meta.get("notas") or "").strip(),
        "font":               "BOE OpenData API",
    }


def _save_incremental(catalog: list[dict], path: str) -> None:
    """Write catalog to JSON (incremental save after each batch)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)


# ─── Fetch single document ─────────────────────────────────────────────────────

def fetch_by_id(session: requests.Session, doc_id: str, categoria: str = "") -> dict | None:
    url  = f"{API_BASE}/legislacion-consolidada/id/{doc_id}/metadatos"
    data = _get(session, url)
    if not data:
        return None

    meta = (
        data.get("data") or
        data.get("metadatos") or
        data.get("legislacion") or
        data
    )
    if isinstance(meta, list) and meta:
        meta = meta[0]
    if not isinstance(meta, dict):
        return None

    if not meta.get("id"):
        meta["id"] = doc_id

    return _build_entry(meta, categoria)


# ─── Thematic search (paginated) ──────────────────────────────────────────────

def search_thematic(
    session:   requests.Session,
    materia:   str,
    categoria: str,
    catalog:   list[dict],
    seen_ids:  set[str],
    save_path: str,
) -> int:
    """
    Paginate through /legislacion-consolidada?materia=...&estado=1.
    Saves incrementally every PAGE_SIZE items.
    Returns count of new entries added.
    """
    added  = 0
    offset = 0

    while True:
        url  = (
            f"{API_BASE}/legislacion-consolidada"
            f"?materia={materia}&estado=1"
            f"&offset={offset}&limite={PAGE_SIZE}"
        )
        data = _get(session, url)
        if not data:
            break

        items = (
            data.get("data") or
            data.get("items") or
            data.get("legislacion") or
            data.get("result") or
            []
        )
        if isinstance(items, dict):
            items = (
                items.get("item") or
                items.get("items") or
                (list(items.values())[0] if items else [])
            )
        if not isinstance(items, list) or not items:
            break

        batch_new = 0
        for raw in items:
            if not isinstance(raw, dict):
                continue
            doc_id = (
                raw.get("id") or
                raw.get("identificador") or
                raw.get("idLeg") or ""
            )
            if doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)
            entry = _build_entry(raw, categoria)
            catalog.append(entry)
            added     += 1
            batch_new += 1

        page = offset // PAGE_SIZE + 1
        print(f"    pagina {page}: {batch_new} noves (total {added})", flush=True)

        # Warn on first page if suspiciously few results (estado=1 filter may be ignored)
        if offset == 0 and len(items) < 5:
            print(f"  [WARNING] Possible filtre estado=1 no reconegut — "
                  f"rebuts {len(items)} resultats. Prova sense filtre si el total sembla baix.")

        # Incremental save after each page
        _save_incremental(catalog, save_path)

        if len(items) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(DELAY)

    return added


# ─── Merge into normativa_annexes.json ────────────────────────────────────────

def merge_into_annexes(catalog: list[dict], annexes_path: str = "normativa_annexes.json") -> None:
    """Add newly discovered DEROGADA entries to normativa_annexes.json."""
    if not os.path.exists(annexes_path):
        print(f"  [INFO] {annexes_path} no trobat, omitint merge")
        return

    import shutil
    backup = annexes_path + ".bak"
    shutil.copy2(annexes_path, backup)

    with open(annexes_path, encoding="utf-8") as f:
        annexes = json.load(f)

    existing_derogada = annexes.get("normativa_derogada", [])
    existing_codis = {e.get("codi", "") for e in existing_derogada}

    added = 0
    for entry in catalog:
        if entry["estat"] != "DEROGADA":
            continue
        codi = entry.get("id", "") or entry.get("codi", "")
        if codi in existing_codis:
            continue
        existing_derogada.append({
            "codi": codi,
            "text": entry.get("text", "")[:200],
            "derogada_per": entry.get("derogada_per", ""),
            "observacions": f"Font: BOE OpenData API. {entry.get('observacions','')}".strip()
        })
        existing_codis.add(codi)
        added += 1

    annexes["normativa_derogada"] = existing_derogada
    with open(annexes_path, "w", encoding="utf-8") as f:
        json.dump(annexes, f, ensure_ascii=False, indent=2)

    print(f"  normativa_annexes.json actualitzat: +{added} entrades derogades (backup: {backup})")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main(output_dir: str = OUTPUT_DIR) -> None:
    global CATALOG_PATH, CATALOG_DIR
    if output_dir != OUTPUT_DIR:
        CATALOG_DIR  = os.path.join(output_dir, "_catalogo")
        CATALOG_PATH = os.path.join(CATALOG_DIR, "catalogo_boe.json")

    print("=== BOE Catalog Builder (OpenData API) ===")

    session  = make_session()
    catalog:  list[dict] = []
    seen_ids: set[str]   = set()

    # 1) Priority IDs
    print("\n[1/3] Prioritat: documents individuals")
    for doc_id in PRIORITY_IDS:
        if doc_id in (
            "BOE-A-2017-12902",   # Ley 9/2017 LCSP
            "BOE-A-2011-17887",   # RDL 3/2011 TRLCSP
            "BOE-A-2001-19995",   # RD 1098/2001 Reglament LCAP
            "BOE-A-2019-15790",   # RDL 14/2019 mesures urgents contractacio
        ):
            cat = "contractes"
        else:
            cat = "carreteres_estat"

        entry = fetch_by_id(session, doc_id, cat)
        if entry and entry["id"] not in seen_ids:
            seen_ids.add(entry["id"])
            catalog.append(entry)
            print(f"  [OK] {doc_id} -> {entry['estat']}: {entry['text'][:70]}")
        else:
            print(f"  [WARNING] {doc_id} -> no result / ja existent")
        time.sleep(DELAY)

    _save_incremental(catalog, CATALOG_PATH)

    # 2) Thematic searches
    print("\n[2/3] Cerca tematica")
    for materia, categoria in THEMATIC_SEARCHES:
        print(f"\n  Buscant materia={materia} ({categoria})...")
        n_new = search_thematic(
            session, materia, categoria,
            catalog, seen_ids, CATALOG_PATH,
        )
        print(f"  -> {n_new} noves entrades per {materia}")
        time.sleep(DELAY)

    # 3) Enrich derogated entries that are missing derogada_per
    print("\n[3/3] Enriquint entrades derogades sense successor...")
    enriched = 0
    for entry in catalog:
        if entry["estat"] == "DEROGADA" and not entry["derogada_per"] and entry["id"]:
            data = _get(session, f"{API_BASE}/legislacion-consolidada/id/{entry['id']}/metadatos")
            if data:
                meta = data.get("data") or data.get("metadatos") or data
                if isinstance(meta, dict):
                    refs = meta.get("referencias") or []
                    if isinstance(refs, dict):
                        refs = refs.get("referencia", [])
                    if isinstance(refs, dict):
                        refs = [refs]
                    dp = _extract_derogada_per(refs)
                    if dp:
                        entry["derogada_per"] = dp
                        enriched += 1
            time.sleep(DELAY / 2)

    if enriched:
        print(f"  {enriched} entrades enriquides amb derogada_per")
        _save_incremental(catalog, CATALOG_PATH)

    # Summary table
    def _count(cat: str, estat: str) -> int:
        return sum(1 for e in catalog if e["categoria"] == cat and e["estat"] == estat)

    cv = _count("contractes",      "VIGENT")
    cd = _count("contractes",      "DEROGADA")
    rv = _count("carreteres_estat", "VIGENT")
    rd = _count("carreteres_estat", "DEROGADA")
    av = _count("altres",           "VIGENT")
    ad = _count("altres",           "DEROGADA")

    total_vigent = sum(1 for e in catalog if e["estat"] == "VIGENT")
    total_derog  = sum(1 for e in catalog if e["estat"] == "DEROGADA")
    total_pend   = sum(1 for e in catalog if e["estat"] == "PENDENT")

    print(f"\n{'='*55}")
    print(f"  CONTRACTES:  {cv} vigents, {cd} derogades")
    print(f"  CARRETERES:  {rv} vigents, {rd} derogades")
    print(f"  ALTRES:      {av} vigents, {ad} derogades")
    print(f"{'='*55}")
    print(f"  Total:       {len(catalog):,}  "
          f"({total_vigent} vigents | {total_derog} derogades | {total_pend} pendents)")
    print(f"  Cataleg:     {CATALOG_PATH}")

    print("\n[Extra] Sincronitzant derogades amb normativa_annexes.json...")
    merge_into_annexes(catalog)


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else OUTPUT_DIR
    main(out)
