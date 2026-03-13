"""
download_missing_pdfs.py
Descarrega els PDFs que falten en tots els catàlegs JSON del projecte.

Ús:
    python download_missing_pdfs.py [base_dir]
"""

import json
import os
import re
import sys
import time
from datetime import date

import requests
from bs4 import BeautifulSoup

# ── Configuració ───────────────────────────────────────────────────────────────

DELAY = 1.5

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":          "application/pdf,text/html,*/*;q=0.8",
    "Accept-Language": "ca-ES,ca;q=0.9,es;q=0.7",
}

CATALOG_CONFIGS = [
    {
        "name":    "BOE",
        "json":    "normativa_boe/_catalogo/catalogo_boe.json",
        "pdf_dir": "normativa_boe/pdfs",
        "id_key":  "id",
        "url_key": "url_pdf",
        "eli_key": None,
    },
    {
        "name":    "INDUSTRIA",
        "json":    "normativa_industria/_catalogo/catalogo_industria.json",
        "pdf_dir": "normativa_industria/pdfs",
        "id_key":  "boe_id",
        "url_key": "url_pdf",
        "eli_key": None,
    },
    {
        "name":    "TERRITORI",
        "json":    "normativa_territori/_catalogo/catalogo_territori.json",
        "pdf_dir": "normativa_territori/pdfs",
        "id_key":  "id",
        "url_key": "url_pdf",
        "eli_key": None,
    },
    {
        "name":    "PJCAT",
        "json":    "normativa_pjcat/_catalogo/catalogo_pjcat.json",
        "pdf_dir": "normativa_pjcat/pdfs",
        "id_key":  "id",
        "url_key": None,
        "eli_key": "eli",
    },
]

# Catàlegs amb JSON que possiblement és un dict wrappat (no llista plana)
WRAPPED_CONFIGS = [
    {
        "name":    "CTE",
        "json":    "normativa_cte/_catalogo/catalogo_cte.json",
        "pdf_dir": "normativa_cte/pdfs",
    },
    {
        "name":    "ERA",
        "json":    "normativa_era/_catalogo/catalogo_era.json",
        "pdf_dir": "normativa_era/pdfs",
    },
]

_PDF_URL_RE = re.compile(r"\.pdf(\?|#|$)", re.IGNORECASE)


# ── Sessió HTTP ────────────────────────────────────────────────────────────────

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    adapter = requests.adapters.HTTPAdapter(max_retries=3)
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    return s


# ── Helpers ────────────────────────────────────────────────────────────────────

def _safe_filename(id_val: str) -> str:
    return re.sub(r"[^\w\-.]", "_", str(id_val)) + ".pdf"


def _load_json(path: str):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _save_json(data, path: str) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)


def _resolve_eli(url: str, session: requests.Session) -> str | None:
    """GET l'URL ELI i busca el primer link a un PDF al HTML."""
    try:
        r = session.get(url, timeout=20, allow_redirects=True)
        r.raise_for_status()
    except requests.RequestException:
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if _PDF_URL_RE.search(href):
            if href.startswith("http"):
                return href
            # URL relativa → absoluta
            from urllib.parse import urljoin
            return urljoin(url, href)
    return None


def _download_pdf(url: str, dest_path: str, session: requests.Session) -> bool:
    """Descarrega el PDF a dest_path. Retorna True si ha reeixit."""
    try:
        r = session.get(url, timeout=60, stream=True)
        r.raise_for_status()
        content = r.content
        if not content[:4] == b"%PDF":
            return False
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        with open(dest_path, "wb") as fh:
            fh.write(content)
        return True
    except requests.RequestException:
        return False


# ── Processament d'un catàleg ─────────────────────────────────────────────────

def _process_catalog(
    entries: list,
    name: str,
    json_path: str,
    pdf_dir: str,
    id_key: str,
    url_key: str | None,
    eli_key: str | None,
    base_dir: str,
    session: requests.Session,
) -> dict:
    """
    Processa totes les entrades d'un catàleg.
    Retorna les estadístiques i modifica les entrades al lloc.
    """
    abs_pdf_dir = os.path.join(base_dir, pdf_dir)
    log_entries = []

    stats = {"descarregats": 0, "ja_existien": 0, "sense_pdf": 0, "errors": 0}

    for i, entry in enumerate(entries):
        id_val   = entry.get(id_key) or str(i)
        filename = _safe_filename(id_val)
        dest     = os.path.join(abs_pdf_dir, filename)
        rel_path = os.path.join(pdf_dir, filename).replace("\\", "/")

        # ── Ja existeix? ──────────────────────────────────────────────────
        if os.path.exists(dest) and os.path.getsize(dest) > 1000:
            entry["path_local"]      = rel_path
            entry["pdf_descarregat"] = True
            stats["ja_existien"]    += 1
            log_entries.append({"id": id_val, "estat": "ja_existia"})
            continue

        # ── Obté URL del PDF ──────────────────────────────────────────────
        pdf_url: str | None = None

        if url_key:
            pdf_url = entry.get(url_key) or None
            if pdf_url:
                pdf_url = pdf_url.strip() or None

        if not pdf_url and eli_key:
            eli = (entry.get(eli_key) or "").strip()
            if eli:
                time.sleep(DELAY)
                pdf_url = _resolve_eli(eli, session)

        if not pdf_url:
            entry["pdf_descarregat"] = False
            stats["sense_pdf"]      += 1
            log_entries.append({"id": id_val, "estat": "sense_pdf"})
            continue

        # ── Descarrega ────────────────────────────────────────────────────
        time.sleep(DELAY)
        ok = _download_pdf(pdf_url, dest, session)
        if ok:
            entry["path_local"]      = rel_path
            entry["pdf_descarregat"] = True
            stats["descarregats"]   += 1
            log_entries.append({"id": id_val, "estat": "descarregat", "url": pdf_url})
        else:
            entry["pdf_descarregat"] = False
            stats["errors"]         += 1
            log_entries.append({"id": id_val, "estat": "error", "url": pdf_url})

    # ── Desa JSON actualitzat ─────────────────────────────────────────────
    _save_json(entries, os.path.join(base_dir, json_path))

    # ── Desa log ──────────────────────────────────────────────────────────
    log = {
        "data":    str(date.today()),
        "cataleg": name,
        "stats":   stats,
        "entries": log_entries,
    }
    log_name = f"download_log_{date.today().strftime('%Y%m%d')}.json"
    log_path = os.path.join(base_dir, os.path.dirname(json_path), log_name)
    _save_json(log, log_path)

    return stats


# ── Resolució d'entrades d'un JSON wrappat ────────────────────────────────────

def _unwrap_entries(data) -> list | None:
    """Extreu la llista de documents d'un JSON pla o wrappat."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key, val in data.items():
            if isinstance(val, list) and val and isinstance(val[0], dict):
                return val
    return None


# ── Orquestrador principal ────────────────────────────────────────────────────

def download_all(base_dir: str = ".") -> None:
    session = _make_session()

    totals = {"descarregats": 0, "ja_existien": 0, "sense_pdf": 0, "errors": 0}

    # ── Catàlegs estàndard ────────────────────────────────────────────────
    for cfg in CATALOG_CONFIGS:
        json_path = os.path.join(base_dir, cfg["json"])
        if not os.path.exists(json_path):
            print(f"[{cfg['name']}] JSON no trobat: {cfg['json']} — skip")
            continue

        data = _load_json(json_path)
        entries = _unwrap_entries(data)
        if entries is None:
            print(f"[{cfg['name']}] No s'ha pogut extreure llista d'entrades — skip")
            continue

        print(f"[{cfg['name']}] {len(entries)} entrades...")
        stats = _process_catalog(
            entries    = entries,
            name       = cfg["name"],
            json_path  = cfg["json"],
            pdf_dir    = cfg["pdf_dir"],
            id_key     = cfg["id_key"],
            url_key    = cfg.get("url_key"),
            eli_key    = cfg.get("eli_key"),
            base_dir   = base_dir,
            session    = session,
        )
        print(
            f"  descarregats: {stats['descarregats']} | "
            f"ja existien: {stats['ja_existien']} | "
            f"sense_pdf: {stats['sense_pdf']} | "
            f"errors: {stats['errors']}"
        )
        for k in totals:
            totals[k] += stats.get(k, 0)

    # ── Catàlegs wrappats ─────────────────────────────────────────────────
    for cfg in WRAPPED_CONFIGS:
        json_path = os.path.join(base_dir, cfg["json"])
        if not os.path.exists(json_path):
            print(f"[{cfg['name']}] JSON no trobat: {cfg['json']} — skip")
            continue

        data = _load_json(json_path)
        entries = _unwrap_entries(data)
        if entries is None:
            print(f"[{cfg['name']}] No s'ha pogut extreure llista d'entrades — skip")
            continue

        # Filtra entrades amb url_pdf
        with_url = [e for e in entries if isinstance(e, dict) and e.get("url_pdf")]
        if not with_url:
            print(f"[{cfg['name']}] Cap entrada amb url_pdf — skip")
            continue

        print(f"[{cfg['name']}] {len(with_url)}/{len(entries)} entrades amb url_pdf...")

        # id_key: intenta "id", "codi", index
        id_key = "id" if any("id" in e for e in with_url[:3]) else "codi"
        stats = _process_catalog(
            entries    = entries,          # passa la llista completa per desar correctament
            name       = cfg["name"],
            json_path  = cfg["json"],
            pdf_dir    = cfg["pdf_dir"],
            id_key     = id_key,
            url_key    = "url_pdf",
            eli_key    = None,
            base_dir   = base_dir,
            session    = session,
        )
        print(
            f"  descarregats: {stats['descarregats']} | "
            f"ja existien: {stats['ja_existien']} | "
            f"sense_pdf: {stats['sense_pdf']} | "
            f"errors: {stats['errors']}"
        )
        for k in totals:
            totals[k] += stats.get(k, 0)

    # ── Resum global ──────────────────────────────────────────────────────
    print()
    print(f"TOTAL PDFs nous descarregats: {totals['descarregats']}")
    print(f"TOTAL sense URL PDF:          {totals['sense_pdf']}")
    print(f"TOTAL errors:                 {totals['errors']}")


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    base = sys.argv[1] if len(sys.argv) > 1 else "."
    download_all(base)
