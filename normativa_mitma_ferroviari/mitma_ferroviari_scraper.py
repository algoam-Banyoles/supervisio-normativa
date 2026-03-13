"""
normativa_mitma_ferroviari/mitma_ferroviari_scraper.py
Catàleg estàtic de reglaments ferroviaris espanyols (MITMA / BOE).

Catàleg completament estàtic (normes estables, no necessita scraping).

Ús:
    python normativa_mitma_ferroviari/mitma_ferroviari_scraper.py
"""

import json
import os
import sys
from datetime import datetime

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_catalogo")

_STATIC_DOCS = [
    {
        "codi":  "RD-2387-2004",
        "titol": "Reglament del Sector Ferroviari",
        "any":   2004,
        "estat": "VIGENT",
        "url":   "https://www.boe.es/buscar/act.php?id=BOE-A-2005-740",
        "observacions": "Desenvolupa la Llei 39/2003 del Sector Ferroviari",
    },
    {
        "codi":  "RD-354-2006",
        "titol": "RD 354/2006 condicions tècniques de seguretat ferroviària",
        "any":   2006,
        "estat": "VIGENT",
        "url":   "https://www.boe.es/buscar/doc.php?id=BOE-A-2006-5388",
        "observacions": "",
    },
    {
        "codi":  "RD-810-2007",
        "titol": "RD 810/2007 circulació ferroviària",
        "any":   2007,
        "estat": "VIGENT",
        "url":   "https://www.boe.es/buscar/doc.php?id=BOE-A-2007-12422",
        "observacions": "",
    },
    {
        "codi":  "RD-664-2015",
        "titol": "RD 664/2015 certificats de seguretat ferroviaris",
        "any":   2015,
        "estat": "VIGENT",
        "url":   None,
        "observacions": "",
    },
    {
        "codi":  "LEI-39-2003",
        "titol": "Llei 39/2003 del Sector Ferroviari",
        "any":   2003,
        "estat": "VIGENT",
        "url":   "https://www.boe.es/buscar/act.php?id=BOE-A-2003-22239",
        "observacions": "",
    },
    {
        "codi":  "RD-256-2021",
        "titol": "RD 256/2021 condicions tècniques ADIF",
        "any":   2021,
        "estat": "VIGENT",
        "url":   None,
        "observacions": "",
    },
    {
        "codi":  "OM-FOM-233-2006",
        "titol": "Ordre FOM/233/2006 requisits tècnics ferroviaris",
        "any":   2006,
        "estat": "VIGENT",
        "url":   None,
        "observacions": "",
    },
    {
        "codi":  "RD-1247-1995",
        "titol": "RD 1247/1995 normes tècniques ferroviàries (RENFE)",
        "any":   1995,
        "estat": "HISTORICA",
        "url":   None,
        "observacions": "Derogat per RD 2387/2004 i normativa ETI",
    },
]


def _build_doc(raw: dict) -> dict:
    return {
        "codi":           raw["codi"],
        "titol":          raw["titol"],
        "any_publicacio": raw.get("any"),
        "estat":          raw.get("estat", "VIGENT"),
        "font":           "MITMA-F",
        "url_fitxa":      raw.get("url"),
        "url_document":   None,
        "temes":          ["ferroviari"],
        "observacions":   raw.get("observacions", ""),
    }


def build_catalog() -> list[dict]:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    docs = [_build_doc(r) for r in _STATIC_DOCS]

    catalog = {
        "metadata": {
            "font":            "Ministeri de Transports, Mobilitat i Agenda Urbana (MITMA) — Ferroviari",
            "url_base":        "https://www.boe.es",
            "data_scraping":   datetime.now().strftime("%Y-%m-%d"),
            "total_documents": len(docs),
            "versio":          "1.0",
        },
        "documents": docs,
    }

    out = os.path.join(OUTPUT_DIR, "catalogo_mitma_ferroviari.json")
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(catalog, fh, ensure_ascii=False, indent=2)

    return docs


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    print("=" * 55)
    print(" MITMA Ferroviari — catàleg estàtic")
    print("=" * 55)

    docs = build_catalog()
    historiques = [d for d in docs if d["estat"] == "HISTORICA"]

    print()
    print("✅ MITMA Ferroviari catalog complete")
    print(f"📄 Documents: {len(docs)} ({len(historiques)} HISTORICA)")
    print(f"💾 Saved to {os.path.join(OUTPUT_DIR, 'catalogo_mitma_ferroviari.json')}")
