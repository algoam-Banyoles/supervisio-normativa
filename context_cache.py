"""
context_cache.py — Cache temporal del context d'analisi del PDF.
"""

from __future__ import annotations

import json
from pathlib import Path


CACHE_VERSION = 1


def cache_path_for_pdf(pdf_path: Path) -> Path:
    return pdf_path.with_suffix(pdf_path.suffix + ".checker_cache.json")


def fingerprint_pdf(pdf_path: Path) -> dict:
    st = pdf_path.stat()
    return {
        "size": int(st.st_size),
        "mtime_ns": int(st.st_mtime_ns),
    }


def load_cache(cache_path: Path) -> dict | None:
    if not cache_path.exists():
        return None

    try:
        with cache_path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        return None

    if data.get("version") != CACHE_VERSION:
        return None

    return data


def save_cache(cache_path: Path, data: dict) -> None:
    payload = {
        "version": CACHE_VERSION,
        **data,
    }
    with cache_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
