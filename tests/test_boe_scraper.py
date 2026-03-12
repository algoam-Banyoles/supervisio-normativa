"""
tests/test_boe_scraper.py — Smoke tests for boe_scraper.py

Run:
    python -m pytest tests/test_boe_scraper.py -v
  or:
    python tests/test_boe_scraper.py
"""

import sys
import os

# Allow importing from parent directory without installing
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boe_scraper as boe


# ─── Test 1: fetch single priority document ────────────────────────────────────

def test_fetch_lcsp_vigent():
    """
    BOE-A-2017-12902 (Ley 9/2017 LCSP) must be VIGENT.
    """
    session = boe.make_session()
    entry   = boe.fetch_by_id(session, "BOE-A-2017-12902", "contractes")

    assert entry is not None, "fetch_by_id returned None for BOE-A-2017-12902"
    assert entry["estat"] == "VIGENT", (
        f"Expected VIGENT, got {entry['estat']} for BOE-A-2017-12902"
    )
    print(f"  [OK] BOE-A-2017-12902 -> {entry['estat']}: {entry['text'][:80]}")


# ─── Test 2: thematic CONTRATACION search returns > 5 items ───────────────────

def test_contratacion_search_returns_results():
    """
    CONTRATACION+PUBLICA search must return at least 6 documents.
    """
    session  = boe.make_session()
    catalog:  list[dict] = []
    seen_ids: set[str]   = set()

    # Fetch only the first page (offset=0) to keep the test fast
    import requests as req

    url = (
        f"{boe.API_BASE}/legislacion-consolidada"
        f"?materia=CONTRATACION+PUBLICA&estado=1"
        f"&offset=0&limite=50"
    )
    data = boe._get(session, url)
    assert data is not None, "API returned None for CONTRATACION search"

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

    assert isinstance(items, list), f"items is not a list: {type(items)}"
    assert len(items) > 5, (
        f"Expected > 5 results for CONTRATACION, got {len(items)}"
    )

    entries = [boe._build_entry(r, "contractes") for r in items if isinstance(r, dict)]
    vigents  = sum(1 for e in entries if e["estat"] == "VIGENT")
    derogades = sum(1 for e in entries if e["estat"] == "DEROGADA")
    pendents  = sum(1 for e in entries if e["estat"] == "PENDENT")

    print(
        f"  [OK] CONTRATACION first page: {len(entries)} items "
        f"({vigents} vigents, {derogades} derogades, {pendents} pendents)"
    )


# ─── Summary helper ────────────────────────────────────────────────────────────

def _print_summary(catalog: list[dict]) -> None:
    vigents  = sum(1 for e in catalog if e["estat"] == "VIGENT")
    derogades = sum(1 for e in catalog if e["estat"] == "DEROGADA")
    pendents  = sum(1 for e in catalog if e["estat"] == "PENDENT")
    print(f"\n  Summary: {vigents} vigents, {derogades} derogades, {pendents} pendents")


# ─── Standalone runner ────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        ("test_fetch_lcsp_vigent",                test_fetch_lcsp_vigent),
        ("test_contratacion_search_returns_results", test_contratacion_search_returns_results),
    ]

    passed = 0
    failed = 0

    print("=== BOE Scraper Smoke Tests ===\n")

    for name, fn in tests:
        print(f"[RUN] {name}")
        try:
            fn()
            passed += 1
            print(f"[PASS] {name}\n")
        except Exception as exc:
            failed += 1
            print(f"[FAIL] {name}: {exc}\n")

    print(f"Results: {passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)
