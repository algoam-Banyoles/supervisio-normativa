import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import boe_scraper as bs

def test_pdf_url_consolidated():
    """PDF URL must point to -consolidado.pdf for known IDs."""
    meta = {"id": "BOE-A-2017-12902"}
    url = bs._extract_pdf_url(meta)
    assert "consolidado" in url, f"Expected consolidado in URL, got: {url}"
    assert "2017" in url

def test_pdf_url_fallback():
    """Fallback for unknown ID pattern."""
    meta = {"id": "BOE-X-2020-999"}
    url = bs._extract_pdf_url(meta)
    assert "boe.es" in url

def test_classify_estat_vigent():
    meta = {"estado": "Finalizado"}
    assert bs._classify_estat(meta) == "VIGENT"

def test_classify_estat_derogada_anulacion():
    meta = {"estado": "Finalizado", "fecha_anulacion": "2024-01-01"}
    assert bs._classify_estat(meta) == "DEROGADA"

def test_priority_ids_no_wrong_law():
    """BOE-A-2015-11430 (Ley 24/2015 insolvencia) must NOT be in priority list."""
    assert "BOE-A-2015-11430" not in bs.PRIORITY_IDS, \
        "Ley 24/2015 (insolvencia) is not a road law — remove it"

def test_smoke_fetch(monkeypatch):
    """Smoke test: fetch BOE-A-2017-12902 metadata and check estat."""
    import requests
    session = bs.make_session()
    entry = bs.fetch_by_id(session, "BOE-A-2017-12902", "contractes")
    assert entry is not None, "fetch_by_id returned None for L9/2017"
    assert entry["estat"] == "VIGENT", f"Expected VIGENT, got {entry['estat']}"
    assert "contrat" in entry["text"].lower() or "9/2017" in entry["text"], \
        f"Unexpected title: {entry['text']}"
    print(f"\n  OK: {entry['id']} -> {entry['estat']}: {entry['text'][:60]}")

if __name__ == "__main__":
    test_pdf_url_consolidated()
    test_pdf_url_fallback()
    test_classify_estat_vigent()
    test_classify_estat_derogada_anulacion()
    test_priority_ids_no_wrong_law()
    print("All static tests passed. Running smoke test...")
    test_smoke_fetch(None)
    print("All tests passed.")
