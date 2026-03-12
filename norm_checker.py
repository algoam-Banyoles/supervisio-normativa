from __future__ import annotations

import json
import os
import re
import sqlite3
from pathlib import Path

import chromadb
from sentence_transformers import SentenceTransformer

from env_utils import load_local_env
from norm_indexer import CHROMA_PATH, DB_PATH, EMBEDDING_MODEL


ARTICLE_REF_RE = re.compile(r"\b(?:art(?:icle|iculo|\.)?)\s+(\d+[a-z]?(?:\.\d+)?)", re.IGNORECASE)
REF_CODE_RE    = re.compile(r"\b(\d+/\d{4}|\d{5})\b")
NTE_PATTERN    = re.compile(r"\b(\d{2}\.\d{3}\.\d{3,})\b", re.IGNORECASE)
ISO_PATTERN    = re.compile(
    r"\b(ISO(?:/IEC)?(?:/TR|/TS|/PAS)?\s*\d+(?:-\d+)*(?::\d{4})?)\b",
    re.IGNORECASE,
)
UNE_PATTERN    = re.compile(
    r"\b(UNE(?:-EN)?(?:-ISO)?(?:/IEC)?\s*\d+(?:-\d+)*(?::\d{4})?)\b",
    re.IGNORECASE,
)
MANDATORY_BY_TYPE = {
    "carretera": ["9/2017", "3/2007", "1627/1997", "105/2008", "1359/2011", "1098/2001"],
    "ferroviari": ["38/2015", "2387/2004", "50128", "50129"],
}

_MODEL: SentenceTransformer | None = None
_ADIF_CATALOG: dict = {}
_ISO_CATALOG:  dict = {}
_UNE_CATALOG:  dict = {}


def _load_adif_catalog() -> None:
    global _ADIF_CATALOG
    catalog_path = os.path.join("normativa_adif", "_catalogo", "catalogo_adif.json")
    if not os.path.exists(catalog_path):
        return
    try:
        with open(catalog_path, encoding="utf-8") as f:
            docs = json.load(f)
        for doc in docs:
            codigo = (doc.get("codigo") or "").strip()
            if codigo:
                _ADIF_CATALOG[codigo] = doc
    except Exception:
        pass


_load_adif_catalog()


def _load_iso_catalog() -> None:
    global _ISO_CATALOG
    path = os.path.join("normativa_iso", "_catalogo", "catalogo_iso.json")
    if not os.path.exists(path):
        return
    try:
        with open(path, encoding="utf-8") as f:
            docs = json.load(f)
        for doc in docs:
            ref = (doc.get("referencia") or "").strip()
            if not ref:
                continue
            _ISO_CATALOG[ref] = doc
            # Also index without edition year: "ISO 1234"
            base = re.sub(r":\d{4}.*$", "", ref).strip()
            if base not in _ISO_CATALOG:
                _ISO_CATALOG[base] = doc
    except Exception:
        pass


def _load_une_catalog() -> None:
    global _UNE_CATALOG
    path = os.path.join("normativa_une", "_catalogo", "catalogo_une.json")
    if not os.path.exists(path):
        return
    try:
        with open(path, encoding="utf-8") as f:
            docs = json.load(f)
        for doc in docs:
            ref = (doc.get("referencia") or "").strip()
            if not ref:
                continue
            _UNE_CATALOG[ref] = doc
            base = re.sub(r":\d{4}.*$", "", ref).strip()
            if base not in _UNE_CATALOG:
                _UNE_CATALOG[base] = doc
    except Exception:
        pass


_load_iso_catalog()
_load_une_catalog()


def norm_db_available() -> bool:
    return Path(DB_PATH).exists() and Path(CHROMA_PATH).exists()


def search_norm_text(query: str, n_results: int = 5) -> list[dict]:
    if not norm_db_available():
        return []

    client = chromadb.PersistentClient(path=CHROMA_PATH)
    collection = client.get_or_create_collection(name="normativa")
    embedding = _get_embedding_model().encode([query], show_progress_bar=False, convert_to_numpy=True)[0].tolist()
    result = collection.query(
        query_embeddings=[embedding],
        n_results=n_results,
        include=["documents", "metadatas", "distances"],
    )

    docs = result.get("documents", [[]])[0]
    metas = result.get("metadatas", [[]])[0]
    distances = result.get("distances", [[]])[0]
    rows = []
    for text, meta, distance in zip(docs, metas, distances):
        rows.append(
            {
                "text": text,
                "doc_codi": (meta or {}).get("doc_codi", ""),
                "doc_titol": (meta or {}).get("doc_titol", ""),
                "page": (meta or {}).get("page", 0),
                "chunk_index": (meta or {}).get("chunk_index", 0),
                "distance": float(distance),
            }
        )
    return rows


def find_article(codi: str, article_num: str) -> dict | None:
    if not norm_db_available():
        return None

    code_key = _normalize_codi(codi)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT c.text, c.page_num, c.chunk_index, d.codi, d.titol
            FROM chunks c
            JOIN articles a ON a.chunk_id = c.id
            JOIN documents d ON a.doc_id = d.id
            WHERE lower(d.codi) LIKE ? AND a.article_num = ?
            LIMIT 1
            """,
            (f"%{code_key.lower()}%", article_num),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return None

    return {
        "article_text": row["text"],
        "page": row["page_num"],
        "chunk_index": row["chunk_index"],
        "doc_codi": row["codi"],
        "doc_titol": row["titol"],
    }


def check_reference_exists(ref_text: str) -> dict:
    ref_text = ref_text or ""
    code_key = _normalize_codi(ref_text)
    article_num = _extract_article_num(ref_text)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT id, codi, titol, vigent
            FROM documents
            WHERE lower(codi) LIKE ? OR lower(titol) LIKE ?
            ORDER BY vigent DESC, id ASC
            LIMIT 1
            """,
            (f"%{code_key.lower()}%", f"%{code_key.lower()}%"),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return {
            "found": False,
            "vigent": False,
            "article_found": False,
            "doc_codi": "",
            "doc_titol": "",
            "article_text": None,
            "confidence": 0.0,
        }

    article_payload = find_article(row["codi"], article_num) if article_num else None
    article_found = bool(article_payload) if article_num else True
    confidence = 0.85
    if article_num and article_found:
        confidence = 1.0
    elif article_num and not article_found:
        confidence = 0.65

    return {
        "found": True,
        "vigent": bool(row["vigent"]),
        "article_found": article_found,
        "doc_codi": row["codi"] or "",
        "doc_titol": row["titol"] or "",
        "article_text": article_payload["article_text"] if article_payload else None,
        "confidence": confidence,
    }


def check_compliance(requirement_text: str, project_text: str, norm_chunks: list[dict]) -> dict:
    load_local_env()
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return {
            "compleix": False,
            "parcial": False,
            "observacio": "Manca ANTHROPIC_API_KEY per validar el compliment.",
            "severitat": "INFO",
        }

    import anthropic

    system_prompt = (
        "Ets un enginyer revisor de la DGIM. Verifica si el text del projecte "
        "compleix el requisit normatiu indicat. Respon en JSON: "
        "{'compleix': true/false, 'parcial': true/false, "
        "'observacio': 'explicacio breu en catala', "
        "'severitat': 'NO OK'/'INFO'/'OK'}"
    )
    user_message = (
        f"REQUISIT NORMATIU:\n{requirement_text}\n\n"
        f"TEXT NORMA (context):\n{(norm_chunks[0]['text'] if norm_chunks else '')[:3000]}\n\n"
        f"TEXT PROJECTE:\n{(project_text or '')[:2000]}\n\n"
        "Compleix el projecte aquest requisit?"
    )

    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=1024,
        system=system_prompt,
        messages=[{"role": "user", "content": user_message}],
    )
    raw = "\n".join(getattr(block, "text", "") for block in response.content).strip()

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if match:
            return json.loads(match.group(0))
        return {
            "compleix": False,
            "parcial": False,
            "observacio": raw or "Resposta no parsejable.",
            "severitat": "INFO",
        }


def detect_missing_norms(project_type: str, cited_norms: list[str], annex_key: str) -> list[dict]:
    if not norm_db_available():
        return []

    mandatory = MANDATORY_BY_TYPE.get(project_type, [])
    cited = {_normalize_codi(ref) for ref in cited_norms or []}
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("SELECT codi, titol, vigent FROM documents").fetchall()
    finally:
        conn.close()

    missing = []
    for code in mandatory:
        norm_code = _normalize_codi(code)
        if any(norm_code in cited_ref for cited_ref in cited):
            continue
        found = next((row for row in rows if norm_code in _normalize_codi(row["codi"] or "")), None)
        missing.append(
            {
                "annex_key": annex_key,
                "required_code": code,
                "doc_codi": found["codi"] if found else "",
                "doc_titol": found["titol"] if found else "",
                "vigent": bool(found["vigent"]) if found else True,
            }
        )
    return missing


def check_nte_references(pages: list, doc) -> list[dict]:
    """
    Scan PDF pages for ADIF NTE codes (##.###.###) and check their status
    against the local ADIF catalog.  Silent no-op if catalog not loaded.
    """
    if not _ADIF_CATALOG:
        return []

    # Collect first-seen page for each cited code
    cited: dict[str, int] = {}
    for page in pages:
        text = page.get("text", "") or ""
        for m in NTE_PATTERN.finditer(text):
            code = m.group(1)
            if code not in cited:
                cited[code] = page.get("num", 0)

    findings: list[dict] = []

    for code, page_num in cited.items():
        if code not in _ADIF_CATALOG:
            findings.append({
                "status":  "INFO",
                "item":    "NTE-01",
                "descrip": f"NTE {code} — no trobada al catàleg local",
                "detall":  (
                    f"Referenciada a p.{page_num}. "
                    f"Verificar si és una NTE vàlida."
                ),
                "ref": "Catàleg ADIF Normativa Tècnica",
            })
            continue

        entry   = _ADIF_CATALOG[code]
        estado  = (entry.get("estado") or "").lower()
        titol   = entry.get("titulo") or ""
        derogada_per = (entry.get("iden_drga_por") or "").strip()

        if "derogado" in estado or "histórico" in estado or "historic" in estado:
            detall = (
                f"NTE citada: {code} — {titol}\n"
                f"Estat: DEROGADA\n"
                f"Referenciada a p.{page_num}"
            )
            if derogada_per:
                replacement = _ADIF_CATALOG.get(derogada_per, {})
                repl_titol  = replacement.get("titulo") or derogada_per
                detall += f"\nSubstituïda per: {derogada_per} — {repl_titol}"

            findings.append({
                "status":  "NO OK",
                "item":    "NTE-01",
                "descrip": f"NTE derogada: {code} — {titol[:60]}",
                "detall":  detall,
                "ref":     "Catàleg ADIF Normativa Tècnica",
            })
        else:
            findings.append({
                "status":  "INFO",
                "item":    "NTE-00",
                "descrip": f"NTE vigent: {code} — {titol[:60]}",
                "detall":  f"Referenciada a p.{page_num}. Estat: VIGENT.",
                "ref":     "Catàleg ADIF Normativa Tècnica",
            })

    return findings


def check_iso_une_references(pages: list) -> list[dict]:
    """
    Scan PDF pages for ISO and UNE references and check their status against
    the locally cached catalogs.  Silent no-op if neither catalog is loaded.
    """
    if not _ISO_CATALOG and not _UNE_CATALOG:
        return []

    # Collect first-seen page for each cited reference
    cited: dict[str, int] = {}
    for page in pages:
        text = page.get("text", "") or ""
        for pat in (ISO_PATTERN, UNE_PATTERN):
            for m in pat.finditer(text):
                ref = re.sub(r"\s+", " ", m.group(1).strip())
                if ref not in cited:
                    cited[ref] = page.get("num", 0)

    findings: list[dict] = []

    for ref, page_num in cited.items():
        is_iso = ref.upper().startswith("ISO")
        catalog = _ISO_CATALOG if is_iso else _UNE_CATALOG
        prefix  = "ISO" if is_iso else "UNE"

        base_ref = re.sub(r":\d{4}.*$", "", ref).strip()
        entry = catalog.get(ref) or catalog.get(base_ref)
        if not entry:
            continue

        estat = (entry.get("estat") or "").upper()

        if estat in ("RETIRADA", "ANULADA"):
            sub = (entry.get("substituida_per") or "").strip()
            detall = (
                f"Referència citada: {ref} (p.{page_num})\n"
                f"Títol: {(entry.get('titol') or '')[:80]}\n"
                f"Estat: {estat}"
            )
            if sub:
                detall += f"\nSubstituïda per: {sub}"

            findings.append({
                "status":  "NO OK",
                "item":    f"{prefix}-01",
                "descrip": f"Norma {estat.lower()}: {ref}",
                "detall":  detall,
                "ref":     entry.get("font", f"Catàleg {prefix}"),
            })

    return findings


def check_all_references(pages: list[dict], annex_map: dict) -> list[dict]:
    from checks.normativa_taula import (
        SEARCH_PATTERNS,
        _build_page_annex_lookup,
        _classify_annex_for_pages,
        _clean_reference,
        _numeric_key,
    )

    if not norm_db_available():
        return []

    refs_by_key = {}
    annex_lookup = _build_page_annex_lookup(annex_map or {})

    for page in pages:
        text = page.get("text", "") or ""
        if not text:
            continue
        page_num = page.get("num", 0)
        page_annex = _classify_annex_for_pages([page_num], annex_lookup)
        for _ref_type, pattern in SEARCH_PATTERNS:
            for match in pattern.finditer(text):
                ref = _clean_reference(match.group(0))
                key = _numeric_key(ref)
                bucket = refs_by_key.setdefault(
                    key,
                    {"reference": ref, "pages": [], "annex": page_annex},
                )
                if page_num not in bucket["pages"]:
                    bucket["pages"].append(page_num)

    refs = list(refs_by_key.values())
    findings = []
    findings.append(
        {
            "status": "INFO",
            "item": "NDB-00",
            "descrip": f"Base normativa local consultada: {len(refs)} referencies detectades",
            "detall": "Comprovacio automa tica contra normativa.db i chroma_db.",
            "ref": "Base normativa local",
        }
    )

    item_no = 1
    for ref in refs:
        result = check_reference_exists(ref["reference"])
        article_num = _extract_article_num(ref["reference"])
        pages_text = ", ".join(str(p) for p in ref.get("pages", [])[:10]) or "-"
        annex_text = ref.get("annex", "-")

        if not result["found"]:
            findings.append(
                {
                    "status": "INFO",
                    "item": f"NDB-{item_no:02d}",
                    "descrip": f"Referencia no trobada a la base local: «{ref['reference']}»",
                    "detall": f"Bloc: {annex_text}\nPagines: {pages_text}",
                    "ref": "Base normativa local",
                }
            )
            item_no += 1
            continue

        if not result["vigent"]:
            findings.append(
                {
                    "status": "NO OK",
                    "item": f"NDB-{item_no:02d}",
                    "descrip": f"Referencia no vigent o derogada: «{ref['reference']}»",
                    "detall": (
                        f"Document base: {result['doc_codi']} - {result['doc_titol']}\n"
                        f"Bloc: {annex_text}\nPagines: {pages_text}"
                    ),
                    "ref": "Base normativa local",
                }
            )
            item_no += 1
            continue

        if article_num and not result["article_found"]:
            findings.append(
                {
                    "status": "INFO",
                    "item": f"NDB-{item_no:02d}",
                    "descrip": f"Article no localitzat a la base local: art. {article_num} de «{result['doc_codi']}»",
                    "detall": f"Referencia original: {ref['reference']}\nBloc: {annex_text}\nPagines: {pages_text}",
                    "ref": "Base normativa local",
                }
            )
            item_no += 1
            continue

        detail = (
            f"Document: {result['doc_codi']} - {result['doc_titol']}\n"
            f"Bloc: {annex_text}\nPagines: {pages_text}"
        )
        if result.get("article_text"):
            detail += f"\nText article: {result['article_text'][:600]}"

        findings.append(
            {
                "status": "OK",
                "item": f"NDB-{item_no:02d}",
                "descrip": f"Referencia contrastada a la base local: «{ref['reference']}»",
                "detall": detail,
                "ref": "Base normativa local",
            }
        )
        item_no += 1

    if _ADIF_CATALOG:
        nte_findings = check_nte_references(pages, None)
        if nte_findings:
            findings.extend(nte_findings)

    iso_une = check_iso_une_references(pages)
    if iso_une:
        findings.extend(iso_une)

    return findings


def _get_embedding_model() -> SentenceTransformer:
    global _MODEL
    if _MODEL is None:
        _MODEL = SentenceTransformer(EMBEDDING_MODEL)
    return _MODEL


def _normalize_codi(text: str) -> str:
    text = (text or "").lower()
    match = REF_CODE_RE.search(text)
    if match:
        return match.group(1)
    return re.sub(r"\s+", " ", text).strip()


def _extract_article_num(text: str) -> str | None:
    match = ARTICLE_REF_RE.search(text or "")
    return match.group(1) if match else None