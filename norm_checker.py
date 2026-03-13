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

# ─── CTE patterns ─────────────────────────────────────────────────────────────
CTE_PATTERNS = [
    re.compile(r'\bCTE\b', re.IGNORECASE),
    re.compile(r'\bDB[-\s]SE\b', re.IGNORECASE),
    re.compile(r'\bDB[-\s]SI\b', re.IGNORECASE),
    re.compile(r'\bDB[-\s]SUA?\b', re.IGNORECASE),
    re.compile(r'\bDB[-\s]HE\b', re.IGNORECASE),
    re.compile(r'\bDB[-\s]HS\b', re.IGNORECASE),
    re.compile(r'\bDB[-\s]HR\b', re.IGNORECASE),
    re.compile(r'\bDB[-\s]SE[-\s]AE\b', re.IGNORECASE),
    re.compile(r'\bDB[-\s]SE[-\s]C\b', re.IGNORECASE),
    re.compile(r'\bDB[-\s]SE[-\s]A\b', re.IGNORECASE),
    re.compile(r'\bDB[-\s]SE[-\s]F\b', re.IGNORECASE),
    re.compile(r'\bDB[-\s]SE[-\s]M\b', re.IGNORECASE),
    re.compile(r"Codi\s+T[eè]cnic\s+de\s+l['']Edificaci[oó]", re.IGNORECASE),
    re.compile(r'C[oó]digo\s+T[eé]cnico\s+de\s+la\s+Edificaci[oó]n', re.IGNORECASE),
    re.compile(r'RD\s+314/2006', re.IGNORECASE),
]

# Labels for INFO findings
_CTE_LABEL = {
    r'\bCTE\b':                                     "CTE (marc general)",
    r'\bDB[-\s]SE\b':                               "DB-SE Seguretat Estructural",
    r'\bDB[-\s]SI\b':                               "DB-SI Seguretat Incendi",
    r'\bDB[-\s]SUA?\b':                             "DB-SUA Seguretat Utilització/Accessibilitat",
    r'\bDB[-\s]HE\b':                               "DB-HE Estalvi Energia",
    r'\bDB[-\s]HS\b':                               "DB-HS Salubritat",
    r'\bDB[-\s]HR\b':                               "DB-HR Protecció Soroll",
    r'\bDB[-\s]SE[-\s]AE\b':                        "DB-SE-AE Accions a l'Edificació",
    r'\bDB[-\s]SE[-\s]C\b':                         "DB-SE-C Fonaments",
    r'\bDB[-\s]SE[-\s]A\b':                         "DB-SE-A Acer",
    r'\bDB[-\s]SE[-\s]F\b':                         "DB-SE-F Fàbrica",
    r'\bDB[-\s]SE[-\s]M\b':                         "DB-SE-M Fusta",
    r"Codi\s+T[eè]cnic\s+de\s+l['']Edificaci[oó]": "Codi Tècnic de l'Edificació",
    r'C[oó]digo\s+T[eé]cnico\s+de\s+la\s+Edificaci[oó]n': "Código Técnico de la Edificación",
    r'RD\s+314/2006':                               "RD 314/2006 (CTE)",
}

NBE_DEROGADA_PATTERNS = [
    re.compile(r'\bNBE[-\s]CT[-\s]?\d+', re.IGNORECASE),
    re.compile(r'\bNBE[-\s]CPI[-\s]?\d+', re.IGNORECASE),
    re.compile(r'\bNBE[-\s]AE[-\s]?\d+', re.IGNORECASE),
    re.compile(r'\bNBE[-\s]FL[-\s]?\d+', re.IGNORECASE),
    re.compile(r'\bNBE[-\s]QB[-\s]?\d+', re.IGNORECASE),
    re.compile(r'\bNormas?\s+B[aá]sicas?\s+de\s+la\s+Edificaci[oó]n', re.IGNORECASE),
]

# ─── IFI / IFE (OrdTMA/135/2023) patterns ────────────────────────────────────
IFI_IFE_PATTERNS = [
    re.compile(r'\bIFI\b', re.IGNORECASE),
    re.compile(r'\bIFE\b', re.IGNORECASE),
    re.compile(r'instrucci[oó]n?\s+ferroviari[ao]\s+(?:per\s+al\s+projecte|para\s+el\s+proyecto)', re.IGNORECASE),
    re.compile(r'subsistema\s+d[\'e]\s*infraestructura\s+ferroviari', re.IGNORECASE),
    re.compile(r'subsistema\s+d[\'e]\s*energia\s+ferroviari', re.IGNORECASE),
    re.compile(r'TMA[/\s]135[/\s]2023', re.IGNORECASE),
    re.compile(r'Ordre\s+TMA', re.IGNORECASE),
    re.compile(r'Orden\s+TMA', re.IGNORECASE),
]

# ─── IP Code / IEC 60529 patterns ────────────────────────────────────────────
IP_CODE_PATTERNS = [
    re.compile(r'\bIP\s*\d{2}\b', re.IGNORECASE),
    re.compile(r'\bIP\s*\d{2}[KX]\b', re.IGNORECASE),
    re.compile(r'\bcodi\s+IP\b', re.IGNORECASE),
    re.compile(r'\bcodigo\s+IP\b', re.IGNORECASE),
    re.compile(r'\bgrau\s+de\s+protecci[oó]\b', re.IGNORECASE),
    re.compile(r'\bgrado\s+de\s+protecci[oó]n\b', re.IGNORECASE),
    re.compile(r'IEC\s*60529', re.IGNORECASE),
    re.compile(r'EN\s*60529', re.IGNORECASE),
    re.compile(r'UNE.{0,5}60529', re.IGNORECASE),
    re.compile(r'Ingress\s+Protection', re.IGNORECASE),
]

_MODEL: SentenceTransformer | None = None
_ADIF_CATALOG:     dict = {}
_ISO_CATALOG:      dict = {}
_UNE_CATALOG:      dict = {}
_ACA_CATALOG:      dict = {}
_ERA_CATALOG:      dict = {}
_MITMA_F_CATALOG:  dict = {}
_IC_CATALOG:       dict = {}
# All HISTORICA entries from new catalogs — used for derogated-norm detection
_HISTORICA_NORMS:  dict = {}


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


def _load_new_catalog(path: str, font: str, target: dict) -> int:
    """
    Generic loader for catalogs that use {metadata, documents:[]} or flat list.
    HISTORICA entries are added to the shared _HISTORICA_NORMS derogated dict.
    Returns the number of entries loaded (0 on missing file or error).
    """
    global _HISTORICA_NORMS
    if not os.path.exists(path):
        print(f"  [INFO] {font}: catalog not found ({path}), skipping")
        return 0
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        docs = data.get("documents", data) if isinstance(data, dict) else data
        if not isinstance(docs, list):
            print(f"  [WARN] {font}: unexpected format in {path}, skipping")
            return 0
        n = 0
        for doc in docs:
            codi = (doc.get("codi") or doc.get("id") or "").strip()
            if not codi:
                continue
            target[codi] = doc
            n += 1
            if (doc.get("estat") or "").upper() == "HISTORICA":
                _HISTORICA_NORMS[codi] = doc
        print(f"  \u2705 Loaded {n} entries from {font}")
        return n
    except Exception as exc:
        print(f"  [WARN] {font}: failed to load {path}: {exc}")
        return 0


def _print_catalog_stats() -> None:
    # Count annexes entries separately (they live in a different structure)
    annexes_n = 0
    ann_path = os.path.join("normativa_annexes.json")
    if os.path.exists(ann_path):
        try:
            with open(ann_path, encoding="utf-8") as f:
                ann_data = json.load(f)
            for a in ann_data.get("annexes", []):
                annexes_n += len(a.get("normativa", []))
        except Exception:
            pass

    total = (
        annexes_n
        + len(_ADIF_CATALOG)
        + len(_ISO_CATALOG)
        + len(_UNE_CATALOG)
        + len(_ACA_CATALOG)
        + len(_ERA_CATALOG)
        + len(_MITMA_F_CATALOG)
        + len(_IC_CATALOG)
    )
    print(f"\n\U0001f4da Normative catalog: {total} entries")
    print(f"   - normativa_annexes.json: {annexes_n}")
    print(f"   - ADIF NTEs: {len(_ADIF_CATALOG)}")
    print(f"   - ISO: {len(_ISO_CATALOG)}")
    print(f"   - UNE: {len(_UNE_CATALOG)}")
    print(f"   - ACA: {len(_ACA_CATALOG)}")
    print(f"   - ERA/CENELEC: {len(_ERA_CATALOG)}")
    print(f"   - MITMA Ferroviari: {len(_MITMA_F_CATALOG)}")
    print(f"   - IC Complement: {len(_IC_CATALOG)}")


_load_iso_catalog()
_load_une_catalog()
_load_new_catalog(
    os.path.join("normativa_aca", "_catalogo", "catalogo_aca.json"),
    "ACA", _ACA_CATALOG,
)
_load_new_catalog(
    os.path.join("normativa_era", "_catalogo", "catalogo_era.json"),
    "ERA/CENELEC", _ERA_CATALOG,
)
_load_new_catalog(
    os.path.join("normativa_mitma_ferroviari", "_catalogo", "catalogo_mitma_ferroviari.json"),
    "MITMA-F", _MITMA_F_CATALOG,
)
_load_new_catalog(
    os.path.join("normativa_dgc", "_catalogo", "ic_complement.json"),
    "MITMA-IC", _IC_CATALOG,
)
_print_catalog_stats()


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


def check_cte_references(pages: list) -> list[dict]:
    """
    Scan PDF pages for CTE document references (INFO) and derogated NBE
    references (NO OK).  Always runs — no catalog dependency required.
    """
    findings: list[dict] = []
    full_text = "\n".join(page.get("text", "") or "" for page in pages)

    # ── NBE derogada (NO OK) ──────────────────────────────────────────────────
    for pat in NBE_DEROGADA_PATTERNS:
        match = pat.search(full_text)
        if not match:
            continue
        cited_text = match.group(0)
        pags = [
            p["num"] for p in pages
            if pat.search(p.get("text", "") or "")
        ]
        findings.append({
            "status":  "NO OK",
            "item":    "CTE-01",
            "descrip": f"Referència a NBE derogada: «{cited_text}»",
            "detall":  (
                f"Pàgines: {', '.join(str(p) for p in pags[:10])}\n"
                f"Totes les Normes Bàsiques de l'Edificació (NBE) van quedar derogades\n"
                f"per l'entrada en vigor del CTE (RD 314/2006). Cal substituir per DB corresponent."
            ),
            "ref":     "RD 314/2006 (CTE) — disposició derogatòria",
        })

    # ── CTE vigent (INFO) ─────────────────────────────────────────────────────
    cte_found: list[str] = []
    for pat in CTE_PATTERNS:
        if pat.search(full_text):
            label = next(
                (v for k, v in _CTE_LABEL.items() if re.search(k, pat.pattern, re.IGNORECASE)),
                pat.pattern,
            )
            cte_found.append(label)

    if cte_found:
        findings.append({
            "status":  "INFO",
            "item":    "CTE-00",
            "descrip": f"Detectades {len(cte_found)} referències al CTE",
            "detall":  "Documents CTE citats: " + ", ".join(dict.fromkeys(cte_found)),
            "ref":     "RD 314/2006 mod. RD 732/2019 — Codi Tècnic de l'Edificació",
        })

    return findings


def check_cte_references(pages: list) -> list[dict]:
    """
    Scan PDF pages for CTE Document Basic references and NBE derogated norms.
    CTE refs → INFO findings. NBE refs → NO OK findings.
    """
    # Collect first-seen page per matched label
    cte_hits: dict[str, int] = {}   # label → first page
    nbe_hits: dict[str, int] = {}   # matched text → first page

    for page in pages:
        text = page.get("text", "") or ""
        page_num = page.get("num", 0)
        for pat in CTE_PATTERNS:
            m = pat.search(text)
            if m:
                label = _CTE_LABEL.get(pat.pattern, pat.pattern)
                if label not in cte_hits:
                    cte_hits[label] = page_num
        for pat in NBE_DEROGADA_PATTERNS:
            for m in pat.finditer(text):
                key = m.group(0).strip()
                if key not in nbe_hits:
                    nbe_hits[key] = page_num

    findings: list[dict] = []
    for label, page_num in cte_hits.items():
        findings.append({
            "status":  "INFO",
            "item":    "CTE-00",
            "descrip": f"Referència CTE detectada: {label}",
            "detall":  f"Primera aparició: p.{page_num}. Normativa vigent: RD 314/2006 mod. RD 732/2019.",
            "ref":     "Codi Tècnic de l'Edificació — codigotecnico.org",
        })
    for nbe_text, page_num in nbe_hits.items():
        findings.append({
            "status":  "NO OK",
            "item":    "CTE-01",
            "descrip": f"Referència a NBE derogada: «{nbe_text}»",
            "detall":  (
                f"Primera aparició: p.{page_num}\n"
                f"Les NBE van quedar derogades per RD 314/2006 (CTE).\n"
                f"Substituir per la secció CTE corresponent."
            ),
            "ref":     "RD 314/2006 (CTE) — Disposició derogatòria",
        })
    return findings


def check_ifi_ife_ip_references(pages: list) -> list[dict]:
    """
    Scan PDF pages for IFI/IFE (OrdTMA/135/2023) and IP code (UNE-EN 60529) references.
    Both are VIGENT norms — findings are INFO only.
    """
    ifi_pages: list[int] = []
    ip_pages:  list[int] = []

    for page in pages:
        text = page.get("text", "") or ""
        page_num = page.get("num", 0)
        if any(pat.search(text) for pat in IFI_IFE_PATTERNS):
            if page_num not in ifi_pages:
                ifi_pages.append(page_num)
        if any(pat.search(text) for pat in IP_CODE_PATTERNS):
            if page_num not in ip_pages:
                ip_pages.append(page_num)

    findings: list[dict] = []
    if ifi_pages:
        pages_txt = ", ".join(str(p) for p in ifi_pages[:10])
        findings.append({
            "status":  "INFO",
            "item":    "IFI-00",
            "descrip": "Referència a instruccions ferroviàries IFI/IFE detectada",
            "detall":  (
                f"Pàgines: {pages_txt}\n"
                f"Normativa aplicable: OrdTMA/135/2023, en vigor des 01/07/2023.\n"
                f"Corregida per OrdTRM/608/2024. Desenvolupa RD 929/2020."
            ),
            "ref":     "OrdTMA/135/2023 — BOE-A-2023-4324",
        })
    if ip_pages:
        pages_txt = ", ".join(str(p) for p in ip_pages[:10])
        findings.append({
            "status":  "INFO",
            "item":    "IP-00",
            "descrip": "Referència a grau de protecció IP / IEC 60529 detectada",
            "detall":  (
                f"Pàgines: {pages_txt}\n"
                f"Normativa aplicable: UNE-EN 60529 / IEC 60529:1989+A1:1999+A2:2013.\n"
                f"Verificar que el grau IP especificat és l'adequat per a l'emplaçament."
            ),
            "ref":     "UNE-EN 60529 / IEC 60529",
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

    cte = check_cte_references(pages)
    if cte:
        findings.extend(cte)

    ifi_ip = check_ifi_ife_ip_references(pages)
    if ifi_ip:
        findings.extend(ifi_ip)

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