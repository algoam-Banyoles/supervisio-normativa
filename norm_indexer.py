from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import unicodedata
from datetime import datetime
from pathlib import Path

import chromadb
import fitz
from docx import Document
from sentence_transformers import SentenceTransformer


NORMATIVA_FOLDER = "normativa"
NORMATIVA_FOLDERS = [
    "normativa_adif",
    "normativa_dgc",
    "normativa_industria",
    "normativa_territori",
]
DB_PATH = "normativa.db"
CHROMA_PATH = "chroma_db"
CHUNK_SIZE = 800
CHUNK_OVERLAP = 150
EMBEDDING_MODEL = "paraphrase-multilingual-MiniLM-L12-v2"

SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS documents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename TEXT NOT NULL,
        codi TEXT,
        titol TEXT,
        tipus TEXT,
        any_aprovacio INTEGER,
        vigent INTEGER DEFAULT 1,
        data_indexat TEXT,
        num_chunks INTEGER,
        file_hash TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS chunks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        doc_id INTEGER REFERENCES documents(id),
        chunk_index INTEGER,
        text TEXT NOT NULL,
        page_num INTEGER,
        chroma_id TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS articles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        doc_id INTEGER REFERENCES documents(id),
        article_num TEXT,
        article_title TEXT,
        chunk_id INTEGER REFERENCES chunks(id)
    )
    """,
]

ARTICLE_HEADER_RE = re.compile(r"^(?:Article|Articulo|Articulo|Art\.)\s+\d+[a-z]?(?:\.\d+)?\b", re.IGNORECASE)
ARTICLE_SCAN_RE = re.compile(
    r"(?:Article|Articulo|Art\.)\s+(\d+[a-z]?(?:\.\d+)?)\s*[.\-–]?\s*([^\n]{0,80})",
    re.IGNORECASE,
)

_MODEL: SentenceTransformer | None = None
_LAST_INDEX_RESULT: dict = {}


def extract_text_from_file(filepath: str) -> list[dict]:
    path = Path(filepath)
    suffix = path.suffix.lower()
    pages = []

    if suffix == ".pdf":
        doc = fitz.open(str(path))
        try:
            for index, page in enumerate(doc, 1):
                pages.append({"text": page.get_text() or "", "page": index})
        finally:
            doc.close()
        return pages

    if suffix == ".docx":
        document = Document(str(path))
        for para in document.paragraphs:
            text = para.text.strip()
            if text:
                pages.append({"text": text, "page": 0})
        return pages

    raise ValueError(f"Format no suportat: {path.suffix}")


def detect_document_metadata(filename: str, text: str) -> dict:
    sample = f"{filename}\n{text[:2000]}"
    sample_norm = _norm(sample)

    patterns = [
        ("RD", re.compile(r"\b(?:rd|reial\s+decret|real\s+decreto)\s+(\d+/\d{4})\b", re.IGNORECASE)),
        ("Llei", re.compile(r"\b(?:llei|ley)\s+(\d+/\d{4})\b", re.IGNORECASE)),
        ("Decret", re.compile(r"\b(?:decret|decreto)\s+(\d+/\d{4})\b", re.IGNORECASE)),
        ("UNE", re.compile(r"\b(?:une(?:-en)?(?:\s+en)?)\s*([0-9][0-9A-Z./-]*)\b", re.IGNORECASE)),
        ("Ordre", re.compile(r"\b(?:ordre|orden|instruccio|instruccion)\s+([A-Z0-9./-]+(?:/\d{4})?)\b", re.IGNORECASE)),
    ]

    tipus = None
    codi = None
    for doc_type, pattern in patterns:
        match = pattern.search(sample)
        if match:
            tipus = doc_type
            codi = _clean_code(match.group(1) if match.groups() else match.group(0), doc_type)
            break

    if codi is None:
        generic = re.search(r"\b(\d+/\d{4})\b", sample)
        if generic:
            codi = generic.group(1)

    any_aprovacio = None
    year_match = re.search(r"\d+/(\d{4})\b", sample)
    if year_match:
        any_aprovacio = int(year_match.group(1))

    title = _detect_title(text)
    vigent = 1
    catalog = _load_norm_catalog()
    if codi:
        code_norm = _norm(codi)
        if code_norm in catalog["derogada_aliases"]:
            vigent = 0
    if vigent and re.search(r"\bderogad[ao]\b", sample_norm):
        vigent = 0

    return {
        "codi": codi,
        "titol": title,
        "tipus": tipus,
        "any_aprovacio": any_aprovacio,
        "vigent": vigent,
    }


def chunk_text(pages: list[dict], chunk_size: int, overlap: int) -> list[dict]:
    paragraphs = _paragraphs_from_pages(pages)
    chunks = []
    current_parts: list[dict] = []
    current_len = 0

    def flush_chunk() -> None:
        nonlocal current_parts, current_len
        if not current_parts:
            return

        text = "\n\n".join(part["text"] for part in current_parts if part.get("text"))
        page = current_parts[0].get("page", 0)
        chunks.append(
            {
                "text": text,
                "page": page,
                "chunk_index": len(chunks),
            }
        )

        tail = text[-overlap:].strip() if overlap > 0 else ""
        current_parts = [{"text": tail, "page": page}] if tail else []
        current_len = len(tail)

    for para in paragraphs:
        para_text = para["text"].strip()
        if not para_text:
            continue

        if len(para_text) > chunk_size:
            if current_parts:
                flush_chunk()
            for subtext in _split_long_paragraph(para_text, chunk_size, overlap):
                chunks.append(
                    {
                        "text": subtext,
                        "page": para["page"],
                        "chunk_index": len(chunks),
                    }
                )
            continue

        proposed = current_len + len(para_text) + (2 if current_parts else 0)
        is_header = bool(ARTICLE_HEADER_RE.match(para_text))

        if proposed > chunk_size and current_parts:
            flush_chunk()

        if is_header and current_parts and current_len > chunk_size * 0.6:
            flush_chunk()

        current_parts.append({"text": para_text, "page": para["page"]})
        current_len = current_len + len(para_text) + (2 if current_len else 0)

    flush_chunk()
    return chunks


def detect_articles(chunks: list[dict], doc_id: int) -> list[dict]:
    articles = []
    for chunk in chunks:
        text = chunk.get("text", "")
        chunk_id = chunk.get("chunk_id", chunk.get("db_chunk_id", chunk.get("chunk_index")))
        for match in ARTICLE_SCAN_RE.finditer(text):
            article_num = (match.group(1) or "").strip()
            article_title = (match.group(2) or "").strip(" .-–")
            if not article_num:
                continue
            articles.append(
                {
                    "doc_id": doc_id,
                    "article_num": article_num,
                    "article_title": article_title,
                    "chunk_id": chunk_id,
                }
            )
    return articles


def index_document(filepath: str, collection, conn) -> bool:
    global _LAST_INDEX_RESULT

    path = Path(filepath)
    filename = str(path)
    file_hash = _md5_file(path)
    cur = conn.cursor()
    row = cur.execute(
        "SELECT id, file_hash FROM documents WHERE filename = ?",
        (filename,),
    ).fetchone()

    if row and row[1] == file_hash:
        _LAST_INDEX_RESULT = {
            "status": "skipped",
            "filename": filename,
            "chunks": 0,
            "articles": 0,
        }
        return False

    if row:
        _delete_existing_document(conn, collection, row[0])

    pages = extract_text_from_file(filepath)
    full_text = "\n\n".join(page.get("text", "") for page in pages)
    metadata = detect_document_metadata(path.name, full_text)
    chunks = chunk_text(pages, CHUNK_SIZE, CHUNK_OVERLAP)

    cur.execute(
        """
        INSERT INTO documents (
            filename, codi, titol, tipus, any_aprovacio, vigent,
            data_indexat, num_chunks, file_hash
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            filename,
            metadata.get("codi"),
            metadata.get("titol"),
            metadata.get("tipus"),
            metadata.get("any_aprovacio"),
            metadata.get("vigent", 1),
            datetime.utcnow().isoformat(timespec="seconds"),
            len(chunks),
            file_hash,
        ),
    )
    doc_id = cur.lastrowid

    embeddings = _get_embedding_model().encode(
        [chunk["text"] for chunk in chunks],
        show_progress_bar=False,
        convert_to_numpy=True,
    )
    chroma_ids = [f"doc_{doc_id}_chunk_{chunk['chunk_index']}" for chunk in chunks]
    metadatas = [
        {
            "doc_id": doc_id,
            "doc_codi": metadata.get("codi") or "",
            "doc_titol": metadata.get("titol") or path.name,
            "page": int(chunk.get("page", 0) or 0),
            "chunk_index": int(chunk["chunk_index"]),
            "vigent": int(metadata.get("vigent", 1)),
        }
        for chunk in chunks
    ]

    collection.upsert(
        ids=chroma_ids,
        documents=[chunk["text"] for chunk in chunks],
        metadatas=metadatas,
        embeddings=embeddings.tolist(),
    )

    for chunk, chroma_id in zip(chunks, chroma_ids):
        cur.execute(
            "INSERT INTO chunks (doc_id, chunk_index, text, page_num, chroma_id) VALUES (?, ?, ?, ?, ?)",
            (
                doc_id,
                chunk["chunk_index"],
                chunk["text"],
                chunk.get("page", 0),
                chroma_id,
            ),
        )
        chunk["chunk_id"] = cur.lastrowid

    articles = detect_articles(chunks, doc_id)
    for article in articles:
        cur.execute(
            "INSERT INTO articles (doc_id, article_num, article_title, chunk_id) VALUES (?, ?, ?, ?)",
            (
                article["doc_id"],
                article["article_num"],
                article["article_title"],
                article["chunk_id"],
            ),
        )

    conn.commit()
    label = metadata.get("codi") or path.name
    print(f"✓ {label} -> {len(chunks)} chunks, {len(articles)} articles")

    _LAST_INDEX_RESULT = {
        "status": "indexed",
        "filename": filename,
        "doc_id": doc_id,
        "chunks": len(chunks),
        "articles": len(articles),
    }
    return True


def index_folder(folder: str) -> dict:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        _init_db(conn)
        client = chromadb.PersistentClient(path=CHROMA_PATH)
        collection = client.get_or_create_collection(name="normativa")

        indexed_docs = 0
        skipped_docs = 0
        errors = 0
        chunk_total = 0
        article_total = 0

        for path in sorted(Path(folder).rglob("*")):
            if not path.is_file() or path.suffix.lower() not in {".pdf", ".docx"}:
                continue

            try:
                indexed = index_document(str(path), collection, conn)
                if indexed:
                    indexed_docs += 1
                    chunk_total += int(_LAST_INDEX_RESULT.get("chunks", 0))
                    article_total += int(_LAST_INDEX_RESULT.get("articles", 0))
                else:
                    skipped_docs += 1
            except Exception as exc:
                errors += 1
                conn.rollback()
                print(f"ERROR indexant {path.name}: {type(exc).__name__}: {exc}")

        print(f"Indexats: {indexed_docs} documents | {chunk_total:,} chunks | {article_total:,} articles")
        print(f"Ja indexats (sense canvis): {skipped_docs} documents")
        print(f"Errors: {errors} documents")

        return {
            "indexed": indexed_docs,
            "skipped": skipped_docs,
            "errors": errors,
            "chunks": chunk_total,
            "articles": article_total,
        }
    finally:
        conn.close()


def _init_db(conn: sqlite3.Connection) -> None:
    for statement in SCHEMA:
        conn.execute(statement)
    conn.commit()


def _delete_existing_document(conn: sqlite3.Connection, collection, doc_id: int) -> None:
    cur = conn.cursor()
    chroma_ids = [
        row[0]
        for row in cur.execute("SELECT chroma_id FROM chunks WHERE doc_id = ? AND chroma_id IS NOT NULL", (doc_id,))
    ]
    if chroma_ids:
        try:
            collection.delete(ids=chroma_ids)
        except Exception:
            pass

    cur.execute("DELETE FROM articles WHERE doc_id = ?", (doc_id,))
    cur.execute("DELETE FROM chunks WHERE doc_id = ?", (doc_id,))
    cur.execute("DELETE FROM documents WHERE id = ?", (doc_id,))
    conn.commit()


def _paragraphs_from_pages(pages: list[dict]) -> list[dict]:
    paragraphs = []
    for page in pages:
        text = (page.get("text") or "").replace("\r", "")
        raw_parts = [part.strip() for part in re.split(r"\n\s*\n", text) if part.strip()]
        if not raw_parts:
            raw_parts = [line.strip() for line in text.splitlines() if line.strip()]
        for part in raw_parts:
            paragraphs.append({"text": part, "page": page.get("page", 0)})
    return paragraphs


def _split_long_paragraph(text: str, chunk_size: int, overlap: int) -> list[str]:
    parts = []
    remaining = text.strip()
    while len(remaining) > chunk_size:
        cut = max(0, remaining.rfind(" ", 0, chunk_size))
        if cut < chunk_size * 0.6:
            cut = chunk_size
        part = remaining[:cut].strip()
        parts.append(part)
        remaining = remaining[max(0, cut - overlap):].strip()
    if remaining:
        parts.append(remaining)
    return parts


def _get_embedding_model() -> SentenceTransformer:
    global _MODEL
    if _MODEL is None:
        _MODEL = SentenceTransformer(EMBEDDING_MODEL)
    return _MODEL


def _md5_file(path: Path) -> str:
    hasher = hashlib.md5()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _detect_title(text: str) -> str:
    for line in (text or "").splitlines():
        clean = re.sub(r"\s+", " ", line).strip()
        if len(clean) >= 12:
            return clean[:300]
    return ""


def _load_norm_catalog() -> dict:
    json_path = Path(__file__).resolve().parent / "normativa_annexes.json"
    derogada_aliases: set[str] = set()
    if not json_path.exists():
        return {"derogada_aliases": derogada_aliases}

    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
    except Exception:
        return {"derogada_aliases": derogada_aliases}

    for entry in data.get("normativa_derogada", []):
        for blob in [entry.get("codi", ""), entry.get("text", "")]:
            alias = _norm(blob)
            if alias:
                derogada_aliases.add(alias)
            num = _extract_numeric_code(blob)
            if num:
                derogada_aliases.add(num)

    return {"derogada_aliases": derogada_aliases}


def _clean_code(text: str, tipus: str | None) -> str:
    raw = re.sub(r"\s+", " ", text or "").strip(" .,-")
    num = _extract_numeric_code(raw)
    if tipus == "UNE":
        return raw.upper().replace(" ", "-")
    if tipus and num:
        prefix = {"RD": "RD", "Llei": "Llei", "Decret": "Decret", "Ordre": "Ordre"}.get(tipus, tipus)
        return f"{prefix} {num}"
    return raw


def _extract_numeric_code(text: str) -> str | None:
    match = re.search(r"\b(\d+/\d{4})\b", text or "", re.IGNORECASE)
    return match.group(1) if match else None


def _norm(text: str) -> str:
    base = unicodedata.normalize("NFKD", text or "")
    base = "".join(ch for ch in base if not unicodedata.combining(ch))
    base = base.lower()
    base = re.sub(r"\s+", " ", base)
    return base.strip()


if __name__ == "__main__":
    import sys

    # If a folder argument is given, index only that folder.
    # Otherwise, index all folders listed in NORMATIVA_FOLDERS.
    if len(sys.argv) > 1:
        index_folder(sys.argv[1])
    else:
        totals: dict[str, int] = {"indexed": 0, "skipped": 0, "errors": 0, "chunks": 0, "articles": 0}
        for folder in NORMATIVA_FOLDERS:
            if not os.path.isdir(folder):
                print(f"[SKIP] {folder} (no existeix)")
                continue
            print(f"\n--- Indexant {folder} ---")
            result = index_folder(folder)
            for k in totals:
                totals[k] += result.get(k, 0)
        print(
            f"\nTotal: {totals['indexed']} indexats, "
            f"{totals['skipped']} sense canvis, "
            f"{totals['errors']} errors, "
            f"{totals['chunks']:,} chunks"
        )