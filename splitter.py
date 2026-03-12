"""
splitter.py — Divisio del projecte en blocs PDF.

Regles principals:
- Els marcadors no s'utilitzen directament: primer es validen amb text real
  de portada (DOCUMENT NUM. X ...).
- Els annexos es detecten en portades "quasi buides" on al cos de pagina
  apareix una linia que comenca amb "ANNEX ...".
"""

from __future__ import annotations

import re
import os
import unicodedata
from dataclasses import dataclass
from pathlib import Path

import fitz


@dataclass(frozen=True)
class MainDocDef:
    key: str
    label: str
    doc_num: int
    output_suffix: str
    title_patterns: tuple[str, ...]
    cover_keywords: tuple[str, ...]


MAIN_DOCS: tuple[MainDocDef, ...] = (
    MainDocDef(
        key="doc1",
        label="Memoria i Annexos",
        doc_num=1,
        output_suffix="memoria",
        title_patterns=(r"doc[-_ ]?0*1", r"document\s+num\.?\s*1"),
        cover_keywords=("memoria", "annex"),
    ),
    MainDocDef(
        key="doc2",
        label="Planols",
        doc_num=2,
        output_suffix="planols",
        title_patterns=(r"doc[-_ ]?0*2", r"document\s+num\.?\s*2"),
        cover_keywords=("planol",),
    ),
    MainDocDef(
        key="doc3",
        label="Plec",
        doc_num=3,
        output_suffix="plec",
        title_patterns=(r"doc[-_ ]?0*3", r"document\s+num\.?\s*3"),
        cover_keywords=("ppt", "plec"),
    ),
    MainDocDef(
        key="doc4",
        label="Pressupost",
        doc_num=4,
        output_suffix="pressupost",
        title_patterns=(r"doc[-_ ]?0*4", r"document\s+num\.?\s*4"),
        cover_keywords=("pressupost",),
    ),
)


def split_project_pdf(pdf_path: Path, output_dir: Path | None = None, verbose: bool = False) -> dict:
    output_dir = _resolve_split_output_dir(pdf_path, output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    doc = fitz.open(str(pdf_path))
    try:
        if verbose:
            print(f"  [split] Obrint PDF de {len(doc)} pagines")

        structure = detect_structure_from_doc(doc, verbose=verbose)
        notes = list(structure.get("notes", []))
        parts: list[dict] = []

        documents = structure.get("documents", [])
        memoria_docs = [d for d in documents if d.get("key") == "memoria"]
        other_docs = [d for d in documents if d.get("key") != "memoria"]

        for section in memoria_docs:
            parts.append(
                _export_part(
                    doc,
                    pdf_path,
                    output_dir,
                    label=section["label"],
                    start_page=section["start_page"],
                    end_page=section["end_page"],
                    suffix=section["suffix"],
                    source=section["source"],
                    verbose=verbose,
                )
            )

        for ann in structure.get("annexes", []):
            ann_suffix = _annex_output_suffix(ann["number"], ann["title"])
            parts.append(
                _export_part(
                    doc,
                    pdf_path,
                    output_dir,
                    label=ann["label"],
                    start_page=ann["start_page"],
                    end_page=ann["end_page"],
                    suffix=ann_suffix,
                    source=ann["source"],
                    verbose=verbose,
                )
            )

        for section in other_docs:
            parts.append(
                _export_part(
                    doc,
                    pdf_path,
                    output_dir,
                    label=section["label"],
                    start_page=section["start_page"],
                    end_page=section["end_page"],
                    suffix=section["suffix"],
                    source=section["source"],
                    verbose=verbose,
                )
            )

        starts = [
            {
                "key": a["key"],
                "label": a["label"],
                "page": a["page"],
                "source": a["source"],
            }
            for a in structure.get("anchors", [])
        ]

        return {
            "parts": parts,
            "starts": starts,
            "notes": notes,
            "method": "anchors-doc-num+annex-covers",
            "page_count": len(doc),
            "output_dir": output_dir,
            "structure": structure,
        }
    finally:
        doc.close()


def _resolve_split_output_dir(pdf_path: Path, output_dir: Path | None) -> Path:
    base_dir = Path(output_dir) if output_dir is not None else pdf_path.parent

    try:
        same_as_parent = base_dir.resolve() == pdf_path.parent.resolve()
    except Exception:
        same_as_parent = str(base_dir) == str(pdf_path.parent)

    if same_as_parent:
        stem_slug = _slug(pdf_path.stem) or "pdf"
        return pdf_path.parent / f"parts_{stem_slug}"

    return base_dir


def detect_structure_from_doc(doc, verbose: bool = False) -> dict:
    notes: list[str] = []
    anchors = _detect_main_doc_anchors(doc, verbose=verbose, notes=notes)

    result = {
        "anchors": [],
        "documents": [],
        "annexes": [],
        "notes": notes,
    }

    for key, val in anchors.items():
        result["anchors"].append(
            {
                "key": key,
                "label": val["label"],
                "page": val["page"],
                "source": val["source"],
            }
        )

    if "doc1" not in anchors:
        notes.append("No s'ha pogut detectar la portada del DOCUMENT NUM. 1")
        return result

    doc1_start = anchors["doc1"]["page"]
    doc2_start = anchors.get("doc2", {}).get("page")
    doc3_start = anchors.get("doc3", {}).get("page")
    doc4_start = anchors.get("doc4", {}).get("page")

    doc1_end = _next_boundary([doc2_start, doc3_start, doc4_start], default=len(doc)) - 1
    annex_starts, annex_notes = _detect_annex_cover_starts(doc, doc1_start, doc1_end, verbose=verbose)
    notes.extend(annex_notes)

    if annex_starts:
        memoria_end = annex_starts[0]["page"] - 1
        if memoria_end >= doc1_start:
            result["documents"].append(
                {
                    "key": "memoria",
                    "label": "Memoria",
                    "start_page": doc1_start,
                    "end_page": memoria_end,
                    "source": anchors["doc1"]["source"],
                    "suffix": "memoria",
                }
            )
    else:
        notes.append("No s'han detectat portades d'annex; DOC-01 s'exporta com a memoria unica")
        result["documents"].append(
            {
                "key": "memoria",
                "label": "Memoria",
                "start_page": doc1_start,
                "end_page": doc1_end,
                "source": anchors["doc1"]["source"],
                "suffix": "memoria",
            }
        )

    for i, ann in enumerate(annex_starts):
        ann_start = ann["page"]
        ann_end = annex_starts[i + 1]["page"] - 1 if i + 1 < len(annex_starts) else doc1_end
        if ann_end < ann_start:
            continue

        result["annexes"].append(
            {
                "key": f"annex_{ann['number']:02d}",
                "label": ann["title"],
                "title": ann["title"],
                "number": ann["number"],
                "start_page": ann_start,
                "end_page": ann_end,
                "source": ann["source"],
            }
        )

    if doc2_start:
        doc2_end = _next_boundary([doc3_start, doc4_start], default=len(doc)) - 1
        result["documents"].append(
            {
                "key": "planols",
                "label": "Planols",
                "start_page": doc2_start,
                "end_page": doc2_end,
                "source": anchors["doc2"]["source"],
                "suffix": "planols",
            }
        )
    else:
        notes.append("No s'ha detectat inici de Planols (DOCUMENT NUM. 2)")

    if doc3_start:
        doc3_end = _next_boundary([doc4_start], default=len(doc)) - 1
        result["documents"].append(
            {
                "key": "plec",
                "label": "Plec",
                "start_page": doc3_start,
                "end_page": doc3_end,
                "source": anchors["doc3"]["source"],
                "suffix": "plec",
            }
        )
    else:
        notes.append("No s'ha detectat inici de Plec (DOCUMENT NUM. 3)")

    if doc4_start:
        result["documents"].append(
            {
                "key": "pressupost",
                "label": "Pressupost",
                "start_page": doc4_start,
                "end_page": len(doc),
                "source": anchors["doc4"]["source"],
                "suffix": "pressupost",
            }
        )
    else:
        notes.append("No s'ha detectat inici de Pressupost (DOCUMENT NUM. 4)")

    return result


def _detect_main_doc_anchors(doc, verbose: bool, notes: list[str]) -> dict[str, dict]:
    toc = doc.get_toc(simple=True)
    if verbose:
        print(f"  [split] TOC detectat: {len(toc)} marcadors")

    normalized_toc = []
    for level, title, page in toc:
        if page < 1 or page > len(doc):
            continue
        normalized_toc.append((level, title or "", page, _norm(title or "")))

    anchors: dict[str, dict] = {}
    min_page = 1

    for section in MAIN_DOCS:
        page = None
        source = None

        for _, raw_title, raw_page, norm_title in normalized_toc:
            if raw_page < min_page:
                continue
            if any(re.search(pat, norm_title, flags=re.IGNORECASE) for pat in section.title_patterns):
                if _validate_doc_cover(doc, raw_page, section):
                    page = raw_page
                    source = "bookmark_validat"
                    if verbose:
                        print(
                            f"  [split] {section.label}: marcador validat p. {raw_page} "
                            f"(titol: {raw_title})"
                        )
                    break

        if page is None:
            page = _find_doc_cover_by_text(doc, section, min_page=min_page)
            if page is not None:
                source = "text_document_cover"
                if verbose:
                    print(f"  [split] {section.label}: detectat per text a p. {page}")

        if page is not None:
            anchors[section.key] = {
                "label": section.label,
                "page": page,
                "source": source,
            }
            min_page = page + 1
        else:
            notes.append(f"No detectat inici de {section.label}")
            if verbose:
                print(f"  [split] {section.label}: no detectat")

    return anchors


def _validate_doc_cover(doc, page_num: int, section: MainDocDef) -> bool:
    windows = [page_num]
    if page_num + 1 <= len(doc):
        windows.append(page_num + 1)

    for p in windows:
        text = _norm(doc[p - 1].get_text("text") or "")
        if not _contains_document_number(text, section.doc_num):
            continue

        if section.doc_num == 3:
            # Al PDF real apareix "PPT"; no exigim explicitament "plec".
            if any(k in text for k in section.cover_keywords):
                return True
            return True

        if any(k in text for k in section.cover_keywords):
            return True

    return False


def _find_doc_cover_by_text(doc, section: MainDocDef, min_page: int) -> int | None:
    for p in range(max(1, min_page), len(doc) + 1):
        text = _norm(doc[p - 1].get_text("text") or "")
        if not _contains_document_number(text, section.doc_num):
            continue

        if section.doc_num == 3:
            return p

        if any(k in text for k in section.cover_keywords):
            return p
    return None


def _contains_document_number(text: str, num: int) -> bool:
    return bool(
        re.search(rf"\bdocument\b.{{0,30}}\b{num}\b", text)
        or re.search(rf"doc\s*[-_ ]?0*{num}", text)
    )


def _detect_annex_cover_starts(
    doc,
    start_page: int,
    end_page: int,
    verbose: bool = False,
) -> tuple[list[dict], list[str]]:
    starts: list[dict] = []
    notes: list[str] = []

    raw_candidates: list[dict] = []
    total_scan = end_page - start_page + 1

    for p in range(start_page, end_page + 1):
        if (p - start_page) % 200 == 0 or p == end_page:
            pct = ((p - start_page) / total_scan) * 100
            print(f"\r  🔎 Escanejant annexos: p. {p}/{end_page} ({pct:.0f}%)", end="", flush=True)

        title = _extract_annex_cover_title(doc, p)
        if not title:
            continue

        raw_candidates.append(
            {
                "page": p,
                "title": title,
                "number": _extract_annex_number(title),
            }
        )

    print()  # nova linia despres del \r

    expected_number = 1
    for cand in raw_candidates:
        p = cand["page"]
        title = cand["title"]
        number = cand["number"]

        if number is None:
            notes.append(f"Portada d'annex sense numero recognoscible a p. {p}: {title}")
            if verbose:
                print(f"  [split] Descartat p. {p}: annex sense numero ({title})")
            continue

        if starts and p - starts[-1]["page"] <= 1 and number == starts[-1]["number"]:
            # Evita duplicats de la mateixa portada/continuacio immediata
            # amb el mateix numero d'annex.
            if _is_more_descriptive_annex_title(title, starts[-1]["title"], number):
                starts[-1]["title"] = title
            continue

        if number < expected_number:
            notes.append(
                f"Descartat annex {number} a p. {p}: annex esperat {expected_number}"
            )
            if verbose:
                print(
                    f"  [split] Descartat p. {p}: annex {number} no coincideix "
                    f"amb annex esperat {expected_number}"
                )
            continue

        if number > expected_number:
            notes.append(
                f"No detectats annexos {expected_number}-{number - 1} abans de p. {p}"
            )
            if verbose:
                print(
                    f"  [split] Avanc de numeracio: annex esperat {expected_number}, "
                    f"detectat {number} (p. {p})"
                )

        starts.append(
            {
                "page": p,
                "title": title,
                "source": "cover_annex_text",
                "number": number,
            }
        )
        expected_number = number + 1

        if verbose:
            print(f"  [split] Annex detectat a p. {p}: {title} [n={number}]")

    return starts, notes


def _extract_annex_cover_title(doc, page_num: int) -> str | None:
    body_text = _page_body_text(doc, page_num)
    if not body_text.strip():
        return None

    body_lines = [ln.strip() for ln in body_text.splitlines() if ln.strip()]
    if not body_lines:
        return None

    # Portada d'annex: cos gairebe buit + linia ANNEX ...
    compact = " ".join(body_lines)
    compact_norm = _norm(compact)
    if len(compact_norm) > 280:
        return None
    if len(body_lines) > 8:
        return None

    for idx, ln in enumerate(body_lines):
        ln_norm = _norm(ln)
        if re.match(r"^annex\b", ln_norm):
            base = _clean_title_line(ln)
            number = _extract_annex_number(base)
            inline_core = _annex_title_core(base, number)
            if inline_core:
                return base

            if idx + 1 < len(body_lines):
                nxt = _clean_title_line(body_lines[idx + 1])
                if nxt and not re.match(r"^annex\b", _norm(nxt)):
                    return _clean_title_line(f"{base} - {nxt}")

            return base

    return None


def _page_body_text(doc, page_num: int) -> str:
    page = doc[page_num - 1]
    rect = page.rect
    h = rect.height

    # Excloem capcalera/peu on acostuma a haver-hi text repetit.
    body_rect = fitz.Rect(rect.x0, rect.y0 + h * 0.12, rect.x1, rect.y1 - h * 0.12)
    return page.get_text("text", clip=body_rect) or ""


def _export_part(
    doc,
    pdf_path: Path,
    output_dir: Path,
    label: str,
    start_page: int,
    end_page: int,
    suffix: str,
    source: str,
    verbose: bool,
) -> dict:
    out_name = f"{pdf_path.stem}_{suffix}.pdf"
    out_path = output_dir / out_name
    tmp_path = output_dir / f".{out_name}.tmp"

    out_doc = fitz.open()
    try:
        out_doc.insert_pdf(doc, from_page=start_page - 1, to_page=end_page - 1)
        if tmp_path.exists():
            tmp_path.unlink()
        # garbage=1 removes orphaned objects (correctness); skip deflate to avoid
        # re-compressing every image stream which hangs on large sections.
        out_doc.save(str(tmp_path), garbage=1, deflate=False)
    finally:
        out_doc.close()

    import time

    max_retries = 5
    for attempt in range(max_retries):
        try:
            os.replace(tmp_path, out_path)
            break
        except PermissionError:
            if attempt < max_retries - 1:
                time.sleep(1)
                continue

            fallback_path = output_dir / f"{out_path.stem}__new{out_path.suffix}"
            if fallback_path.exists():
                fallback_path.unlink()
            os.replace(tmp_path, fallback_path)
            out_path = fallback_path
            break

    if verbose:
        print(f"  [split] Exportat {label:<24} p. {start_page}-{end_page} -> {out_path.name}")

    return {
        "label": label,
        "start_page": start_page,
        "end_page": end_page,
        "output": out_path,
        "source": source,
    }


def _next_boundary(candidates: list[int | None], default: int) -> int:
    vals = [v for v in candidates if isinstance(v, int) and v is not None]
    return min(vals) if vals else default


def _clean_title_line(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", text.strip())
    return cleaned[:120]


def _extract_annex_number(title: str) -> int | None:
    norm = _norm(title)
    # Formats admesos: "ANNEX 3", "ANNEX N. 3", "ANNEX NUM. 3", "ANNEX NÚM. 3", "ANNEX Nº 3", "ANNEX NÚMERO 3".
    m = re.search(r"^annex\s+(?:(?:n(?:um(?:ero)?)?|num|no|numero)\.?\s*)?0*(\d{1,2})\b", norm)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            pass

    # Fallback: numeracio romana (p. ex. "ANNEX II").
    m = re.search(r"^annex\s+(?:(?:n(?:um(?:ero)?)?|num|no|numero)\.?\s*)?([ivxlcdm]{1,6})\b", norm)
    if not m:
        return None

    value = _roman_to_int(m.group(1))
    if value is None or value < 1 or value > 99:
        return None
    return value


def _roman_to_int(text: str) -> int | None:
    values = {
        "i": 1,
        "v": 5,
        "x": 10,
        "l": 50,
        "c": 100,
        "d": 500,
        "m": 1000,
    }

    s = (text or "").strip().lower()
    if not s:
        return None
    if any(ch not in values for ch in s):
        return None

    total = 0
    prev = 0
    for ch in reversed(s):
        cur = values[ch]
        if cur < prev:
            total -= cur
        else:
            total += cur
            prev = cur

    return total


def _slug(text: str) -> str:
    t = _norm(text)
    t = re.sub(r"[^a-z0-9]+", "_", t)
    return t.strip("_")[:48]


def _annex_output_suffix(number: int, title: str) -> str:
    base = f"annex_{number:02d}"
    title_core = _annex_title_core(title, number)
    title_slug = _slug(title_core)
    if not title_slug:
        return base
    return f"{base}_{title_slug}"


def _annex_title_core(title: str, number: int | None = None) -> str:
    text = _clean_title_line(title or "")
    text = re.sub(
        r"^\s*annex\s+(?:(?:n(?:[úu]m(?:ero)?)?|num|numero|n[º°o])\.?\s*)?\d{1,2}\s*[-:–.]?\s*",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"^\s*annex\s*[-:–.]?\s*", "", text, flags=re.IGNORECASE)
    if number is not None:
        text = re.sub(rf"^\s*0*{number}\s*[-:–.]?\s*", "", text)
    text = text.strip(" -–:._")

    # Evita tornar a generar sufixos genericos com annex_num_01.
    generic_slug = _slug(text)
    if re.fullmatch(r"annex(?:_num)?_\d{1,2}", generic_slug):
        return ""
    return text


def _is_more_descriptive_annex_title(candidate: str, current: str, number: int | None = None) -> bool:
    cand_core = _annex_title_core(candidate, number)
    curr_core = _annex_title_core(current, number)
    if not cand_core:
        return False
    if not curr_core:
        return True
    return len(cand_core) > len(curr_core)


def _norm(text: str) -> str:
    text = text.replace("\ufffd", " ")
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"\s+", " ", text).strip()
    return text
