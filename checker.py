"""
checker.py — Orquestrador principal: carrega el PDF i executa tots els checks
"""

import os
import re
import sys
import inspect

# Assegura que el directori del script és al path (necessari a Windows)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import fitz  # PyMuPDF
from pathlib import Path

from checks.blank_pages   import check_blank_pages
from checks.pagination    import check_pagination
from checks.language      import check_castellanismes, check_abreviatures
from checks.normativa     import check_normativa_derogada, check_banc_preus
from checks.normativa_taula import check_normativa_taula
from checks.geotecnia     import check_geotecnia
from checks.imports       import check_imports
from checks.terminis      import check_terminis
from checks.annex_map     import build_annex_map
from checks.bookmarks     import check_bookmarks
from checks.signatures    import check_signatures
from checks.documents     import check_documents_obligatoris
from context_cache        import cache_path_for_pdf, fingerprint_pdf, load_cache, save_cache
from splitter             import detect_structure_from_doc
from config               import LOW_TEXT_DOC_KEYS


def _norm_db_available() -> bool:
    try:
        from norm_checker import norm_db_available

        return norm_db_available()
    except Exception:
        return False


def _run_norm_db_check(pages: list[dict], annex_map: dict) -> list[dict]:
    from norm_checker import check_all_references

    return check_all_references(pages, annex_map)


def _dedup_derogated_findings(results: list[dict]) -> list[dict]:
    """
    Remove NT-xx DEROGADA findings from normativa_taula that duplicate an
    existing AG-13 finding from normativa.  Comparison is by normalised
    number/year token (e.g. '304/2002') extracted from the «…» quote in
    the descrip field.
    """
    _QUOTE_RE = re.compile(r"«(.+?)»")
    _NUM_YEAR_RE = re.compile(r"\d{1,4}/\d{4}")

    def _tokens(text: str) -> set[str]:
        """Return all N/YYYY tokens found in text (lowercased)."""
        return {m.lower() for m in _NUM_YEAR_RE.findall(text)}

    # Collect all numeric tokens referenced by AG-13 findings
    ag13_tokens: set[str] = set()
    for row in results:
        for f in row.get("findings", []):
            if f.get("item", "").startswith("AG-13") and f.get("status") == "NO OK":
                m = _QUOTE_RE.search(f.get("descrip", ""))
                if m:
                    ag13_tokens |= _tokens(m.group(1))

    if not ag13_tokens:
        return results   # nothing to dedup

    # Filter NT-xx DEROGADA findings whose reference overlaps with AG-13 tokens
    _NT_TITLE = "📋 Taula de normativa"
    for row in results:
        if row.get("title") != _NT_TITLE:
            continue
        filtered = []
        for f in row["findings"]:
            if (
                f.get("item", "").startswith("NT-")
                and f.get("status") == "NO OK"
                and "derogad" in f.get("descrip", "").lower()
            ):
                m = _QUOTE_RE.search(f.get("descrip", ""))
                ref_tokens = _tokens(m.group(1)) if m else set()
                if ref_tokens & ag13_tokens:
                    continue   # duplicate — skip
            filtered.append(f)
        row["findings"] = filtered

    return results


class ProjectChecker:
    def __init__(
        self,
        pdf_path: Path,
        verbose: bool = False,
        use_cache: bool = True,
        rebuild_cache: bool = False,
        full_text_all: bool = False,
    ):
        self.pdf_path = pdf_path
        self.verbose  = verbose
        self.use_cache = use_cache
        self.rebuild_cache = rebuild_cache
        self.full_text_all = full_text_all
        self.cache_path = cache_path_for_pdf(pdf_path)
        self.doc      = fitz.open(str(pdf_path))
        self.annex_map = {}
        self.map_findings = []

        print(f"  📖 Carregant PDF ({len(self.doc)} pàgines)...", flush=True)
        self.pages, self.structure = self._load_or_build_context()
        print(f"  ✔  Context de text i estructura preparat\n", flush=True)

    def _load_or_build_context(self) -> tuple[list[dict], dict]:
        current_fp = fingerprint_pdf(self.pdf_path)
        extraction_profile = {
            "full_text_all": self.full_text_all,
            "low_text_doc_keys": sorted([] if self.full_text_all else LOW_TEXT_DOC_KEYS),
        }

        if self.use_cache and not self.rebuild_cache:
            cached = load_cache(self.cache_path)
            if (
                cached
                and cached.get("fingerprint") == current_fp
                and cached.get("page_count") == len(self.doc)
                and cached.get("extraction_profile") == extraction_profile
                and isinstance(cached.get("pages"), list)
            ):
                if self.verbose:
                    print(f"  ⚡ Reutilitzant cache: {self.cache_path.name}")
                return cached["pages"], cached.get("structure", {})

        print("  🔎 Detectant estructura del projecte (documents i annexos)...", flush=True)
        structure = detect_structure_from_doc(self.doc, verbose=self.verbose)

        print("  ⏳ Extraient text de totes les pàgines...", flush=True)
        pages = self._extract_pages(structure)

        if self.use_cache:
            payload = {
                "fingerprint": current_fp,
                "page_count": len(self.doc),
                "extraction_profile": extraction_profile,
                "pages": pages,
                "structure": structure,
            }
            save_cache(self.cache_path, payload)
            if self.verbose:
                print(f"  💾 Cache actualitzada: {self.cache_path.name}")

        return pages, structure

    def _extract_pages(self, structure: dict) -> list[dict]:
        low_text_pages = set()
        if not self.full_text_all:
            low_text_doc_keys = set(LOW_TEXT_DOC_KEYS)
            for section in structure.get("documents", []):
                if section.get("key") in low_text_doc_keys:
                    for p in range(section["start_page"], section["end_page"] + 1):
                        low_text_pages.add(p)

        # Portades estructurals (secció + annexos): la pàgina de coberta i la
        # pàgina en blanc que la precedeix en impressió a doble cara.
        cover_pages: set[int] = set()
        for section in structure.get("documents", []):
            p = section["start_page"]
            cover_pages.add(p)
            if p > 1:
                cover_pages.add(p - 1)
        for ann in structure.get("annexes", []):
            p = ann["start_page"]
            cover_pages.add(p)
            if p > 1:
                cover_pages.add(p - 1)

        pages = []
        total_pages = len(self.doc)

        for i, page in enumerate(self.doc, 1):
            is_low_text = i in low_text_pages
            rect      = page.rect
            h = rect.height

            header_rect = fitz.Rect(rect.x0, rect.y0,        rect.x1, rect.y0 + h * 0.10)
            footer_rect = fitz.Rect(rect.x0, rect.y1 - h * 0.10, rect.x1, rect.y1)
            body_rect   = fitz.Rect(rect.x0, rect.y0 + h*0.10, rect.x1, rect.y1 - h*0.10)

            header_text = page.get_text("text", clip=header_rect)
            footer_text = page.get_text("text", clip=footer_rect)
            body_text   = page.get_text("text", clip=body_rect)

            if is_low_text:
                # Per planols/pressupost fem text reduit per estalviar memoria i temps.
                body_lines = [ln.strip() for ln in body_text.splitlines() if ln.strip()]
                body_text = "\n".join(body_lines[:8])

            # Construim el text complet a partir de les 3 zones (evitem una extraccio redundant)
            text = "\n".join(filter(None, [header_text.strip(), body_text.strip(), footer_text.strip()]))

            char_count = len(text.replace(" ", "").replace("\n", ""))

            pages.append({
                "num":        i,
                "text":       text,
                "header":     header_text,
                "footer":     footer_text,
                "body":       body_text,
                "char_count": char_count,
                "skip_text_checks": is_low_text,
                "is_cover":   i in cover_pages,
            })

            if i == 1 or i % 50 == 0 or i == total_pages:
                pct = (i / total_pages) * 100
                print(f"\r  ⏳ Extraccio: pagina {i}/{total_pages} ({pct:5.1f}%)", end="", flush=True)

        print()

        return pages

    def _pages_in_range(self, start_page: int, end_page: int) -> list[dict]:
        start_idx = max(0, start_page - 1)
        end_idx = min(len(self.pages), end_page)
        return self.pages[start_idx:end_idx]

    def _hybrid_scopes(self, include_low_text: bool = True) -> list[dict]:
        structure = self.structure or {}
        scopes = []
        low_keys = set(LOW_TEXT_DOC_KEYS)

        for doc in structure.get("documents", []):
            if not include_low_text and doc.get("key") in low_keys:
                continue
            scopes.append({
                "key": doc.get("key"),
                "label": doc["label"],
                "start_page": doc["start_page"],
                "end_page": doc["end_page"],
            })

        for ann in structure.get("annexes", []):
            scopes.append({
                "key": ann.get("key"),
                "label": f"Annex {ann['number']:02d}",
                "start_page": ann["start_page"],
                "end_page": ann["end_page"],
            })

        scopes.sort(key=lambda s: s["start_page"])
        return scopes

    def _run_pagination_hybrid(self) -> list[dict]:
        scopes = self._hybrid_scopes(include_low_text=True)
        if not scopes:
            return check_pagination(self.pages, self.doc)

        findings = []
        scope_issues = 0

        for scope in scopes:
            sub_pages = self._pages_in_range(scope["start_page"], scope["end_page"])
            local_findings = check_pagination(sub_pages, self.doc)
            local_nook = [f for f in local_findings if f["status"] == "NO OK"]
            if not local_nook:
                continue

            scope_issues += 1
            for finding in local_nook:
                f = dict(finding)
                f["descrip"] = f"[{scope['label']}] {finding['descrip']}"
                findings.append(f)

        if not findings:
            return [{
                "status": "OK",
                "item": "AG-15",
                "descrip": "No s'han detectat incidències de paginacio als ambits detectats",
                "detall": f"Ambits analitzats: {len(scopes)}",
                "ref": "Checklist Aspectes Generics, item 15",
            }]

        findings.insert(0, {
            "status": "INFO",
            "item": "AG-15",
            "descrip": f"Analisi hibrida de paginacio: {scope_issues}/{len(scopes)} ambits amb incidencies",
            "detall": "Resultat calculat per seccions i annexos a partir de context estructurat",
            "ref": "Checklist Aspectes Generics, item 15",
        })
        return findings

    def _run_castellanismes_hybrid(self) -> list[dict]:
        scopes = self._hybrid_scopes(include_low_text=False)
        if not scopes:
            return check_castellanismes(self.pages, self.doc)

        findings = []
        pattern_scopes: dict[str, set] = {}

        for scope in scopes:
            sub_pages = self._pages_in_range(scope["start_page"], scope["end_page"])
            local_findings = check_castellanismes(sub_pages, self.doc)

            for finding in local_findings:
                if finding["status"] != "NO OK":
                    continue

                f = dict(finding)
                f["descrip"] = f"[{scope['label']}] {finding['descrip']}"
                findings.append(f)

                m = re.search(r"«([^»]+)»", finding.get("descrip", ""))
                if m:
                    pattern = m.group(1)
                    pattern_scopes.setdefault(pattern, set()).add(scope["label"])

        if not findings:
            return [{
                "status": "OK",
                "item": "AG-10",
                "descrip": "No s'han detectat castellanismes als ambits detectats",
                "detall": f"Ambits analitzats: {len(scopes)}",
                "ref": "Checklist Aspectes Generics, item 10",
            }]

        lines = []
        for pattern, labels in sorted(pattern_scopes.items()):
            lines.append(f"  - {pattern}: {len(labels)} ambits")
        findings.insert(0, {
            "status": "INFO",
            "item": "AG-10",
            "descrip": "Analisi hibrida de castellanismes per ambits",
            "detall": "\n".join(lines[:20]) if lines else "Sense resum de patrons",
            "ref": "Checklist Aspectes Generics, item 10",
        })
        return findings

    def _run_abreviatures_hybrid(self) -> list[dict]:
        scopes = self._hybrid_scopes(include_low_text=False)
        if not scopes:
            return check_abreviatures(self.pages, self.doc)

        findings = []
        scope_issues = 0

        for scope in scopes:
            sub_pages = self._pages_in_range(scope["start_page"], scope["end_page"])
            local_findings = check_abreviatures(sub_pages, self.doc)
            local_nook = [f for f in local_findings if f["status"] == "NO OK"]

            if not local_nook:
                continue

            scope_issues += 1
            for finding in local_nook:
                f = dict(finding)
                f["descrip"] = f"[{scope['label']}] {finding['descrip']}"
                findings.append(f)

        if not findings:
            return [{
                "status": "OK",
                "item": "AG-9",
                "descrip": "No s'han detectat inconsistencies d'abreviatures als ambits detectats",
                "detall": f"Ambits analitzats: {len(scopes)}",
                "ref": "Checklist Aspectes Generics, item 9",
            }]

        findings.insert(0, {
            "status": "INFO",
            "item": "AG-9",
            "descrip": f"Analisi hibrida d'abreviatures: {scope_issues}/{len(scopes)} ambits amb incidencies",
            "detall": "Resultat calculat per seccions i annexos a partir de context estructurat",
            "ref": "Checklist Aspectes Generics, item 9",
        })
        return findings

    def run_all_checks(self) -> list[dict]:
        results = []

        print("  🗺  Detectant estructura d'annexes...", end="", flush=True)
        try:
            annex_map, map_findings = build_annex_map(self.pages, self.doc)
            detected = [
                f"{k}→A{v['num']}(p.{v['pages'][0]}-{v['pages'][-1]})"
                for k, v in annex_map.items()
                if v.get("num")
            ]
            print(
                f"\r  🗺  Annexes detectats: {len(annex_map)} "
                f"({', '.join(detected[:5])}{'...' if len(detected) > 5 else ''})"
            )
        except Exception as e:
            annex_map = {}
            map_findings = [{
                "status": "INFO",
                "item": "AM-00",
                "descrip": f"Error construint mapa d'annexes: {e}",
                "detall": "",
                "ref": "",
            }]

        self.annex_map = annex_map
        self.map_findings = map_findings

        results.append({"title": "🗺  Mapa d'annexes detectats", "findings": map_findings})

        checks = [
            ("📄 Documents obligatoris",  lambda: check_documents_obligatoris(self.pages, self.doc)),
            ("🔖 Marcadors (bookmarks)",  lambda: check_bookmarks(self.pages, self.doc, self.structure)),
            ("⬜ Pàgines en blanc",       lambda: check_blank_pages(self.pages, self.doc, annex_map)),
            ("🔢 Paginació (peus pàg.)",  lambda p, d: check_pagination(p, d, annex_map=annex_map)),
            ("✍  Signatures",             lambda: check_signatures(self.pages, self.doc, self.structure)),
            ("🗣 Castellanismes",          self._run_castellanismes_hybrid),
            ("🔤 Abreviatures",           self._run_abreviatures_hybrid),
            ("⚖ Normativa derogada",      lambda: check_normativa_derogada(self.pages, self.doc)),
            ("💰 Banc de preus",          lambda: check_banc_preus(self.pages, self.doc)),
            ("📋 Taula de normativa",     lambda p, d: check_normativa_taula(p, d, annex_map=annex_map)),
            ("🪨 Paràmetres geotècnics",   lambda p, d: check_geotecnia(p, d, annex_map=annex_map)),
            ("💶 Coherència d'imports",   lambda p, d: check_imports(p, d, annex_map=annex_map)),
            ("⏱ Coherència de terminis",  lambda p, d: check_terminis(p, d, annex_map=annex_map)),
        ]

        if _norm_db_available():
            checks.append(
                ("📚 Base normativa local", lambda p, d: _run_norm_db_check(p, annex_map))
            )

        total   = len(checks)
        BAR_W   = 20

        for idx, (title, check_fn) in enumerate(checks, 1):
            done = int((idx - 1) / total * BAR_W)
            bar  = "█" * done + "░" * (BAR_W - done)
            print(f"  [{bar}] {idx}/{total}  {title:<32} ...", end="", flush=True)

            try:
                sig = inspect.signature(check_fn)
                if len(sig.parameters) >= 2:
                    findings = check_fn(self.pages, self.doc)
                else:
                    findings = check_fn()
                results.append({"title": title, "findings": findings})

                nook = len([f for f in findings if f["status"] == "NO OK"])
                done2 = int(idx / total * BAR_W)
                bar2  = "█" * done2 + "░" * (BAR_W - done2)
                tag   = f"❌ {nook} NO OK" if nook else "✅ OK"
                print(f"\r  [{bar2}] {idx}/{total}  {title:<32} {tag}")

            except Exception as e:
                results.append({"title": title, "findings": [{
                    "status": "INFO", "item": "ERROR",
                    "descrip": f"Error executant el mòdul: {e}",
                    "detall": str(type(e).__name__), "ref": ""
                }]})
                print(f"\r  [{'█'*int(idx/total*BAR_W)}{'░'*(BAR_W-int(idx/total*BAR_W))}] {idx}/{total}  {title:<32} ⚠️  ERROR")
                if self.verbose:
                    import traceback; traceback.print_exc()

        # Guardem una sola seccio per titol per evitar duplicacions accidentals.
        unique_results = []
        seen_titles = set()
        for row in results:
            title = row.get("title")
            if title in seen_titles:
                continue
            seen_titles.add(title)
            unique_results.append(row)
        results = unique_results

        results = _dedup_derogated_findings(results)

        print(f"  DEBUG len(results): {len(results)}")
        for r in results:
            print(r["title"])
        print()
        return results
