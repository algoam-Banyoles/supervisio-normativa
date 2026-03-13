"""
norm_index.py — In-memory index of all downloaded normative catalogs.

Loads and indexes:
  • normativa_annexes.json
  • normativa_adif/_catalogo/catalogo_adif.json
  • normativa_iso/_catalogo/catalogo_iso.json
  • normativa_une/_catalogo/catalogo_une.json
  • normativa_dgc/_catalogo/catalogo_completo.json

Provides O(1) lookup of any normative reference by its canonical code,
with fuzzy fallback for near-miss number typos.

Usage:
    from norm_index import NormIndex
    idx = NormIndex(".")           # base_dir = project root
    result = idx.lookup("RD 1627/1997")
    # → {"status": "VIGENT", "source": "ANNEXES", "title": "...", ...}
"""

from __future__ import annotations

import json
import logging
import os
import re

from norm_resolver import normalize_code, resolve

logger = logging.getLogger(__name__)

# ─── Priority order when the same key appears in multiple catalogs ────────────
_PRIORITY = {"DEROGADA": 3, "VIGENT": 2, "REFERENCIA": 1, "PENDENT": 0}


class NormIndex:
    """In-memory lookup index built from all downloaded normative catalogs."""

    def __init__(self, base_dir: str) -> None:
        self._base_dir = os.path.abspath(base_dir)
        # canonical_key → entry dict
        self._index: dict[str, dict] = {}
        # loaded-entry counts per source (includes unresolvable entries)
        self._counts: dict[str, int] = {
            "ANNEXES": 0, "DGC": 0, "ADIF": 0, "ISO": 0, "UNE": 0, "INDUSTRIA": 0
        }
        self._load_all()

    # ─── Internal helpers ─────────────────────────────────────────────────────

    def _store(self, key: str, entry: dict) -> None:
        """Insert *entry* under *key*, letting DEROGADA > VIGENT > REFERENCIA."""
        existing = self._index.get(key)
        if existing is None:
            self._index[key] = entry
        elif _PRIORITY.get(entry["status"], 0) > _PRIORITY.get(existing["status"], 0):
            self._index[key] = entry

    def _index_entry(self, ref_text: str, entry: dict) -> bool:
        """Resolve *ref_text*, store *entry* if resolvable; return True on hit."""
        resolved = resolve(ref_text)
        if resolved is None:
            return False
        self._store(normalize_code(resolved), entry)
        return True

    # ─── Catalog loaders ─────────────────────────────────────────────────────

    def _load_annexes(self) -> None:
        path = os.path.join(self._base_dir, "normativa_annexes.json")
        if not os.path.exists(path):
            logger.warning("normativa_annexes.json not found — skipped")
            return

        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)

        count = 0

        # annex entries (default VIGENT, may be DEROGADA via observacions)
        for annex in data.get("annexes", []):
            norms = annex.get("normativa", [])
            # tolerate flat list at top level too
            if isinstance(annex, dict) and "codi" in annex:
                norms = [annex]
            for norm in norms:
                codi = norm.get("codi", "")
                text = norm.get("text", "")
                obs  = norm.get("observacions", "") or ""

                is_derogada = bool(re.search(
                    r"derog|substitu[ïi]da|anulad|anul[·l]",
                    obs, re.IGNORECASE
                ))
                status = "DEROGADA" if is_derogada else "VIGENT"

                sub = None
                if is_derogada:
                    m = re.search(
                        r"(?:substitu[ïi]da|reemplazada)\s+per\s+(.+?)(?:\.|$)",
                        obs, re.IGNORECASE
                    )
                    if m:
                        sub = m.group(1).strip()

                entry = {
                    "status": status,
                    "source": "ANNEXES",
                    "title": text or codi,
                    "raw_ref": codi,
                    "substituted_by": sub,
                }
                # Try codi first, fall back to first 120 chars of text
                if not self._index_entry(codi, entry):
                    self._index_entry(text[:120], entry)
                count += 1

        # explicitly derogated list
        derogada_list = data.get("normativa_derogada", [])
        # may be a list of entries or a dict organised by id
        if isinstance(derogada_list, dict):
            derogada_list = list(derogada_list.values())
        for norm in derogada_list:
            if not isinstance(norm, dict):
                continue
            codi = norm.get("codi", "")
            text = norm.get("text", "")
            obs  = norm.get("observacions", "") or ""
            entry = {
                "status": "DEROGADA",
                "source": "ANNEXES",
                "title": text or codi,
                "raw_ref": codi,
                "substituted_by": obs or None,
            }
            if not self._index_entry(codi, entry):
                self._index_entry(text[:120], entry)
            count += 1

        self._counts["ANNEXES"] = count
        logger.debug("ANNEXES: %d entries loaded", count)

    def _load_adif(self) -> None:
        path = os.path.join(
            self._base_dir, "normativa_adif", "_catalogo", "catalogo_adif.json"
        )
        if not os.path.exists(path):
            logger.warning("catalogo_adif.json not found — skipped")
            return

        with open(path, encoding="utf-8") as fh:
            entries = json.load(fh)

        count = 0
        for entry in entries:
            titulo = entry.get("titulo", "")
            codigo = entry.get("codigo", "")
            estado = (entry.get("estado") or "").lower()

            status = "DEROGADA" if any(
                w in estado for w in ("derogad", "retirad", "anulad")
            ) else "VIGENT"

            catalog_entry = {
                "status": status,
                "source": "ADIF",
                "title": titulo,
                "raw_ref": codigo,
                "substituted_by": None,
            }
            # ADIF titles are plain descriptions; try codigo first (NTE codes)
            if not self._index_entry(codigo, catalog_entry):
                self._index_entry(titulo, catalog_entry)
            count += 1

        self._counts["ADIF"] = count
        logger.debug("ADIF: %d entries loaded", count)

    def _load_iso(self) -> None:
        path = os.path.join(
            self._base_dir, "normativa_iso", "_catalogo", "catalogo_iso.json"
        )
        if not os.path.exists(path):
            logger.warning("catalogo_iso.json not found — skipped")
            return

        with open(path, encoding="utf-8") as fh:
            entries = json.load(fh)

        _iso_vigent = {"VIGENT", "PUBLISHED", "PUBLICADA"}
        _iso_derog  = {"RETIRADA", "WITHDRAWN", "DEROGADA", "ANULADA"}

        count = 0
        for entry in entries:
            ref   = entry.get("referencia", "")
            titol = entry.get("titol", "")
            estat = (entry.get("estat") or "").upper()
            sub   = entry.get("substituida_per", "") or None

            if estat in _iso_derog:
                status = "DEROGADA"
            else:
                status = "VIGENT"  # includes DESCONEGUT — assume valid until known otherwise

            catalog_entry = {
                "status": status,
                "source": "ISO",
                "title": titol or ref,
                "raw_ref": ref,
                "substituted_by": sub,
            }
            self._index_entry(ref, catalog_entry)
            count += 1

        self._counts["ISO"] = count
        logger.debug("ISO: %d entries loaded", count)

    def _load_une(self) -> None:
        path = os.path.join(
            self._base_dir, "normativa_une", "_catalogo", "catalogo_une.json"
        )
        if not os.path.exists(path):
            logger.warning("catalogo_une.json not found — skipped")
            return

        with open(path, encoding="utf-8") as fh:
            entries = json.load(fh)

        _une_derog = {"ANULADA", "DEROGADA", "WITHDRAWN", "RETIRADA"}

        count = 0
        for entry in entries:
            ref   = entry.get("referencia", "")
            estat = (entry.get("estat") or "").upper()
            desc  = entry.get("descripcio", "") or ""

            status = "DEROGADA" if estat in _une_derog else "VIGENT"

            catalog_entry = {
                "status": status,
                "source": "UNE",
                "title": desc or ref,
                "raw_ref": ref,
                "substituted_by": None,
            }
            self._index_entry(ref, catalog_entry)
            count += 1

        self._counts["UNE"] = count
        logger.debug("UNE: %d entries loaded", count)

    def _load_dgc(self) -> None:
        path = os.path.join(
            self._base_dir, "normativa_dgc", "_catalogo", "catalogo_completo.json"
        )
        if not os.path.exists(path):
            logger.warning("catalogo_completo.json not found — skipped")
            return

        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)

        # Accept both flat list and legacy dict-of-lists
        if isinstance(data, dict):
            entries: list = []
            for val in data.values():
                if isinstance(val, list):
                    entries.extend(val)
                elif isinstance(val, dict):
                    entries.append(val)
        else:
            entries = data

        _dgc_status = {
            "normativa":  "VIGENT",
            "referencia": "REFERENCIA",
            "historica":  "DEROGADA",
        }

        count = 0
        for entry in entries:
            titol = entry.get("titol", "")
            tipus = (entry.get("tipus") or "normativa").lower()
            status = _dgc_status.get(tipus, "VIGENT")

            catalog_entry = {
                "status": status,
                "source": "DGC",
                "title": titol,
                "raw_ref": titol,
                "substituted_by": None,
            }
            self._index_entry(titol, catalog_entry)
            count += 1

        self._counts["DGC"] = count
        logger.debug("DGC: %d entries loaded", count)

    def _load_industria(self) -> None:
        path = os.path.join(
            self._base_dir, "normativa_industria", "_catalogo", "catalogo_industria.json"
        )
        if not os.path.exists(path):
            logger.warning("catalogo_industria.json not found — skipped")
            return

        with open(path, encoding="utf-8") as fh:
            entries = json.load(fh)

        _industria_vigent = {"VIGENT", "PENDENT_VERIFICAR"}

        count = 0
        for entry in entries:
            boe_id = entry.get("boe_id", "")
            titol  = entry.get("titol", "")
            estat  = (entry.get("estat") or "").upper()

            status = "VIGENT" if estat in _industria_vigent else "PENDENT"

            catalog_entry = {
                "status":         status,
                "source":         "INDUSTRIA",
                "title":          titol,
                "raw_ref":        titol,
                "substituted_by": None,
                "boe_id":         boe_id,
            }
            self._index_entry(titol, catalog_entry)
            count += 1

        self._counts["INDUSTRIA"] = count
        logger.debug("INDUSTRIA: %d entries loaded", count)

    def _load_all(self) -> None:
        self._index = {}
        self._counts = {"ANNEXES": 0, "DGC": 0, "ADIF": 0, "ISO": 0, "UNE": 0, "INDUSTRIA": 0}
        self._load_annexes()
        self._load_adif()
        self._load_iso()
        self._load_une()
        self._load_dgc()
        self._load_industria()

    # ─── Public API ───────────────────────────────────────────────────────────

    def lookup(self, raw_text: str) -> dict | None:
        """Look up *raw_text* in the index.

        Returns:
          • The matching entry dict (with all stored fields) on exact hit.
          • The same dict with ``"fuzzy": True`` added on near-miss hit.
          • ``{"status": "PENDENT", "source": None, ...}`` when resolvable
            but not found in any catalog.
          • ``None`` when the string cannot be parsed by norm_resolver.
        """
        resolved = resolve(raw_text)
        if resolved is None:
            return None

        key = normalize_code(resolved)

        # Exact match
        if key in self._index:
            return self._index[key]

        # Fuzzy: same type, numeric number within ±4
        typ = resolved.get("type")
        num = resolved.get("number")
        if typ and num and isinstance(num, str) and num.isdigit():
            num_int = int(num)
            best: dict | None = None
            best_dist = 5  # exclusive upper bound
            prefix = f"{typ}-"
            for k, entry in self._index.items():
                if not k.startswith(prefix):
                    continue
                rest = k[len(prefix):]
                cand = rest.split("/")[0]
                if not cand.isdigit():
                    continue
                dist = abs(int(cand) - num_int)
                if dist < best_dist:
                    best = dict(entry)
                    best["fuzzy"] = True
                    best_dist = dist
            if best:
                return best

        return {
            "status": "PENDENT",
            "source": None,
            "title": None,
            "raw_ref": raw_text,
            "substituted_by": None,
        }

    def stats(self) -> dict:
        """Return counts: total indexed, per source (loaded), per status."""
        status_counts: dict[str, int] = {}
        for entry in self._index.values():
            s = entry.get("status", "UNKNOWN")
            status_counts[s] = status_counts.get(s, 0) + 1
        return {
            "total_indexed": len(self._index),
            "per_source":    dict(self._counts),
            "per_status":    status_counts,
        }

    def reload(self) -> None:
        """Re-read all catalog files from disk."""
        self._load_all()


# ─── __main__ ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    logging.basicConfig(level=logging.WARNING)

    base = sys.argv[1] if len(sys.argv) > 1 else "."
    idx = NormIndex(base)
    s = idx.stats()

    src = s["per_source"]
    print(
        f"Index carregat: {s['total_indexed']} normes indexades  "
        f"(DGC: {src['DGC']}, ADIF: {src['ADIF']}, "
        f"ISO: {src['ISO']}, UNE: {src['UNE']}, "
        f"ANNEXES: {src['ANNEXES']}, INDUSTRIA: {src.get('INDUSTRIA', 0)})"
    )
    print(f"Estat:  {s['per_status']}")
