# Project Checker — Guia per a Claude Code

Eina de revisió automàtica de projectes d'obra civil en PDF per al
Servei de Supervisió de Projectes de la DGIM (contracte PTOP-2026-7,
fins a 60 revisions/any). Genera un informe Word (.docx) amb incidents
classificats per severitat.

---

## Directori i entorn

```
C:\Users\algoa\OneDrive\Escritorio\Supervisio\
.venv\   ← sempre actiu: .venv\Scripts\activate
```

Execució principal:
```bash
python main.py               # demana el PDF interactivament
python main.py projecte.pdf  # amb argument
```

---

## Arquitectura de fitxers

```
Supervisio/
├── main.py              # CLI: entrada, arguments, resum consola
├── checker.py           # Orquestrador: carrega PDF, executa checks,
│                        # construeix annex_map, passa resultats a report
├── config.py            # Patrons, llistes negres, llindars configurables
├── report.py            # Genera informe Word (python-docx)
│
├── norm_resolver.py     # Normalitza referències normatives crues
│                        # → (type, number, year) canònic
├── norm_index.py        # Índex en memòria de tots els catàlegs
│                        # 28.832 normes: DGC+ADIF+ISO+UNE+ANNEXES
│
├── checks/
│   ├── annex_map.py     # S'executa primer: detecta estructura annexes
│   ├── blank_pages.py   # AG-18: pàgines en blanc (filtra gràfics)
│   ├── bookmarks.py     # AG-3/4: marcadors PDF
│   ├── documents.py     # AG-1: documents obligatoris
│   ├── geotecnia.py     # Extreu φ, c', γ, E, SPT... per taula revisió
│   ├── imports.py       # Coherència PEM/PEC/PBL/ESS/EGR/PCQ entre docs
│   ├── language.py      # AG-9/10: castellanismes + abreviatures
│   ├── normativa.py     # AG-13/16: normes derogades (config.py)
│   ├── normativa_taula.py  # Taula completa normativa + NormIndex.lookup()
│   ├── pagination.py    # AG-15: numeració al peu
│   ├── signatures.py    # AG-12: pàgines de signatura
│   └── terminis.py      # Coherència terminis (mesos) entre docs
│
├── agents/              # 13 agents Claude API (A1-A6, B1-B8) — WIP
├── supervisor.py        # Orquestra agents — WIP
├── norm_checker.py      # Validació normativa via RAG — WIP
│
└── normativa_*/         # Catàlegs descarregats pels scrapers
    ├── normativa_annexes.json        # v1.3, 132 normes generals
    ├── normativa_adif/_catalogo/     # 1496 NTEs ADIF
    ├── normativa_iso/_catalogo/      # ~25k normes ISO
    ├── normativa_une/_catalogo/      # parcial UNE (fix pendent)
    └── normativa_dgc/_catalogo/     # DGC per categories
```

---

## Scrapers de normativa

| Scraper | Estat | Notes |
|---------|-------|-------|
| `norm_scraper.py` | ✅ Funciona | DGC Ministeri Transport, CDN obert |
| `adif_scraper.py` | ⚠️ Fix pendent | 1496 docs OK, **tots surten "sense annexos"**. Cal cridar `loadDocumentosAnexos(object_id)` i després `descargarDocumentoAnexo(annex_id)` per cada fitxer |
| `iso_catalog.py` | ✅ Funciona | ISO open data CSV |
| `une_catalog.py` | ⚠️ Fix pendent | ASP.NET via curl_cffi. Problemes: `drpEstado` són checkboxes no select; botó Submit té `id="idButton"` no text "Aplicar"; sempre retorna 32 resultats (filtre ICS no s'aplica) |

Classificació DGC:
- `"normativa"` → **VIGENT** (obligatori)
- `"referencia"` → **REFERENCIA** (recomanable, no error)
- `"historica"` → **DEROGADA** → `NO OK` si el projecte la cita

---

## Mòduls de normalització normativa

### norm_resolver.py
Funcions pures, sense I/O. Normalitza qualsevol string cru:
```
"Real Decreto 1627/1997" → {type:"RD", number:"1627", year:"1997"}
"UNE-EN ISO 9001:2015"  → {type:"UNE", suffix:"UNE-EN-ISO-9001"}
"IAP-11"                → {type:"IAP"}
```
Test: `python norm_resolver.py`

### norm_index.py
Índex en memòria (dict), carregat un cop i reutilitzat:
```python
idx = NormIndex(base_dir)
result = idx.lookup("RD 1627/1997")
# → {"status": "VIGENT", "source": "DGC", "title": "...", ...}
# → {"status": "DEROGADA", "substituted_by": "...", ...}
# → {"status": "PENDENT", "source": None}  ← no al catàleg
```
Stats: `python norm_index.py`
→ `Index carregat: 28832 normes (DGC: 338, ADIF: 1496, ISO: 80357, UNE: 6007, ANNEXES: 147)`

---

## Flux d'execució del checker

```
main.py
  └─ checker.py → run_all_checks()
       ├─ 1. _extract_pages()        # PyMuPDF → llista de dicts
       ├─ 2. build_annex_map()       # detecta ESS, EGR, PCQ, etc.
       ├─ 3. checks (en ordre):
       │    📄 documents_obligatoris
       │    🔖 bookmarks
       │    ⬜ blank_pages
       │    🔢 pagination
       │    ✍  signatures
       │    🗣  castellanismes
       │    🔤 abreviatures
       │    ⚖  normativa_derogada
       │    💰 banc_de_preus
       │    📋 normativa_taula     ← usa NormIndex
       │    🪨 geotecnia
       │    💶 imports
       │    ⏱  terminis
       └─ report.py → save_docx()
```

---

## Format dels findings

Cada check retorna `list[dict]`:
```python
{
    "status":  "OK" | "NO OK" | "INFO",
    "item":    "AG-18",          # codi únic
    "descrip": "Pàgines en blanc confirmades",
    "detall":  "Pàg: 12, 45 | Font: DGC",
    "ref":     "normativa_annexes.json"
}
```

Severitat a l'informe: `NO OK` = error greu · `INFO` = advertència · `OK` = correcte

---

## Informe Word (report.py)

Seccions en ordre:
1. Bloc títol (nom fitxer, data, pàgines)
2. Taula resum executiu (total / OK / NO OK / INFO)
3. Una secció per check (taula amb findings)
4. Taula de normativa aplicada (VIGENT / DEROGADA / PENDENT)
5. Aspectes més rellevants (bullets amb tots els NO OK)

**Important**: `_add_section()` es crida NOMÉS al loop principal,
MAI dins `_add_most_relevant()` ni `_add_normativa_aplicada()`
(causa duplicació de seccions).

---

## Dependències

```
PyMuPDF          # extracció text PDF
python-docx      # generació informe Word
anthropic        # agents Claude API
curl_cffi        # scraping ASP.NET (UNE)
requests         # HTTP scrapers
beautifulsoup4   # parsing HTML scrapers
chromadb         # RAG (fase futura)
sentence-transformers  # embeddings (fase futura)
```

---

## Pendents actius

- [ ] Fix `adif_scraper.py`: implementar `loadDocumentosAnexos` + `descargarDocumentoAnexo`
- [ ] Fix `une_catalog.py`: checkboxes Vigentes/Anuladas, botó `id=idButton`, filtre ICS
- [ ] Integrar `norm_checker.py` al checker principal
- [ ] Sincronització incremental (upsert) als scrapers
- [ ] Provar checker complet sobre projecte GI-631
- [ ] Agents A1-A6, B1-B8: implementació i integració via supervisor.py

---

## Convencions de codi

- Tots els mòduls de check: `def check_X(pages: list[dict], doc, annex_map=None) -> list[dict]`
- `annex_map=None` sempre com a fallback — mai crash si manca
- Només stdlib + dependències ja instal·lades (no afegir pip sense avisar)
- Paths amb `os.path`, no `pathlib` hardcoded (compatibilitat Windows)
- Resultats de checks: exactament una entrada per mòdul a `results`
- Singleton `_norm_index` a `normativa_taula.py` — carregat un cop

---

## Projecte de prova

**C-233 road resurfacing** (projecte inicial de test)
**GI-631** (projecte actiu per validar el checker complet)

Els projectes DGIM són públics (licitació pública) → no hi ha restriccions de confidencialitat per usar APIs cloud.