# Project Checker
Eina de revisió automàtica de projectes constructius (PDF).

## Instal·lació

```bash
pip install -r requirements.txt
```

Única dependència: **PyMuPDF** (`fitz`) — extracció de text, marcadors i anàlisi de pàgines.

## Ús

```bash
# Revisió bàsica (genera informe HTML al mateix directori)
python main.py projecte.pdf

# Amb sortida personalitzada i mode verbose
python main.py projecte.pdf --output informe_revisio.html --verbose

# Forçar reconstrucció del context temporal
python main.py projecte.pdf --rebuild-cache

# Desactivar reutilització de cache
python main.py projecte.pdf --no-cache

# Forçar extracció de text completa a tot el document
python main.py projecte.pdf --full-text-all

# Revisió + divisió del PDF en blocs
python main.py projecte.pdf --split

# Només divisió del PDF (més ràpid, sense checks)
python main.py projecte.pdf --split-only --split-dir parts
```

## Divisió intel·ligent del PDF

L'split no es fa "a cegues" sobre marcadors.

- Primer es detecten marcadors candidats per a: **memòria**, **annex individual**, **plànols**, **plec** i **pressupost**.
- Cada marcador es **valida contra el text real de la pàgina** (i la següent) per confirmar que correspon a la secció.
- Si el marcador no és fiable, es fa **fallback a detecció per capçaleres/text** de pàgina.
- Per als annexos, es detecten portades quasi buides amb línia `ANNEX ...` i es força la **coherència de numeració** (annex esperat vs annex detectat).
- Els **subannexos** o portades repetides (numeració que retrocedeix o es repeteix fora de context) es descarten com a tall principal.

## Execució híbrida per àmbits

El checker crea una **cache temporal JSON** del context (text per pàgina + estructura detectada de documents/annexos), i després alguns checks s'executen per àmbits:

- **Paginació** (`AG-15`) per document i per annex.
- **Llengua** (`AG-10`, `AG-9`) per document i per annex, amb agregació de patrons.
- Per defecte, fa **extracció lleugera** a **plànols** i **pressupost** per reduir cost de processament.
- Amb `--verbose` mostra el progrés d'extracció: **pàgina actual / total**.

Això permet mantenir traçabilitat local (per annex) i context global compartit, evitant reextreure text si el PDF no ha canviat.

Sortida: fitxers `<projecte>_memoria.pdf`, `<projecte>_annex_individual.pdf`, `<projecte>_planols.pdf`, `<projecte>_plec.pdf`, `<projecte>_pressupost.pdf` (quan les seccions es detecten).

## Checks implementats

| Mòdul | Ítems checklist | Descripció |
|-------|-----------------|------------|
| `documents.py` | AG-1 | Documents obligatoris (memòria, annexos, plànols, plec, pressupost, ESS, EGR) |
| `bookmarks.py` | AG-3, AG-4 | Marcadors PDF: existència, títols buits, pàgines fora de rang, duplicats |
| `blank_pages.py` | AG-18 | Pàgines en blanc o quasi en blanc |
| `pagination.py` | AG-15 | Numeració de pàgina al peu de pàgina |
| `signatures.py` | AG-12 | Detecció de pàgines de signatura buides |
| `language.py` | AG-8, AG-9, AG-10 | Castellanismes, abreviatures inconsistents |
| `normativa.py` | AG-13 | Normativa derogada (llista negra configurable) |
| `normativa.py` | AG-16 | Versió del banc de preus |

## Configuració

Edita `config.py` per personalitzar:

- **CASTELLANISMES** — llista de patrons lingüístics a detectar
- **NORMATIVA_DEROGADA** — normes derogades a detectar + substituts
- **BANC_PREUS** — versions obsoletes i versió actual esperada
- **DOCUMENTS_OBLIGATORIS** — llista de documents que han de constar
- **BLANK_PAGE_THRESHOLD** — llindar de caràcters per considerar pàgina en blanc

## Ampliar el sistema

Cada check és un mòdul independent a `/checks/`. Per afegir-ne un de nou:

1. Crear `checks/nou_check.py` amb la funció `def check_nou(pages, doc) -> list[dict]`
2. Importar-lo a `checker.py` i afegir-lo a la llista `checks`

Cada finding ha de tenir:
```python
{
    "status":  "OK" | "NO OK" | "INFO",
    "item":    "codi de l'ítem (ex: AG-15)",
    "descrip": "descripció breu del resultat",
    "detall":  "text tècnic detallat (pàgines, contextos, etc.)",
    "ref":     "referència al checklist o normativa"
}
```

## Arquitectura híbrida planificada

```
PDF del projecte
      ↓
[Project Checker] → checks formals automàtics → findings NO OK
      ↓
[Agent NotebookLM/LLM local] → checks semàntics + literatura
      ↓
[Informe final] = automàtics + observacions LLM
```
