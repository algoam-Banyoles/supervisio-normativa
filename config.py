"""
config.py — Configuració central: patrons, llistes negres, llindars
Edita aquest fitxer per adaptar el checker a nous projectes.
"""

# ---------------------------------------------------------------------------
# CASTELLANISMES — paraules/expressions a detectar
# ---------------------------------------------------------------------------
CASTELLANISMES = [
    # ── Verbs d'ús incorrecte ──────────────────────────────────────────────
    ("contempla",        "Usar 'preveu' en lloc de 'contempla'", "NO OK"),
    ("contemplen",       "Usar 'preveuen' en lloc de 'contemplen'", "NO OK"),
    ("contemplar",       "Usar 'preveure' en lloc de 'contemplar'", "NO OK"),
    ("s'ha de destacar", "Usar 'cal destacar' o 'cal assenyalar'", "NO OK"),
    ("cal destacar que", "Redundant; simplificar o usar 'cal assenyalar'", "INFO"),
    ("s'ha procedit a",  "Usar 's'ha fet' o 's'ha executat'", "INFO"),
    ("se ha de",         "Castellanisme; usar 's'ha de'", "NO OK"),
    ("realitzar-se",     "Usar 'fer-se' o 'executar-se'", "INFO"),

    # ── Expressions castellanistes ────────────────────────────────────────
    ("a nivell de",      "Expresió castellanista; usar 'pel que fa a' o 'quant a'", "NO OK"),
    ("en base a",        "Castellanisme; usar 'basant-se en' o 'd'acord amb'", "NO OK"),
    ("a l'efecte de",    "Castellanisme; usar 'per a' o 'amb la finalitat de'", "NO OK"),
    ("en relació a",     "Usar 'en relació amb' (amb preposició 'amb', no 'a')", "NO OK"),
    ("amb motiu de",     "Verificar; sovint substituïble per 'per' o 'a causa de'", "INFO"),
    ("a través de",      "Acceptable però freqüentment substituïble per 'mitjançant'", "INFO"),
    ("tal i com",        "Usar 'tal com' (sense 'i')", "NO OK"),
    ("tal y como",       "Castellanisme directe; usar 'tal com'", "NO OK"),
    ("degut a",          "Usar 'a causa de' o 'per'", "NO OK"),
    ("degut als",        "Usar 'a causa dels' o 'pels'", "NO OK"),
    ("a on",             "Usar 'on' (sense 'a')", "NO OK"),
    ("a on es",          "Usar 'on es'", "NO OK"),

    # ── Ortografia tècnica ────────────────────────────────────────────────
    ("tot-ú",            "Usar 'tot-u' (sense accent)", "NO OK"),
    ("annexes",          "Usar 'annexos' en lloc de 'annexes'", "NO OK"),
    ("apèndixs",         "Usar 'apèndixos' en lloc de 'apèndixs'", "NO OK"),
    ("adecuat",          "Castellanisme; usar 'adequat'", "NO OK"),
    ("adecuada",         "Castellanisme; usar 'adequada'", "NO OK"),
    ("suficients",       "Verificar acord de gènere; pot ser correcte", "INFO"),
    ("cumpliment",       "Castellanisme; usar 'compliment'", "NO OK"),
    ("cumplir",          "Castellanisme; usar 'complir'", "NO OK"),
    ("realització",      "Acceptable; però sovint preferible 'execució' en context d'obres", "INFO"),
    ("medi ambient",     "Verificar: la forma correcta és 'medi ambient' (dos mots) o 'mediambiental'", "INFO"),

    # ── Substantius d'ús incorrecte ───────────────────────────────────────
    ("plazo",            "Castellanisme directe; usar 'termini'", "NO OK"),
    ("plazos",           "Castellanisme directe; usar 'terminis'", "NO OK"),
    ("medidas",          "Castellanisme directe; usar 'mesures'", "NO OK"),
    ("obras",            "Castellanisme directe; usar 'obres'", "NO OK"),
    ("proyecto",         "Castellanisme directe; usar 'projecte'", "NO OK"),
    ("presupuesto",      "Castellanisme directe; usar 'pressupost'", "NO OK"),
    ("ejecución",        "Castellanisme directe; usar 'execució'", "NO OK"),
]

# ---------------------------------------------------------------------------
# ABREVIATURES — parelles inconsistents a detectar
# ---------------------------------------------------------------------------
ABREVIATURES = [
    ("nº",  "núm.", "Unificar l'abreviatura de número: usar 'núm.' o 'nº', no les dues"),
]

# ---------------------------------------------------------------------------
# NORMATIVA DEROGADA: deprecated — now managed via normativa_annexes.json
# Kept for reference only. Do not add new entries here.
# ---------------------------------------------------------------------------
NORMATIVA_DEROGADA = [
    {
        "text":    "MAM/304/2002",
        "motiu":   "Derogada per la Llei 7/2022, de 8 d'abril, de residus i sòls contaminants",
        "substitut": "Llei 7/2022, de 8 d'abril"
    },
    {
        "text":    "Real Decreto 1098/2001",
        "motiu":   "Reglament general de la LCAP; verificar si la referència és pertinent o cal actualitzar a LCSP 9/2017",
        "substitut": "Llei 9/2017, de 8 de novembre (LCSP)"
    },
]

# ---------------------------------------------------------------------------
# BANCS DE PREUS — versions vàlides i obsoletes
# ---------------------------------------------------------------------------
BANC_PREUS = {
    "versions_obsoletes": [
        "2024-06", "2024-03", "2024-01",
        "2023-06", "2023-01",
        "banc de 2024", "banc de 2023",
    ],
    "versio_actual": "2025 / actualització 2026-01",
    "bedec_actual":  "BEDEC 2026-01",
}

# ---------------------------------------------------------------------------
# PÀGINES EN BLANC — llindar de caràcters per considerar pàgina en blanc
# ---------------------------------------------------------------------------
BLANK_PAGE_THRESHOLD = 50   # caràcters (sense espais)

# ---------------------------------------------------------------------------
# SIGNATURES — patrons que indiquen pàgina de signatures
# ---------------------------------------------------------------------------
SIGNATURE_PAGE_PATTERNS = [
    "signatura",
    "signat electrònicament",
    "firmat",
    "l'enginyer",
    "l'enginyera",
    "el director",
    "la directora",
    "autor de l'informe",
    "autor del projecte",
]

# ---------------------------------------------------------------------------
# DOCUMENTS OBLIGATORIS — seccions/marcadors que han d'existir
# ---------------------------------------------------------------------------
DOCUMENTS_OBLIGATORIS = [
    "memòria",
    "annexos",
    "plànols",
    "plec de condicions",
    "pressupost",
    "estudi de seguretat i salut",
    "estudi de gestió de residus",
]

# ---------------------------------------------------------------------------
# EXTRACCIO DE TEXT LLEUGERA — documents on no cal text complet per defecte
# ---------------------------------------------------------------------------
LOW_TEXT_DOC_KEYS = [
    "planols",
    "pressupost",
]

# ---------------------------------------------------------------------------
# RÀTIOS DE PRESSUPOST — límits normatius
# ---------------------------------------------------------------------------
RATIO_ESS_PEM_MIN = 0.005   # 0.5% mínim orientatiu
RATIO_PCQ_PEM_MAX = 0.015   # 1.5% màxim PCQ sobre PEM obra civil
