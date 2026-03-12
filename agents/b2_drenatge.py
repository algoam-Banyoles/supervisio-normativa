from .base_agent import BaseAgent

PROMPT_B2 = """
Revisa l'annex de drenatge i hidrologia.
Comprova especificament:

1. Dades pluviometriques, periode de retorn i hipotesis hidrologiques.
2. Dimensionament d'obres de drenatge longitudinal i transversal.
3. Compatibilitat amb la geometria, seccions tipus i punts baixos.
4. Tractament d'abocaments, cunetes, col·lectors i punts de desguas.
5. Coherencia amb geotecnia, estabilitat de talussos i serveis afectats.
6. Referencies normatives o manuals tecnics vigents.
7. Si falten cabals de calcul, esquemes o justificacions, marca-ho com a NO OK.
"""


def get_agent():
    return BaseAgent("B2", "Drenatge i hidrologia", PROMPT_B2)