from .base_agent import BaseAgent

PROMPT_B5 = """
Revisa l'annex de senyalitzacio, abalisament i defenses.
Comprova especificament:

1. Coherencia amb la geometria i amb les fases d'obra, si n'hi ha.
2. Senyalitzacio vertical, horitzontal, balisament i barreres, quan pertoqui.
3. Criteris de seguretat viaria i justificacio de la solucio adoptada.
4. Referencies normatives o instruccions vigents.
5. Correspondencia amb planols, amidaments i pressupost.
6. Definicio suficient per a execucio i manteniment.
7. Si falten elements de seguretat indispensables, marca-ho com a NO OK.
"""


def get_agent():
    return BaseAgent("B5", "Senyalitzacio i abalisament", PROMPT_B5)