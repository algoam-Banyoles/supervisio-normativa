from .base_agent import BaseAgent

PROMPT_B7 = """
Revisa l'annex ambiental i de mesures correctores.
Comprova especificament:

1. Identificacio d'impactes principals durant execucio i explotacio.
2. Mesures preventives, correctores o compensatories concretes.
3. Coherencia amb permisos, condicionants ambientals o resolucions, si consten.
4. Tractament de soroll, pols, fauna, vegetacio, aigua i paisatge quan pertoqui.
5. Seguiment ambiental, indicadors i responsabilitats.
6. Coherencia amb l'EGR, el plec i el pressupost.
7. Si l'annex es generic o no adapta les mesures a l'obra, marca-ho com a NO OK.
"""


def get_agent():
    return BaseAgent("B7", "Ambiental", PROMPT_B7)