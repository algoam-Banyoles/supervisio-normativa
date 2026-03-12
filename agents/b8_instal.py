from .base_agent import BaseAgent

PROMPT_B8 = """
Revisa l'annex d'instal·lacions.
Comprova especificament:

1. Definicio funcional de cada instal·lacio projectada.
2. Esquemes, quadres, potencies, subministraments i connexions necessaries.
3. Coherencia amb planols, amidaments, plec i pressupost.
4. Referencies normatives vigents segons el tipus d'instal·lacio.
5. Mesures de proteccio, manteniment, accessibilitat i seguretat.
6. Compatibilitat amb serveis afectats i amb la resta d'annexos tecnics.
7. Si falten dades basiques de dimensionament o explotacio, marca-ho com a NO OK.
"""


def get_agent():
    return BaseAgent("B8", "Instal·lacions", PROMPT_B8)