from .base_agent import BaseAgent

PROMPT_B4 = """
Revisa l'annex relacionat amb elements de contencio, defenses o estabilitzacio lateral.
Comprova especificament:

1. Tipologia de murs, talussos reforcats, pantalles o defenses projectades.
2. Verificacions d'estabilitat global, lliscament, bolcada i capacitat portant, si escau.
3. Drenatge posterior i relacio amb geotecnia.
4. Coherencia amb planols, amidaments i pressupost.
5. Detalls constructius, materials i sequencia d'execucio.
6. Si l'annex real es de defenses o barreres, comprova que la solucio esta prou definida.
7. Si falten verificacions essencials, marca-ho com a NO OK.
"""


def get_agent():
    return BaseAgent("B4", "Contencio i defenses", PROMPT_B4)