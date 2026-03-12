from .base_agent import BaseAgent

PROMPT_B6 = """
Revisa l'annex geotecnic.
Comprova especificament:

1. Campanya de reconeixement suficient: sondejos, cales o assaigs.
2. Descripcio estratigrafica i caracteritzacio del terreny.
3. Parametres geotecnics utilitzats per al disseny i coherencia amb estructures i moviments de terres.
4. Nivell freatic, agressivitat, expansivitat o riscos geologics, si escau.
5. Recomanacions per a fonamentacio, talussos, drenatge i execucio.
6. Referencies normatives o guies tecniques pertinents.
7. Si falten dades basiques o justificacions dels parametres, marca-ho com a NO OK.
"""


def get_agent():
    return BaseAgent("B6", "Geotecnia", PROMPT_B6)