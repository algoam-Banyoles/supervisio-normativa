from .base_agent import BaseAgent

PROMPT_B1 = """
Revisa l'annex de ferms i paviments.
Comprova especificament:

1. Categoria de transit o hipotesi de carrega utilitzada.
2. Dimensionament de capes, materials i gruixos.
3. Referencies normatives i recomanacions tecniques vigents.
4. Coherencia amb geotecnia, drenatge i seccions tipus.
5. Prescripcions de compactacio, control i recepcio.
6. Justificacio de solucio adoptada i condicions d'esplanada.
7. Si hi ha absencia de calculs o dades basiques, marca-ho com a NO OK.
"""


def get_agent():
    return BaseAgent("B1", "Ferms i paviments", PROMPT_B1)