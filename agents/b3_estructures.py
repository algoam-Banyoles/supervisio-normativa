from .base_agent import BaseAgent

PROMPT_B3 = """
Revisa l'annex d'estructures.
Comprova especificament:

1. Tipologia estructural, hipotesis de carrega i combinacions.
2. Coherencia entre calculs, planols i amidaments.
3. Referencies normatives vigents, especialment Codi Estructural i normativa aplicable.
4. Dades geotecniques utilitzades i justificacio de recolzaments o fonaments.
5. Definicio de materials, classes de formigo/acer i durabilitat.
6. Existencia de detalls constructius i criteris d'execucio/control.
7. Si el calcul no permet validar la solucio adoptada, marca-ho com a NO OK.
"""


def get_agent():
    return BaseAgent("B3", "Estructures", PROMPT_B3)