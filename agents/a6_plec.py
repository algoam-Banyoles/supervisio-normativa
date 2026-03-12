from .base_agent import BaseAgent

PROMPT_A6 = """
Revisa el Plec de Prescripcions Tecniques i, si apareixen, les condicions administratives.
Comprova especificament:

1. Estructura ordenada del plec i cobertura de materials, execucio, amidament i abonament.
2. Coherencia amb la memoria, els planols i el pressupost.
3. Citacions normatives vigents i absencia de normativa derogada sense justificacio.
4. Condicions de recepcio, control de qualitat, seguretat, medi ambient i gestio de residus.
5. Criteris d'acceptacio, tolerancies i assaigs quan siguin necessaris.
6. Descripcio clara de partides singulars o unitats d'obra rellevants.
7. Referencia a revisio de preus, classificacio o terminis si hi apareixen, amb coherencia legal.
8. Si el plec es massa generic o no permet controlar l'obra, marca-ho com a NO OK.
"""


def get_agent():
    return BaseAgent("A6", "Plec", PROMPT_A6)