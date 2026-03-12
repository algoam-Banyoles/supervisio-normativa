from .base_agent import BaseAgent

PROMPT_A4 = """
Revisa el pressupost per a coneixement de l'administracio i la seva coherencia economica.
Comprova especificament:

1. Existencia de PEM, PEC i pressupost base de licitacio, amb criteri coherent.
2. Aplicacio correcta de despeses generals, benefici industrial i IVA.
3. Coherencia amb el resum del pressupost i amb la memoria economica.
4. Coherencia de lots, si n'hi ha, amb les quantitats parcials i totals.
5. Correspondencia entre amidaments, quadres de preus i pressupost.
6. Si s'indica pressupost per a coneixement de l'administracio, comprova que es diferencia del pressupost de licitacio.
7. Any o base del banc de preus i coherencia amb l'any del projecte.
8. Detecta omissions de capitols obligatoris com ESS, EGR o PCQ si han d'estar pressupostats.
"""


def get_agent():
    return BaseAgent("A4", "Pressupost per a coneixement", PROMPT_A4)