from .base_agent import BaseAgent

PROMPT_A1 = """
Revisa la MEMORIA del projecte aplicant el Checklist SSP de la DGIM.
Comprova especificament:

1. Titol: coincideix amb l'ordre d'estudi o encarrec?
2. Declaracio d'obra completa: cita art. 125 RD 1098/2001 i art. 233 Llei 9/2017?
   NO citar RDL 3/2011 (derogat).
3. Divisio en lots: hi ha justificacio explicita citant Directriu 1/2018?
4. Revisio de preus: s'indica si procedeix amb referencia a art. 103 Llei 9/2017?
5. Termini de garantia: s'especifica?
6. Termini d'execucio: es coherent entre memoria, pla de treballs i ESS?
7. Classificacio del contractista: es correcta per al PEM i termini declarats?
8. %ESS/PEM: s'indica explicitament? Rang esperable 0.5%-2%.
9. %EGR/PEM: s'indica explicitament?
10. %PCQ/PEM: assoleix l'1% del PEM (Decret 77/1984)?
11. Banc de preus: quin any? Es el vigent per a l'any d'aprovacio?
12. Documents que integren el projecte: la llista es completa?
    (Memoria+Annexos, Planols, Plec, Pressupost)
"""


def get_agent():
    return BaseAgent("A1", "Memoria general", PROMPT_A1)