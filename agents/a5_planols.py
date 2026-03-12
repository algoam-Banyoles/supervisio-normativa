from .base_agent import BaseAgent

PROMPT_A5 = """
Revisa els planols del projecte des del punt de vista de supervisio.
Comprova especificament:

1. Existencia de caixo de dades, titol, num. de planol, escala, data i autoria.
2. Correspondencia del titol dels planols amb el projecte i les seves fases.
3. Llegibilitat general: escales, cotes, llegenda, orientacio i referencies creuades.
4. Coherencia entre planols generals, detall, amidaments i memoria.
5. Definicio suficient per executar l'obra sense indefinicions critiques.
6. Existencia de seccions, perfils o detalls constructius quan siguin necessaris.
7. Identificacio d'incompatibilitats evidents entre planols o absencia de planols essencials.
8. Si manca un planol imprescindible, marca-ho com a NO OK.
"""


def get_agent():
    return BaseAgent("A5", "Planols", PROMPT_A5)