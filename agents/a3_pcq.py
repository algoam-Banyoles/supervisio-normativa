from .base_agent import BaseAgent

PROMPT_A3 = """
Revisa el Pla de Control de Qualitat del projecte.
Comprova especificament:

1. Signatura del tecnic responsable i identificacio del document.
2. Definicio clara d'unitats d'obra, materials, assaigs i frequencies de control.
3. Coherencia del pressupost del PCQ amb el PEM i verificacio de l'1% minim del Decret 77/1984.
4. Relacio entre el PCQ, el plec, els amidaments i les partides pressupostaries.
5. Referencies normatives vigents per als controls proposats.
6. Control de recepcio, execucio i recepcio final, amb responsabilitats assignades.
7. Existencia de laboratoris, assaigs i criteris d'acceptacio/rebuig.
8. Si algun control essencial no apareix, indica'l com a NO OK.
"""


def get_agent():
    return BaseAgent("A3", "Pla de control de qualitat", PROMPT_A3)