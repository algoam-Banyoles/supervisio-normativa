from .base_agent import BaseAgent

PROMPT_A2 = """
Revisa l'Estudi de Seguretat i Salut i l'Estudi de Gestio de Residus aplicant el Checklist SSP.
Comprova com a minim:

1. ESS signat per tecnic competent i, si escau, visat o signatura verificable.
2. Pressupost de l'ESS identificat com a capitol independent al pressupost del projecte.
3. %ESS/PEM raonable, orientativament entre l'1% i el 2%, o justificat si s'allunya.
4. Normativa vigent correctament citada: RD 1627/1997, Llei 31/1995, RD 773/1997, RD 485/1997.
5. No citar normativa derogada o incompleta quan afecti la coordinacio de seguretat.
6. Identificacio de riscos principals i mesures preventives associades.
7. Coherencia del termini de l'ESS amb la memoria i el pla de treballs.
8. EGR redactat d'acord amb el RD 105/2008.
9. Quantificacio de residus per tipus, amb criteri clar de classificacio.
10. Import de l'EGR previst al pressupost i coherencia amb amidaments o capitols.
11. Mesures de segregacio, gestio, transport i destinacio final dels residus.
12. Si falten dades per revisar l'EGR, indica-ho com a INFO i descriu la carencia.
"""


def get_agent():
    return BaseAgent("A2", "ESS i EGR", PROMPT_A2)