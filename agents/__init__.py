ALL_AGENTS = {
    "A1": ("a1_memoria", "MEMORIA"),
    "A2": ("a2_ess_egr", "ESS"),
    "A3": ("a3_pcq", "PCQ"),
    "A4": ("a4_pressupost", "PRESSUPOST_CONEIXEMENT"),
    "A5": ("a5_planols", "PLANOLS"),
    "A6": ("a6_plec", "PLEC"),
    "B1": ("b1_ferms", "FERMS"),
    "B2": ("b2_drenatge", "DRENATGE"),
    "B3": ("b3_estructures", "ESTRUCTURES"),
    "B4": ("b4_contencio", "SENYALITZACIO"),
    "B5": ("b5_senyalitzacio", "SENYALITZACIO"),
    "B6": ("b6_geotecnia", "GEOTECNIA"),
    "B7": ("b7_ambiental", "AMBIENTAL"),
    "B8": ("b8_instal", "INSTAL"),
}


def get_all_agents():
    agents = {}
    for agent_id, (module_name, annex_key) in ALL_AGENTS.items():
        module = __import__(f"agents.{module_name}", fromlist=["get_agent"])
        agents[agent_id] = {
            "agent": module.get_agent(),
            "annex_key": annex_key,
        }
    return agents