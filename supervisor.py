from __future__ import annotations

import json
import os
from pathlib import Path


AGENT_EXTRA_ANNEX_KEYS = {
    "A2": ["EGR"],
}


def extract_annex_text(pdf_path: str, page_range: list, max_chars: int = 120000) -> str:
    """Extract text from specific pages of a PDF using PyMuPDF."""
    import fitz

    doc = fitz.open(pdf_path)
    try:
        texts = []
        for page_num in list(page_range or [])[:200]:
            if page_num - 1 < len(doc):
                page = doc[page_num - 1]
                texts.append(page.get_text())
    finally:
        doc.close()

    full_text = "\n".join(texts)
    return full_text[:max_chars]


def run_supervision(
    pdf_path: str,
    annex_map: dict,
    project_context: dict,
    agent_ids: list | None = None,
    output_dir: str = "agents_output",
) -> dict:
    """Run all applicable agents on the project."""
    from agents import get_all_agents

    os.makedirs(output_dir, exist_ok=True)
    all_agents = get_all_agents()

    if agent_ids:
        wanted = set(agent_ids)
        all_agents = {k: v for k, v in all_agents.items() if k in wanted}

    results = {}
    total_cost = 0.0
    total_tokens = 0

    for agent_id, agent_info in all_agents.items():
        agent = agent_info["agent"]
        annex_key = agent_info["annex_key"]
        pages = _collect_pages_for_agent(agent_id, annex_key, annex_map)

        if not pages:
            print(f"  [{agent_id}] Annex '{annex_key}' no detectat - omes")
            results[agent_id] = {
                "agent_id": agent_id,
                "agent_name": agent.agent_name,
                "status": "SKIPPED",
                "reason": f"Annex {annex_key} not found in annex_map",
                "findings": [],
                "tokens_used": 0,
                "cost_eur": 0.0,
            }
            continue

        print(f"  [{agent_id}] {agent.agent_name} - {len(pages)} pagines...", end=" ", flush=True)
        annex_text = extract_annex_text(pdf_path, pages, max_chars=120000)

        result = agent.run(annex_text, project_context)
        results[agent_id] = result

        total_cost += result.get("cost_eur", 0.0)
        total_tokens += result.get("tokens_used", 0)

        status = result.get("status", "ERROR")
        n_findings = len(result.get("findings", []))
        n_nook = sum(1 for f in result.get("findings", []) if f.get("severity") == "NO OK")
        print(f"{status} | {n_findings} incidencies ({n_nook} NO OK) | {result.get('cost_eur', 0):.4f}EUR")

        out_path = Path(output_dir) / f"{agent_id}_{_safe_name(agent.agent_name)}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

    summary = {
        "project": project_context.get("project_name", ""),
        "agents_run": len(results),
        "agents_ok": sum(1 for r in results.values() if r.get("status") == "OK"),
        "agents_skipped": sum(1 for r in results.values() if r.get("status") == "SKIPPED"),
        "total_findings": sum(len(r.get("findings", [])) for r in results.values()),
        "total_no_ok": sum(
            sum(1 for f in r.get("findings", []) if f.get("severity") == "NO OK")
            for r in results.values()
        ),
        "total_tokens": total_tokens,
        "total_cost_eur": round(total_cost, 4),
        "results": results,
    }

    summary_path = Path(output_dir) / "supervision_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(
        f"\n  RESUM IA: {summary['total_no_ok']} NO OK | "
        f"{summary['total_findings']} incidencies | "
        f"{summary['total_cost_eur']:.4f}EUR | "
        f"{summary['total_tokens']:,} tokens"
    )
    return summary


def _collect_pages_for_agent(agent_id: str, annex_key: str, annex_map: dict) -> list[int]:
    annex_keys = [annex_key] + AGENT_EXTRA_ANNEX_KEYS.get(agent_id, [])
    pages = []
    for key in annex_keys:
        annex_data = (annex_map or {}).get(key, {})
        pages.extend(annex_data.get("pages", []))
    return sorted(set(pages))


def _safe_name(text: str) -> str:
    return "_".join((text or "agent").replace("/", " ").split())