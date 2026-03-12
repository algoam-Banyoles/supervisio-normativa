from __future__ import annotations

import json
import os
import re
import time

from env_utils import load_local_env


class BaseAgent:
    def __init__(self, agent_id: str, agent_name: str, prompt: str):
        self.agent_id = agent_id
        self.agent_name = agent_name
        self.prompt = prompt

    def run(self, annex_text: str, project_context: dict) -> dict:
        """
        Calls Claude API and returns a structured result.
        """
        load_local_env()
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            return {
                "agent_id": self.agent_id,
                "agent_name": self.agent_name,
                "status": "ERROR",
                "findings": [],
                "raw_response": "Manca la variable d'entorn ANTHROPIC_API_KEY.",
                "tokens_used": 0,
                "cost_eur": 0.0,
                "elapsed_seconds": 0.0,
            }

        system_prompt = (
            "Ets un enginyer revisor de la DGIM (Servei de Supervisio de Projectes "
            "de la Generalitat de Catalunya). Revises projectes d'obra civil aplicant "
            "el Checklist SSP de la DGIM. Respon sempre en catala. "
            "El teu output ha de ser NOMES una llista JSON d'incidencies, "
            "sense cap text addicional, sense blocs de codi, sense markdown. "
            "Format exacte de cada element:\n"
            '{"item": "...", "document": "...", "problem": "...", '
            '"normativa": "...", "recommendation": "...", "severity": "NO OK|INFO|OK"}'
        )

        user_message = (
            f"Projecte: {project_context.get('project_name', 'Desconegut')}\n"
            f"PEM aproximat: {project_context.get('pem', 'No disponible')}\n"
            f"Termini: {project_context.get('termini', 'No disponible')}\n"
            f"Lots: {project_context.get('lots', 'No disponible')}\n\n"
            f"INSTRUCCIONS DE REVISIO:\n{self.prompt}\n\n"
            f"TEXT DE L'ANNEX A REVISAR:\n{(annex_text or '')[:120000]}"
        )

        import anthropic

        client = anthropic.Anthropic(api_key=api_key)
        start = time.time()

        try:
            response = client.messages.create(
                model="claude-sonnet-4-5",
                max_tokens=4096,
                system=system_prompt,
                messages=[{"role": "user", "content": user_message}],
            )
            elapsed = time.time() - start

            raw = self._extract_text(response).strip()
            input_tok = getattr(response.usage, "input_tokens", 0)
            output_tok = getattr(response.usage, "output_tokens", 0)

            cost_usd = (input_tok * 3 + output_tok * 15) / 1_000_000
            cost_eur = cost_usd * 0.92
            findings = self._parse_findings(raw)

            return {
                "agent_id": self.agent_id,
                "agent_name": self.agent_name,
                "status": "OK",
                "findings": findings,
                "raw_response": raw,
                "tokens_used": input_tok + output_tok,
                "cost_eur": round(cost_eur, 4),
                "elapsed_seconds": round(elapsed, 1),
            }

        except Exception as e:
            return {
                "agent_id": self.agent_id,
                "agent_name": self.agent_name,
                "status": "ERROR",
                "findings": [],
                "raw_response": str(e),
                "tokens_used": 0,
                "cost_eur": 0.0,
                "elapsed_seconds": 0.0,
            }

    def _extract_text(self, response) -> str:
        chunks = []
        for block in getattr(response, "content", []) or []:
            text = getattr(block, "text", None)
            if text:
                chunks.append(text)
        return "\n".join(chunks)

    def _parse_findings(self, raw: str) -> list[dict]:
        findings = []
        try:
            parsed = json.loads(raw)
            findings = parsed if isinstance(parsed, list) else [parsed]
        except json.JSONDecodeError:
            match = re.search(r"\[.*\]", raw or "", re.DOTALL)
            if match:
                parsed = json.loads(match.group(0))
                findings = parsed if isinstance(parsed, list) else [parsed]

        return self._normalize_findings(findings)

    def _normalize_findings(self, findings: list[dict]) -> list[dict]:
        normalized = []
        for finding in findings or []:
            if not isinstance(finding, dict):
                continue

            severity = str(finding.get("severity", "INFO") or "INFO").upper()
            if severity not in {"NO OK", "INFO", "OK"}:
                severity = "INFO"

            normalized.append(
                {
                    "item": str(finding.get("item", "") or ""),
                    "document": str(finding.get("document", self.agent_name) or self.agent_name),
                    "problem": str(finding.get("problem", "") or ""),
                    "normativa": str(finding.get("normativa", "") or ""),
                    "recommendation": str(finding.get("recommendation", "") or ""),
                    "severity": severity,
                }
            )
        return normalized