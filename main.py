#!/usr/bin/env python3
"""
Project Checker — Revisio automatica de projectes d'obra civil
Us: python main.py [fitxer.pdf] [--output informe.docx]
"""

import argparse
import os
import re
import sys
from pathlib import Path

# Assegura que el directori del script es al path (necessari a Windows)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Força UTF-8 a la sortida estàndard (Windows usa cp1252 per defecte)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from checker import ProjectChecker
from env_utils import load_local_env
from report  import ReportGenerator
from splitter import split_project_pdf
from supervisor import run_supervision


def main():
    load_local_env()

    parser = argparse.ArgumentParser(
        description="Revisio automatica de projectes constructius (PDF)"
    )
    parser.add_argument("pdf", nargs="?", default=None,
                        help="Fitxer PDF del projecte (opcional; es demana si no s'indica)")
    parser.add_argument("--output", "-o", default=None,
                        help="Fitxer de sortida (.docx). Per defecte: <nom>_informe.docx")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Mostra detall de proces (inclou progrés del split) i traceback en cas d'error")
    parser.add_argument("--split", action="store_true",
                        help="Divideix el PDF en blocs (memoria, annex individual, planols, plec, pressupost)")
    parser.add_argument("--split-only", action="store_true",
                        help="Nomes fa la divisio del PDF (sense checks ni informe)")
    parser.add_argument("--split-dir", default=None,
                        help="Directori de sortida dels PDFs dividits (per defecte, el mateix directori del PDF)")
    parser.add_argument("--no-cache", action="store_true",
                        help="No reutilitza cache temporal de context del PDF")
    parser.add_argument("--rebuild-cache", action="store_true",
                        help="Forca la reconstruccio de la cache temporal del context")
    parser.add_argument("--full-text-all", action="store_true",
                        help="Extreu text complet de totes les pagines (inclou planols i pressupost)")
    parser.add_argument("--supervise", action="store_true",
                        help="Executa agents d'IA despres del checker automatic")
    parser.add_argument("--agents", default=None,
                        help="Llista d'agents separats per comes (ex.: A1,A2,B1). Per defecte: tots")
    parser.add_argument("--api-key", default=None,
                        help="Clau API d'Anthropic. Si no s'indica, es llegeix ANTHROPIC_API_KEY")
    args = parser.parse_args()

    # ── Demanem el fitxer si no s'ha passat per argument ────────────────────
    if args.pdf:
        pdf_path = Path(args.pdf)
    else:
        while True:
            raw = input("📂 Introdueix el cami al fitxer PDF del projecte: ").strip()
            if raw:
                pdf_path = Path(raw)
                break
            print("  [!] El cami no pot ser buit. Torna-ho a intentar.")

    if not pdf_path.exists():
        print(f"\n[ERROR] No es troba el fitxer: {pdf_path}", file=sys.stderr)
        sys.exit(1)

    if args.api_key:
        os.environ["ANTHROPIC_API_KEY"] = args.api_key

    output_path = (
        Path(args.output)
        if args.output
        else pdf_path.with_name(pdf_path.stem + "_informe.docx")
    )

    print(f"\n{'='*60}")
    print(f"  PROJECT CHECKER  —  Revisio automatica de projectes")
    print(f"{'='*60}")
    print(f"  Fitxer  : {pdf_path.name}")
    print(f"  Sortida : {output_path.name}")
    print(f"{'='*60}\n")

    do_split = args.split or args.split_only

    if args.split_only:
        split_dir = Path(args.split_dir) if args.split_dir else pdf_path.parent
        print("  ✂  Dividint PDF (mode split-only)...", flush=True)
        split_result = split_project_pdf(pdf_path, output_dir=split_dir, verbose=args.verbose)
        actual_split_dir = Path(split_result.get("output_dir", split_dir))

        print(f"\n{'='*60}")
        print("  SPLIT PDF")
        print(f"{'='*60}")
        for part in split_result["parts"]:
            print(
                f"  - {part['label']:<16} pàg. {part['start_page']:>4}-{part['end_page']:<4} "
                f"[{part['source']}] -> {part['output'].name}"
            )
        if split_result["notes"]:
            print("\n  Notes:")
            for note in split_result["notes"]:
                print(f"   * {note}")
        print(f"\n  Carpeta de sortida: {actual_split_dir}")
        print(f"{'='*60}\n")
        return

    # ── Execucio ─────────────────────────────────────────────────────────────
    checker = ProjectChecker(
        pdf_path,
        verbose=args.verbose,
        use_cache=not args.no_cache,
        rebuild_cache=args.rebuild_cache,
        full_text_all=args.full_text_all,
    )
    results = checker.run_all_checks()

    # ── Generacio informe Word ────────────────────────────────────────────────
    print("  📝 Generant informe Word...", end="", flush=True)
    reporter = ReportGenerator(pdf_path.name, results)
    reporter.save_docx(output_path)
    print(f"\r  📝 Informe Word generat correctament                    ")

    supervision_summary = None
    if args.supervise:
        if not os.environ.get("ANTHROPIC_API_KEY"):
            print("  [IA] Supervisio omesa: manca ANTHROPIC_API_KEY")
        else:
            selected_agents = [a.strip().upper() for a in (args.agents or "").split(",") if a.strip()]
            project_context = {
                "project_name": pdf_path.stem,
                "pem": _extract_pem_from_results(results),
                "termini": _extract_termini_from_results(results),
                "lots": _extract_lots_from_results(results),
            }
            print("  🤖 Executant supervisio IA...", flush=True)
            supervision_summary = run_supervision(
                str(pdf_path),
                checker.annex_map,
                project_context,
                agent_ids=selected_agents or None,
                output_dir=str(pdf_path.parent / "agents_output"),
            )

    if do_split:
        split_dir = Path(args.split_dir) if args.split_dir else pdf_path.parent
        print("  ✂  Dividint PDF en blocs...", flush=True)
        split_result = split_project_pdf(pdf_path, output_dir=split_dir, verbose=args.verbose)
        actual_split_dir = Path(split_result.get("output_dir", split_dir))
        print("  ✂  Divisio finalitzada")

    # ── Resum consola ─────────────────────────────────────────────────────────
    total = sum(len(r["findings"]) for r in results)
    nook  = sum(len([f for f in r["findings"] if f["status"] == "NO OK"]) for r in results)
    ok    = sum(len([f for f in r["findings"] if f["status"] == "OK"])    for r in results)
    info  = sum(len([f for f in r["findings"] if f["status"] == "INFO"])  for r in results)

    print(f"\n{'='*60}")
    print(f"  RESUM")
    print(f"{'='*60}")
    print(f"  OK    : {ok}")
    print(f"  NO OK : {nook}")
    print(f"  INFO  : {info}")
    print(f"  Total : {total} checks")
    print(f"\n  Informe: {output_path}")
    if supervision_summary is not None:
        print(
            f"  IA     : {supervision_summary['total_no_ok']} NO OK / "
            f"{supervision_summary['total_findings']} incidencies / "
            f"{supervision_summary['total_cost_eur']:.4f}EUR"
        )
        print(f"  IA out : {pdf_path.parent / 'agents_output'}")
    if do_split:
        print(f"  Split  : {len(split_result['parts'])} PDFs a {actual_split_dir}")
        for part in split_result["parts"]:
            print(
                f"           - {part['label']:<16} pàg. {part['start_page']:>4}-{part['end_page']:<4} "
                f"[{part['source']}]"
            )
        if split_result["notes"]:
            print("           Notes: " + " | ".join(split_result["notes"]))
    print(f"{'='*60}\n")


def _extract_pem_from_results(results: list[dict]) -> float | None:
    for row in results:
        if "imports" not in (row.get("title", "")).lower():
            continue
        for finding in row.get("findings", []):
            if finding.get("item") != "IMP-00":
                continue
            detail = finding.get("detall", "") or ""
            for line in detail.splitlines():
                if not line.strip().startswith("PEM |"):
                    continue
                cells = [cell.strip() for cell in line.split("|")]
                candidate_cells = []
                if len(cells) >= 3:
                    candidate_cells.append(cells[-2])
                candidate_cells.extend(cells[1:])
                for cell in candidate_cells:
                    value = _parse_eur_amount(cell)
                    if value is not None:
                        return value
    return None


def _extract_lots_from_results(results: list[dict]) -> int | None:
    for row in results:
        if "imports" not in (row.get("title", "")).lower():
            continue
        for finding in row.get("findings", []):
            if finding.get("item") != "IMP-00":
                continue
            detail = finding.get("detall", "") or ""
            for line in detail.splitlines():
                if not line.strip().startswith("Concepte |"):
                    continue
                cells = [cell.strip() for cell in line.split("|")]
                lot_cells = [cell for cell in cells if cell.lower().startswith("lot")]
                return len(lot_cells) or None
    return None


def _extract_termini_from_results(results: list[dict]) -> str | None:
    term_re = re.compile(r"\b\d+\s*(?:mesos|mes|setmanes|setmana|dies|dia)\b", re.IGNORECASE)
    for row in results:
        haystacks = [row.get("title", "")]
        for finding in row.get("findings", []):
            haystacks.append(finding.get("descrip", ""))
            haystacks.append(finding.get("detall", ""))
        for text in haystacks:
            match = term_re.search(text or "")
            if match:
                return match.group(0)
    return None


def _parse_eur_amount(text: str) -> float | None:
    cleaned = (text or "").replace("€", "")
    cleaned = cleaned.replace(".", "").replace(" ", "")
    cleaned = cleaned.replace(",", ".")
    if cleaned in {"", "-"}:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


if __name__ == "__main__":
    main()
