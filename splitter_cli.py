"""
Standalone CLI for PDF splitting.

This file is intended to be packaged as a portable executable so end users
can split project PDFs without installing Python dependencies.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from splitter import split_project_pdf


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Split a project PDF into memoria/annexes/planols/plec/pressupost blocks."
    )
    parser.add_argument(
        "pdf",
        nargs="?",
        help="Path to an input PDF file or a folder containing PDFs. If omitted, it will be prompted.",
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        default=None,
        help="Output folder. If omitted, a parts_<pdf_name> folder is created automatically.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show detailed detection logs.",
    )
    return parser


def _prompt_pdf() -> Path:
    default_dir = _runtime_dir()
    while True:
        raw = input(f'PDF path (Enter = {default_dir}): ').strip().strip('"')
        if raw:
            return Path(raw)
        return default_dir


def _runtime_dir() -> Path:
    # Quan va empaquetat amb PyInstaller, sys.executable apunta a l'EXE.
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def _collect_input_pdfs(target: Path) -> list[Path]:
    if target.is_file():
        return [target]

    if not target.is_dir():
        return []

    return sorted(p for p in target.iterdir() if p.is_file() and p.suffix.lower() == ".pdf")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    input_target = Path(args.pdf.strip('"')) if args.pdf else _prompt_pdf()
    pdf_paths = _collect_input_pdfs(input_target)
    if not pdf_paths:
        print(f"ERROR: no valid PDF found at: {input_target}")
        return 1

    output_dir = Path(args.output_dir) if args.output_dir else None

    print("=" * 60)
    print(" Project Splitter")
    print("=" * 60)
    print(f" Input : {input_target}")
    print(f" PDFs found: {len(pdf_paths)}")

    total_parts = 0
    for pdf_path in pdf_paths:
        print(f"\n Splitting: {pdf_path.name}")
        result = split_project_pdf(pdf_path, output_dir=output_dir, verbose=args.verbose)
        real_output_dir = Path(result.get("output_dir") or (output_dir or pdf_path.parent))
        generated = len(result.get("parts", []))
        total_parts += generated

        print(f"  Output folder : {real_output_dir}")
        print(f"  Generated PDFs: {generated}")

        notes = result.get("notes") or []
        if notes:
            print("  Notes:")
            for note in notes:
                print(f"   - {note}")

    print("\nSplit completed")
    print(f" Total generated PDFs: {total_parts}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
