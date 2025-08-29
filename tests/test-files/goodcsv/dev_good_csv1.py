#!/usr/bin/env python3
"""Dev helper: run forklift Engine on good_csv1 sample CSV + schema.

Usage:
  python dev_good_csv1.py [--clean]

Creates/overwrites a dev_output directory beside this script containing:
  _manifest.json   (read/kept/rejected counts)
  _quarantine.jsonl (if any rejected rows)

Environment variables (optional):
  DEST: override output directory path.
"""
from __future__ import annotations
import json
import os
import sys
import argparse
from pathlib import Path
import shutil
from datetime import datetime

# Ensure project root on sys.path when running directly
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from forklift.engine.engine import Engine  # noqa: E402

BASE_DIR = Path(__file__).parent
SCHEMA_PATH = BASE_DIR / "good_csv1.json"
DATA_PATH = BASE_DIR / "good_csv1.txt"
DEFAULT_DEST = BASE_DIR / "dev_output"


def load_schema() -> dict:
    with SCHEMA_PATH.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def derive_input_opts(schema: dict) -> dict:
    xcsv = (schema.get("x-csv") or {})
    opts: dict = {}
    if "delimiter" in xcsv:
        opts["delimiter"] = xcsv["delimiter"]
    # Additional future options could be surfaced here.
    return opts


def run(clean: bool, dest: Path) -> None:
    if clean and dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True, exist_ok=True)

    schema = load_schema()
    input_opts = derive_input_opts(schema)

    engine = Engine(
        input_kind="csv",
        output_kind="parquet",
        schema=schema,
        preprocessors=["type_coercion"],
        header_mode="auto",
        **input_opts,
    )

    print(f"[forklift dev] Running Engine at {datetime.now().isoformat()}Z")
    print(f"  Source : {DATA_PATH}")
    print(f"  Schema : {SCHEMA_PATH}")
    print(f"  Dest   : {dest}")

    engine.run(str(DATA_PATH), str(dest))

    manifest_path = dest / "_manifest.json"
    if manifest_path.exists():
        counts = json.loads(manifest_path.read_text(encoding="utf-8"))
        print("\nManifest counts:")
        for k, v in counts.items():
            print(f"  {k:8s}: {v}")
    else:
        print("[warn] Manifest not found.")

    quarantine_path = dest / "_quarantine.jsonl"
    if quarantine_path.exists():
        lines = quarantine_path.read_text(encoding="utf-8").strip().splitlines()
        print(f"\nQuarantine rows: {len(lines)} (showing up to first 5)")
        for line in lines[:5]:
            print("  ", line)
    else:
        print("No quarantine file (all rows accepted).")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run forklift Engine on good_csv1 sample")
    parser.add_argument("--clean", action="store_true", help="Remove existing output directory before running")
    parser.add_argument("--dest", type=str, help="Override destination output directory")
    args = parser.parse_args(argv)
    dest_override = Path(os.environ.get("DEST", args.dest or DEFAULT_DEST))
    run(clean=args.clean, dest=dest_override)


if __name__ == "__main__":
    main()

