from __future__ import annotations
import argparse, json, sys
from .engine.engine import Engine
from . import __version__

def main() -> None:
    p = argparse.ArgumentParser("forklift")
    sub = p.add_subparsers(dest="cmd", required=True)

    ingest = sub.add_parser("ingest", help="Clean & write to Parquet")
    ingest.add_argument("source")
    ingest.add_argument("--dest", required=True)
    ingest.add_argument("--input-kind", choices=["csv","fwf","excel"], required=True)
    ingest.add_argument("--schema", help="Path to JSON Schema file")
    ingest.add_argument("--pre", nargs="*", default=[], help="Preprocessors by name")
    # common input args
    ingest.add_argument("--encoding-priority", nargs="*", default=["utf-8-sig","utf-8","latin-1"])
    ingest.add_argument("--delimiter")
    ingest.add_argument("--sheet")  # excel
    ingest.add_argument("--fwf-spec")  # path to JSON with x-fwf fields (or part of schema)
    ingest.add_argument(
        "--header-mode",
        choices=["present", "auto", "absent"],
        default="present",
        help="Explicit header handling: 'present' (file has header), 'absent' (no header, use override), 'auto'"
    )

    args = p.parse_args()

    if args.cmd == "ingest":
        schema = None
        if args.schema:
            with open(args.schema) as f:
                schema = json.load(f)
        opts = dict(
            encoding_priority=args.encoding_priority,
            delimiter=args.delimiter,
            sheet=args.sheet,
            header_mode=args.header_mode,
        )
        if args.fwf_spec:
            with open(args.fwf_spec) as f:
                opts["fwf_spec"] = json.load(f)

        eng = Engine(
            input_kind=args.input_kind,
            output_kind="parquet",
            schema=schema,
            preprocessors=args.pre,
            **opts
        )
        eng.run(args.source, args.dest)