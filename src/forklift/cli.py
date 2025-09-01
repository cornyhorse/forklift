from __future__ import annotations
import argparse
from .engine.forklift_core import ForkliftCore, ImportConfig, HeaderMode


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
        # Create ImportConfig from CLI arguments
        config = ImportConfig(
            input_path=args.source,
            output_path=args.dest,
            schema_file=args.schema,
            header_mode=HeaderMode(args.header_mode),
            encoding=args.encoding_priority[0] if args.encoding_priority else "utf-8",
            delimiter=args.delimiter or ",",
        )

        # Handle FWF spec if provided
        if args.fwf_spec:
            print(f"Warning: FWF spec processing not yet implemented in new ForkliftCore: {args.fwf_spec}")

        # Handle preprocessors if provided
        if args.pre:
            print(f"Warning: Preprocessors not yet implemented in new ForkliftCore: {args.pre}")

        # Handle Excel sheet if provided
        if args.sheet:
            print(f"Warning: Excel sheet processing not yet implemented in new ForkliftCore: {args.sheet}")

        # Create and run ForkliftCore
        core = ForkliftCore(config)

        # Currently ForkliftCore only has process_csv method
        if args.input_kind == "csv":
            results = core.process_csv()
            print(f"Processing complete. Processed {results.total_rows} rows.")
            print(f"Valid rows: {results.valid_rows}, Invalid rows: {results.invalid_rows}")
            if results.output_files:
                print(f"Output files: {', '.join(results.output_files)}")
        else:
            print(f"Error: Input kind '{args.input_kind}' not yet implemented in new ForkliftCore. Only 'csv' is currently supported.")
