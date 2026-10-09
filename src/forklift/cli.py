"""Command line interface: ``forklift ingest`` and ``forklift generate-schema``.

Exit codes: 0 on success, 1 when processing fails, 2 for usage errors and for input kinds that
are not implemented (argparse itself also exits with 2 for invalid arguments).
"""

from __future__ import annotations

import argparse
import dataclasses
import sys
from typing import Any, Dict, Optional, Sequence

from .engine.forklift_core import (
    ForkliftCore,
    HeaderMode,
    ImportConfig,
    import_excel,
    import_fwf,
)
from .io import is_s3_path
from .schema.schema_generator import (
    FileType,
    OutputTarget,
    SchemaGenerationConfig,
    SchemaGenerator,
)

EXIT_FAILURE = 1
EXIT_USAGE = 2


def _warn(message: str) -> None:
    print(f"Warning: {message}", file=sys.stderr)


def _fail(message: str, code: int = EXIT_FAILURE) -> None:
    """Print ``message`` to stderr and exit with ``code``."""
    print(message, file=sys.stderr)
    sys.exit(code)


def _value_statistics_kwargs(config_cls: Any, requested: bool) -> Dict[str, Any]:
    """``include_value_statistics=True`` if requested and ``config_cls`` supports it."""
    if not requested:
        return {}
    supported = dataclasses.is_dataclass(config_cls) and "include_value_statistics" in {
        f.name for f in dataclasses.fields(config_cls)
    }
    if not supported:
        _warn("--include-value-stats is not supported by this version and is ignored")
        return {}
    return {"include_value_statistics": True}


def _print_results(results: Any) -> None:
    print(f"Processing complete. Processed {results.total_rows} rows.")
    print(f"Valid rows: {results.valid_rows}, Invalid rows: {results.invalid_rows}")
    if results.output_files:
        print(f"Output files: {', '.join(results.output_files)}")
    if results.manifest_file:
        print(f"Manifest file: {results.manifest_file}")
    if results.metadata_file:
        print(f"Metadata file: {results.metadata_file}")

    errors = getattr(results, "errors", None)
    if isinstance(errors, list) and errors:
        for message in errors:
            print(f"Error: {message}", file=sys.stderr)
        sys.exit(EXIT_FAILURE)


def _build_parser():
    p = argparse.ArgumentParser("forklift")
    sub = p.add_subparsers(dest="cmd", required=True)

    ingest = sub.add_parser("ingest", help="Clean & write to Parquet")
    ingest.add_argument("source", help="Input path (local file or S3 URI: s3://bucket/key)")
    ingest.add_argument(
        "--dest",
        required=True,
        help="Output path (local directory or S3 URI: s3://bucket/prefix/)",
    )
    ingest.add_argument("--input-kind", choices=["csv", "fwf", "excel"], required=True)
    ingest.add_argument("--schema", help="Path to JSON Schema file (local or S3)")
    ingest.add_argument("--pre", nargs="*", default=[], help="Preprocessors by name")
    # common input args
    ingest.add_argument(
        "--encoding-priority",
        nargs="*",
        default=["utf-8-sig", "utf-8", "latin-1"],
        help=(
            "Candidate encodings. Only the first one is used: the engine takes a single "
            "encoding and does not fall back to the others (default: utf-8-sig)"
        ),
    )
    ingest.add_argument("--delimiter")
    ingest.add_argument(
        "--sheet",
        help=(
            "Excel only: sheet name to import, or a 0-based index if no sheet has that name "
            "(default: all sheets)"
        ),
    )
    ingest.add_argument("--fwf-spec")  # path to JSON with x-fwf fields (or part of schema)
    ingest.add_argument(
        "--header-mode",
        choices=["present", "auto", "absent"],
        default="present",
        help=(
            "Explicit header handling: 'present' (file has header), "
            "'absent' (no header, use override), 'auto'"
        ),
    )
    ingest.add_argument(
        "--include-value-stats",
        action="store_true",
        help=(
            "Include statistics that expose real cell values (top values, min/max, quantiles) "
            "in the output metadata. Off by default because the metadata can hold PII."
        ),
    )

    # Add schema generation command
    schema_gen = sub.add_parser("generate-schema", help="Generate schema from data file")
    schema_gen.add_argument("source", help="Input path (local file or S3 URI: s3://bucket/key)")
    schema_gen.add_argument(
        "--file-type",
        choices=["csv", "excel", "parquet"],
        required=True,
        help="Type of input file",
    )
    schema_gen.add_argument("--nrows", type=int, help="Number of rows to analyze (default: 1000)")
    schema_gen.add_argument(
        "--output", choices=["stdout", "file", "clipboard"], default="stdout", help="Output target"
    )
    schema_gen.add_argument("--output-path", help="Output file path (required when --output=file)")
    schema_gen.add_argument("--delimiter", default=",", help="CSV delimiter (default: comma)")
    schema_gen.add_argument("--encoding", default="utf-8", help="File encoding (default: utf-8)")
    schema_gen.add_argument("--sheet", help="Excel sheet name or index")
    schema_gen.add_argument(
        "--include-sample", action="store_true", help="Include sample data in schema"
    )
    schema_gen.add_argument(
        "--infer-primary-key", action="store_true", help="Infer primary key from metadata analysis"
    )

    # New metadata generation options
    schema_gen.add_argument(
        "--no-metadata", action="store_true", help="Disable metadata generation (default: enabled)"
    )
    schema_gen.add_argument("--metadata-output", help="Output path for separate metadata file")
    schema_gen.add_argument(
        "--enum-threshold",
        type=float,
        default=0.1,
        help="Threshold for suggesting enum types (default: 0.1)",
    )
    schema_gen.add_argument(
        "--uniqueness-threshold",
        type=float,
        default=0.95,
        help="Threshold for considering column too unique for enum (default: 0.95)",
    )
    schema_gen.add_argument(
        "--top-n-values",
        type=int,
        default=10,
        help="Number of top/bottom values to include (default: 10)",
    )
    schema_gen.add_argument(
        "--quantiles",
        nargs="*",
        type=float,
        help="Custom quantiles for numeric columns (default: 0.25 0.5 0.75 0.9 0.95 0.99)",
    )
    schema_gen.add_argument(
        "--include-value-stats",
        action="store_true",
        help=(
            "Include statistics that expose real cell values (top values, min/max, quantiles) "
            "in the generated metadata. Off by default because the metadata can hold PII."
        ),
    )
    return p, ingest, schema_gen


def _run_ingest(args: argparse.Namespace) -> None:
    # Check for S3 paths and provide user feedback
    if is_s3_path(args.source):
        print(f"Reading from S3: {args.source}")
    if is_s3_path(args.dest):
        print(f"Writing to S3: {args.dest}")

    # Options that do not apply to the chosen input kind must not be silently dropped
    if args.fwf_spec and args.input_kind != "fwf":
        _warn("--fwf-spec only applies to --input-kind fwf and is ignored")
    if args.sheet and args.input_kind != "excel":
        _warn("--sheet only applies to --input-kind excel and is ignored")
    if args.include_value_stats and args.input_kind != "csv":
        _warn("--include-value-stats only affects the CSV output metadata and is ignored")
    if args.pre:
        _warn(f"Preprocessors not yet implemented in new ForkliftCore: {args.pre}")

    if args.input_kind == "csv":
        # Create ImportConfig from CLI arguments
        config = ImportConfig(
            input_path=args.source,
            output_path=args.dest,
            schema_file=args.schema,
            header_mode=HeaderMode(args.header_mode),
            encoding=args.encoding_priority[0] if args.encoding_priority else "utf-8",
            delimiter=args.delimiter or ",",
            **_value_statistics_kwargs(ImportConfig, args.include_value_stats),
        )
        results = ForkliftCore(config).process_csv()
    elif args.input_kind == "excel":
        excel_kwargs = {"sheet": args.sheet} if args.sheet else {}
        results = import_excel(args.source, args.dest, args.schema, **excel_kwargs)
    else:  # fwf
        try:
            results = import_fwf(args.source, args.dest, args.schema or args.fwf_spec)
        except NotImplementedError as exc:
            _fail(f"Error: input kind 'fwf' is not implemented yet ({exc}).", EXIT_USAGE)

    _print_results(results)


def _run_generate_schema(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    # Validate output arguments
    if args.output == "file" and not args.output_path:
        parser.error("--output-path is required when --output=file")

    # Create schema generation config
    config = SchemaGenerationConfig(
        input_path=args.source,
        file_type=FileType(args.file_type),
        nrows=args.nrows,
        output_target=OutputTarget(args.output),
        output_path=args.output_path,
        delimiter=args.delimiter,
        encoding=args.encoding,
        sheet_name=args.sheet,
        include_sample_data=args.include_sample,
        infer_primary_key_from_metadata=args.infer_primary_key,  # Use metadata-based inference
        # New metadata generation options
        generate_metadata=not args.no_metadata,
        metadata_output_path=args.metadata_output,
        enum_threshold=args.enum_threshold,
        uniqueness_threshold=args.uniqueness_threshold,
        top_n_values=args.top_n_values,
        quantiles=args.quantiles if args.quantiles else None,
        **_value_statistics_kwargs(SchemaGenerationConfig, args.include_value_stats),
    )

    try:
        # Generate schema
        generator = SchemaGenerator(config)
        schema = generator.generate_schema()
        generator.output_schema(schema)

        # Generate and save separate metadata file if requested
        if config.metadata_output_path:
            if not config.generate_metadata:
                _warn("--metadata-output is ignored because --no-metadata was given")
            else:
                # Read the sample again (same rows as the schema analysis) for the metadata
                table = generator._read_sample_data()
                metadata_file = generator.generate_and_save_metadata(table)
                if metadata_file:
                    print(f"Metadata file written to: {metadata_file}")

    except Exception as e:
        _fail(f"Error generating schema: {e}")


def main(argv: Optional[Sequence[str]] = None) -> None:
    """Run the CLI. Exits non-zero on any failure (see module docstring)."""
    p, _ingest, schema_gen = _build_parser()
    args = p.parse_args(argv)

    if args.cmd == "ingest":
        _run_ingest(args)
    elif args.cmd == "generate-schema":
        _run_generate_schema(schema_gen, args)
