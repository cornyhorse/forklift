from __future__ import annotations
import argparse
from .engine.forklift_core import ForkliftCore, ImportConfig, HeaderMode
from .io import is_s3_path
from .schema.schema_generator import SchemaGenerator, SchemaGenerationConfig, OutputTarget, FileType


def main() -> None:
    p = argparse.ArgumentParser("forklift")
    sub = p.add_subparsers(dest="cmd", required=True)

    ingest = sub.add_parser("ingest", help="Clean & write to Parquet")
    ingest.add_argument("source", help="Input path (local file or S3 URI: s3://bucket/key)")
    ingest.add_argument("--dest", required=True, help="Output path (local directory or S3 URI: s3://bucket/prefix/)")
    ingest.add_argument("--input-kind", choices=["csv","fwf","excel"], required=True)
    ingest.add_argument("--schema", help="Path to JSON Schema file (local or S3)")
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

    # Add schema generation command
    schema_gen = sub.add_parser("generate-schema", help="Generate schema from data file")
    schema_gen.add_argument("source", help="Input path (local file or S3 URI: s3://bucket/key)")
    schema_gen.add_argument("--file-type", choices=["csv", "excel", "parquet"], required=True, help="Type of input file")
    schema_gen.add_argument("--nrows", type=int, help="Number of rows to analyze (default: 1000)")
    schema_gen.add_argument("--output", choices=["stdout", "file", "clipboard"], default="stdout", help="Output target")
    schema_gen.add_argument("--output-path", help="Output file path (required when --output=file)")
    schema_gen.add_argument("--delimiter", default=",", help="CSV delimiter (default: comma)")
    schema_gen.add_argument("--encoding", default="utf-8", help="File encoding (default: utf-8)")
    schema_gen.add_argument("--sheet", help="Excel sheet name or index")
    schema_gen.add_argument("--include-sample", action="store_true", help="Include sample data in schema")
    schema_gen.add_argument("--infer-primary-key", action="store_true", help="Infer primary key from metadata analysis")

    # New metadata generation options
    schema_gen.add_argument("--no-metadata", action="store_true", help="Disable metadata generation (default: enabled)")
    schema_gen.add_argument("--metadata-output", help="Output path for separate metadata file")
    schema_gen.add_argument("--enum-threshold", type=float, default=0.1, help="Threshold for suggesting enum types (default: 0.1)")
    schema_gen.add_argument("--uniqueness-threshold", type=float, default=0.95, help="Threshold for considering column too unique for enum (default: 0.95)")
    schema_gen.add_argument("--top-n-values", type=int, default=10, help="Number of top/bottom values to include (default: 10)")
    schema_gen.add_argument("--quantiles", nargs="*", type=float, help="Custom quantiles for numeric columns (default: 0.25 0.5 0.75 0.9 0.95 0.99)")

    args = p.parse_args()

    if args.cmd == "ingest":
        # Check for S3 paths and provide user feedback
        input_is_s3 = is_s3_path(args.source)
        output_is_s3 = is_s3_path(args.dest)

        if input_is_s3:
            print(f"Reading from S3: {args.source}")
        if output_is_s3:
            print(f"Writing to S3: {args.dest}")

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
            if results.manifest_file:
                print(f"Manifest file: {results.manifest_file}")
            if results.metadata_file:
                print(f"Metadata file: {results.metadata_file}")
        else:
            print(f"Error: Input kind '{args.input_kind}' not yet implemented in new ForkliftCore. Only 'csv' is currently supported.")
    elif args.cmd == "generate-schema":
        # Validate output arguments
        if args.output == "file" and not args.output_path:
            print("Error: --output-path is required when --output=file")
            return

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
            infer_primary_key_from_metadata=args.infer_primary_key,  # Use new metadata-based inference
            # New metadata generation options
            generate_metadata=not args.no_metadata,
            metadata_output_path=args.metadata_output,
            enum_threshold=args.enum_threshold,
            uniqueness_threshold=args.uniqueness_threshold,
            top_n_values=args.top_n_values,
            quantiles=args.quantiles if args.quantiles else None
        )

        try:
            # Generate schema
            generator = SchemaGenerator(config)
            schema = generator.generate_schema()
            generator.output_schema(schema)

            # Generate and save separate metadata file if requested
            if config.metadata_output_path:
                # Read the data again for full metadata generation
                if config.file_type == FileType.CSV:
                    table = generator._read_csv_sample()
                elif config.file_type == FileType.EXCEL:
                    table = generator._read_excel_sample()
                elif config.file_type == FileType.PARQUET:
                    table = generator._read_parquet_sample()

                metadata_file = generator.generate_and_save_metadata(table)
                if metadata_file:
                    print(f"Metadata file written to: {metadata_file}")

        except Exception as e:
            print(f"Error generating schema: {e}")
            return
