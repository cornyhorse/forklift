"""API functions for Forklift schema generation.

This module provides programmatic access to schema generation functionality
that can be used by other Python applications.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Union, List
from pathlib import Path

from .schema.schema_generator import SchemaGenerator, SchemaGenerationConfig, OutputTarget, FileType


def generate_schema_from_csv(
    input_path: Union[str, Path],
    nrows: Optional[int] = None,  # Default to None (analyze entire file)
    delimiter: str = ",",
    encoding: str = "utf-8",
    include_sample_data: bool = False,  # Default to False to avoid sensitive data
    infer_primary_key_from_metadata: bool = False,  # Use metadata-based inference
    user_specified_primary_key: Optional[List[str]] = None  # Allow manual specification
) -> Dict[str, Any]:
    """Generate a Forklift schema from a CSV file.

    Args:
        input_path: Path to the CSV file (local or S3)
        nrows: Number of rows to analyze (default: None - analyze entire file)
        delimiter: CSV field delimiter
        encoding: File encoding
        include_sample_data: Include sample data in the schema (default: False)
        infer_primary_key_from_metadata: Infer primary key from metadata analysis (default: False)
        user_specified_primary_key: Manually specify primary key columns (default: None)

    Returns:
        Dictionary containing the generated schema

    Example:
        >>> schema = generate_schema_from_csv("data.csv")  # Analyzes entire file
        >>> print(schema["title"])
        Forklift CSV Schema - Generated

        >>> # Limit analysis to first 1000 rows
        >>> schema = generate_schema_from_csv("data.csv", nrows=1000)

        >>> # With primary key inference
        >>> schema = generate_schema_from_csv("data.csv", infer_primary_key_from_metadata=True)

        >>> # With manual primary key specification
        >>> schema = generate_schema_from_csv("data.csv", user_specified_primary_key=["user_id"])
    """
    # Validate input_path
    if input_path is None:
        raise ValueError("input_path cannot be None")
    if isinstance(input_path, str) and not input_path.strip():
        raise ValueError("input_path cannot be empty")

    config = SchemaGenerationConfig(
        input_path=input_path,
        file_type=FileType.CSV,
        nrows=nrows,
        output_target=OutputTarget.STDOUT,  # Not used for API calls
        delimiter=delimiter,
        encoding=encoding,
        include_sample_data=include_sample_data,
        infer_primary_key_from_metadata=infer_primary_key_from_metadata,
        user_specified_primary_key=user_specified_primary_key
    )

    generator = SchemaGenerator(config)
    return generator.generate_schema()


def generate_schema_from_excel(
    input_path: Union[str, Path],
    nrows: Optional[int] = 1000,  # Default to 1000 rows
    sheet_name: Optional[str] = None,
    include_sample_data: bool = False,  # Default to False to avoid sensitive data
    infer_primary_key_from_metadata: bool = False,  # Use metadata-based inference
    user_specified_primary_key: Optional[List[str]] = None  # Allow manual specification
) -> Dict[str, Any]:
    """Generate a Forklift schema from an Excel file.

    Args:
        input_path: Path to the Excel file (local or S3)
        nrows: Number of rows to analyze (default: 1000)
        sheet_name: Name or index of the Excel sheet
        include_sample_data: Include sample data in the schema (default: False)
        infer_primary_key_from_metadata: Infer primary key from metadata analysis (default: False)
        user_specified_primary_key: Manually specify primary key columns (default: None)

    Returns:
        Dictionary containing the generated schema

    Example:
        >>> schema = generate_schema_from_excel("data.xlsx", sheet_name="Sheet1")
        >>> print(len(schema["properties"]))
        5

        >>> # With primary key inference
        >>> schema = generate_schema_from_excel("data.xlsx", infer_primary_key_from_metadata=True)

        >>> # With manual primary key specification
        >>> schema = generate_schema_from_excel("data.xlsx", user_specified_primary_key=["record_id"])
    """
    # Validate input_path
    if input_path is None:
        raise ValueError("input_path cannot be None")
    if isinstance(input_path, str) and not input_path.strip():
        raise ValueError("input_path cannot be empty")

    config = SchemaGenerationConfig(
        input_path=input_path,
        file_type=FileType.EXCEL,
        nrows=nrows,
        output_target=OutputTarget.STDOUT,  # Not used for API calls
        sheet_name=sheet_name,
        include_sample_data=include_sample_data,
        infer_primary_key_from_metadata=infer_primary_key_from_metadata,
        user_specified_primary_key=user_specified_primary_key
    )

    generator = SchemaGenerator(config)
    return generator.generate_schema()


def generate_schema_from_parquet(
    input_path: Union[str, Path],
    nrows: Optional[int] = None,  # Default to None (analyze entire file)
    include_sample_data: bool = False,  # Default to False to avoid sensitive data
    infer_primary_key_from_metadata: bool = False,  # Use metadata-based inference
    user_specified_primary_key: Optional[List[str]] = None  # Allow manual specification
) -> Dict[str, Any]:
    """Generate a Forklift schema from a Parquet file.

    Args:
        input_path: Path to the Parquet file (local or S3)
        nrows: Number of rows to analyze (default: None - analyze entire file)
        include_sample_data: Include sample data in the schema (default: False)
        infer_primary_key_from_metadata: Infer primary key from metadata analysis (default: False)
        user_specified_primary_key: Manually specify primary key columns (default: None)

    Returns:
        Dictionary containing the generated schema

    Example:
        >>> schema = generate_schema_from_parquet("data.parquet")  # Analyzes entire file
        >>> print(schema["$schema"])
        https://json-schema.org/draft/2020-12/schema

        >>> # Limit analysis to first 1000 rows
        >>> schema = generate_schema_from_parquet("data.parquet", nrows=1000)

        >>> # With primary key inference
        >>> schema = generate_schema_from_parquet("data.parquet", infer_primary_key_from_metadata=True)

        >>> # With manual primary key specification
        >>> schema = generate_schema_from_parquet("data.parquet", user_specified_primary_key=["id"])
    """
    # Validate input_path
    if input_path is None:
        raise ValueError("input_path cannot be None")
    if isinstance(input_path, str) and not input_path.strip():
        raise ValueError("input_path cannot be empty")
    
    config = SchemaGenerationConfig(
        input_path=input_path,
        file_type=FileType.PARQUET,
        nrows=nrows,
        output_target=OutputTarget.STDOUT,  # Not used for API calls
        include_sample_data=include_sample_data,
        infer_primary_key_from_metadata=infer_primary_key_from_metadata,
        user_specified_primary_key=user_specified_primary_key
    )

    generator = SchemaGenerator(config)
    return generator.generate_schema()


def generate_and_save_schema(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    file_type: str,
    nrows: Optional[int] = None,
    **kwargs
) -> None:
    """Generate a schema and save it to a file.

    Args:
        input_path: Path to the input file
        output_path: Path where the schema should be saved
        file_type: Type of input file ("csv", "excel", "parquet")
        nrows: Number of rows to analyze
        **kwargs: Additional arguments passed to the generator

    Example:
        >>> generate_and_save_schema(
        ...     "data.csv",
        ...     "schema.json",
        ...     "csv",
        ...     nrows=1000
        ... )
    """
    config = SchemaGenerationConfig(
        input_path=input_path,
        file_type=FileType(file_type),
        nrows=nrows,
        output_target=OutputTarget.FILE,
        output_path=output_path,
        **kwargs
    )

    generator = SchemaGenerator(config)
    schema = generator.generate_schema()
    generator.output_schema(schema)


def generate_and_copy_schema(
    input_path: Union[str, Path],
    file_type: str,
    nrows: Optional[int] = None,
    **kwargs
) -> Dict[str, Any]:
    """Generate a schema and copy it to the clipboard.

    Args:
        input_path: Path to the input file
        file_type: Type of input file ("csv", "excel", "parquet")
        nrows: Number of rows to analyze
        **kwargs: Additional arguments passed to the generator

    Returns:
        Dictionary containing the generated schema

    Example:
        >>> schema = generate_and_copy_schema("data.csv", "csv")
        Schema copied to clipboard
    """
    config = SchemaGenerationConfig(
        input_path=input_path,
        file_type=FileType(file_type),
        nrows=nrows,
        output_target=OutputTarget.CLIPBOARD,
        **kwargs
    )

    generator = SchemaGenerator(config)
    schema = generator.generate_schema()
    generator.output_schema(schema)
    return schema
