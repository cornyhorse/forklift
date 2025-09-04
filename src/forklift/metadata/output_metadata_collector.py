"""Output data metadata collector for Forklift processing pipeline.

This module provides functionality to collect comprehensive metadata about the final
transformed data that gets written to Parquet files, not the raw input data.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from pathlib import Path
import numpy as np

import pyarrow as pa
import pandas as pd

from ..io import UnifiedIOHandler, is_s3_path


class OutputMetadataCollector:
    """Collects metadata from final transformed output data."""

    def __init__(self,
                 enabled: bool = True,
                 enum_threshold: float = 0.1,
                 uniqueness_threshold: float = 0.95,
                 top_n_values: int = 10,
                 quantiles: List[float] = None):
        """Initialize metadata collector.

        Args:
            enabled: Whether metadata collection is enabled
            enum_threshold: Threshold for suggesting enum types (ratio of distinct values)
            uniqueness_threshold: Threshold for considering column too unique for enum
            top_n_values: Number of top/bottom values to include
            quantiles: Quantiles to calculate for numeric columns
        """
        self.enabled = enabled
        self.enum_threshold = enum_threshold
        self.uniqueness_threshold = uniqueness_threshold
        self.top_n_values = top_n_values
        self.quantiles = quantiles or [0.25, 0.5, 0.75, 0.9, 0.95, 0.99]

        # Accumulate data across batches
        self.accumulated_data: Dict[str, List] = {}
        self.total_rows = 0
        self.io_handler = UnifiedIOHandler()

    def add_batch(self, batch: pa.RecordBatch) -> None:
        """Add a batch of output data for metadata analysis.

        Args:
            batch: PyArrow RecordBatch of processed/transformed data
        """
        if not self.enabled or len(batch) == 0:
            return

        self.total_rows += len(batch)

        # Convert to pandas for easier analysis
        df = batch.to_pandas()

        # Accumulate data for each column
        for column_name in df.columns:
            if column_name not in self.accumulated_data:
                self.accumulated_data[column_name] = []

            # Store non-null values for analysis
            column_data = df[column_name].dropna()
            if len(column_data) > 0:
                self.accumulated_data[column_name].extend(column_data.tolist())

    def generate_metadata(self, schema: pa.Schema, source_info: Dict[str, Any] = None) -> Dict[str, Any]:
        """Generate comprehensive metadata from accumulated output data.

        Args:
            schema: PyArrow schema of the output data
            source_info: Additional information about the data source

        Returns:
            Dictionary containing comprehensive metadata about the output data
        """
        if not self.enabled:
            return {}

        metadata = {
            "description": "Metadata analysis of final transformed output data",
            "version": "1.0.0",
            "generated_at": datetime.now().isoformat(),
            "analysis_config": {
                "total_rows_processed": self.total_rows,
                "enum_threshold": self.enum_threshold,
                "uniqueness_threshold": self.uniqueness_threshold,
                "top_n_values": self.top_n_values,
                "quantiles": self.quantiles
            },
            "output_data_metadata": {
                "row_count": self.total_rows,
                "column_count": len(schema),
                "data_source": source_info or {}
            },
            "column_metadata": {},
            "enum_suggestions": {},
            "data_quality_summary": {}
        }

        # Analyze each column
        for i, field in enumerate(schema):
            column_name = field.name
            arrow_type = field.type

            if column_name not in self.accumulated_data:
                # Column has no data
                metadata["column_metadata"][column_name] = {
                    "name": column_name,
                    "type": str(arrow_type),
                    "null_count": self.total_rows,
                    "non_null_count": 0,
                    "null_percentage": 100.0,
                    "note": "No non-null values found in output data"
                }
                continue

            column_data = self.accumulated_data[column_name]
            non_null_count = len(column_data)
            null_count = self.total_rows - non_null_count

            # Basic metadata
            column_metadata = {
                "name": column_name,
                "type": str(arrow_type),
                "parquet_type": self._get_parquet_type_string(arrow_type),
                "nullable": field.nullable,
                "null_count": null_count,
                "non_null_count": non_null_count,
                "null_percentage": float(null_count / self.total_rows * 100) if self.total_rows > 0 else 0.0
            }

            if non_null_count > 0:
                # Convert to pandas series for analysis
                series = pd.Series(column_data)

                # Calculate distinct values and uniqueness
                distinct_count = series.nunique()
                column_metadata["distinct_count"] = distinct_count
                column_metadata["uniqueness_ratio"] = float(distinct_count / non_null_count)

                # Value frequency analysis
                value_counts = series.value_counts()

                # Top N values
                top_values = []
                for value, count in value_counts.head(self.top_n_values).items():
                    top_values.append({
                        "value": str(value),
                        "count": int(count),
                        "percentage": float(count / non_null_count * 100)
                    })
                column_metadata["top_values"] = top_values

                # Bottom N values (if there are enough unique values)
                if distinct_count > self.top_n_values:
                    bottom_values = []
                    for value, count in value_counts.tail(self.top_n_values).items():
                        bottom_values.append({
                            "value": str(value),
                            "count": int(count),
                            "percentage": float(count / non_null_count * 100)
                        })
                    column_metadata["bottom_values"] = bottom_values

                # Enum type suggestions
                enum_suggestion = self._analyze_enum_potential(column_name, series, value_counts)
                if enum_suggestion:
                    metadata["enum_suggestions"][column_name] = enum_suggestion

                # Type-specific statistics
                if pa.types.is_floating(arrow_type) or pa.types.is_integer(arrow_type):
                    numeric_stats = self._calculate_numeric_statistics(series)
                    column_metadata.update(numeric_stats)
                elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
                    string_stats = self._calculate_string_statistics(series)
                    column_metadata.update(string_stats)
                elif pa.types.is_boolean(arrow_type):
                    boolean_stats = self._calculate_boolean_statistics(series)
                    column_metadata.update(boolean_stats)

            metadata["column_metadata"][column_name] = column_metadata

        # Generate data quality summary
        metadata["data_quality_summary"] = self._generate_quality_summary(metadata["column_metadata"])

        return metadata

    def _analyze_enum_potential(self, column_name: str, series: pd.Series, value_counts: pd.Series) -> Optional[Dict[str, Any]]:
        """Analyze if a column is a good candidate for enum type."""
        if len(series) == 0:
            return None

        distinct_count = series.nunique()
        total_count = len(series)
        uniqueness_ratio = distinct_count / total_count

        # Check if it meets enum criteria
        is_enum_candidate = (
            uniqueness_ratio <= self.enum_threshold and  # Low uniqueness
            distinct_count <= 50 and  # Reasonable number of distinct values
            uniqueness_ratio < self.uniqueness_threshold  # Not too unique
        )

        if is_enum_candidate:
            # Calculate distribution balance
            top_value_percentage = value_counts.iloc[0] / total_count * 100
            distribution_balance = "balanced" if top_value_percentage < 50 else "skewed"

            return {
                "is_enum_candidate": True,
                "confidence": "high" if uniqueness_ratio <= 0.05 else "medium",
                "distinct_count": int(distinct_count),
                "uniqueness_ratio": float(uniqueness_ratio),
                "distribution_balance": distribution_balance,
                "top_value_dominance_percentage": float(top_value_percentage),
                "suggested_enum_values": value_counts.index.tolist(),
                "recommendation": f"Output column '{column_name}' appears to be categorical with {distinct_count} distinct values after processing. "
                               f"Consider using enum type with values: {', '.join(map(str, value_counts.head(10).index.tolist()))}"
            }

        return {
            "is_enum_candidate": False,
            "reason": f"Too unique ({uniqueness_ratio:.2%}) or too many distinct values ({distinct_count})",
            "distinct_count": int(distinct_count),
            "uniqueness_ratio": float(uniqueness_ratio)
        }

    def _calculate_numeric_statistics(self, series: pd.Series) -> Dict[str, Any]:
        """Calculate comprehensive numeric statistics."""
        if len(series) == 0:
            return {}

        try:
            stats = {
                "min_value": float(series.min()),
                "max_value": float(series.max()),
                "mean": float(series.mean()),
                "median": float(series.median()),
                "std_dev": float(series.std()),
                "variance": float(series.var())
            }

            # Calculate quantiles
            quantile_dict = {}
            for q in self.quantiles:
                quantile_dict[f"quantile_{int(q*100)}"] = float(series.quantile(q))
            stats["quantiles"] = quantile_dict

            # Additional statistics
            stats["range"] = float(stats["max_value"] - stats["min_value"])
            stats["coefficient_of_variation"] = float(stats["std_dev"] / stats["mean"]) if stats["mean"] != 0 else None

            # Detect potential outliers using IQR method
            q1 = series.quantile(0.25)
            q3 = series.quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            outliers = series[(series < lower_bound) | (series > upper_bound)]

            stats["outlier_count"] = len(outliers)
            stats["outlier_percentage"] = float(len(outliers) / len(series) * 100)

            return stats
        except Exception as e:
            return {"error": f"Failed to calculate numeric statistics: {str(e)}"}

    def _calculate_string_statistics(self, series: pd.Series) -> Dict[str, Any]:
        """Calculate string-specific statistics."""
        if len(series) == 0:
            return {}

        try:
            # Convert to string to handle mixed types
            str_series = series.astype(str)

            lengths = str_series.str.len()

            stats = {
                "min_length": int(lengths.min()),
                "max_length": int(lengths.max()),
                "avg_length": float(lengths.mean()),
                "median_length": float(lengths.median())
            }

            # Pattern analysis
            stats["empty_strings"] = int((str_series == "").sum())
            stats["contains_whitespace"] = int(str_series.str.contains(r'\s', na=False).sum())
            stats["contains_numbers"] = int(str_series.str.contains(r'\d', na=False).sum())
            stats["contains_special_chars"] = int(str_series.str.contains(r'[^a-zA-Z0-9\s]', na=False).sum())
            stats["all_uppercase"] = int(str_series.str.isupper().sum())
            stats["all_lowercase"] = int(str_series.str.islower().sum())

            # Character encoding analysis
            try:
                ascii_count = sum(1 for s in str_series if s.isascii())
                stats["ascii_only"] = ascii_count
                stats["non_ascii_count"] = len(str_series) - ascii_count
            except:
                stats["ascii_only"] = None
                stats["non_ascii_count"] = None

            return stats
        except Exception as e:
            return {"error": f"Failed to calculate string statistics: {str(e)}"}

    def _calculate_boolean_statistics(self, series: pd.Series) -> Dict[str, Any]:
        """Calculate boolean-specific statistics."""
        if len(series) == 0:
            return {}

        try:
            value_counts = series.value_counts()
            true_count = value_counts.get(True, 0)
            false_count = value_counts.get(False, 0)
            total = len(series)

            return {
                "true_count": int(true_count),
                "false_count": int(false_count),
                "true_percentage": float(true_count / total * 100) if total > 0 else 0.0,
                "false_percentage": float(false_count / total * 100) if total > 0 else 0.0
            }
        except Exception as e:
            return {"error": f"Failed to calculate boolean statistics: {str(e)}"}

    def _generate_quality_summary(self, column_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Generate overall data quality summary."""
        total_columns = len(column_metadata)
        if total_columns == 0:
            return {}

        columns_with_nulls = sum(1 for col in column_metadata.values() if col.get("null_count", 0) > 0)
        avg_null_percentage = sum(col.get("null_percentage", 0) for col in column_metadata.values()) / total_columns

        enum_candidates = sum(1 for col_name in column_metadata.keys() if col_name in self.accumulated_data)

        return {
            "total_columns": total_columns,
            "columns_with_nulls": columns_with_nulls,
            "average_null_percentage": float(avg_null_percentage),
            "data_completeness": float(100 - avg_null_percentage),
            "potential_enum_columns": enum_candidates,
            "total_rows_analyzed": self.total_rows
        }

    def _get_parquet_type_string(self, arrow_type: pa.DataType) -> str:
        """Convert Arrow type to Parquet type string."""
        if pa.types.is_int8(arrow_type):
            return "int8"
        elif pa.types.is_int16(arrow_type):
            return "int16"
        elif pa.types.is_int32(arrow_type):
            return "int32"
        elif pa.types.is_int64(arrow_type):
            return "int64"
        elif pa.types.is_uint8(arrow_type):
            return "uint8"
        elif pa.types.is_uint16(arrow_type):
            return "uint16"
        elif pa.types.is_uint32(arrow_type):
            return "uint32"
        elif pa.types.is_uint64(arrow_type):
            return "uint64"
        elif pa.types.is_float32(arrow_type):
            return "float32"
        elif pa.types.is_float64(arrow_type):
            return "double"
        elif pa.types.is_boolean(arrow_type):
            return "bool"
        elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return "string"
        elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
            return "binary"
        elif pa.types.is_date32(arrow_type):
            return "date32"
        elif pa.types.is_date64(arrow_type):
            return "date64"
        elif pa.types.is_timestamp(arrow_type):
            return "timestamp[ms]"  # Default to milliseconds
        elif pa.types.is_duration(arrow_type):
            return "duration[ms]"   # Default to milliseconds
        elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
            if hasattr(arrow_type, 'value_type'):
                return f"list<{self._get_parquet_type_string(arrow_type.value_type)}>"
            else:
                return "list<string>"  # fallback
        elif pa.types.is_struct(arrow_type):
            return "struct"
        elif pa.types.is_dictionary(arrow_type):
            return "dictionary<values=string, indices=int32>"
        else:
            return "string"  # Default fallback

    def save_metadata(self, output_path: Union[str, Path], filename: str = "output_metadata.json") -> Optional[str]:
        """Save collected metadata to file.

        Args:
            output_path: Directory path where metadata should be saved
            filename: Name of the metadata file

        Returns:
            Path to saved metadata file, or None if not enabled
        """
        if not self.enabled or self.total_rows == 0:
            return None

        if is_s3_path(str(output_path)):
            # S3 path
            from ..io import S3Path
            s3_path = S3Path(str(output_path))
            metadata_path = str(s3_path.join(filename))
        else:
            # Local path
            metadata_path = Path(output_path) / filename

        # Generate metadata with basic schema info
        basic_schema = pa.schema([
            pa.field(name, pa.string()) for name in self.accumulated_data.keys()
        ])

        metadata = self.generate_metadata(
            basic_schema,
            {"note": "Schema types approximated from accumulated data"}
        )

        metadata_json = json.dumps(metadata, indent=2, default=str)

        if is_s3_path(str(output_path)):
            with self.io_handler.open_for_write(metadata_path, encoding='utf-8') as f:
                f.write(metadata_json)
        else:
            with open(metadata_path, 'w', encoding='utf-8') as f:
                f.write(metadata_json)

        return str(metadata_path)
