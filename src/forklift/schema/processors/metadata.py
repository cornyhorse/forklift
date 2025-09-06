"""Metadata generation and analysis."""

from typing import Dict, Any, List, Optional
from datetime import datetime
import pandas as pd
import pyarrow as pa
from ..utils.helpers import get_parquet_type_string


class MetadataGenerator:
    """Generates comprehensive metadata for data analysis and profiling."""

    def generate_metadata(self, table: pa.Table, config: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive metadata object from PyArrow table.

        Args:
            table: PyArrow table to analyze
            config: Configuration with thresholds and settings

        Returns:
            Dict: Comprehensive metadata object
        """
        metadata = {
            "description": "Column-level metadata analysis for data profiling and enum type suggestions",
            "version": "1.0.0",
            "generated_at": datetime.now().isoformat(),
            "analysis_config": {
                "rows_analyzed": table.num_rows,
                "enum_threshold": config.get('enum_threshold', 0.1),
                "uniqueness_threshold": config.get('uniqueness_threshold', 0.95),
                "top_n_values": config.get('top_n_values', 10),
                "quantiles": config.get('quantiles', [0.25, 0.5, 0.75, 0.9, 0.95, 0.99])
            },
            "table_metadata": {
                "row_count": table.num_rows,
                "column_count": len(table.schema),
                "source_file": config.get('source_file', 'unknown')
            },
            "column_metadata": {},
            "enum_suggestions": {}
        }

        # Generate column-level metadata
        for i, field in enumerate(table.schema):
            column_name = field.name
            column_data = table.column(i)
            arrow_type = field.type

            # Convert to pandas for easier analysis
            pandas_series = column_data.to_pandas()

            # Basic metadata
            column_metadata = {
                "name": column_name,
                "type": str(field.type),
                "parquet_type": get_parquet_type_string(arrow_type),
                "nullable": field.nullable,
                "null_count": int(column_data.null_count),
                "non_null_count": int(len(pandas_series) - column_data.null_count),
                "null_percentage": float(column_data.null_count / len(pandas_series) * 100) if len(pandas_series) > 0 else 0.0
            }

            # Add NaN count for numeric types
            if pa.types.is_floating(arrow_type):
                nan_count = int(pandas_series.isna().sum() - column_data.null_count)
                column_metadata["nan_count"] = nan_count
                column_metadata["nan_percentage"] = float(nan_count / len(pandas_series) * 100) if len(pandas_series) > 0 else 0.0

            # Calculate distinct values and uniqueness
            non_null_series = pandas_series.dropna()
            if len(non_null_series) > 0:
                distinct_count = non_null_series.nunique()
                column_metadata["distinct_count"] = int(distinct_count)
                column_metadata["uniqueness_ratio"] = float(distinct_count / len(non_null_series))

                # Generate value frequency analysis
                value_counts = non_null_series.value_counts()

                # Top N values
                top_values = []
                top_n = config.get('top_n_values', 10)
                for value, count in value_counts.head(top_n).items():
                    top_values.append({
                        "value": str(value),
                        "count": int(count),
                        "percentage": float(count / len(non_null_series) * 100)
                    })
                column_metadata["top_values"] = top_values

                # Bottom N values (if there are enough unique values)
                if distinct_count > top_n:
                    bottom_values = []
                    for value, count in value_counts.tail(top_n).items():
                        bottom_values.append({
                            "value": str(value),
                            "count": int(count),
                            "percentage": float(count / len(non_null_series) * 100)
                        })
                    column_metadata["bottom_values"] = bottom_values

                # Enum type suggestions
                enum_suggestion = self._analyze_enum_potential(column_name, non_null_series, value_counts, config)
                if enum_suggestion:
                    metadata["enum_suggestions"][column_name] = enum_suggestion

            # Type-specific statistics
            if pa.types.is_floating(arrow_type) or pa.types.is_integer(arrow_type):
                numeric_stats = self._calculate_numeric_statistics(non_null_series, config)
                column_metadata.update(numeric_stats)
            elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
                string_stats = self._calculate_string_statistics(pandas_series)
                column_metadata.update(string_stats)
            elif pa.types.is_boolean(arrow_type):
                boolean_stats = self._calculate_boolean_statistics(non_null_series)
                column_metadata.update(boolean_stats)

            metadata["column_metadata"][column_name] = column_metadata

        return metadata

    def _analyze_enum_potential(self, column_name: str, series: pd.Series, value_counts: pd.Series, config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Analyze if a column is a good candidate for enum type."""
        if len(series) == 0:
            return None

        distinct_count = series.nunique()
        total_count = len(series)
        uniqueness_ratio = distinct_count / total_count

        enum_threshold = config.get('enum_threshold', 0.1)
        uniqueness_threshold = config.get('uniqueness_threshold', 0.95)

        # Check if it meets enum criteria
        is_enum_candidate = (
            uniqueness_ratio <= enum_threshold and
            distinct_count <= 50 and
            uniqueness_ratio < uniqueness_threshold
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
                "recommendation": f"Column '{column_name}' appears to be categorical with {distinct_count} distinct values. "
                               f"Consider using enum type with values: {', '.join(map(str, value_counts.head(10).index.tolist()))}"
            }

        return {
            "is_enum_candidate": False,
            "reason": f"Too unique ({uniqueness_ratio:.2%}) or too many distinct values ({distinct_count})",
            "distinct_count": int(distinct_count),
            "uniqueness_ratio": float(uniqueness_ratio)
        }

    def _calculate_numeric_statistics(self, series: pd.Series, config: Dict[str, Any]) -> Dict[str, Any]:
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
            quantiles = config.get('quantiles', [0.25, 0.5, 0.75, 0.9, 0.95, 0.99])
            for q in quantiles:
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
            original_series = series.copy()
            str_series = series.astype(str)

            # Count empty strings and NaN values
            empty_string_count = 0
            for val in original_series:
                if pd.isna(val):
                    continue
                elif str(val) == "":
                    empty_string_count += 1

            nan_count = original_series.isna().sum()
            str_series_for_analysis = str_series.copy()
            lengths = str_series_for_analysis.str.len()

            stats = {
                "min_length": int(lengths.min()),
                "max_length": int(lengths.max()),
                "avg_length": float(lengths.mean()),
                "median_length": float(lengths.median())
            }

            # Pattern analysis
            actual_empty_strings = int((str_series_for_analysis == "").sum())
            stats["empty_strings"] = actual_empty_strings + int(nan_count)
            stats["contains_whitespace"] = int(str_series_for_analysis.str.contains(r'\s', na=False).sum())
            stats["contains_numbers"] = int(str_series_for_analysis.str.contains(r'\d', na=False).sum())
            stats["contains_special_chars"] = int(str_series_for_analysis.str.contains(r'[^a-zA-Z0-9\s]', na=False).sum())
            stats["all_uppercase"] = int(str_series_for_analysis.str.isupper().sum())
            stats["all_lowercase"] = int(str_series_for_analysis.str.islower().sum())

            # Character encoding analysis
            try:
                ascii_count = sum(1 for s in str_series_for_analysis if isinstance(s, str) and s.isascii())
                stats["ascii_only"] = ascii_count
                stats["non_ascii_count"] = len(str_series_for_analysis) - ascii_count
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
