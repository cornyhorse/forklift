"""Metadata generation and analysis.

All statistics are computed with ``pyarrow.compute`` (no pandas). Value-bearing statistics
(top/bottom values, enum value lists, min/max/median/quantiles) are only produced when
``include_value_statistics`` is set in the configuration: they embed raw cell values, which
may be personal data, into the generated schema.
"""

import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc

from ..utils.helpers import (
    DEFAULT_QUANTILES,
    get_parquet_type_string,
    quantile_label,
    source_basename,
    to_json_safe,
    validate_quantiles,
)

# Enum candidates must not have more distinct values than this.
MAX_ENUM_DISTINCT_VALUES = 50


def _finite(value: Any) -> Optional[float]:
    """Return ``value`` as a float, or ``None`` if it is missing, NaN or infinite."""
    if value is None:
        return None
    value = float(value)
    return value if math.isfinite(value) else None


def _value_text(value: Any) -> str:
    """Text form of a cell value used in ``top_values`` / ``bottom_values``."""
    if isinstance(value, float):
        return str(value)  # "inf" is a perfectly good string, unlike a JSON number
    safe = to_json_safe(value)
    return safe if isinstance(safe, str) else str(safe)


def _enum_value(value: Any) -> Any:
    """JSON-safe enum candidate value (non-finite floats are kept as text, not nulled)."""
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    return to_json_safe(value)


class MetadataGenerator:
    """Generates comprehensive metadata for data analysis and profiling."""

    def generate_metadata(self, table: pa.Table, config: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive metadata object from PyArrow table.

        Args:
            table: PyArrow table to analyze
            config: Configuration with thresholds and settings. Recognised keys:
                ``enum_threshold``, ``uniqueness_threshold``, ``top_n_values``, ``quantiles``,
                ``source_file`` (only the file name is recorded) and
                ``include_value_statistics`` (default ``False``).

        Returns:
            Dict: Comprehensive metadata object

        Raises:
            ValueError: If the configured quantiles are not within [0, 1]
        """
        quantiles = validate_quantiles(config.get("quantiles", DEFAULT_QUANTILES))
        config = {**config, "quantiles": quantiles}
        include_values = bool(config.get("include_value_statistics", False))

        metadata = {
            "description": "Column-level metadata analysis for data "
            "profiling and enum type suggestions",
            "version": "1.0.0",
            "generated_at": datetime.now().isoformat(),
            "analysis_config": {
                "rows_analyzed": table.num_rows,
                "enum_threshold": config.get("enum_threshold", 0.1),
                "uniqueness_threshold": config.get("uniqueness_threshold", 0.95),
                "top_n_values": config.get("top_n_values", 10),
                "quantiles": quantiles,
                "include_value_statistics": include_values,
            },
            "table_metadata": {
                "row_count": table.num_rows,
                "column_count": len(table.schema),
                "source_file": source_basename(config.get("source_file", "unknown")),
            },
            "column_metadata": {},
            "enum_suggestions": {},
        }

        # Generate column-level metadata
        for i, field in enumerate(table.schema):
            column_name = field.name
            column_metadata, enum_suggestion = self._analyze_column(
                column_name, field, table.column(i), config
            )
            metadata["column_metadata"][column_name] = column_metadata
            if enum_suggestion:
                metadata["enum_suggestions"][column_name] = enum_suggestion

        return to_json_safe(metadata)

    def _analyze_column(
        self, column_name: str, field: pa.Field, column: pa.ChunkedArray, config: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
        """Build the metadata entry (and enum suggestion) for one column."""
        include_values = bool(config.get("include_value_statistics", False))
        arrow_type = field.type
        total = len(column)
        null_count = int(column.null_count)

        column_metadata = {
            "name": column_name,
            "type": str(arrow_type),
            "parquet_type": get_parquet_type_string(arrow_type),
            "nullable": field.nullable,
            "null_count": null_count,
            "non_null_count": int(total - null_count),
            "null_percentage": float(null_count / total * 100) if total > 0 else 0.0,
        }

        # Dictionary-encoded columns are analysed on their decoded values
        values = self._decode(column)
        value_type = values.type
        is_float = pa.types.is_floating(value_type)

        # NaN is a value, not a null: report it separately for floating point columns
        if is_float:
            nan_count = self._count_true(pc.is_nan(values))
            column_metadata["nan_count"] = nan_count
            column_metadata["nan_percentage"] = (
                float(nan_count / total * 100) if total > 0 else 0.0
            )

        non_null = self._without_nulls(values)
        non_null_total = len(non_null)
        enum_suggestion = None

        # Distinct values and uniqueness (not defined for nested/unhashable types)
        distinct_count = None
        if non_null_total > 0 and self._is_hashable(value_type):
            try:
                distinct_count = int(pc.count_distinct(non_null, mode="only_valid").as_py())
            except pa.ArrowException:
                distinct_count = None

        if distinct_count is not None:
            column_metadata["distinct_count"] = distinct_count
            column_metadata["uniqueness_ratio"] = float(distinct_count / non_null_total)

            counted = self._sorted_value_counts(non_null) if include_values else None
            enum_suggestion = self._analyze_enum_potential(
                column_name, non_null, distinct_count, config, include_values, counted
            )

            if include_values:
                top_n = config.get("top_n_values", 10)
                column_metadata["top_values"] = self._frequency_entries(
                    counted, 0, top_n, non_null_total
                )
                # Bottom N values (if there are enough unique values)
                if distinct_count > top_n:
                    column_metadata["bottom_values"] = self._frequency_entries(
                        counted, max(len(counted[0]) - top_n, 0), top_n, non_null_total
                    )

        # Type-specific statistics
        if pa.types.is_floating(value_type) or pa.types.is_integer(value_type):
            column_metadata.update(self._calculate_numeric_statistics(non_null, config))
        elif self._is_string(value_type):
            column_metadata.update(self._calculate_string_statistics(values))
        elif pa.types.is_boolean(value_type):
            column_metadata.update(self._calculate_boolean_statistics(non_null))

        return column_metadata, enum_suggestion

    # ------------------------------------------------------------------
    # Arrow helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _decode(column):
        """Decode dictionary-encoded data so statistics work on the real values."""
        if pa.types.is_dictionary(column.type):
            return column.cast(column.type.value_type)
        return column

    @staticmethod
    def _without_nulls(values):
        """Drop nulls and (for floating point data) NaN values."""
        non_null = values.drop_null()
        if pa.types.is_floating(non_null.type) and len(non_null) > 0:
            non_null = non_null.filter(pc.invert(pc.is_nan(non_null)))
        return non_null

    @staticmethod
    def _count_true(boolean_values) -> int:
        counted = pc.sum(boolean_values).as_py()
        return int(counted) if counted else 0

    @staticmethod
    def _is_hashable(arrow_type: pa.DataType) -> bool:
        """Whether Arrow can compute distinct values/value counts for the type."""
        if isinstance(arrow_type, pa.BaseExtensionType):
            return False
        return not (pa.types.is_nested(arrow_type) or pa.types.is_union(arrow_type))

    @staticmethod
    def _is_string(arrow_type: pa.DataType) -> bool:
        if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return True
        return bool(getattr(pa.types, "is_string_view", lambda _t: False)(arrow_type))

    @staticmethod
    def _sorted_value_counts(non_null) -> Tuple[pa.Array, pa.Array]:
        """Value counts ordered by descending frequency (ties keep first-seen order)."""
        counted = pc.value_counts(non_null)
        counts = counted.field("counts")
        order = pc.array_sort_indices(counts, order="descending")
        return counted.field("values").take(order), counts.take(order)

    @staticmethod
    def _frequency_entries(
        counted: Tuple[pa.Array, pa.Array], start: int, count: int, total: int
    ) -> List[Dict[str, Any]]:
        values, counts = counted
        window_values = values.slice(start, count).to_pylist()
        window_counts = counts.slice(start, count).to_pylist()
        return [
            {
                "value": _value_text(value),
                "count": int(frequency),
                "percentage": float(frequency / total * 100),
            }
            for value, frequency in zip(window_values, window_counts)
        ]

    # ------------------------------------------------------------------
    # Enum analysis
    # ------------------------------------------------------------------

    def _analyze_enum_potential(
        self,
        column_name: str,
        non_null,
        distinct_count: int,
        config: Dict[str, Any],
        include_values: bool = False,
        counted: Optional[Tuple[pa.Array, pa.Array]] = None,
    ) -> Dict[str, Any]:
        """Analyze if a column is a good candidate for enum type.

        Args:
            column_name: Name of the column
            non_null: Non-null values of the column (hashable type, at least one value)
            distinct_count: Number of distinct values in ``non_null``
            config: Analysis configuration (thresholds)
            include_values: Whether the enum value list may be embedded in the result
            counted: Value counts by descending frequency, if already computed
        """
        total_count = len(non_null)
        uniqueness_ratio = distinct_count / total_count

        enum_threshold = config.get("enum_threshold", 0.1)
        uniqueness_threshold = config.get("uniqueness_threshold", 0.95)

        # Check if it meets enum criteria
        is_enum_candidate = (
            uniqueness_ratio <= enum_threshold
            and distinct_count <= MAX_ENUM_DISTINCT_VALUES
            and uniqueness_ratio < uniqueness_threshold
        )

        if is_enum_candidate:
            if counted is None:
                counted = self._sorted_value_counts(non_null)
            # Calculate distribution balance
            top_value_percentage = counted[1][0].as_py() / total_count * 100
            distribution_balance = "balanced" if top_value_percentage < 50 else "skewed"

            suggestion = {
                "is_enum_candidate": True,
                "confidence": "high" if uniqueness_ratio <= 0.05 else "medium",
                "distinct_count": int(distinct_count),
                "uniqueness_ratio": float(uniqueness_ratio),
                "distribution_balance": distribution_balance,
                "top_value_dominance_percentage": float(top_value_percentage),
            }
            recommendation = (
                f"Column '{column_name}' appears to be categorical with "
                f"{distinct_count} distinct values. Consider using enum type"
            )
            if include_values:
                enum_values = counted[0].to_pylist()
                suggestion["suggested_enum_values"] = [_enum_value(v) for v in enum_values]
                recommendation += " with values: " + ", ".join(
                    _value_text(v) for v in enum_values[:10]
                )
            else:
                recommendation += " (enable include_value_statistics to list the values)"
            suggestion["recommendation"] = recommendation
            return suggestion

        return {
            "is_enum_candidate": False,
            "reason": f"Too unique ({uniqueness_ratio:.2%}) or "
            f"too many distinct values ({distinct_count})",
            "distinct_count": int(distinct_count),
            "uniqueness_ratio": float(uniqueness_ratio),
        }

    # ------------------------------------------------------------------
    # Type-specific statistics
    # ------------------------------------------------------------------

    def _calculate_numeric_statistics(self, array, config: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate comprehensive numeric statistics.

        Args:
            array: Numeric Arrow array/chunked array (nulls and NaN are ignored)
            config: Analysis configuration (``quantiles``, ``include_value_statistics``)

        Returns:
            Dict of statistics. ``min_value``, ``max_value``, ``median``, ``quantiles`` and
            ``range`` are only present when ``include_value_statistics`` is true. Values that are
            undefined (for example the standard deviation of a single value) are ``None``.
        """
        include_values = bool(config.get("include_value_statistics", False))
        quantiles = validate_quantiles(config.get("quantiles"))

        try:
            data = self._without_nulls(array)
            if len(data) == 0:
                return {}
            # Float64 avoids integer overflow in sums and mirrors the reported float values
            data = data.cast(pa.float64(), safe=False)

            value_range = pc.min_max(data)
            minimum = _finite(value_range["min"].as_py())
            maximum = _finite(value_range["max"].as_py())
            mean = _finite(pc.mean(data).as_py())
            std_dev = _finite(pc.stddev(data, ddof=1).as_py())
            variance = _finite(pc.variance(data, ddof=1).as_py())

            stats: Dict[str, Any] = {}
            if include_values:
                stats["min_value"] = minimum
                stats["max_value"] = maximum
            stats["mean"] = mean
            if include_values:
                stats["median"] = _finite(pc.quantile(data, q=0.5)[0].as_py())
            stats["std_dev"] = std_dev
            stats["variance"] = variance

            if include_values:
                # Calculate quantiles
                quantile_values = pc.quantile(data, q=quantiles, interpolation="linear")
                stats["quantiles"] = {
                    f"quantile_{quantile_label(q)}": _finite(value)
                    for q, value in zip(quantiles, quantile_values.to_pylist())
                }

                # Additional statistics
                stats["range"] = (
                    float(maximum - minimum)
                    if minimum is not None and maximum is not None
                    else None
                )
            stats["coefficient_of_variation"] = (
                float(std_dev / mean) if std_dev is not None and mean not in (None, 0.0) else None
            )

            # Detect potential outliers using IQR method
            q1, q3 = (_finite(v) for v in pc.quantile(data, q=[0.25, 0.75]).to_pylist())
            outlier_count = 0
            if q1 is not None and q3 is not None:
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                outside = pc.or_(pc.less(data, lower_bound), pc.greater(data, upper_bound))
                outlier_count = self._count_true(outside)

            stats["outlier_count"] = outlier_count
            stats["outlier_percentage"] = float(outlier_count / len(data) * 100)

            return stats
        except Exception as e:
            # Exception text is not included: it can quote data values
            return {"error": f"Failed to calculate numeric statistics ({type(e).__name__})"}

    def _calculate_string_statistics(self, array) -> Dict[str, Any]:
        """Calculate string-specific statistics.

        Args:
            array: String Arrow array/chunked array including its nulls. ``empty_strings``
                counts zero-length strings plus nulls (empty CSV cells are read as nulls).
        """
        if len(array) == 0:
            return {}

        try:
            non_null = array.drop_null()
            null_count = int(array.null_count)

            stats: Dict[str, Any] = {}
            if len(non_null) > 0:
                lengths = pc.utf8_length(non_null)
                lengths_float = lengths.cast(pa.float64())
                length_range = pc.min_max(lengths)
                stats["min_length"] = int(length_range["min"].as_py())
                stats["max_length"] = int(length_range["max"].as_py())
                stats["avg_length"] = float(pc.mean(lengths_float).as_py())
                stats["median_length"] = float(pc.quantile(lengths_float, q=0.5)[0].as_py())
                empty_strings = self._count_true(pc.equal(lengths, 0))
            else:
                stats["min_length"] = None
                stats["max_length"] = None
                stats["avg_length"] = None
                stats["median_length"] = None
                empty_strings = 0

            # Pattern analysis
            stats["empty_strings"] = empty_strings + null_count
            stats["contains_whitespace"] = self._count_matches(non_null, r"[\s\p{Z}]")
            stats["contains_numbers"] = self._count_matches(non_null, r"\p{Nd}")
            stats["contains_special_chars"] = self._count_matches(non_null, r"[^a-zA-Z0-9\s\p{Z}]")
            stats["all_uppercase"] = self._count_true(pc.utf8_is_upper(non_null))
            stats["all_lowercase"] = self._count_true(pc.utf8_is_lower(non_null))

            # Character encoding analysis
            ascii_count = self._count_true(pc.string_is_ascii(non_null))
            stats["ascii_only"] = ascii_count
            stats["non_ascii_count"] = len(non_null) - ascii_count

            return stats
        except Exception as e:
            return {"error": f"Failed to calculate string statistics ({type(e).__name__})"}

    def _count_matches(self, strings, pattern: str) -> int:
        if len(strings) == 0:
            return 0
        return self._count_true(pc.match_substring_regex(strings, pattern))

    def _calculate_boolean_statistics(self, array) -> Dict[str, Any]:
        """Calculate boolean-specific statistics."""
        try:
            non_null = array.drop_null()
            total = len(non_null)
            if total == 0:
                return {}

            true_count = self._count_true(non_null)
            false_count = total - true_count

            return {
                "true_count": int(true_count),
                "false_count": int(false_count),
                "true_percentage": float(true_count / total * 100),
                "false_percentage": float(false_count / total * 100),
            }
        except Exception as e:
            return {"error": f"Failed to calculate boolean statistics ({type(e).__name__})"}
