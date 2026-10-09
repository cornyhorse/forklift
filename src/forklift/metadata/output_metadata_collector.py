"""Output metadata collector for processing statistics and data profiling.

Privacy
-------
Value-bearing statistics (``top_values``, ``min_value``/``max_value`` of numeric and temporal
columns, ``median``/``mode``/``quantiles``) copy real cell contents into the metadata file, which
is usually stored next to the data with weaker access control. They are therefore **opt-in**:
pass ``include_value_statistics=True`` to :class:`OutputMetadataCollector`. With the default
(``False``) the metadata only carries counts, null counts, distinct counts, types, string length
statistics and the aggregate ``mean``/``standard_deviation``/``variance`` of numeric columns.
(A mean/variance over a column with one or two non-null values still reveals those values.)

Statistics design
-----------------
* Distinct values are tracked exactly up to ``max_distinct_tracked`` per column. Beyond that the
  reported distinct count is a lower bound (``distinct_count_is_lower_bound`` is ``True``) and
  ``uniqueness_ratio`` / ``likely_categorical`` / ``too_unique`` are ``None`` rather than a
  misleading number.
* Count, min, max, mean and variance of numeric columns are exact (running count/mean/M2,
  Welford/Chan merge), computed over finite values; NaN/inf are only counted
  (``non_finite_count``).
* Median/quantiles come from a uniform reservoir sample of ``sample_size`` values per numeric
  column (fixed seed, so runs are reproducible). They are exact while the column has no more than
  ``sample_size`` finite values and estimates afterwards (``quantiles_are_estimated``).
* ``top_values`` use exact counts and are only reported while the distinct count is exact.
"""

from __future__ import annotations

import json
import logging
import math
import random
import statistics
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pyarrow as pa
import pyarrow.compute as pc

from ..io import UnifiedIOHandler, is_s3_path

logger = logging.getLogger(__name__)

DEFAULT_QUANTILES = [0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
DEFAULT_MAX_DISTINCT_TRACKED = 10_000
DEFAULT_SAMPLE_SIZE = 10_000
# Fixed seed so that the same input always yields the same sampled quantiles.
SAMPLE_SEED = 20250826

# Keys of the numeric statistics that expose (or are derived from individual) data values.
_VALUE_BEARING_NUMERIC_KEYS = ("median", "mode", "quantiles")

# Stands in for every NaN when tracking distinct values (NaN != NaN would count each one).
_NAN_KEY = object()


class MetadataWriteError(RuntimeError):
    """Raised when the output metadata file cannot be written."""


def _quantile_label(q: float) -> str:
    """Label for a quantile, e.g. 0.25 -> ``p25``, 0.29 -> ``p29``, 0.999 -> ``p99.9``."""
    return f"p{round(q * 100, 6):g}"


def _validate_quantiles(quantiles: List[float]) -> List[float]:
    """Return ``quantiles`` if every entry is a real number within [0, 1], else raise."""
    for q in quantiles:
        if isinstance(q, bool) or not isinstance(q, (int, float)):
            raise ValueError(f"quantiles must be numbers between 0 and 1, got {q!r}")
        if not math.isfinite(q) or q < 0 or q > 1:
            raise ValueError(f"quantiles must be between 0 and 1 inclusive, got {q!r}")
    return list(quantiles)


def _json_safe(obj: Any) -> Any:
    """Recursively replace non-finite floats with ``None`` and sets with lists."""
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, (set, frozenset)):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, float) and not math.isfinite(obj):
        return None
    return obj


class _Reservoir:
    """Uniform fixed-size reservoir sample of a numeric stream (Li's "Algorithm L").

    The first ``size`` values are kept; afterwards value ``i`` replaces a random slot with
    probability ``size / (i + 1)``. Algorithm L draws the *gaps* between replacements, so only
    the selected values of a batch are materialised. The random sequence depends only on the
    seed and the order of values, not on how the stream is cut into batches.
    """

    def __init__(self, size: int, seed: int):
        self.size = size
        self.items: List[Union[int, float]] = []
        self.seen = 0
        self._rng = random.Random(seed)
        self._w = 0.0
        self._next = 0

    def __len__(self) -> int:
        return len(self.items)

    @property
    def is_complete(self) -> bool:
        """True while the sample still holds every value seen (quantiles are exact)."""
        return self.seen <= self.size

    def _draw(self) -> float:
        return 1.0 - self._rng.random()  # (0, 1]

    def _advance(self) -> None:
        """Compute the stream index of the next value to be selected."""
        w = min(max(self._w, 1e-300), 1.0 - 1e-16)
        self._next += int(math.log(self._draw()) / math.log(1.0 - w)) + 1

    def extend(self, values: pa.Array) -> None:
        """Offer every value of a non-null (finite) Arrow array to the reservoir."""
        n = len(values)
        pos = 0
        if len(self.items) < self.size:
            take = min(self.size - len(self.items), n)
            self.items.extend(values.slice(0, take).to_pylist())
            self.seen += take
            pos = take
            if len(self.items) == self.size:
                self._w = math.exp(math.log(self._draw()) / self.size)
                self._next = self.seen - 1
                self._advance()
        if pos >= n:
            return

        start = self.seen
        end = start + (n - pos)
        positions: List[int] = []
        slots: List[int] = []
        while self._next < end:
            positions.append(self._next - start + pos)
            slots.append(self._rng.randrange(self.size))
            self._w *= math.exp(math.log(self._draw()) / self.size)
            self._advance()
        self.seen = end
        if positions:
            chosen = values.take(pa.array(positions, type=pa.int64())).to_pylist()
            for slot, value in zip(slots, chosen):
                self.items[slot] = value


class OutputMetadataCollector:
    """Collects metadata and statistics from output data batches.

    This class accumulates statistics and metadata from processed data batches
    to generate comprehensive metadata about the final output dataset.
    """

    def __init__(
        self,
        enabled: bool = True,
        enum_threshold: float = 0.1,
        uniqueness_threshold: float = 0.95,
        top_n_values: int = 10,
        quantiles: Optional[List[float]] = None,
        include_value_statistics: bool = False,
        max_distinct_tracked: int = DEFAULT_MAX_DISTINCT_TRACKED,
        sample_size: int = DEFAULT_SAMPLE_SIZE,
    ):
        """Initialize the output metadata collector.

        Args:
            enabled: Whether metadata collection is enabled
            enum_threshold: Threshold for detecting enumerable columns (uniqueness ratio)
            uniqueness_threshold: Threshold for detecting too-unique columns
            top_n_values: Number of top values to track for categorical columns
            quantiles: Quantiles (each in [0, 1]) to calculate for numeric columns
            include_value_statistics: Also write statistics that expose real cell values
                (top values, numeric/temporal min/max, median, mode, quantiles). Default
                False because the metadata file may be less protected than the data.
            max_distinct_tracked: Per-column cap for exact distinct-value tracking; beyond it
                the distinct count becomes a lower bound.
            sample_size: Per-column reservoir size used for quantiles (only used when
                ``include_value_statistics`` is True).

        Raises:
            ValueError: If a quantile is outside [0, 1] or a size is not positive.
        """
        if max_distinct_tracked < 1:
            raise ValueError("max_distinct_tracked must be at least 1")
        if sample_size < 1:
            raise ValueError("sample_size must be at least 1")

        self.enabled = enabled
        self.enum_threshold = enum_threshold
        self.uniqueness_threshold = uniqueness_threshold
        self.top_n_values = top_n_values
        self.quantiles = _validate_quantiles(quantiles or DEFAULT_QUANTILES)
        self.include_value_statistics = include_value_statistics
        self.max_distinct_tracked = max_distinct_tracked
        self.sample_size = sample_size

        # Statistics tracking
        self.total_rows = 0
        self.column_stats: Dict[str, Dict[str, Any]] = {}
        self.schema_info: Optional[pa.Schema] = None
        self.batch_count = 0

        # Value tracking (only populated when include_value_statistics is True)
        self._value_counters: Dict[str, Counter] = {}
        self._numeric_values: Dict[str, _Reservoir] = {}

    def add_batch(self, batch: pa.RecordBatch) -> None:
        """Add a batch of data for metadata collection.

        Args:
            batch: PyArrow RecordBatch to analyze
        """
        if not self.enabled:
            return

        self.batch_count += 1
        self.total_rows += len(batch)

        # Store schema info from first batch
        if self.schema_info is None:
            self.schema_info = batch.schema

        # Initialize column stats if needed
        for field in batch.schema:
            if field.name not in self.column_stats:
                self.column_stats[field.name] = {
                    "data_type": str(field.type),
                    "null_count": 0,
                    "non_null_count": 0,
                    "unique_values": set(),
                    "distinct_supported": not pa.types.is_nested(field.type),
                    "distinct_overflow": False,
                    "min_value": None,
                    "max_value": None,
                    "non_finite_count": 0,
                    "numeric_count": 0,
                    "numeric_mean": 0.0,
                    "numeric_m2": 0.0,
                    "is_numeric": self._is_numeric_type(field.type),
                    "is_string": self._is_string_type(field.type),
                    "is_temporal": self._is_temporal_type(field.type),
                }

        # Collect statistics for each column
        for i, field in enumerate(batch.schema):
            column = batch.column(i)
            self._update_column_stats(field.name, column)

    def _is_numeric_type(self, data_type: pa.DataType) -> bool:
        """Check if data type is numeric."""
        return pa.types.is_integer(data_type) or pa.types.is_floating(data_type)

    def _is_string_type(self, data_type: pa.DataType) -> bool:
        """Check if data type is string-like."""
        return pa.types.is_string(data_type) or pa.types.is_large_string(data_type)

    def _is_temporal_type(self, data_type: pa.DataType) -> bool:
        """Check if data type is temporal."""
        return (
            pa.types.is_date(data_type)
            or pa.types.is_timestamp(data_type)
            or pa.types.is_time(data_type)
        )

    def _update_column_stats(self, column_name: str, column: pa.Array) -> None:
        """Update statistics for a single column.

        Args:
            column_name: Name of the column
            column: PyArrow Array containing the column data
        """
        stats = self.column_stats[column_name]

        # Count nulls
        null_count = pc.count(column, mode="only_null").as_py()
        stats["null_count"] += null_count
        stats["non_null_count"] += len(column) - null_count

        # Skip further processing if all values are null
        if null_count == len(column):
            return

        # Get non-null values for analysis
        non_null_column = pc.drop_null(column)

        if len(non_null_column) == 0:
            return

        try:
            if stats["is_numeric"]:
                self._update_numeric_stats(column_name, stats, non_null_column)
            elif stats["is_temporal"]:
                self._update_min_max(stats, pc.min(non_null_column), pc.max(non_null_column))
            elif stats["is_string"]:
                # For strings, track length stats
                lengths = pc.utf8_length(non_null_column)
                self._update_min_max(stats, pc.min(lengths), pc.max(lengths))
        except Exception as exc:  # pragma: no cover - defensive, never log values
            logger.debug("Skipping value statistics for column %r: %s", column_name, exc)

        self._track_distinct(column_name, stats, non_null_column)

    @staticmethod
    def _update_min_max(stats: Dict[str, Any], col_min: pa.Scalar, col_max: pa.Scalar) -> None:
        """Fold a batch minimum/maximum into the running exact minimum/maximum."""
        col_min, col_max = col_min.as_py(), col_max.as_py()
        if col_min is None or col_max is None:
            return
        if stats["min_value"] is None or col_min < stats["min_value"]:
            stats["min_value"] = col_min
        if stats["max_value"] is None or col_max > stats["max_value"]:
            stats["max_value"] = col_max

    def _update_numeric_stats(
        self, column_name: str, stats: Dict[str, Any], non_null_column: pa.Array
    ) -> None:
        """Update exact min/max/mean/variance and the quantile reservoir of a numeric column."""
        values = non_null_column
        if pa.types.is_floating(values.type):
            values = pc.filter(values, pc.is_finite(values))
            stats["non_finite_count"] += len(non_null_column) - len(values)
        n_b = len(values)
        if n_b == 0:
            return

        self._update_min_max(stats, pc.min(values), pc.max(values))

        # Exact running mean / M2 (Chan et al. parallel merge of Welford accumulators)
        mean_b = pc.mean(values).as_py()
        m2_b = pc.variance(values, ddof=0).as_py() * n_b
        n_a = stats["numeric_count"]
        if n_a == 0:
            stats["numeric_mean"], stats["numeric_m2"], stats["numeric_count"] = mean_b, m2_b, n_b
        else:
            n = n_a + n_b
            delta = mean_b - stats["numeric_mean"]
            stats["numeric_mean"] += delta * n_b / n
            stats["numeric_m2"] += m2_b + delta * delta * n_a * n_b / n
            stats["numeric_count"] = n

        if self.include_value_statistics:
            reservoir = self._numeric_values.get(column_name)
            if reservoir is None:
                reservoir = _Reservoir(self.sample_size, SAMPLE_SEED)
                self._numeric_values[column_name] = reservoir
            reservoir.extend(values)

    def _track_distinct(
        self, column_name: str, stats: Dict[str, Any], non_null_column: pa.Array
    ) -> None:
        """Track distinct values (and their exact counts) up to ``max_distinct_tracked``."""
        if not stats["distinct_supported"] or stats["distinct_overflow"]:
            return

        track_counts = self.include_value_statistics
        try:
            if track_counts:
                counted = pc.value_counts(non_null_column)
                batch_values = counted.field("values").to_pylist()
                batch_counts = counted.field("counts").to_pylist()
            else:
                batch_values = pc.unique(non_null_column).to_pylist()
                batch_counts = None
            if pa.types.is_floating(non_null_column.type):
                batch_values = [_NAN_KEY if v != v else v for v in batch_values]
            seen = stats["unique_values"]
            unseen = [v for v in batch_values if v not in seen]
        except (pa.ArrowException, TypeError) as exc:
            logger.debug("Distinct tracking unavailable for column %r: %s", column_name, exc)
            stats["distinct_supported"] = False
            self._value_counters.pop(column_name, None)
            return

        room = self.max_distinct_tracked - len(seen)
        if len(unseen) > room:
            # Cap reached: keep what fits, the count is now a lower bound and counts are inexact.
            seen.update(unseen[:room])
            stats["distinct_overflow"] = True
            self._value_counters.pop(column_name, None)
            return
        seen.update(unseen)

        if track_counts:
            counter = self._value_counters.setdefault(column_name, Counter())
            for value, count in zip(batch_values, batch_counts):
                counter[value] += count

    def generate_metadata(
        self, schema: Optional[pa.Schema], source_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate comprehensive metadata about the collected data.

        Args:
            schema: PyArrow schema of the output data
            source_info: Information about the data source and processing

        Returns:
            Dictionary containing comprehensive metadata
        """
        if not self.enabled:
            return {}

        # Get the schema to use
        working_schema = schema or self.schema_info

        metadata = {
            "generation_timestamp": datetime.now().isoformat(),
            "source_info": source_info,
            "data_summary": {
                "total_rows": self.total_rows,
                "total_columns": len(self.column_stats),
                "batches_processed": self.batch_count,
                "schema": (
                    {
                        "fields": [
                            {
                                "name": working_schema.field(i).name,
                                "type": str(working_schema.field(i).type),
                                "nullable": working_schema.field(i).nullable,
                            }
                            for i in range(len(working_schema))
                        ]
                    }
                    if working_schema
                    else None
                ),
            },
            "column_statistics": self._generate_column_statistics(),
            "data_quality": self._generate_data_quality_metrics(),
            "profiling_config": {
                "enum_threshold": self.enum_threshold,
                "uniqueness_threshold": self.uniqueness_threshold,
                "top_n_values": self.top_n_values,
                "quantiles": self.quantiles,
                "include_value_statistics": self.include_value_statistics,
                "max_distinct_tracked": self.max_distinct_tracked,
                "sample_size": self.sample_size,
            },
        }

        return metadata

    def _distinct_summary(self, stats: Dict[str, Any]) -> Dict[str, Any]:
        """Distinct count, whether it is only a lower bound, and the ratio if it is exact."""
        non_null = stats["non_null_count"]
        if not stats["distinct_supported"]:
            return {"count": None, "lower_bound": True, "ratio": None}
        count = len(stats["unique_values"])
        if stats["distinct_overflow"]:
            return {"count": count, "lower_bound": True, "ratio": None}
        return {"count": count, "lower_bound": False, "ratio": count / non_null if non_null else 0}

    def _generate_column_statistics(self) -> Dict[str, Dict[str, Any]]:
        """Generate detailed statistics for each column."""
        column_statistics = {}

        for column_name, stats in self.column_stats.items():
            total_values = stats["non_null_count"] + stats["null_count"]
            null_percentage = (stats["null_count"] / total_values * 100) if total_values > 0 else 0
            distinct = self._distinct_summary(stats)
            ratio = distinct["ratio"]

            column_stat = {
                "data_type": stats["data_type"],
                "total_values": total_values,
                "null_count": stats["null_count"],
                "non_null_count": stats["non_null_count"],
                "null_percentage": round(null_percentage, 2),
                "unique_values_count": distinct["count"],
                "distinct_count_is_lower_bound": distinct["lower_bound"],
                "uniqueness_ratio": ratio,
                # Unknown (None) rather than guessed once distinct tracking is capped
                "likely_categorical": None if ratio is None else ratio <= self.enum_threshold,
                "too_unique": None if ratio is None else ratio >= self.uniqueness_threshold,
            }

            if stats["is_string"]:
                # String "min/max" are lengths, not values
                if stats["min_value"] is not None:
                    column_stat["min_length"] = stats["min_value"]
                if stats["max_value"] is not None:
                    column_stat["max_length"] = stats["max_value"]
            elif self.include_value_statistics:
                if stats["min_value"] is not None:
                    column_stat["min_value"] = stats["min_value"]
                if stats["max_value"] is not None:
                    column_stat["max_value"] = stats["max_value"]

            if stats["is_numeric"] and stats["non_finite_count"]:
                column_stat["non_finite_count"] = stats["non_finite_count"]

            # Top values: exact counts, only while the distinct count itself is exact
            if self.include_value_statistics:
                counter = self._value_counters.get(column_name)
                if counter:
                    column_stat["top_values"] = [
                        {
                            "value": "NaN" if value is _NAN_KEY else str(value),
                            "count": count,
                            "percentage": round(count / stats["non_null_count"] * 100, 2),
                        }
                        for value, count in counter.most_common(self.top_n_values)
                    ]
                elif distinct["lower_bound"] and stats["non_null_count"] > 0:
                    column_stat["top_values_unavailable"] = "distinct values exceed tracking cap"

            # Add numeric statistics
            if stats["is_numeric"] and stats["numeric_count"] > 0:
                reservoir = self._numeric_values.get(column_name)
                numeric = self._calculate_numeric_statistics(
                    reservoir.items if reservoir else [],
                    aggregate=stats,
                    estimated=bool(reservoir) and not reservoir.is_complete,
                )
                if not self.include_value_statistics:
                    numeric = {
                        k: v for k, v in numeric.items() if k not in _VALUE_BEARING_NUMERIC_KEYS
                    }
                if numeric:
                    column_stat["numeric_statistics"] = numeric

            column_statistics[column_name] = column_stat

        return _json_safe(column_statistics)

    def _calculate_numeric_statistics(
        self,
        values: List[Union[int, float]],
        aggregate: Optional[Dict[str, Any]] = None,
        estimated: bool = False,
    ) -> Dict[str, Any]:
        """Calculate numeric statistics.

        Args:
            values: The values (or reservoir sample) used for median, mode and quantiles
            aggregate: Optional column stats holding the exact running ``numeric_count``,
                ``numeric_mean`` and ``numeric_m2``; when given, mean/variance/std come from
                them instead of from ``values``
            estimated: True if ``values`` is a sample of the column rather than all of it
        """
        if not values and not aggregate:
            return {}

        try:
            stats: Dict[str, Any] = {}
            if aggregate:
                n = aggregate["numeric_count"]
                variance = aggregate["numeric_m2"] / (n - 1) if n > 1 else 0
                stats["mean"] = aggregate["numeric_mean"]
                stats["standard_deviation"] = math.sqrt(variance)
                stats["variance"] = variance
            else:
                stats["mean"] = statistics.mean(values)
                stats["standard_deviation"] = statistics.stdev(values) if len(values) > 1 else 0
                stats["variance"] = statistics.variance(values) if len(values) > 1 else 0

            if values:
                stats["median"] = statistics.median(values)
                if not estimated:
                    stats["mode"] = statistics.mode(values) if len(values) > 1 else values[0]

                # Nearest-rank quantiles over the (sampled) values
                if len(values) > 1:
                    sorted_values = sorted(values)
                    top = len(sorted_values) - 1
                    stats["quantiles"] = {
                        _quantile_label(q): sorted_values[min(top, int(q * top + 0.5))]
                        for q in self.quantiles
                    }
                    stats["quantiles_are_estimated"] = estimated
                    stats["sample_size"] = len(values)

            return {k: round(v, 4) if isinstance(v, float) else v for k, v in stats.items()}

        except (statistics.StatisticsError, ValueError):
            return {}

    def _generate_data_quality_metrics(self) -> Dict[str, Any]:
        """Generate overall data quality metrics."""
        if not self.column_stats:
            return {}

        total_columns = len(self.column_stats)
        columns_with_nulls = sum(
            1 for stats in self.column_stats.values() if stats["null_count"] > 0
        )

        # Calculate overall null percentage
        total_values = sum(
            stats["non_null_count"] + stats["null_count"] for stats in self.column_stats.values()
        )
        total_nulls = sum(stats["null_count"] for stats in self.column_stats.values())
        overall_null_percentage = (total_nulls / total_values * 100) if total_values > 0 else 0

        # Identify potentially problematic columns
        high_null_columns = []
        too_unique_columns = []
        likely_categorical_columns = []

        for column_name, stats in self.column_stats.items():
            null_pct = (
                (stats["null_count"] / (stats["non_null_count"] + stats["null_count"]) * 100)
                if (stats["non_null_count"] + stats["null_count"]) > 0
                else 0
            )

            if null_pct >= 40:  # 40% or more nulls (changed from > 40% to >= 40%)
                high_null_columns.append(
                    {"column": column_name, "null_percentage": round(null_pct, 2)}
                )

            uniqueness_ratio = self._distinct_summary(stats)["ratio"]
            if uniqueness_ratio is None:
                continue  # distinct count is only a lower bound: make no claim

            if uniqueness_ratio >= self.uniqueness_threshold:
                too_unique_columns.append(
                    {"column": column_name, "uniqueness_ratio": round(uniqueness_ratio, 4)}
                )

            if uniqueness_ratio <= self.enum_threshold and stats["non_null_count"] > 0:
                likely_categorical_columns.append(
                    {
                        "column": column_name,
                        "unique_values": len(stats["unique_values"]),
                        "uniqueness_ratio": round(uniqueness_ratio, 4),
                    }
                )

        return {
            "overall_null_percentage": round(overall_null_percentage, 2),
            "columns_with_nulls": columns_with_nulls,
            "columns_with_nulls_percentage": round(columns_with_nulls / total_columns * 100, 2),
            "high_null_columns": high_null_columns,
            "too_unique_columns": too_unique_columns,
            "likely_categorical_columns": likely_categorical_columns,
            "data_completeness_score": round(100 - overall_null_percentage, 2),
        }

    def save_metadata(
        self,
        output_path: Union[str, Path],
        filename: str = "output_metadata.json",
        s3_client=None,
        *,
        schema: Optional[pa.Schema] = None,
        source_info: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Save collected metadata to a JSON file (local directory or ``s3://`` prefix).

        Args:
            output_path: Directory (or S3 prefix such as ``s3://bucket/prefix``) for the file
            filename: Name of the metadata file
            s3_client: Optional S3 client used for ``s3://`` destinations
            schema: Schema of the output data (defaults to the schema seen while collecting)
            source_info: Provenance recorded in the file (defaults to the output location)

        Returns:
            Path/URI of the saved metadata file, or None if there is nothing to save
            (collector disabled or no rows seen)

        Raises:
            MetadataWriteError: If the metadata cannot be serialised or written. The caller
                decides whether that is fatal.
            ValueError: If ``output_path`` is a pathlib-collapsed S3 URI (``s3:/bucket``).
        """
        if not self.enabled or self.total_rows == 0:
            return None

        output_text = str(output_path)
        if output_text.startswith("s3:/") and not output_text.startswith("s3://"):
            raise ValueError("S3 destination was collapsed by pathlib; pass 's3://...' as str")
        to_s3 = is_s3_path(output_text)
        destination = f"{output_text.rstrip('/')}/{filename}" if to_s3 else None

        try:
            # Generate metadata without schema (will use stored schema)
            metadata = self.generate_metadata(
                schema,
                source_info
                or {
                    "output_path": output_text,
                    "filename": filename,
                    "generation_method": "output_metadata_collector",
                },
            )
            # allow_nan=False: NaN/inf must never reach the file (they are not valid JSON)
            payload = json.dumps(
                _json_safe(metadata), indent=2, ensure_ascii=False, default=str, allow_nan=False
            )

            # Serialise first, then write once, so a failure never leaves a partial upload
            if to_s3:
                handler = UnifiedIOHandler(s3_client)
                with handler.open_for_write(destination, encoding="utf-8") as f:
                    f.write(payload)
                return destination

            output_dir = Path(output_path)
            output_dir.mkdir(parents=True, exist_ok=True)
            metadata_path = output_dir / filename
            with open(metadata_path, "w", encoding="utf-8") as f:
                f.write(payload)
            return str(metadata_path)

        except Exception as exc:
            logger.error("Failed to save output metadata: %s", type(exc).__name__)
            raise MetadataWriteError(
                f"Could not write output metadata to {destination or output_text}: "
                f"{type(exc).__name__}"
            ) from exc

    def reset(self) -> None:
        """Reset the collector to initial state."""
        self.total_rows = 0
        self.column_stats.clear()
        self.schema_info = None
        self.batch_count = 0
        self._value_counters.clear()
        self._numeric_values.clear()
