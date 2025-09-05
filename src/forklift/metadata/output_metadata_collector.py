"""Output metadata collector for processing statistics and data profiling."""

from __future__ import annotations
import json
import statistics
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union

import pyarrow as pa
import pyarrow.compute as pc


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
        quantiles: Optional[List[float]] = None
    ):
        """Initialize the output metadata collector.

        Args:
            enabled: Whether metadata collection is enabled
            enum_threshold: Threshold for detecting enumerable columns (uniqueness ratio)
            uniqueness_threshold: Threshold for detecting too-unique columns
            top_n_values: Number of top values to track for categorical columns
            quantiles: List of quantiles to calculate for numeric columns
        """
        self.enabled = enabled
        self.enum_threshold = enum_threshold
        self.uniqueness_threshold = uniqueness_threshold
        self.top_n_values = top_n_values
        self.quantiles = quantiles or [0.25, 0.5, 0.75, 0.9, 0.95, 0.99]

        # Statistics tracking
        self.total_rows = 0
        self.column_stats: Dict[str, Dict[str, Any]] = {}
        self.schema_info: Optional[pa.Schema] = None
        self.batch_count = 0

        # Value tracking for categorical analysis
        self._value_counters: Dict[str, Counter] = defaultdict(Counter)
        self._numeric_values: Dict[str, List] = defaultdict(list)

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
                    'data_type': str(field.type),
                    'null_count': 0,
                    'non_null_count': 0,
                    'unique_values': set(),
                    'min_value': None,
                    'max_value': None,
                    'is_numeric': self._is_numeric_type(field.type),
                    'is_string': self._is_string_type(field.type),
                    'is_temporal': self._is_temporal_type(field.type)
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
        return (pa.types.is_date(data_type) or
                pa.types.is_timestamp(data_type) or
                pa.types.is_time(data_type))

    def _update_column_stats(self, column_name: str, column: pa.Array) -> None:
        """Update statistics for a single column.

        Args:
            column_name: Name of the column
            column: PyArrow Array containing the column data
        """
        stats = self.column_stats[column_name]

        # Count nulls
        null_count = pc.count(column, mode='only_null').as_py()
        stats['null_count'] += null_count
        stats['non_null_count'] += len(column) - null_count

        # Skip further processing if all values are null
        if null_count == len(column):
            return

        # Get non-null values for analysis
        non_null_column = pc.drop_null(column)

        if len(non_null_column) == 0:
            return

        # Update min/max values
        try:
            if stats['is_numeric'] or stats['is_temporal']:
                col_min = pc.min(non_null_column).as_py()
                col_max = pc.max(non_null_column).as_py()

                if stats['min_value'] is None or col_min < stats['min_value']:
                    stats['min_value'] = col_min
                if stats['max_value'] is None or col_max > stats['max_value']:
                    stats['max_value'] = col_max

                # Collect numeric values for quantile calculation (sample to avoid memory issues)
                if stats['is_numeric'] and len(self._numeric_values[column_name]) < 10000:
                    values = non_null_column.to_pylist()
                    self._numeric_values[column_name].extend(values[:1000])  # Limit per batch

            elif stats['is_string']:
                # For strings, track length stats
                lengths = pc.utf8_length(non_null_column)
                min_len = pc.min(lengths).as_py()
                max_len = pc.max(lengths).as_py()

                if stats['min_value'] is None or min_len < stats['min_value']:
                    stats['min_value'] = min_len
                if stats['max_value'] is None or max_len > stats['max_value']:
                    stats['max_value'] = max_len

        except Exception:
            # Handle any type conversion errors gracefully
            pass

        # Track unique values for categorical analysis (limit to avoid memory issues)
        if len(stats['unique_values']) < 10000:
            try:
                unique_values = pc.unique(non_null_column).to_pylist()
                stats['unique_values'].update(unique_values[:1000])  # Limit unique values tracked

                # Update value counters for categorical columns
                if len(stats['unique_values']) <= 1000:  # Only for manageable cardinality
                    values = non_null_column.to_pylist()
                    self._value_counters[column_name].update(values)

            except Exception:
                # Handle any errors in unique value calculation
                pass

    def generate_metadata(self, schema: Optional[pa.Schema], source_info: Dict[str, Any]) -> Dict[str, Any]:
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
            'generation_timestamp': datetime.now().isoformat(),
            'source_info': source_info,
            'data_summary': {
                'total_rows': self.total_rows,
                'total_columns': len(self.column_stats),
                'batches_processed': self.batch_count,
                'schema': {
                    'fields': [
                        {
                            'name': working_schema.field(i).name,
                            'type': str(working_schema.field(i).type),
                            'nullable': working_schema.field(i).nullable
                        }
                        for i in range(len(working_schema))
                    ]
                } if working_schema else None
            },
            'column_statistics': self._generate_column_statistics(),
            'data_quality': self._generate_data_quality_metrics(),
            'profiling_config': {
                'enum_threshold': self.enum_threshold,
                'uniqueness_threshold': self.uniqueness_threshold,
                'top_n_values': self.top_n_values,
                'quantiles': self.quantiles
            }
        }

        return metadata

    def _generate_column_statistics(self) -> Dict[str, Dict[str, Any]]:
        """Generate detailed statistics for each column."""
        column_statistics = {}

        for column_name, stats in self.column_stats.items():
            total_values = stats['non_null_count'] + stats['null_count']
            null_percentage = (stats['null_count'] / total_values * 100) if total_values > 0 else 0

            column_stat = {
                'data_type': stats['data_type'],
                'total_values': total_values,
                'null_count': stats['null_count'],
                'non_null_count': stats['non_null_count'],
                'null_percentage': round(null_percentage, 2),
                'unique_values_count': len(stats['unique_values']),
                'uniqueness_ratio': len(stats['unique_values']) / stats['non_null_count'] if stats['non_null_count'] > 0 else 0
            }

            # Add min/max values
            if stats['min_value'] is not None:
                column_stat['min_value'] = stats['min_value']
            if stats['max_value'] is not None:
                column_stat['max_value'] = stats['max_value']

            # Determine if column is likely categorical or too unique (always calculate this)
            uniqueness_ratio = len(stats['unique_values']) / stats['non_null_count'] if stats['non_null_count'] > 0 else 0
            column_stat['likely_categorical'] = uniqueness_ratio <= self.enum_threshold
            column_stat['too_unique'] = uniqueness_ratio >= self.uniqueness_threshold

            # Add categorical statistics with top values
            if column_name in self._value_counters and len(self._value_counters[column_name]) > 0:
                top_values = self._value_counters[column_name].most_common(self.top_n_values)
                column_stat['top_values'] = [
                    {'value': str(value), 'count': count, 'percentage': round(count / stats['non_null_count'] * 100, 2)}
                    for value, count in top_values
                ]

            # Add numeric statistics
            if stats['is_numeric'] and column_name in self._numeric_values:
                numeric_values = self._numeric_values[column_name]
                if len(numeric_values) > 0:
                    column_stat['numeric_statistics'] = self._calculate_numeric_statistics(numeric_values)

            column_statistics[column_name] = column_stat

        return column_statistics

    def _calculate_numeric_statistics(self, values: List[Union[int, float]]) -> Dict[str, Any]:
        """Calculate numeric statistics for a list of values."""
        if not values:
            return {}

        try:
            stats = {
                'mean': statistics.mean(values),
                'median': statistics.median(values),
                'mode': statistics.mode(values) if len(values) > 1 else values[0],
                'standard_deviation': statistics.stdev(values) if len(values) > 1 else 0,
                'variance': statistics.variance(values) if len(values) > 1 else 0
            }

            # Calculate quantiles
            if len(values) > 1:
                sorted_values = sorted(values)
                quantile_stats = {}
                for q in self.quantiles:
                    idx = int(q * (len(sorted_values) - 1))
                    quantile_stats[f'p{int(q*100)}'] = sorted_values[idx]
                stats['quantiles'] = quantile_stats

            return {k: round(v, 4) if isinstance(v, float) else v for k, v in stats.items()}

        except (statistics.StatisticsError, ValueError):
            return {}

    def _generate_data_quality_metrics(self) -> Dict[str, Any]:
        """Generate overall data quality metrics."""
        if not self.column_stats:
            return {}

        total_columns = len(self.column_stats)
        columns_with_nulls = sum(1 for stats in self.column_stats.values() if stats['null_count'] > 0)

        # Calculate overall null percentage
        total_values = sum(stats['non_null_count'] + stats['null_count'] for stats in self.column_stats.values())
        total_nulls = sum(stats['null_count'] for stats in self.column_stats.values())
        overall_null_percentage = (total_nulls / total_values * 100) if total_values > 0 else 0

        # Identify potentially problematic columns
        high_null_columns = []
        too_unique_columns = []
        likely_categorical_columns = []

        for column_name, stats in self.column_stats.items():
            null_pct = (stats['null_count'] / (stats['non_null_count'] + stats['null_count']) * 100) if (stats['non_null_count'] + stats['null_count']) > 0 else 0

            if null_pct >= 40:  # 40% or more nulls (changed from > 40% to >= 40%)
                high_null_columns.append({
                    'column': column_name,
                    'null_percentage': round(null_pct, 2)
                })

            uniqueness_ratio = len(stats['unique_values']) / stats['non_null_count'] if stats['non_null_count'] > 0 else 0

            if uniqueness_ratio >= self.uniqueness_threshold:
                too_unique_columns.append({
                    'column': column_name,
                    'uniqueness_ratio': round(uniqueness_ratio, 4)
                })

            if uniqueness_ratio <= self.enum_threshold and stats['non_null_count'] > 0:
                likely_categorical_columns.append({
                    'column': column_name,
                    'unique_values': len(stats['unique_values']),
                    'uniqueness_ratio': round(uniqueness_ratio, 4)
                })

        return {
            'overall_null_percentage': round(overall_null_percentage, 2),
            'columns_with_nulls': columns_with_nulls,
            'columns_with_nulls_percentage': round(columns_with_nulls / total_columns * 100, 2),
            'high_null_columns': high_null_columns,
            'too_unique_columns': too_unique_columns,
            'likely_categorical_columns': likely_categorical_columns,
            'data_completeness_score': round(100 - overall_null_percentage, 2)
        }

    def save_metadata(self, output_path: Union[str, Path], filename: str = "output_metadata.json") -> Optional[str]:
        """Save collected metadata to a JSON file.

        Args:
            output_path: Directory path where metadata file should be saved
            filename: Name of the metadata file

        Returns:
            Path to the saved metadata file, or None if saving failed
        """
        if not self.enabled or self.total_rows == 0:
            return None

        try:
            output_dir = Path(output_path)
            output_dir.mkdir(parents=True, exist_ok=True)

            metadata_path = output_dir / filename

            # Generate metadata without schema (will use stored schema)
            metadata = self.generate_metadata(None, {
                'output_path': str(output_path),
                'filename': filename,
                'generation_method': 'output_metadata_collector'
            })

            # Convert sets to lists for JSON serialization
            def convert_sets_to_lists(obj):
                if isinstance(obj, dict):
                    return {k: convert_sets_to_lists(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [convert_sets_to_lists(item) for item in obj]
                elif isinstance(obj, set):
                    return list(obj)
                else:
                    return obj

            serializable_metadata = convert_sets_to_lists(metadata)

            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(serializable_metadata, f, indent=2, ensure_ascii=False, default=str)

            return str(metadata_path)

        except Exception as e:
            print(f"Error saving metadata: {e}")
            return None

    def reset(self) -> None:
        """Reset the collector to initial state."""
        self.total_rows = 0
        self.column_stats.clear()
        self.schema_info = None
        self.batch_count = 0
        self._value_counters.clear()
        self._numeric_values.clear()
