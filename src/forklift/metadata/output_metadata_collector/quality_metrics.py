"""Data quality metrics generation for comprehensive data profiling."""

from __future__ import annotations
from typing import Dict, List, Any


class QualityMetricsGenerator:
    """Handles generation of data quality metrics and analysis."""

    @staticmethod
    def generate_data_quality_metrics(
        column_stats: Dict[str, Dict[str, Any]],
        enum_threshold: float,
        uniqueness_threshold: float
    ) -> Dict[str, Any]:
        """Generate overall data quality metrics."""
        if not column_stats:
            return {}

        total_columns = len(column_stats)
        columns_with_nulls = sum(1 for stats in column_stats.values() if stats['null_count'] > 0)

        # Calculate overall null percentage
        total_values = sum(stats['non_null_count'] + stats['null_count'] for stats in column_stats.values())
        total_nulls = sum(stats['null_count'] for stats in column_stats.values())
        overall_null_percentage = (total_nulls / total_values * 100) if total_values > 0 else 0

        # Identify potentially problematic columns
        high_null_columns = []
        too_unique_columns = []
        likely_categorical_columns = []

        for column_name, stats in column_stats.items():
            null_pct = (stats['null_count'] / (stats['non_null_count'] + stats['null_count']) * 100) if (stats['non_null_count'] + stats['null_count']) > 0 else 0

            if null_pct >= 40:  # 40% or more nulls
                high_null_columns.append({
                    'column': column_name,
                    'null_percentage': round(null_pct, 2)
                })

            uniqueness_ratio = len(stats['unique_values']) / stats['non_null_count'] if stats['non_null_count'] > 0 else 0

            if uniqueness_ratio >= uniqueness_threshold:
                too_unique_columns.append({
                    'column': column_name,
                    'uniqueness_ratio': round(uniqueness_ratio, 4)
                })

            if uniqueness_ratio <= enum_threshold and stats['non_null_count'] > 0:
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
