"""Statistics calculation utilities for numeric and categorical data analysis."""

from __future__ import annotations
import statistics
from collections import Counter
from typing import Dict, List, Any, Union


class StatisticsCalculator:
    """Handles statistical calculations for data profiling."""

    @staticmethod
    def calculate_numeric_statistics(values: List[Union[int, float]], quantiles: List[float]) -> Dict[str, Any]:
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
                for q in quantiles:
                    idx = int(q * (len(sorted_values) - 1))
                    quantile_stats[f'p{int(q*100)}'] = sorted_values[idx]
                stats['quantiles'] = quantile_stats

            return {k: round(v, 4) if isinstance(v, float) else v for k, v in stats.items()}

        except (statistics.StatisticsError, ValueError):
            return {}

    @staticmethod
    def calculate_categorical_statistics(value_counter: Counter, non_null_count: int, top_n: int) -> List[Dict[str, Any]]:
        """Calculate statistics for categorical data."""
        if not value_counter or non_null_count == 0:
            return []

        top_values = value_counter.most_common(top_n)
        return [
            {
                'value': str(value),
                'count': count,
                'percentage': round(count / non_null_count * 100, 2)
            }
            for value, count in top_values
        ]

    @staticmethod
    def calculate_uniqueness_metrics(unique_count: int, non_null_count: int) -> Dict[str, Any]:
        """Calculate uniqueness metrics for a column."""
        if non_null_count == 0:
            return {'uniqueness_ratio': 0, 'unique_values_count': 0}

        return {
            'uniqueness_ratio': unique_count / non_null_count,
            'unique_values_count': unique_count
        }
