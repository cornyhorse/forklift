"""Configuration parsing and validation."""

from typing import Dict, Any, List, Optional
import pyarrow as pa
from ..types.transformations import TransformationAnalyzer


class ConfigurationParser:
    """Parses and validates transformation configurations."""

    def __init__(self):
        self.analyzer = TransformationAnalyzer()

    def generate_transformation_extension(self, table: pa.Table) -> Dict[str, Any]:
        """Generate comprehensive data transformation extension configuration.

        Args:
            table: PyArrow table to analyze

        Returns:
            Dict: Transformation extension configuration
        """
        # Analyze columns to suggest appropriate transformations
        column_transformations = {}

        for i, field in enumerate(table.schema):
            column_name = field.name
            arrow_type = field.type
            column_data = table.column(i)

            # Analyze column data to suggest transformations
            suggestions = self.analyzer.analyze_column_for_transformations(column_name, column_data, arrow_type)
            if suggestions:
                column_transformations[column_name] = suggestions

        return {
            "description": "Data transformation configurations for cleaning and standardizing data",
            "version": "1.0.0",
            "global_settings": {
                "nan_handling": {
                    "allow_nan": True,
                    "nan_values": ["", "N/A", "NA", "NULL", "null", "NaN", "nan", "#N/A", "#NULL!", "None"],
                    "convert_to_null": True,
                    "error_on_nan": False
                },
                "error_handling": {
                    "on_transformation_error": "log",
                    "max_errors": 1000,
                    "continue_on_error": True
                }
            },
            "column_transformations": column_transformations,
            "transformation_types": self.analyzer.get_transformation_types_config()
        }

    def generate_primary_key_config(self, table: pa.Table, config) -> Optional[Dict[str, Any]]:
        """Generate primary key configuration based on user input or inference.

        Args:
            table: PyArrow table to analyze
            config: Schema generation configuration

        Returns:
            Optional[Dict]: Primary key configuration or None
        """
        if config.user_specified_primary_key:
            # Use user-specified primary key columns
            pk_columns = config.user_specified_primary_key
            return {
                "description": "User-specified primary key",
                "columns": pk_columns,
                "type": "composite" if len(pk_columns) > 1 else "single",
                "enforceUniqueness": True,
                "allowNulls": False,
                "description_detail": f"User-defined primary key on {', '.join(pk_columns)} field(s)"
            }
        elif config.infer_primary_key_from_metadata:
            # Infer primary key from the metadata
            return self._infer_primary_key_from_metadata(table)

        return None

    def _infer_primary_key_from_metadata(self, table: pa.Table) -> Optional[Dict[str, Any]]:
        """Infer primary key from metadata analysis.

        Args:
            table: PyArrow table to analyze

        Returns:
            Optional[Dict]: Inferred primary key configuration or None
        """
        # Import here to avoid circular imports
        from .metadata import MetadataGenerator

        metadata_gen = MetadataGenerator()
        metadata = metadata_gen.generate_metadata(table, {
            'enum_threshold': 0.1,
            'uniqueness_threshold': 0.95,
            'top_n_values': 10,
            'quantiles': [0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
        })

        if not metadata or "column_metadata" not in metadata:
            return None

        candidates = []

        # Analyze each column's metadata for primary key characteristics
        for column_name, col_meta in metadata["column_metadata"].items():
            is_not_null = col_meta.get("null_percentage", 100) == 0.0
            uniqueness_ratio = col_meta.get("uniqueness_ratio", 0.0)
            is_highly_unique = uniqueness_ratio >= 0.95
            distinct_count = col_meta.get("distinct_count", 0)

            # Check for typical primary key naming patterns
            has_pk_name_pattern = any(pattern in column_name.lower()
                                    for pattern in ['id', 'key', 'pk', 'uuid', 'guid'])

            if (is_not_null and is_highly_unique and has_pk_name_pattern and distinct_count <= 1000000):
                # Calculate a score for ranking candidates
                score = 0
                if uniqueness_ratio == 1.0:
                    score += 10
                elif uniqueness_ratio >= 0.99:
                    score += 8
                elif uniqueness_ratio >= 0.95:
                    score += 5

                # Bonus for good naming patterns - check specific patterns first
                if any(pattern in column_name.lower() for pattern in ['uuid', 'guid']):
                    score += 4
                elif any(pattern in column_name.lower() for pattern in ['key', 'pk']):
                    score += 3
                elif 'id' in column_name.lower():
                    score += 5

                # Penalty for very large distinct counts
                if distinct_count > 100000:
                    score -= 2
                elif distinct_count > 10000:
                    score -= 1

                candidates.append({
                    'column': column_name,
                    'score': score,
                    'uniqueness_ratio': uniqueness_ratio,
                    'distinct_count': distinct_count
                })

        if not candidates:
            return None

        # Sort by score and select the best candidate
        candidates.sort(key=lambda x: x['score'], reverse=True)
        best_candidate = candidates[0]

        # Only return if the score is reasonable
        if best_candidate['score'] >= 8:
            return {
                "description": "Inferred primary key from metadata analysis",
                "columns": [best_candidate['column']],
                "type": "single",
                "enforceUniqueness": True,
                "allowNulls": False,
                "description_detail": f"Inferred primary key on {best_candidate['column']} field "
                                    f"(uniqueness: {best_candidate['uniqueness_ratio']:.1%}, "
                                    f"distinct values: {best_candidate['distinct_count']}, "
                                    f"score: {best_candidate['score']})",
                "inference_metadata": {
                    "method": "metadata_analysis",
                    "score": best_candidate['score'],
                    "uniqueness_ratio": best_candidate['uniqueness_ratio'],
                    "distinct_count": best_candidate['distinct_count'],
                    "alternative_candidates": [c['column'] for c in candidates[1:3]]
                }
            }

        return None
