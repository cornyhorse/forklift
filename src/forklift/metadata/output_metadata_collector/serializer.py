"""Metadata serialization utilities for saving and loading metadata files."""

from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Union


class MetadataSerializer:
    """Handles serialization and file operations for metadata."""

    @staticmethod
    def save_metadata(
        metadata: Dict[str, Any],
        output_path: Union[str, Path],
        filename: str = "output_metadata.json"
    ) -> Optional[str]:
        """Save metadata to a JSON file.

        Args:
            metadata: Metadata dictionary to save
            output_path: Directory path where metadata file should be saved
            filename: Name of the metadata file

        Returns:
            Path to the saved metadata file, or None if saving failed
        """
        try:
            output_dir = Path(output_path)
            output_dir.mkdir(parents=True, exist_ok=True)

            metadata_path = output_dir / filename

            # Convert sets to lists for JSON serialization
            serializable_metadata = MetadataSerializer._convert_sets_to_lists(metadata)

            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(serializable_metadata, f, indent=2, ensure_ascii=False, default=str)

            return str(metadata_path)

        except Exception as e:
            print(f"Error saving metadata: {e}")
            return None

    @staticmethod
    def _convert_sets_to_lists(obj):
        """Convert sets to lists for JSON serialization."""
        if isinstance(obj, dict):
            return {k: MetadataSerializer._convert_sets_to_lists(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [MetadataSerializer._convert_sets_to_lists(item) for item in obj]
        elif isinstance(obj, set):
            return list(obj)
        else:
            return obj

    @staticmethod
    def create_source_info(output_path: Union[str, Path], filename: str) -> Dict[str, Any]:
        """Create source information dictionary for metadata."""
        return {
            'output_path': str(output_path),
            'filename': filename,
            'generation_method': 'output_metadata_collector',
            'generation_timestamp': datetime.now().isoformat()
        }
