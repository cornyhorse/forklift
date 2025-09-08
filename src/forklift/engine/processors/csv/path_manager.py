"""Path management utilities for CSV processing."""

from pathlib import Path
from typing import Union, Tuple
from ....io import is_s3_path, S3Path


class PathManager:
    """Handles path operations for both local and S3 locations."""

    @staticmethod
    def prepare_output_paths(output_path: Union[str, Path]) -> Tuple[str, str, bool]:
        """Prepare output file paths for both local and S3 outputs.

        Args:
            output_path: Base output path (local or S3)

        Returns:
            Tuple of (good_file_path, bad_file_path, use_s3_output)
        """
        if is_s3_path(output_path):
            # S3 output path
            output_s3_path = S3Path(str(output_path))
            good_file = str(output_s3_path.join("data.parquet"))
            bad_file = str(output_s3_path.join("bad_rows.parquet"))
            use_s3_output = True
        else:
            # Local output path
            output_dir = Path(output_path)
            output_dir.mkdir(parents=True, exist_ok=True)
            good_file = str(output_dir / "data.parquet")
            bad_file = str(output_dir / "bad_rows.parquet")
            use_s3_output = False

        return good_file, bad_file, use_s3_output
