"""Parquet output adapter.

Thin wrapper around Polars Parquet writing so engine stays format-agnostic.
"""
from __future__ import annotations
from typing import Dict, Any
import polars as pl

def write(df: pl.DataFrame, destination_path: str, options: Dict[str, Any]):
    """Write DataFrame to Parquet.

    Parameters
    ----------
    df : pl.DataFrame
        Frame to serialize.
    destination_path : str
        Target file path (or compatible sink) for Parquet output.
    options : Dict[str, Any]
        Additional keyword args forwarded to ``DataFrame.write_parquet``.
    """
    df.write_parquet(destination_path, **options)

__all__ = ["write"]

