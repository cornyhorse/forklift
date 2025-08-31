"""CSV input adapter.

This module provides a thin wrapper around :func:`polars.read_csv` so the
engine registry can decouple builtin reader logic from the core registry
implementation. Additional preprocessing, auto-detection, and validation
steps can be added here later without touching the engine.
"""
from __future__ import annotations
from typing import Dict, Any
import polars as pl


def read(source_path: str, options: Dict[str, Any]) -> pl.DataFrame:
    """Read a CSV file into a Polars DataFrame.

    Parameters
    ----------
    source_path : str
        Path (or file-like accepted by Polars) to the CSV file.
    options : Dict[str, Any]
        Keyword-style options passed through from the engine (e.g. has_header, separator, dtypes).

    Returns
    -------
    pl.DataFrame
        Parsed CSV contents.
    """
    return pl.read_csv(source_path, **options)

__all__ = ["read"]

