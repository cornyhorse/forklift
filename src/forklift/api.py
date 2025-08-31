from __future__ import annotations
from typing import Any
import polars as pl

from .engine import read_csv as _engine_read_csv

class ForkliftFrame:
    """Lightweight wrapper around a polars DataFrame.

    Exposes the underlying polars frame via ._pl for advanced use.
    Attribute access falls back to the underlying DataFrame.
    """
    def __init__(self, df: pl.DataFrame):
        self._pl = df

    def __getattr__(self, item: str):  # pragma: no cover - thin passthrough
        return getattr(self._pl, item)

    def to_polars(self) -> pl.DataFrame:
        return self._pl


def read_csv(path: str | bytes | Any, *, schema_mode: str = "accept", **options: Any) -> pl.DataFrame:
    """Read a CSV file returning a polars DataFrame.

    Parameters
    ----------
    path : str | bytes | Any
        Path (or file-like object accepted by Polars) to the CSV file.
    schema_mode : {"accept", "infer", "enforce"}, default "accept"
        Placeholder schema handling mode forwarded to the engine layer.
    **options : Any
        Additional keyword arguments forwarded to the engine reader.
    """
    return _engine_read_csv(str(path), schema_mode=schema_mode, **options)


def read_fwf(*_, **__):  # pragma: no cover - placeholder
    raise NotImplementedError("read_fwf not yet implemented")


def read_excel(*_, **__):  # pragma: no cover - placeholder
    raise NotImplementedError("read_excel not yet implemented")


def read_sql(*_, **__):  # pragma: no cover - placeholder
    raise NotImplementedError("read_sql not yet implemented")

__all__ = [
    "ForkliftFrame",
    "read_csv",
    "read_fwf",
    "read_excel",
    "read_sql",
]
