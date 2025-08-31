"""Engine core registry for forklift data readers.

Simplified instance-based implementation: an Engine object maintains a mapping
from reader kind -> callable. A single shared instance is created in
``forklift.engine.__init__``. Builtin readers are NOT auto-registered; users
must explicitly register them (or call a helper) to avoid silent defaults.
"""
from __future__ import annotations
from typing import Callable, Dict, Any
import polars as pl

ReaderFunction = Callable[[str, Dict[str, Any]], pl.DataFrame]
"""Callable signature for reader implementations.

Parameters
----------
str
    Source path (or identifier) to read.
Dict[str, Any]
    Options dictionary supplied by the public ``read`` interface.

Returns
-------
pl.DataFrame
    A populated Polars DataFrame.
"""


class Engine:
    """Registry of reader functions keyed by a short kind string.

    Starts empty; no implicit defaults are registered to ensure the caller
    intentionally configures available input formats.
    """

    def __init__(self) -> None:
        self._registered_readers: Dict[str, ReaderFunction] = {}

    # ---------------------------------------------------------------------
    def register_reader(
        self,
        reader_kind: str,
        reader_function: ReaderFunction,
        override: bool = False,
    ) -> None:
        normalized_kind = reader_kind.lower()
        if not override and normalized_kind in self._registered_readers:  # pragma: no cover - defensive
            raise ValueError(f"Reader already registered for kind '{normalized_kind}'")
        self._registered_readers[normalized_kind] = reader_function

    # ---------------------------------------------------------------------
    def read(self, reader_kind: str, source_path: str, **options: Any):
        normalized_kind = reader_kind.lower()
        try:
            reader = self._registered_readers[normalized_kind]
        except KeyError as e:  # pragma: no cover - defensive
            raise ValueError(
                f"No reader registered for kind '{normalized_kind}'. "
                f"Registered kinds: {sorted(self._registered_readers.keys()) or 'NONE'}"
            ) from e
        return reader(source_path, options)
