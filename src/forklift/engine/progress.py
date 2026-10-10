"""Progress reporting and cancellation for the importers.

``import_csv``, ``import_excel`` and ``import_sql`` accept two optional callbacks:

* ``progress(event)`` is called at every batch boundary (per batch for CSV and SQL, per sheet for
  Excel) with ``{"rows_read": int, "rows_rejected": int, "bytes_read": int}``: the rows read so
  far (valid and rejected), the rows rejected so far, and the bytes of the input consumed so far
  (0 where the engine cannot tell, for example for SQL sources and ``s3://`` CSV inputs).
* ``cancel()`` is called right after each progress event; when it returns True the import stops
  with :class:`~forklift.engine.exceptions.ImportCancelled` and its outputs are discarded.

A progress callback may itself raise an :class:`~forklift.engine.exceptions.ImportInterrupted`
(``forklift.jobs`` raises ``LimitExceededError`` this way) to stop the import the same way.
"""

from __future__ import annotations

from typing import Callable, Dict, Optional

from .exceptions import ImportCancelled

ProgressCallback = Callable[[Dict[str, int]], None]
CancelCallback = Callable[[], bool]


class ImportHooks:
    """The progress and cancellation callbacks of one import (both optional).

    Args:
        progress: Called with a progress event at every batch boundary
        cancel: Asked after every progress event whether the import should stop

    Raises:
        TypeError: If a callback is given but not callable
    """

    def __init__(
        self,
        progress: Optional[ProgressCallback] = None,
        cancel: Optional[CancelCallback] = None,
    ):
        for name, value in (("progress", progress), ("cancel", cancel)):
            if value is not None and not callable(value):
                raise TypeError(f"{name} must be callable, got {type(value).__name__}")
        self.progress = progress
        self.cancel = cancel

    def report(self, rows_read: int, rows_rejected: int = 0, bytes_read: int = 0) -> None:
        """Report a batch boundary, then stop the import if ``cancel()`` asks for it.

        Raises:
            ImportCancelled: ``cancel()`` returned True
        """
        if self.progress is not None:
            self.progress(
                {"rows_read": rows_read, "rows_rejected": rows_rejected, "bytes_read": bytes_read}
            )
        if self.cancel is not None and self.cancel():
            raise ImportCancelled(
                f"The import was cancelled after {rows_read} row(s) "
                "(the cancel callback returned True); no output was kept"
            )


#: Hooks that do nothing (an import without callbacks)
NO_HOOKS = ImportHooks()
