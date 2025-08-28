from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Iterable, Dict, Any
from ..types import Row

class BaseInput(ABC):
    """
    Abstract base class for all input types.

    :param source: The data source (e.g., file path, connection string).
    :type source: str
    :param opts: Additional options for the input type.
    :type opts: Any
    """
    def __init__(self, source: str, **opts: Any):
        """
        Initialize the input with a source and options.

        :param source: The data source (e.g., file path, connection string).
        :type source: str
        :param opts: Additional options for the input type.
        :type opts: Any
        """
        self.source = source
        self.opts = opts

    @abstractmethod
    def iter_rows(self) -> Iterable[Row]:
        """
        Iterate over rows in the input source.

        :return: An iterable of Row objects.
        :rtype: Iterable[Row]
        """
