from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict

class Preprocessor(ABC):
    """Abstract row preprocessor interface.

    Implementations receive and return a row dict, possibly mutating values or
    raising errors which upstream code can capture.
    """
    @abstractmethod
    def apply(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a row in-place or return a new dict.

        :param row: Input row dictionary.
        :return: Transformed row dictionary.
        :raises Exception: Implementations may raise to signal row rejection.
        """
