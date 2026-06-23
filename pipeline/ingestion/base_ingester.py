"""
Abstract base ingester — defines the contract all concrete ingesters must fulfil.
"""
from __future__ import annotations

from abc import ABC, abstractmethod


class BaseIngester(ABC):
    """
    All ingesters must implement ``ingest()``, which returns a list of raw
    dicts representing one record each.  No cleaning or validation is done
    here; that is delegated to the cleaning layer.
    """

    @abstractmethod
    def ingest(self) -> list[dict]:
        """Read raw records from the source and return them as plain dicts."""
        ...

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}>"
