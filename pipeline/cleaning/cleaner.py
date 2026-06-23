"""
DataCleaner — pre-validation cleaning of raw dicts.

Runs before Pydantic validation:
  1. Strip whitespace from all string fields
  2. Replace common null-like sentinels with None
  3. Truncate oversized string fields
  4. Normalize encoding issues (replace mojibake)
"""
from __future__ import annotations

import unicodedata

NULL_SENTINELS = frozenset(
    {"", "null", "none", "n/a", "na", "nil", "undefined", "-", "--"}
)

MAX_LENGTHS: dict[str, int] = {
    "name": 256,
    "email": 256,
    "sku": 64,
    "phone": 32,
    "status": 32,
    "category": 128,
}


class DataCleaner:
    """Stateless cleaning utility — all methods are class-level."""

    @classmethod
    def clean_record(cls, record: dict) -> dict:
        """Return a new cleaned dict; input is not mutated."""
        cleaned: dict = {}
        for key, value in record.items():
            value = cls._fix_encoding(value)
            value = cls._strip(value)
            value = cls._nullify(value)
            value = cls._truncate(key, value)
            cleaned[key] = value
        return cleaned

    @classmethod
    def clean_records(cls, records: list[dict]) -> list[dict]:
        return [cls.clean_record(r) for r in records]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _strip(value: object) -> object:
        if isinstance(value, str):
            return value.strip()
        return value

    @staticmethod
    def _nullify(value: object) -> object:
        if isinstance(value, str) and value.lower() in NULL_SENTINELS:
            return None
        return value

    @staticmethod
    def _truncate(key: str, value: object) -> object:
        max_len = MAX_LENGTHS.get(key)
        if max_len and isinstance(value, str) and len(value) > max_len:
            return value[:max_len]
        return value

    @staticmethod
    def _fix_encoding(value: object) -> object:
        """Normalize unicode to NFC and replace non-printable control chars."""
        if not isinstance(value, str):
            return value
        value = unicodedata.normalize("NFC", value)
        # Drop control characters (except tab/newline which may appear in text)
        return "".join(
            ch for ch in value if unicodedata.category(ch)[0] != "C" or ch in "\t\n"
        )
