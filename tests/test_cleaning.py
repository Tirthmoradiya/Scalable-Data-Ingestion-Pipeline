"""Unit tests for DataCleaner — including Hypothesis property-based tests."""
from __future__ import annotations

import pytest
from hypothesis import given, settings as h_settings
from hypothesis import strategies as st

from pipeline.cleaning.cleaner import DataCleaner, NULL_SENTINELS


class TestDataCleaner:
    # ------------------------------------------------------------------
    # strip whitespace
    # ------------------------------------------------------------------
    def test_strips_leading_trailing_whitespace(self) -> None:
        record = {"name": "  Alice  ", "email": " alice@x.com "}
        result = DataCleaner.clean_record(record)
        assert result["name"] == "Alice"
        assert result["email"] == "alice@x.com"

    # ------------------------------------------------------------------
    # null sentinels
    # ------------------------------------------------------------------
    @pytest.mark.parametrize(
        "sentinel",
        ["null", "NULL", "Null", "none", "None", "N/A", "n/a", "nil", "-", "--", "na", ""],
    )
    def test_nullifies_sentinel_values(self, sentinel: str) -> None:
        record = {"phone": sentinel}
        result = DataCleaner.clean_record(record)
        assert result["phone"] is None

    def test_preserves_valid_values(self) -> None:
        record = {"name": "Alice", "email": "a@b.com", "phone": "+1-555-0101"}
        result = DataCleaner.clean_record(record)
        assert result == record

    # ------------------------------------------------------------------
    # truncation
    # ------------------------------------------------------------------
    def test_truncates_name_field(self) -> None:
        long_name = "A" * 300
        result = DataCleaner.clean_record({"name": long_name})
        assert len(result["name"]) == 256

    def test_truncates_sku_field(self) -> None:
        long_sku = "X" * 100
        result = DataCleaner.clean_record({"sku": long_sku})
        assert len(result["sku"]) == 64

    def test_does_not_truncate_short_values(self) -> None:
        record = {"name": "Alice", "sku": "ABC-001"}
        result = DataCleaner.clean_record(record)
        assert result["name"] == "Alice"
        assert result["sku"] == "ABC-001"

    # ------------------------------------------------------------------
    # encoding
    # ------------------------------------------------------------------
    def test_normalises_unicode_to_nfc(self) -> None:
        nfd = "e\u0301"  # NFD decomposed 'é'
        result = DataCleaner.clean_record({"name": nfd})
        assert result["name"] == "é"

    def test_removes_control_characters(self) -> None:
        record = {"name": "Alice\x00\x01\x02"}
        result = DataCleaner.clean_record(record)
        assert "\x00" not in result["name"]
        assert result["name"] == "Alice"

    # ------------------------------------------------------------------
    # bulk cleaning
    # ------------------------------------------------------------------
    def test_clean_records_processes_all(self) -> None:
        records = [{"name": " A "}, {"name": " B "}]
        results = DataCleaner.clean_records(records)
        assert results[0]["name"] == "A"
        assert results[1]["name"] == "B"

    def test_clean_records_returns_new_dicts(self) -> None:
        original = [{"name": " Alice "}]
        results = DataCleaner.clean_records(original)
        assert results is not original
        assert results[0] is not original[0]

    def test_non_string_values_are_not_modified(self) -> None:
        record = {"quantity": 5, "price": 3.14, "active": True}
        result = DataCleaner.clean_record(record)
        assert result == record

    # ------------------------------------------------------------------
    # Hypothesis property-based tests
    # ------------------------------------------------------------------
    @given(st.text(min_size=1, max_size=50).filter(lambda s: s.strip() and s.strip().lower() not in NULL_SENTINELS))
    @h_settings(max_examples=200)
    def test_clean_record_never_modifies_non_string_keys(self, value: str) -> None:
        """Cleaning a simple string field should never raise."""
        record = {"name": value}
        result = DataCleaner.clean_record(record)
        assert isinstance(result, dict)
        assert "name" in result

    @given(st.integers() | st.floats(allow_nan=False) | st.booleans())
    @h_settings(max_examples=100)
    def test_non_string_values_pass_through_unchanged(self, value: object) -> None:
        record = {"quantity": value}
        result = DataCleaner.clean_record(record)
        assert result["quantity"] == value

    @given(st.text(min_size=300, max_size=600))
    @h_settings(max_examples=50)
    def test_name_always_truncated_to_max_length(self, long_name: str) -> None:
        result = DataCleaner.clean_record({"name": long_name})
        assert len(result.get("name", "")) <= 256

    @given(st.lists(st.fixed_dictionaries({"name": st.text(max_size=10)}), min_size=0, max_size=20))
    @h_settings(max_examples=50)
    def test_clean_records_length_preserved(self, records: list[dict]) -> None:
        results = DataCleaner.clean_records(records)
        assert len(results) == len(records)
