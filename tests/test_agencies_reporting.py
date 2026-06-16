"""Tests for the "Zero Stops" parser folded into the combine step (issue #41).

`parse_zero_stop_agencies` reads the prose "Zero Stops" section out of the
layout-extracted report text with a regex, and `zero_stop_rows` turns it into
synthetic all-stops=0 records. Both are brittle, so we pin them against the real
source files.
"""
from pathlib import Path

import pytest

from missouri_vsr.assets.extract import parse_zero_stop_agencies, zero_stop_rows

REPORTS = Path("data/src/reports")


def _names(year: int) -> list[str]:
    f = REPORTS / f"VSRreport{year}.layout.txt"
    return parse_zero_stop_agencies(f.read_text(encoding="utf-8", errors="replace"))


def _has_layout(year: int) -> bool:
    return (REPORTS / f"VSRreport{year}.layout.txt").exists()


@pytest.mark.skipif(not _has_layout(2024), reason="2024 report text not present")
def test_zero_stops_2024_known_agencies():
    names = _names(2024)
    # The 2024 "Zero Stops" section lists exactly 12 agencies.
    assert len(names) == 12, names
    joined = " | ".join(names)
    for expected in ("Corder Police Dept", "St. Louis Community College Police Dept",
                     "Missouri Department of Revenue"):
        assert expected in joined, (expected, names)
    assert not any(n.isdigit() for n in names)
    assert not any("Agency Results" in n for n in names)


@pytest.mark.skipif(not _has_layout(2023), reason="2023 report text not present")
def test_zero_stops_2023_count():
    assert len(_names(2023)) == 24


@pytest.mark.skipif(not _has_layout(2025), reason="2025 report text not present")
def test_zero_stops_2025_count():
    assert len(_names(2025)) == 23


def test_parse_empty_text_returns_empty():
    assert parse_zero_stop_agencies("") == []


@pytest.mark.skipif(not _has_layout(2021), reason="2021 report text not present")
def test_zero_stops_absent_section_is_empty():
    # 2021's report does not carry a parseable Zero Stops section.
    assert _names(2021) == []


@pytest.mark.skipif(not _has_layout(2024), reason="2024 report text not present")
def test_zero_stop_rows_shape_and_dedup():
    rows = zero_stop_rows(REPORTS, 2024)
    assert len(rows) == 12
    r = rows[0]
    # Synthetic all-stops=0 row, canonical-mappable to 'stops'.
    assert r["row_key"] == "rates-by-race--totals--all-stops"
    assert r["Total"] == 0 and r["White"] == 0
    assert r["year"] == 2024 and r["table_id"] == "rates-by-race"
    # `existing` suppresses agencies already present in the tables.
    suppressed = zero_stop_rows(REPORTS, 2024, existing={r["agency"]})
    assert len(suppressed) == 11


def test_zero_stop_rows_missing_year_returns_empty():
    assert zero_stop_rows(REPORTS, 1999) == []
