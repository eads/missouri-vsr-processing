"""Tests for agency name normalization via the agency crosswalk."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from missouri_vsr.assets.reports import (
    _build_agency_name_lookup,
    _normalize_agency_names,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakeLog:
    def __init__(self):
        self.messages: list[str] = []

    def info(self, msg: str, *args) -> None:
        self.messages.append(msg % args if args else msg)


def _make_crosswalk(tmp_path: Path, rows: list[dict]) -> Path:
    """Write a minimal agency_crosswalk.csv and return its path."""
    df = pd.DataFrame(rows, columns=["Normalized", "Raw", "Canonical"])
    p = tmp_path / "agency_crosswalk.csv"
    df.to_csv(p, index=False)
    return p


# ---------------------------------------------------------------------------
# _build_agency_name_lookup
# ---------------------------------------------------------------------------

def test_build_lookup_basic(tmp_path):
    crosswalk = _make_crosswalk(tmp_path, [
        {"Normalized": "foo police department",   "Raw": "Foo Police Dept.",   "Canonical": "Foo PD"},
        {"Normalized": "bar sheriffs department",  "Raw": "Bar Sheriff's Office", "Canonical": "Bar Sheriff"},
    ])
    lookup = _build_agency_name_lookup(crosswalk)
    assert lookup["foo police department"] == "Foo PD"
    assert lookup["bar sheriffs department"] == "Bar Sheriff"


def test_build_lookup_skips_null_canonical(tmp_path):
    crosswalk = _make_crosswalk(tmp_path, [
        {"Normalized": "ghost agency", "Raw": "Ghost Agency", "Canonical": None},
        {"Normalized": "real agency",  "Raw": "Real Agency",  "Canonical": "Real PD"},
    ])
    lookup = _build_agency_name_lookup(crosswalk)
    assert "ghost agency" not in lookup
    assert "real agency" in lookup


def test_build_lookup_missing_file(tmp_path):
    lookup = _build_agency_name_lookup(tmp_path / "nonexistent.csv")
    assert lookup == {}


# ---------------------------------------------------------------------------
# _normalize_agency_names
# ---------------------------------------------------------------------------

def test_normalize_replaces_matched_names(tmp_path):
    crosswalk = _make_crosswalk(tmp_path, [
        {"Normalized": "foo police department", "Raw": "Foo Police Dept.", "Canonical": "Foo PD"},
    ])
    df = pd.DataFrame({"agency": ["Foo Police Dept.", "Bar PD"], "year": [2020, 2020]})
    log = _FakeLog()
    result = _normalize_agency_names(df, crosswalk, log)
    assert result.loc[0, "agency"] == "Foo PD"
    assert result.loc[1, "agency"] == "Bar PD"   # unmatched: unchanged
    assert any("1 rows updated" in m for m in log.messages)


def test_normalize_no_updates_when_already_canonical(tmp_path):
    crosswalk = _make_crosswalk(tmp_path, [
        {"Normalized": "foo police department", "Raw": "Foo PD", "Canonical": "Foo PD"},
    ])
    df = pd.DataFrame({"agency": ["Foo PD"], "year": [2020]})
    log = _FakeLog()
    result = _normalize_agency_names(df, crosswalk, log)
    assert result.loc[0, "agency"] == "Foo PD"
    # _normalize_name("Foo PD") → "foo pd" which does NOT match "foo police department",
    # so no update occurs.
    assert not any("rows updated" in m for m in log.messages)


def test_normalize_preserves_na(tmp_path):
    crosswalk = _make_crosswalk(tmp_path, [
        {"Normalized": "foo police department", "Raw": "Foo Police Dept.", "Canonical": "Foo PD"},
    ])
    df = pd.DataFrame({"agency": [None, "Foo Police Dept."], "year": [2020, 2020]})
    log = _FakeLog()
    result = _normalize_agency_names(df, crosswalk, log)
    assert pd.isna(result.loc[0, "agency"])
    assert result.loc[1, "agency"] == "Foo PD"


def test_normalize_empty_dataframe(tmp_path):
    crosswalk = _make_crosswalk(tmp_path, [
        {"Normalized": "foo police department", "Raw": "Foo Police Dept.", "Canonical": "Foo PD"},
    ])
    df = pd.DataFrame({"agency": pd.Series([], dtype=str), "year": pd.Series([], dtype=int)})
    log = _FakeLog()
    result = _normalize_agency_names(df, crosswalk, log)
    assert len(result) == 0


def test_normalize_no_agency_column(tmp_path):
    crosswalk = _make_crosswalk(tmp_path, [
        {"Normalized": "foo police department", "Raw": "Foo Police Dept.", "Canonical": "Foo PD"},
    ])
    df = pd.DataFrame({"year": [2020]})
    log = _FakeLog()
    result = _normalize_agency_names(df, crosswalk, log)
    assert "agency" not in result.columns


def test_normalize_missing_crosswalk(tmp_path):
    df = pd.DataFrame({"agency": ["Foo Police Dept."], "year": [2020]})
    log = _FakeLog()
    result = _normalize_agency_names(df, tmp_path / "missing.csv", log)
    # Should return unchanged when crosswalk is absent
    assert result.loc[0, "agency"] == "Foo Police Dept."


# ---------------------------------------------------------------------------
# UMKC-specific: all pre-2020 variants collapse to one canonical
# ---------------------------------------------------------------------------

UMKC_CANONICAL = "University of Missouri-Kansas City Police Dept"

UMKC_VARIANTS = [
    ("Univ of Missouri-Kansas City PD",           [2004, 2007, 2009, 2014, 2017, 2018]),
    ("Univ of Mo/KC Police Dept",                 [2001, 2002]),
    ("University of Missouri-Kansas City P",      [2000, 2005, 2019]),
    ("University of Missouri-Kansas City PD",     [2003]),
    ("University of Missouri-Kansas City Police Dept", [2020, 2021, 2022, 2023, 2024]),
    ("University of MO - K.C. Police Dept.",      [1999]),  # from reference Raw column
]

REAL_CROSSWALK = Path("data/src/agency_crosswalk.csv")


@pytest.mark.skipif(not REAL_CROSSWALK.exists(), reason="agency_crosswalk.csv not available")
def test_umkc_all_variants_map_to_canonical():
    """All historical UMKC name variants must resolve to the single canonical slug."""
    lookup = _build_agency_name_lookup(REAL_CROSSWALK)
    for raw_name, _ in UMKC_VARIANTS:
        from missouri_vsr.cli.crosswalk import _normalize_name
        norm = _normalize_name(raw_name)
        assert lookup.get(norm) == UMKC_CANONICAL, (
            f"{raw_name!r} (normalized: {norm!r}) did not map to canonical; "
            f"got {lookup.get(norm)!r}"
        )


@pytest.mark.skipif(not REAL_CROSSWALK.exists(), reason="agency_crosswalk.csv not available")
def test_umkc_normalization_produces_one_slug():
    """After normalization, all UMKC rows use a single agency name → single slug."""
    from slugify import slugify
    rows = [
        {"agency": raw, "year": yr}
        for raw, years in UMKC_VARIANTS
        for yr in years
    ]
    df = pd.DataFrame(rows)
    log = _FakeLog()
    result = _normalize_agency_names(df, REAL_CROSSWALK, log)

    unique_agencies = result["agency"].unique()
    assert len(unique_agencies) == 1, f"Expected 1 unique agency after normalization; got {unique_agencies!r}"
    assert unique_agencies[0] == UMKC_CANONICAL

    slug = slugify(UMKC_CANONICAL.replace("'", ""), lowercase=True)
    assert slug == "university-of-missouri-kansas-city-police-dept"
