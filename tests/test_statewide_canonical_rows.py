"""Tests for the statewide-aggregate row injection (issue #34)."""
from __future__ import annotations

import pandas as pd

from missouri_vsr.assets.processed import (
    STATEWIDE_AGENCY_NAME,
    STATEWIDE_AGENCY_SLUG,
    _build_statewide_canonical_rows,
    _exclude_statewide_aggregate,
)


def _canonical_frame() -> pd.DataFrame:
    """Two agencies, two years, a mix of count rows + the rate rows that should be re-derived."""
    cols = [
        "agency", "year",
        "table_id", "table", "section_id", "section", "metric_id", "metric",
        "row_id", "row_key", "canonical_key",
        "Total", "White", "Black", "Hispanic", "Native American", "Asian", "Other",
    ]
    rows = [
        # Stops: agency A 2023
        ["Agency A", 2023, "ki", "Key Indicators", None, None, "stops", "Stops",
         "2023-agency-a-stops", "stops", "stops",
         100.0, 60.0, 30.0, 5.0, 2.0, 2.0, 1.0],
        # Stops: agency B 2023
        ["Agency B", 2023, "ki", "Key Indicators", None, None, "stops", "Stops",
         "2023-agency-b-stops", "stops", "stops",
         200.0, 150.0, 30.0, 10.0, 5.0, 3.0, 2.0],
        # Searches: agency A 2023
        ["Agency A", 2023, "ki", "Key Indicators", None, None, "searches", "Searches",
         "2023-agency-a-searches", "searches", "searches",
         10.0, 5.0, 4.0, 1.0, 0.0, 0.0, 0.0],
        # Searches: agency B 2023
        ["Agency B", 2023, "ki", "Key Indicators", None, None, "searches", "Searches",
         "2023-agency-b-searches", "searches", "searches",
         20.0, 10.0, 6.0, 2.0, 1.0, 1.0, 0.0],
        # Pre-existing rate row — should be dropped from aggregation source, re-derived from totals
        ["Agency A", 2023, "ki", "Key Indicators", None, None, "search-rate", "Search rate",
         "2023-agency-a-search-rate", "search-rate", "search-rate",
         0.10, 0.083, 0.133, 0.20, 0.0, 0.0, 0.0],
        # Derived row that should be skipped wholesale
        ["Agency A", 2023, "ki", "Key Indicators", None, None, "stops", "Stops",
         "2023-agency-a-stops-rank", "stops-rank", "stops",
         1, 1, 1, 1, 1, 1, 1],
        # Population row — should be excluded from both aggregation and rate re-derivation
        ["Agency A", 2023, "rbr", "Rates by Race", "pop", "Population", "pop", "Population",
         "2023-agency-a-rates-by-race--population-totals--pop", "rates-by-race--population-totals--pop", "rates-by-race--population-totals--pop",
         50000.0, 30000.0, 15000.0, 3000.0, 500.0, 1000.0, 500.0],
    ]
    return pd.DataFrame(rows, columns=cols)


def test_aggregates_counts_and_derives_rates():
    df = _canonical_frame()
    statewide = _build_statewide_canonical_rows(df)

    assert not statewide.empty
    assert (statewide["agency"] == STATEWIDE_AGENCY_NAME).all()
    assert set(statewide.columns) == set(df.columns)

    stops_row = statewide[(statewide["row_key"] == "stops") & (statewide["year"] == 2023)].iloc[0]
    assert stops_row["Total"] == 300.0
    assert stops_row["White"] == 210.0

    searches_row = statewide[(statewide["row_key"] == "searches") & (statewide["year"] == 2023)].iloc[0]
    assert searches_row["Total"] == 30.0

    search_rate = statewide[(statewide["row_key"] == "search-rate") & (statewide["year"] == 2023)]
    assert len(search_rate) == 1
    rate_row = search_rate.iloc[0]
    # Rates are re-derived on the percentage scale (×100) to match per-agency rate rows.
    assert rate_row["Total"] == pytest_approx(100 * 30.0 / 300.0)
    assert rate_row["White"] == pytest_approx(100 * 15.0 / 210.0)


def test_excludes_population_rows_from_aggregation():
    df = _canonical_frame()
    statewide = _build_statewide_canonical_rows(df)

    pop_rows = statewide[statewide["row_key"].str.startswith("rates-by-race--population", na=False)]
    assert pop_rows.empty, "population rows must not be in statewide aggregate"


def test_skips_pre_existing_derived_rows():
    df = _canonical_frame()
    statewide = _build_statewide_canonical_rows(df)

    derived = statewide[
        statewide["row_key"].astype(str).str.contains(r"-(?:rank|percentile|percentage)$", regex=True)
    ]
    assert derived.empty


def test_metadata_columns_populated():
    df = _canonical_frame()
    statewide = _build_statewide_canonical_rows(df)

    stops_row = statewide[statewide["row_key"] == "stops"].iloc[0]
    assert stops_row["table"] == "Key Indicators"
    assert stops_row["metric"] == "Stops"


def test_row_id_uses_statewide_slug():
    df = _canonical_frame()
    statewide = _build_statewide_canonical_rows(df)

    for _, row in statewide.iterrows():
        assert STATEWIDE_AGENCY_SLUG in str(row["row_id"])
        assert str(row["row_id"]).startswith(f"{int(row['year'])}-{STATEWIDE_AGENCY_SLUG}-")


def test_canonical_key_equals_row_key():
    df = _canonical_frame()
    statewide = _build_statewide_canonical_rows(df)

    assert (statewide["canonical_key"] == statewide["row_key"]).all()


def test_empty_input_returns_empty():
    df = _canonical_frame().iloc[0:0]
    statewide = _build_statewide_canonical_rows(df)
    assert statewide.empty
    assert list(statewide.columns) == list(df.columns)


def test_re_running_on_augmented_frame_is_idempotent():
    """If callers accidentally pass a frame that already has statewide rows, those are dropped first."""
    df = _canonical_frame()
    first = _build_statewide_canonical_rows(df)
    combined = pd.concat([df, first], ignore_index=True)
    second = _build_statewide_canonical_rows(combined)
    pd.testing.assert_frame_equal(
        first.reset_index(drop=True).sort_values(["year", "row_key"]).reset_index(drop=True),
        second.reset_index(drop=True).sort_values(["year", "row_key"]).reset_index(drop=True),
    )


def test_exclude_statewide_aggregate_prevents_double_count():
    """Regression (v2.2): any cross-agency sum must first drop the synthetic
    'Missouri (all agencies)' row, or every statewide total comes out 2×.
    This guards statewide_year_sums*, homepage_*_stats, and the
    missouri-all-agencies agency_year export."""
    df = _canonical_frame()
    real_total = df[(df["row_key"] == "stops") & (df["year"] == 2023)]["Total"].sum()
    assert real_total == 300.0

    # canonical_combined carries both the per-agency rows AND the aggregate row.
    statewide = _build_statewide_canonical_rows(df)
    augmented = pd.concat([df, statewide], ignore_index=True)

    # The aggregate row alone already equals the statewide total...
    agg = statewide[(statewide["row_key"] == "stops") & (statewide["year"] == 2023)].iloc[0]
    assert agg["Total"] == real_total

    # ...so a naive sum over the augmented frame double-counts (the bug).
    naive = augmented[(augmented["row_key"] == "stops") & (augmented["year"] == 2023)]["Total"].sum()
    assert naive == 2 * real_total

    # _exclude_statewide_aggregate removes it, restoring the correct universe.
    cleaned = _exclude_statewide_aggregate(augmented)
    assert (cleaned["agency"] != STATEWIDE_AGENCY_NAME).all()
    fixed = cleaned[(cleaned["row_key"] == "stops") & (cleaned["year"] == 2023)]["Total"].sum()
    assert fixed == real_total


def test_exclude_statewide_aggregate_matches_on_slug():
    """The aggregate is dropped whether identified by agency name or agency_slug."""
    df = pd.DataFrame(
        {
            "agency": ["Agency A", STATEWIDE_AGENCY_NAME, "Agency B"],
            "agency_slug": ["agency-a", STATEWIDE_AGENCY_SLUG, "agency-b"],
            "Total": [1.0, 99.0, 2.0],
        }
    )
    cleaned = _exclude_statewide_aggregate(df)
    assert list(cleaned["agency"]) == ["Agency A", "Agency B"]
    assert cleaned["Total"].sum() == 3.0


def test_exclude_statewide_aggregate_noop_without_aggregate():
    """A frame with no aggregate row is returned unchanged."""
    df = _canonical_frame()
    assert len(_exclude_statewide_aggregate(df)) == len(df)


# tiny helper to avoid pytest.approx import boilerplate
def pytest_approx(expected, rel=1e-6):
    import pytest
    return pytest.approx(expected, rel=rel)
