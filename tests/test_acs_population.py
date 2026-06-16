"""Tests for the ACS 5-year PUMS 16+ population tabulation (acs_population_16plus)."""
from __future__ import annotations

import pandas as pd

from missouri_vsr.assets.acs_population import _aggregate_16plus_by_race


def _synthetic_pums() -> pd.DataFrame:
    """One person per AG race category (all weight 10, age 16+), plus an under-16 row.

    Covers the exact construction the AG uses, including the non-White-Hispanic
    double-count (a Hispanic Black person lands in BOTH Black and Hispanic).
    """
    rows = [
        # (AGEP, PWGTP, RAC1P, HISP)
        (40, 10, 1, 1),   # White alone, not Hispanic  -> White
        (40, 10, 1, 2),   # White alone, Hispanic      -> Hispanic (NOT White)
        (40, 10, 2, 1),   # Black, not Hispanic         -> Black
        (40, 10, 2, 3),   # Black, Hispanic             -> Black AND Hispanic (double-count)
        (40, 10, 3, 1),   # American Indian alone       -> Native American
        (40, 10, 4, 1),   # Alaska Native alone         -> Native American
        (40, 10, 5, 1),   # AIAN combination            -> Native American
        (40, 10, 6, 1),   # Asian alone                 -> Asian
        (40, 10, 7, 1),   # NHPI alone                  -> Other
        (40, 10, 8, 1),   # Some other race alone       -> Other
        (40, 10, 9, 1),   # Two or more races           -> Other
        (10, 99, 2, 1),   # under 16                    -> excluded entirely
    ]
    return pd.DataFrame(rows, columns=["AGEP", "PWGTP", "RAC1P", "HISP"])


def test_category_construction_matches_ag_definition():
    counts = _aggregate_16plus_by_race(_synthetic_pums())
    assert counts["Total"] == 110               # 11 people x weight 10; under-16 excluded
    assert counts["White"] == 10                # white-alone-not-Hispanic only
    assert counts["Black"] == 20                # incl. the Hispanic Black person
    assert counts["Hispanic"] == 20             # White-Hispanic + Black-Hispanic
    assert counts["Native American"] == 30      # RAC1P in {3,4,5}
    assert counts["Asian"] == 10
    assert counts["Other"] == 30                # RAC1P in {7,8,9}


def test_under_16_excluded():
    """The under-16 row has weight 99; if it leaked in, Total would jump by 99."""
    counts = _aggregate_16plus_by_race(_synthetic_pums())
    assert counts["Total"] == 110


def test_non_white_hispanic_double_count_identity():
    """sum(race categories) - Total == the non-White Hispanic population (counted twice)."""
    counts = _aggregate_16plus_by_race(_synthetic_pums())
    races = ["White", "Black", "Hispanic", "Native American", "Asian", "Other"]
    excess = sum(counts[r] for r in races) - counts["Total"]
    # Only the Hispanic Black person (weight 10) is double-counted; the White-Hispanic
    # person lands in Hispanic only (removed from White), so does NOT inflate the sum.
    assert excess == 10


def test_weighting_uses_pwgtp():
    """Counts are PWGTP-weighted, not row counts."""
    df = pd.DataFrame(
        [(40, 25, 1, 1), (40, 75, 1, 1)],  # two White-NH people, weights 25 + 75
        columns=["AGEP", "PWGTP", "RAC1P", "HISP"],
    )
    counts = _aggregate_16plus_by_race(df)
    assert counts["White"] == 100
    assert counts["Total"] == 100
