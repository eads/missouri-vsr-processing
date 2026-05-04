import json

import pandas as pd
import pytest
from dagster import build_op_context

from missouri_vsr.resources import LocalDirectoryResource, S3Resource
from missouri_vsr.assets import processed
from missouri_vsr.assets.processed import (
    _extract_jurisdiction_geoid,
    _add_canonical_names,
    build_agency_index_records,
)


def _sample_combined_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "agency": "Agency A",
                "year": 2023,
                "row_key": "rates-by-race--totals--all-stops",
                "metric": "All stops",
                "Total": 100,
                "White": 50,
            },
            {
                "agency": "Agency B",
                "year": 2023,
                "row_key": "rates-by-race--totals--all-stops",
                "metric": "All stops",
                "Total": 200,
                "White": 50,
            },
            {
                "agency": "Missouri State Highway Patrol",
                "year": 2023,
                "row_key": "rates-by-race--totals--all-stops",
                "metric": "All stops",
                "Total": 150,
                "White": 25,
            },
        ]
    )


def test_add_rank_percentile_rows_op(tmp_path):
    df = _sample_combined_df()
    context = build_op_context(
        resources={
            "data_dir_processed": LocalDirectoryResource(path=str(tmp_path)),
            "s3": S3Resource(),
        }
    )
    augmented = processed.add_rank_percentile_rows(context, df)

    assert len(augmented) == len(df) * 4

    base_row_key = "rates-by-race--totals--all-stops"
    slug_suffixes = {
        row_key.replace(base_row_key, "")
        for row_key in augmented["row_key"].unique()
        if row_key.startswith(base_row_key)
    }
    assert slug_suffixes == {"", "-percentage", "-rank", "-percentile"}

    rank_row = augmented[
        (augmented["row_key"] == f"{base_row_key}-rank")
        & (augmented["agency"] == "Agency B")
    ]
    assert rank_row["Total"].iloc[0] == 1

    base_row = augmented[
        (augmented["row_key"] == base_row_key)
        & (augmented["agency"] == "Agency B")
    ].iloc[0]
    assert base_row["rank_dense"] == 1
    assert base_row["rank_count"] == 3
    assert base_row["rank_method"] == "dense"


def test_compute_statewide_baselines_op(tmp_path):
    df = _sample_combined_df()
    context = build_op_context(
        resources={
            "data_dir_processed": LocalDirectoryResource(path=str(tmp_path)),
            "s3": S3Resource(),
        }
    )
    baselines = processed.compute_statewide_slug_baselines(context, df)

    out_path = tmp_path / "statewide_slug_baselines.parquet"
    assert out_path.exists()

    assert set(baselines.columns) == {
        "year",
        "row_key",
        "metric",
        "count",
        "mean",
        "median",
        "count__no_mshp",
        "mean__no_mshp",
        "median__no_mshp",
    }
    assert len(baselines) == 2

    total_all = baselines[
        (baselines["metric"] == "Total")
    ].iloc[0]
    assert total_all["count"] == 3
    assert total_all["mean"] == pytest.approx(150.0, rel=1e-6)
    assert total_all["median"] == pytest.approx(150.0, rel=1e-6)

    assert total_all["count__no_mshp"] == 2
    assert total_all["mean__no_mshp"] == pytest.approx(150.0, rel=1e-6)
    assert total_all["median__no_mshp"] == pytest.approx(150.0, rel=1e-6)

    white_all = baselines[
        (baselines["metric"] == "White")
    ].iloc[0]
    assert white_all["count"] == 3
    assert white_all["mean"] == pytest.approx(41.6666667, rel=1e-6)
    assert white_all["median"] == pytest.approx(50.0, rel=1e-6)

    assert white_all["count__no_mshp"] == 2
    assert white_all["mean__no_mshp"] == pytest.approx(50.0, rel=1e-6)
    assert white_all["median__no_mshp"] == pytest.approx(50.0, rel=1e-6)


# ---------------------------------------------------------------------------
# Helpers for issue #8: census GEOIDs + canonical names in download outputs
# ---------------------------------------------------------------------------

def _geocodio_response(state_fips: str, county_fips: str | None = None, place: str | None = None) -> str:
    """Build a minimal Geocodio-style jurisdiction response JSON string."""
    census_record = {"state_fips": state_fips}
    if county_fips is not None:
        census_record["county_fips"] = county_fips
    if place is not None:
        census_record["place"] = place
    return json.dumps({
        "results": [{"fields": {"census": {"2020": census_record}}}]
    })


def test_extract_jurisdiction_geoid_municipal():
    row = pd.Series({
        "AgencyType": "Municipal",
        "geocode_jurisdiction_response": _geocodio_response("29", place="65000"),
    })
    assert _extract_jurisdiction_geoid(row) == "2965000"


def test_extract_jurisdiction_geoid_municipal_full_geoid():
    # If Geocodio returns the full 7-digit GEOID already, return it as-is.
    row = pd.Series({
        "AgencyType": "Municipal",
        "geocode_jurisdiction_response": _geocodio_response("29", place="2965000"),
    })
    assert _extract_jurisdiction_geoid(row) == "2965000"


def test_extract_jurisdiction_geoid_county():
    row = pd.Series({
        "AgencyType": "County",
        "geocode_jurisdiction_response": _geocodio_response("29", county_fips="099"),
    })
    assert _extract_jurisdiction_geoid(row) == "29099"


def test_extract_jurisdiction_geoid_county_full_geoid():
    row = pd.Series({
        "AgencyType": "County",
        "geocode_jurisdiction_response": _geocodio_response("29", county_fips="29099"),
    })
    assert _extract_jurisdiction_geoid(row) == "29099"


def test_extract_jurisdiction_geoid_state_agency_returns_none():
    # State agencies have no jurisdictional GEOID.
    row = pd.Series({
        "AgencyType": "State",
        "geocode_jurisdiction_response": _geocodio_response("29", county_fips="099"),
    })
    assert _extract_jurisdiction_geoid(row) is None


def test_extract_jurisdiction_geoid_missing_response():
    row = pd.Series({"AgencyType": "Municipal", "geocode_jurisdiction_response": None})
    assert _extract_jurisdiction_geoid(row) is None


def test_extract_jurisdiction_geoid_no_census_fields():
    row = pd.Series({
        "AgencyType": "Municipal",
        "geocode_jurisdiction_response": json.dumps({"results": [{"fields": {}}]}),
    })
    assert _extract_jurisdiction_geoid(row) is None


def test_extract_jurisdiction_geoid_place_as_dict():
    # Geocodio sometimes returns place as a nested dict.
    resp = json.dumps({
        "results": [{"fields": {"census": {"2020": {
            "state_fips": "29",
            "place": {"fips": "65000"},
        }}}}]
    })
    row = pd.Series({"AgencyType": "Municipal", "geocode_jurisdiction_response": resp})
    assert _extract_jurisdiction_geoid(row) == "2965000"


def test_add_canonical_names_basic():
    reports = pd.DataFrame([
        {"agency": "Adair County Sheriff's Office", "year": 2023, "Total": 100},
        {"agency": "Adrian Police Dept.", "year": 2023, "Total": 50},
    ])
    ref = pd.DataFrame([
        {"Normalized": "adair county sheriffs department", "Department": "Adair County Sheriff's Dept"},
        {"Normalized": "adrian police department", "Department": "Adrian Police Dept"},
    ])
    result = _add_canonical_names(reports, ref)
    assert "canonical_name" in result.columns
    assert result.loc[result["agency"] == "Adair County Sheriff's Office", "canonical_name"].iloc[0] == "Adair County Sheriff's Dept"
    assert result.loc[result["agency"] == "Adrian Police Dept.", "canonical_name"].iloc[0] == "Adrian Police Dept"


def test_add_canonical_names_unmatched_is_none():
    reports = pd.DataFrame([{"agency": "Unknown Agency XYZ", "year": 2023}])
    ref = pd.DataFrame([
        {"Normalized": "adair county sheriffs department", "Department": "Adair County Sheriff's Dept"},
    ])
    result = _add_canonical_names(reports, ref)
    assert result["canonical_name"].iloc[0] is None


def test_add_canonical_names_empty_reference():
    reports = pd.DataFrame([{"agency": "Some Agency", "year": 2023}])
    result = _add_canonical_names(reports, pd.DataFrame())
    # Empty reference → return reports unchanged, no canonical_name column added
    assert "canonical_name" not in result.columns


def test_add_canonical_names_na_values_in_reference():
    # pd.NA in Department/Canonical/Normalized columns must not raise
    reports = pd.DataFrame([{"agency": "Adair County Sheriff's Office", "year": 2023}])
    ref = pd.DataFrame([
        {"Normalized": "adair county sheriffs department", "Department": "Adair County Sheriff's Dept", "Canonical": pd.NA},
        {"Normalized": pd.NA, "Department": pd.NA, "Canonical": pd.NA},
    ])
    result = _add_canonical_names(reports, ref)
    assert result["canonical_name"].iloc[0] == "Adair County Sheriff's Dept"


def test_build_agency_index_records_includes_census_geoid():
    pivoted = pd.DataFrame([{
        "agency": "Springfield Police Dept",
        "year": 2023,
        "rates-by-race--totals--all-stops__Total": 500,
    }])
    ref = pd.DataFrame([{
        "Department": "Springfield Police Dept",
        "Canonical": "Springfield Police Dept",
        "AgencyType": "Municipal",
        "geocode_jurisdiction_response": _geocodio_response("29", place="70000"),
        "geocode_jurisdiction_county": "Greene",
    }])
    records = build_agency_index_records(pivoted, ref)
    agency = next((r for r in records if r["canonical_name"] == "Springfield Police Dept"), None)
    assert agency is not None
    assert agency["census_geoid"] == "2970000"


def test_build_agency_index_records_years_with_data():
    pivoted = pd.DataFrame([
        {"agency": "Foo PD", "year": 2022, "rates-by-race--totals--all-stops__Total": 100},
        {"agency": "Foo PD", "year": 2023, "rates-by-race--totals--all-stops__Total": 120},
        {"agency": "Bar PD", "year": 2021, "rates-by-race--totals--all-stops__Total": 50},
    ])
    years_by_agency = {
        "Foo PD": [2022, 2023],
        "Bar PD": [2021],
    }
    records = build_agency_index_records(pivoted, pd.DataFrame(), years_by_agency=years_by_agency)
    foo = next(r for r in records if r["canonical_name"] == "Foo PD")
    bar = next(r for r in records if r["canonical_name"] == "Bar PD")

    assert foo["years_with_data"] == [2022, 2023]
    assert foo["latest_year_with_data"] == 2023
    assert bar["years_with_data"] == [2021]
    assert bar["latest_year_with_data"] == 2021


def test_build_agency_index_records_years_with_data_multiple_names():
    """Years are unioned across all names that map to the same entry."""
    pivoted = pd.DataFrame([
        {"agency": "Foo Police Dept",       "year": 2020, "rates-by-race--totals--all-stops__Total": 100},
        {"agency": "Foo Police Department", "year": 2019, "rates-by-race--totals--all-stops__Total": 90},
    ])
    # "Foo Police Department" is an alternate name for the same entry
    ref = pd.DataFrame([{
        "Department": "Foo Police Dept",
        "Canonical": "Foo Police Dept",
        "Raw": "Foo Police Department",
    }])
    years_by_agency = {
        "Foo Police Dept":       [2020],
        "Foo Police Department": [2019],
    }
    records = build_agency_index_records(pivoted, ref, years_by_agency=years_by_agency)
    foo = next(r for r in records if r["canonical_name"] == "Foo Police Dept")
    assert sorted(foo["years_with_data"]) == [2019, 2020]
    assert foo["latest_year_with_data"] == 2020


def test_build_agency_index_records_years_missing_when_not_provided():
    """When years_by_agency is not passed, the fields are absent."""
    pivoted = pd.DataFrame([
        {"agency": "Foo PD", "year": 2023, "rates-by-race--totals--all-stops__Total": 100},
    ])
    records = build_agency_index_records(pivoted, pd.DataFrame())
    foo = next(r for r in records if r["canonical_name"] == "Foo PD")
    assert "years_with_data" not in foo
    assert "latest_year_with_data" not in foo


def test_build_agency_index_records_census_geoid_none_for_unclassified():
    pivoted = pd.DataFrame([{
        "agency": "Some State Agency",
        "year": 2023,
        "rates-by-race--totals--all-stops__Total": 100,
    }])
    ref = pd.DataFrame([{
        "Department": "Some State Agency",
        "Canonical": "Some State Agency",
        "AgencyType": "State",
        "geocode_jurisdiction_response": _geocodio_response("29", county_fips="099"),
        "geocode_jurisdiction_county": None,
    }])
    records = build_agency_index_records(pivoted, ref)
    agency = next((r for r in records if r["canonical_name"] == "Some State Agency"), None)
    assert agency is not None
    assert agency["census_geoid"] is None
