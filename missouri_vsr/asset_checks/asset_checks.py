import csv
from pathlib import Path

import numpy as np
import pandas as pd
from dagster import AssetCheckResult, AssetKey, MetadataValue, asset_check

from missouri_vsr.assets.extract import EXTRACT_COLUMNS, EXTRACT_COLUMNS_PRE2020, RACE_COLUMNS, RACE_COLUMNS_PRE2020, YEAR_URLS
from missouri_vsr.assets.reports import combine_all_reports
from missouri_vsr.assets.reports import _build_agency_name_lookup
from missouri_vsr.cli.crosswalk import _normalize_name

# Combined asset collapses pre-2020 'Am. Indian' into 'Native American' at layer 2,
# so downstream only sees a single Native American column populated across all eras.
EXPECTED_COLUMNS_COMBINED = [
    "Total",
    "White",
    "Black",
    "Hispanic",
    "Native American",
    "Asian",
    "Other",
    "canonical_key",  # era-independent concept name; None for era-specific metrics
    "year",
    "row_key",
    "agency",
    "table",
    "table_id",
    "section",
    "section_id",
    "metric",
    "metric_id",
    "row_id",
]

NUMERIC_COLS_COMBINED = [
    "Total",
    "White",
    "Black",
    "Hispanic",
    "Native American",
    "Asian",
    "Other",
]


def _year_race_cols(year: int) -> list[str]:
    return RACE_COLUMNS_PRE2020 if year < 2020 else RACE_COLUMNS


def _year_expected_cols(year: int) -> list[str]:
    return EXTRACT_COLUMNS_PRE2020 if year < 2020 else EXTRACT_COLUMNS


def _coerce_row(raw: dict, numeric_cols: list[str]) -> dict:
    out = {}
    for k, v in raw.items():
        if v == "":
            out[k] = None
        elif k == "year" and v.isdigit():
            out[k] = int(v)
        elif k in numeric_cols:
            out[k] = int(v) if v.isdigit() else float(v)
        else:
            out[k] = v
    return out


RENAME_CHECK_FIELDS = {
    "slug": "row_key",
    "department": "agency",
}


def _rename_check_fields(d: dict) -> dict:
    return {RENAME_CHECK_FIELDS.get(k, k): v for k, v in d.items()}


def _load_year_expectations(year: int) -> list[dict]:
    csv_path = Path(f"data_checks/row_sanity_checks_{year}.csv")
    if not csv_path.exists():
        return []
    numeric_cols = _year_race_cols(year)
    with open(csv_path) as f:
        return [_rename_check_fields(_coerce_row(r, numeric_cols)) for r in csv.DictReader(f)]


def _convert_types(obj):
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, dict):
        return {k: _convert_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_convert_types(item) for item in obj]
    else:
        return obj


# ------------------------------------------------------------------------------
# Combined asset checks (cross-year consistency)
# ------------------------------------------------------------------------------

@asset_check(asset=combine_all_reports)
def check_expected_columns(df: pd.DataFrame) -> AssetCheckResult:
    missing = [c for c in EXPECTED_COLUMNS_COMBINED if c not in df.columns]
    extra = [c for c in df.columns if c not in EXPECTED_COLUMNS_COMBINED]
    return AssetCheckResult(
        passed=not missing and not extra,
        metadata={"missing_columns": missing, "extra_columns": extra},
    )


@asset_check(asset=combine_all_reports)
def check_no_duplicate_row_keys(df: pd.DataFrame) -> AssetCheckResult:
    dupes = df[df.duplicated(["agency", "row_key", "year"], keep=False)]
    example_duplicates = dupes[["agency", "row_key", "year"]].drop_duplicates().to_dict(orient="records")
    return AssetCheckResult(
        passed=dupes.empty,
        metadata={"duplicate_count": len(dupes), "example_duplicates": example_duplicates},
    )


@asset_check(asset=combine_all_reports)
def check_no_duplicate_row_ids(df: pd.DataFrame) -> AssetCheckResult:
    if "row_id" not in df.columns or "year" not in df.columns:
        return AssetCheckResult(passed=False, metadata={"reason": "row_id/year columns missing"})
    dupes = df[df.duplicated(["row_id", "year"], keep=False)]
    example_duplicates = (
        dupes[["row_id", "year", "agency", "row_key"]]
        .drop_duplicates()
        .head(25)
        .to_dict(orient="records")
    )
    return AssetCheckResult(
        passed=dupes.empty,
        metadata={"duplicate_count": len(dupes), "example_duplicates": example_duplicates},
    )


@asset_check(asset=combine_all_reports)
def check_numeric_columns_parse(df: pd.DataFrame) -> AssetCheckResult:
    cols = [c for c in NUMERIC_COLS_COMBINED if c in df.columns]
    numeric_parsed = df[cols].apply(lambda s: pd.to_numeric(s, errors="coerce"))
    non_numeric_mask = numeric_parsed.isna() & df[cols].notna()
    problem_cells = non_numeric_mask.stack()
    return AssetCheckResult(
        passed=not problem_cells.any(),
        metadata={"non_numeric_cells": int(problem_cells.sum())},
    )


# ------------------------------------------------------------------------------
# Per-year extract asset check factories
# ------------------------------------------------------------------------------

def _make_schema_check(year: int):
    expected = _year_expected_cols(year)
    asset_key = AssetKey(f"extract_pdf_data_{year}")

    @asset_check(name="schema", asset=asset_key)
    def _check(df: pd.DataFrame) -> AssetCheckResult:
        missing = [c for c in expected if c not in df.columns]
        extra = [c for c in df.columns if c not in expected]
        return AssetCheckResult(
            passed=not missing and not extra,
            metadata={"missing_columns": missing, "extra_columns": extra},
        )

    return _check


def _make_no_duplicate_row_keys_check(year: int):
    asset_key = AssetKey(f"extract_pdf_data_{year}")

    @asset_check(name="no_duplicate_row_keys", asset=asset_key)
    def _check(df: pd.DataFrame) -> AssetCheckResult:
        dupes = df[df.duplicated(["agency", "row_key"], keep=False)]
        example_duplicates = dupes[["agency", "row_key"]].drop_duplicates().to_dict(orient="records")
        return AssetCheckResult(
            passed=dupes.empty,
            metadata={"duplicate_count": len(dupes), "example_duplicates": example_duplicates},
        )

    return _check


def _make_numeric_check(year: int):
    race_cols = _year_race_cols(year)
    asset_key = AssetKey(f"extract_pdf_data_{year}")

    @asset_check(name="numeric_columns_parse", asset=asset_key)
    def _check(df: pd.DataFrame) -> AssetCheckResult:
        cols = [c for c in race_cols if c in df.columns]
        numeric_parsed = df[cols].apply(lambda s: pd.to_numeric(s, errors="coerce"))
        non_numeric_mask = numeric_parsed.isna() & df[cols].notna()
        problem_cells = non_numeric_mask.stack()
        return AssetCheckResult(
            passed=not problem_cells.any(),
            metadata={"non_numeric_cells": int(problem_cells.sum())},
        )

    return _check


def _make_row_sanity_check(year: int, rows: list[dict]):
    asset_key = AssetKey(f"extract_pdf_data_{year}")

    @asset_check(name="row_expectations", asset=asset_key)
    def _check(df: pd.DataFrame) -> AssetCheckResult:
        if df.empty:
            return AssetCheckResult(passed=False, metadata={"reason": "empty DataFrame"})

        deduped = df.drop_duplicates(["row_key", "agency"])
        year_index = deduped.set_index(["row_key", "agency"], drop=False)

        missing = []
        mismatches = []
        for check in rows:
            key = (check.get("row_key"), check.get("agency"))
            if key not in year_index.index:
                missing.append({"row_key": key[0], "agency": key[1]})
                continue

            actual_row = year_index.loc[key]
            if isinstance(actual_row, pd.DataFrame):
                actual_row = actual_row.iloc[0]

            row_mismatches = []
            for field, expected_val in check.items():
                if field in ("row_key", "agency", "year", "checked"):
                    continue
                actual_val = actual_row.get(field)
                if pd.isna(actual_val) and expected_val is None:
                    continue
                if actual_val != expected_val:
                    row_mismatches.append({"field": field, "expected": expected_val, "actual": actual_val})

            if row_mismatches:
                mismatches.append({"row_key": key[0], "agency": key[1], "mismatches": row_mismatches})

        sample_limit = 20
        return AssetCheckResult(
            passed=not missing and not mismatches,
            metadata={
                "expected_rows": len(rows),
                "missing_count": len(missing),
                "mismatch_count": len(mismatches),
                "missing_sample": MetadataValue.json(_convert_types(missing[:sample_limit])),
                "mismatch_sample": MetadataValue.json(_convert_types(mismatches[:sample_limit])),
            },
        )

    return _check


# Build per-year extract checks for all years in YEAR_URLS
per_year_extract_checks = []
for _yr in sorted(YEAR_URLS):
    per_year_extract_checks.append(_make_schema_check(_yr))
    per_year_extract_checks.append(_make_no_duplicate_row_keys_check(_yr))
    per_year_extract_checks.append(_make_numeric_check(_yr))
    _rows = _load_year_expectations(_yr)
    if _rows:
        per_year_extract_checks.append(_make_row_sanity_check(_yr, _rows))


_AGENCY_CROSSWALK_PATH = Path("data/src/agency_crosswalk.csv")


@asset_check(asset=combine_all_reports)
def check_agency_names_normalized(df: pd.DataFrame) -> AssetCheckResult:
    """Verify that all agency names in the combined dataset have been canonicalized.

    Any agency name that maps to a *different* canonical via the crosswalk should not
    appear in the output — it means the normalization step was skipped or the crosswalk
    is missing an entry.
    """
    if "agency" not in df.columns:
        return AssetCheckResult(passed=False, metadata={"reason": "agency column missing"})

    lookup = _build_agency_name_lookup(_AGENCY_CROSSWALK_PATH)
    if not lookup:
        return AssetCheckResult(
            passed=True,
            metadata={"skipped": True, "reason": "agency_crosswalk.csv not found or empty"},
        )

    violations: list[dict] = []
    for raw in df["agency"].dropna().unique():
        raw_str = str(raw)
        canonical = lookup.get(_normalize_name(raw_str))
        if canonical and canonical != raw_str:
            count = int((df["agency"] == raw).sum())
            violations.append({"raw": raw_str, "expected_canonical": canonical, "row_count": count})

    return AssetCheckResult(
        passed=not violations,
        metadata={
            "violation_count": len(violations),
            "violations": MetadataValue.json(violations[:25]),
        },
    )


asset_checks = [
    check_expected_columns,
    check_no_duplicate_row_keys,
    check_no_duplicate_row_ids,
    check_numeric_columns_parse,
    check_agency_names_normalized,
    *per_year_extract_checks,
]


# ------------------------------------------------------------------------------
# GIS checks
# ------------------------------------------------------------------------------
@asset_check(asset=AssetKey("us_gpkg"))
def check_us_gpkg(us_gpkg: str) -> AssetCheckResult:
    from pathlib import Path

    path = Path(us_gpkg) if us_gpkg else None
    exists = bool(path) and path.exists()
    return AssetCheckResult(
        passed=exists,
        metadata={"path": us_gpkg, "exists": exists},
    )


def _check_columns(df: pd.DataFrame, required: list[str]) -> AssetCheckResult:
    missing = [col for col in required if col not in df.columns]
    return AssetCheckResult(
        passed=not missing,
        metadata={"missing_columns": missing},
    )


@asset_check(asset=AssetKey("mo_counties"))
def check_mo_county_schema(mo_counties: pd.DataFrame) -> AssetCheckResult:
    required = ["geoid", "name", "statefp", "countyfp", "boundary_type", "geometry"]
    return _check_columns(mo_counties, required)


@asset_check(asset=AssetKey("mo_places"))
def check_mo_place_schema(mo_places: pd.DataFrame) -> AssetCheckResult:
    required = ["geoid", "name", "statefp", "placefp", "boundary_type", "geometry"]
    return _check_columns(mo_places, required)


@asset_check(asset=AssetKey("mo_counties"))
def check_mo_county_count(mo_counties: pd.DataFrame) -> AssetCheckResult:
    count = len(mo_counties)
    return AssetCheckResult(passed=count >= 114, metadata={"row_count": count})


@asset_check(asset=AssetKey("mo_places"))
def check_mo_place_count(mo_places: pd.DataFrame) -> AssetCheckResult:
    count = len(mo_places)
    return AssetCheckResult(passed=count >= 900, metadata={"row_count": count})


@asset_check(asset=AssetKey("mo_counties"))
def check_mo_county_geometries(mo_counties: pd.DataFrame) -> AssetCheckResult:
    if "geometry" not in mo_counties.columns or mo_counties.empty:
        return AssetCheckResult(passed=False, metadata={"reason": "missing geometry"})
    valid_ratio = float(mo_counties.geometry.is_valid.mean())
    return AssetCheckResult(passed=valid_ratio >= 0.99, metadata={"valid_ratio": valid_ratio})


@asset_check(asset=AssetKey("mo_places"))
def check_mo_place_geometries(mo_places: pd.DataFrame) -> AssetCheckResult:
    if "geometry" not in mo_places.columns or mo_places.empty:
        return AssetCheckResult(passed=False, metadata={"reason": "missing geometry"})
    valid_ratio = float(mo_places.geometry.is_valid.mean())
    return AssetCheckResult(passed=valid_ratio >= 0.99, metadata={"valid_ratio": valid_ratio})


@asset_check(asset=AssetKey("mo_jurisdictions_pmtiles"))
def check_mo_pmtiles_size(mo_jurisdictions_pmtiles: str) -> AssetCheckResult:
    from pathlib import Path

    path = Path(mo_jurisdictions_pmtiles)
    size = path.stat().st_size if path.exists() else 0
    return AssetCheckResult(passed=size > 1_000_000, metadata={"size_bytes": size})


@asset_check(asset=AssetKey("agency_boundary_matches"))
def check_county_match_rate(agency_boundary_matches: pd.DataFrame) -> AssetCheckResult:
    if agency_boundary_matches.empty:
        return AssetCheckResult(passed=False, metadata={"reason": "no agency rows"})
    county_rows = agency_boundary_matches[
        agency_boundary_matches["agency_type"].astype(str).str.contains("county", case=False, na=False)
    ]
    total = len(county_rows)
    matched = int(county_rows["matched"].fillna(False).sum())
    match_rate = matched / total if total else 0.0
    return AssetCheckResult(
        passed=match_rate >= 0.9,
        metadata={"county_total": total, "county_matched": matched, "match_rate": match_rate},
    )


@asset_check(asset=AssetKey("agency_relationships"))
def check_relationship_columns(agency_relationships: pd.DataFrame) -> AssetCheckResult:
    required = ["agency_id", "touching_ids", "contained_ids"]
    missing = [col for col in required if col not in agency_relationships.columns]
    if missing:
        return AssetCheckResult(passed=False, metadata={"missing_columns": missing})
    touching_ok = bool(agency_relationships["touching_ids"].apply(lambda v: isinstance(v, list)).all())
    contained_ok = bool(agency_relationships["contained_ids"].apply(lambda v: isinstance(v, list)).all())
    return AssetCheckResult(
        passed=touching_ok and contained_ok,
        metadata={"touching_ok": touching_ok, "contained_ok": contained_ok},
    )


asset_checks.extend(
    [
        check_us_gpkg,
        check_mo_county_schema,
        check_mo_place_schema,
        check_mo_county_count,
        check_mo_place_count,
        check_mo_county_geometries,
        check_mo_place_geometries,
        check_mo_pmtiles_size,
        check_county_match_rate,
        check_relationship_columns,
    ]
)
