"""ACS 16+ population by race for Missouri, self-sourced from ACS 5-year PUMS.

The disparity-index denominator is the ACS 5-year "ages 16+ by race" population.
Standard ACS published tables cannot express a clean 16+ cut by race (the race-
iterated age tables bucket ages as ``...15-17, 18-19...``), so the AG produces it
from a custom tabulation. We reproduce the same definition directly from ACS 5-year
PUMS microdata (filter ``AGEP >= 16``, weight by ``PWGTP``), which lets the
population denominator extend across the full chart window instead of dying at the
last year the AG published a parseable statewide report (2021). See
``state_report.py`` for that format boundary.

We pull PUMS from the Census Data API (``api.census.gov``) rather than the bulk
``www2.census.gov`` zip files: the API serves the identical microdata (verified
to the person against the bulk file for 2023) but reliably, where the bulk CDN
intermittently times out (HTTP 520/524) on the older vintages. Needs CENSUS_API_KEY.

Validated against the AG's published "2023 ACS pop." (2024 statewide report):
Total/White/Black/Hispanic reproduce within <1%; the only loose categories are the
tiny Native American and Other buckets (~7% of population combined), where RAC1P
two-or-more / AIAN-combination assignment is a definitional judgment call.

Category construction (matches the AG, incl. their documented non-White-Hispanic
double-count — every category except White is race-alone *including* Hispanics):
  - White:           RAC1P == 1 and HISP == 1   (White alone, not Hispanic)
  - Black:           RAC1P == 2
  - Native American: RAC1P in {3, 4, 5}         (AIAN alone / Alaska Native / AIAN combos)
  - Asian:           RAC1P == 6
  - Other:           RAC1P in {7, 8, 9}         (NHPI / Some other race / Two+ races)
  - Hispanic:        HISP != 1                  (Hispanic of any race)
  - Total:           everyone 16+

Vintage mapping: stops year Y uses the ACS 5-year vintage ending Y-1 (the AG's
2024 report uses "2023 ACS pop.", the 2025 report "2024 ACS pop.", etc.).
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

from dagster import asset

# ACS 5-year PUMS person records for Missouri (state 29) via the Census Data API.
PUMS_API = "https://api.census.gov/data/{vintage}/acs/acs5/pums"

# 5-year vintages (end year) to ingest. stops_year = vintage + 1, so 2015..2024
# covers the 2016..2025 chart window. 5-year PUMS exists back to 2009 if the window
# is ever widened.
ACS_PUMS_VINTAGES: List[int] = list(range(2015, 2025))

RACES = ["White", "Black", "Hispanic", "Native American", "Asian", "Other"]
RACE_COLS = ["Total"] + RACES
_PUMS_VARS = ["PWGTP", "AGEP", "RAC1P", "HISP"]


def _aggregate_16plus_by_race(df: pd.DataFrame) -> Dict[str, int]:
    """PWGTP-weighted 16+ population by race from a MO PUMS person frame."""
    a = df[df["AGEP"] >= 16]
    w = a["PWGTP"]

    def wsum(mask) -> int:
        return int(w[mask].sum())

    return {
        "Total": int(w.sum()),
        "White": wsum((a["RAC1P"] == 1) & (a["HISP"] == 1)),
        "Black": wsum(a["RAC1P"] == 2),
        "Hispanic": wsum(a["HISP"] != 1),
        "Native American": wsum(a["RAC1P"].isin([3, 4, 5])),
        "Asian": wsum(a["RAC1P"] == 6),
        "Other": wsum(a["RAC1P"].isin([7, 8, 9])),
    }


def _fetch_vintage_rows(context, vintage: int, key: str, cache_dir: Path, attempts: int = 5) -> Optional[list]:
    """Fetch raw PUMS rows for one vintage from the Census API, with caching + retries.

    Cached as JSON per vintage so re-runs don't re-hit the API. Retries on 5xx /
    connection / malformed-JSON errors; returns None (skip vintage) on persistent
    failure or a 4xx (e.g. a vintage the API doesn't serve)."""
    cache = cache_dir / f"acs5_pums_mo_{vintage}.json"
    if cache.exists():
        context.log.debug("Using cached %s", cache)
        return json.loads(cache.read_text())

    url = PUMS_API.format(vintage=vintage)
    params = {"get": ",".join(_PUMS_VARS), "for": "state:29", "key": key}
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.get(url, params=params, timeout=(10, 300))
            if resp.status_code != 200:
                context.log.warning("ACS5 PUMS %s -> HTTP %s (attempt %d/%d)",
                                    vintage, resp.status_code, attempt, attempts)
                if resp.status_code < 500:
                    return None
                time.sleep(3 * attempt)
                continue
            rows = resp.json()  # raises ValueError on a non-JSON error page
            cache.write_text(json.dumps(rows))
            return rows
        except (requests.exceptions.RequestException, ValueError) as exc:
            context.log.warning("ACS5 PUMS %s: %s (attempt %d/%d)", vintage, exc, attempt, attempts)
            time.sleep(3 * attempt)
    return None


def _load_vintage(context, vintage: int, key: str, cache_dir: Path) -> Optional[Dict[str, int]]:
    """Fetch (cached) and aggregate one ACS 5-year PUMS vintage for Missouri."""
    rows = _fetch_vintage_rows(context, vintage, key, cache_dir)
    if not rows or len(rows) < 2:
        context.log.warning("ACS5 PUMS %s unavailable after retries; skipping", vintage)
        return None
    df = pd.DataFrame(rows[1:], columns=rows[0])
    for c in _PUMS_VARS:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    counts = _aggregate_16plus_by_race(df)
    context.log.info("ACS5 PUMS %s: %d MO person rows -> Total 16+ = %d", vintage, len(df), counts["Total"])
    return counts


@asset(
    group_name="acs_population",
    required_resource_keys={"data_dir_source", "data_dir_processed"},
    description="ACS 5-year PUMS 16+ population by race for Missouri, per vintage, via the "
                "Census API. Self-sourced disparity denominator; stops_year = vintage + 1.",
)
def acs_population_16plus(context) -> pd.DataFrame:
    key = os.getenv("CENSUS_API_KEY")
    if not key:
        raise ValueError("CENSUS_API_KEY is not set; required to query the Census PUMS API.")

    cache_dir = Path(context.resources.data_dir_source.get_path()) / "pums"
    cache_dir.mkdir(parents=True, exist_ok=True)

    records: List[dict] = []
    for vintage in ACS_PUMS_VINTAGES:
        counts = _load_vintage(context, vintage, key, cache_dir)
        if counts is None:
            continue
        total = counts["Total"] or 0
        rec = {"acs_vintage": vintage, "stops_year": vintage + 1}
        for c in RACE_COLS:
            rec[c] = counts[c]
        for r in RACES:
            rec[f"{r} pct"] = round(100 * counts[r] / total, 4) if total else None
        records.append(rec)

    df = pd.DataFrame.from_records(records)
    out_dir = Path(context.resources.data_dir_processed.get_path())
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "acs_population_16plus.parquet"
    df.to_parquet(out_path, index=False)
    context.add_output_metadata({
        "rows": len(df),
        "vintages": sorted(df["acs_vintage"].tolist()) if not df.empty else [],
        "path": str(out_path),
    })
    return df
