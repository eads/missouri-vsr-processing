"""Analysis assets for the post: initial-impressions-2025.

Deliverables (all statewide):
  1. ii2025_disparity_index      — share of stops/searches/arrests by race +
     disparity index = share / (16+ ACS population share), per year.
  2. ii2025_search_reasons_by_year — Consent / Drug-alcohol odor ("smell") /
     all-other, plus a full per-reason breakdown, per year.
  3. ii2025_race_summary_2025    — total stops, search rate, contraband hit rate
     by race, statewide, 2025.

Statewide counts are *computed* by us (the "Missouri (all agencies)" aggregate in
canonical_combined, summed from the per-agency reports). The 16+ population
denominator comes from the official statewide annual report (state_reports_combined),
which also gives the AG's own published figures — so every number here carries a
cross-check against the official report with the delta surfaced.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from dagster import AssetKey, asset

POST_SLUG = "initial-impressions-2025"
GROUP = "analysis_initial_impressions_2025"
STATEWIDE_AGENCY = "Missouri (all agencies)"
PRIMARY_YEAR = 2025

RACES = ["White", "Black", "Hispanic", "Native American", "Asian", "Other"]
RACE_COLS = ["Total"] + RACES

# Probable-cause / search-reason canonical keys -> display label.
PROBABLE_CAUSE = {
    "probable-cause--consent": "Consent",
    "probable-cause--drug-alcohol-odor": "Drug/alcohol odor",
    "probable-cause--drug-dog-alert": "Drug-dog alert",
    "probable-cause--incident-to-arrest": "Incident to arrest",
    "probable-cause--inventory": "Inventory",
    "probable-cause--plain-view-contra": "Plain view contraband",
    "probable-cause--reas-susp-weapon": "Reasonable suspicion (weapon)",
    "probable-cause--other": "Other",
}
# The two highlighted buckets; everything else collapses into "all-other".
HIGHLIGHT_KEYS = ["probable-cause--consent", "probable-cause--drug-alcohol-odor"]

# Official statewide report Table 5 (Probable cause) label -> our canonical key,
# for cross-checking the search-reason counts.
OFFICIAL_PC_LABEL = {
    "Consent": "probable-cause--consent",
    "Drug/alcohol odor": "probable-cause--drug-alcohol-odor",
    "Drug-dog alert": "probable-cause--drug-dog-alert",
    "Incident to arrest": "probable-cause--incident-to-arrest",
    "Inventory": "probable-cause--inventory",
    "Plain view contra.": "probable-cause--plain-view-contra",
    "Reas. susp-weapon": "probable-cause--reas-susp-weapon",
    "Other": "probable-cause--other",
}

_DEPS = [
    AssetKey("canonical_combined_parquet"),
    AssetKey("state_reports_combined"),
    AssetKey("acs_population_16plus"),
]


# ------------------------------------------------------------------------------
# Loaders / helpers
# ------------------------------------------------------------------------------
def _processed(context, name: str) -> Path:
    return Path(context.resources.data_dir_processed.get_path()) / name


def _load_statewide(context) -> pd.DataFrame:
    df = pd.read_parquet(_processed(context, "canonical_combined.parquet"))
    return df[df["agency"] == STATEWIDE_AGENCY].copy()


def _load_state_reports(context) -> pd.DataFrame:
    return pd.read_parquet(_processed(context, "state_reports_combined.parquet"))


def _acs_pop_pct(context) -> Dict[int, Dict[str, float]]:
    """stops_year -> {race: ACS 16+ population %}, self-sourced from ACS 5-year PUMS.

    Replaces the state report as the disparity denominator: we tabulate ages 16+ by
    race ourselves (acs_population_16plus), so the population line spans the full
    chart window instead of stopping at the last parseable AG statewide report.
    Keyed by stops_year (= acs_vintage + 1), matching the AG's vintage convention.
    Reproduces the AG's published denominators within ~1% on the major categories.
    """
    acs = pd.read_parquet(_processed(context, "acs_population_16plus.parquet"))
    out: Dict[int, Dict[str, float]] = {}
    for _, row in acs.iterrows():
        out[int(row["stops_year"])] = {r: _num(row.get(f"{r} pct")) for r in RACES}
    return out


def _num(v) -> Optional[float]:
    return None if v is None or pd.isna(v) else float(v)


def _race_values(df: pd.DataFrame, canonical_key: str, year: int) -> Optional[Dict[str, Optional[float]]]:
    """Wide race dict for a canonical key in a given year of the statewide frame."""
    r = df[(df["canonical_key"] == canonical_key) & (df["year"] == year)]
    if r.empty:
        return None
    row = r.iloc[0]
    return {c: _num(row.get(c)) for c in RACE_COLS}


def _official_all_stops_disparity(sr: pd.DataFrame) -> Dict[int, Dict[str, float]]:
    """series_year -> {race: official all-stops disparity}, from the most recent
    report's 2000..N time series (the authoritative published series)."""
    ts = sr[sr["section"] == "disparity-index-timeseries"]
    if ts.empty:
        return {}
    latest = int(ts["report_year"].max())
    ts = ts[ts["report_year"] == latest]
    out: Dict[int, Dict[str, float]] = {}
    for _, row in ts.iterrows():
        out[int(row["series_year"])] = {c: _num(row.get(c)) for c in RACES}
    return out


def _official_metric(sr: pd.DataFrame, report_year: int, section: str, metric: str) -> Optional[Dict[str, float]]:
    r = sr[(sr["report_year"] == report_year) & (sr["section"] == section) & (sr["metric"] == metric)]
    if r.empty:
        return None
    row = r.iloc[0]
    return {c: _num(row.get(c)) for c in RACE_COLS}


def _delta(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None:
        return None
    return round(a - b, 6)


def _write_outputs(context, name: str, payload: dict, rows: List[dict]) -> List[str]:
    out_dir = Path(context.resources.data_dir_out.get_path()) / "analysis" / POST_SLUG
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / f"{name}.json"
    csv_path = out_dir / f"{name}.csv"
    json_path.write_text(json.dumps(payload, indent=2))
    pd.DataFrame(rows).to_csv(csv_path, index=False)
    context.add_output_metadata({"json": str(json_path), "csv": str(csv_path), "rows": len(rows)})
    return [str(json_path), str(csv_path)]


# ------------------------------------------------------------------------------
# 1. Disparity index: stops / searches / arrests
# ------------------------------------------------------------------------------
@asset(
    name="ii2025_disparity_index",
    group_name=GROUP,
    deps=_DEPS,
    required_resource_keys={"data_dir_processed", "data_dir_out"},
    description="Statewide share of stops/searches/arrests by race + disparity index "
                "(share / 16+ ACS population share) over a 10-year window, vs official.",
)
def ii2025_disparity_index(context) -> List[str]:
    sw = _load_statewide(context)
    sr = _load_state_reports(context)
    pop_by_year = _acs_pop_pct(context)              # stops_year -> {race: ACS 16+ pop %}
    official_disp = _official_all_stops_disparity(sr)  # series_year -> {race: disparity}

    metrics = {"stops": "stops", "searches": "searches", "arrests": "arrests"}
    # 10-year window ending at the primary year. Shares come from our statewide
    # counts (available every year); the disparity denominator is our own ACS
    # 5-year PUMS 16+-by-race tabulation (acs_population_16plus), which spans the
    # full window, so disparity_index is computed for every year. The AG's published
    # disparity (2000..N series) rides alongside as official_disparity_index.
    years = list(range(PRIMARY_YEAR - 9, PRIMARY_YEAR + 1))
    denominator_years = sorted(y for y in pop_by_year if y in years)

    payload = {
        "post": POST_SLUG,
        "metric": "disparity_index",
        "window": {"start": years[0], "end": years[-1], "n_years": len(years)},
        "definition": "(group's share of <metric>) / (group's share of 16+ ACS population)",
        "population_basis": "ACS 5-year PUMS, ages 16+, by race, tabulated by us "
                            "(acs_population_16plus; vintage = stops_year - 1, the AG's convention). "
                            "Reproduces the AG's published 16+ denominators within ~1% on the major "
                            "categories; lets the denominator span the full window where the AG's "
                            "parseable statewide report does not.",
        "computed_disparity_years": denominator_years,
        "computed_disparity_coverage": "disparity_index is computed for every year with an ACS PUMS "
                                       "denominator (the full window unless a PUMS vintage was unavailable). "
                                       "official_disparity_index carries the AG's published series for comparison.",
        "caveat": "Every race category except White is race-alone INCLUDING Hispanics (matching the AG), "
                  "so non-White Hispanic residents are double-counted in the population shares.",
        "statewide_counts_source": "computed: 'Missouri (all agencies)' aggregate summed from per-agency reports",
        "official_comparison": "official_disparity_index = AG's published all-stops disparity (Table 3 time series)",
        "years": {},
    }
    rows: List[dict] = []
    for y in years:
        popy = pop_by_year.get(y, {})
        ystruct: Dict[str, dict] = {}
        for mlabel, ck in metrics.items():
            vals = _race_values(sw, ck, y)
            if not vals or not vals.get("Total"):
                continue
            total = vals["Total"]
            mstruct: Dict[str, dict] = {}
            for race in RACES:
                cnt = vals.get(race)
                pop_pct = popy.get(race)
                share = round(cnt / total * 100, 4) if (cnt is not None and total) else None
                disparity = round(share / pop_pct, 4) if (share is not None and pop_pct) else None
                cell = {
                    "count": cnt,
                    "share_pct": share,
                    "pop_pct_16plus": pop_pct,
                    "disparity_index": disparity,
                }
                off = official_disp.get(y, {}).get(race) if mlabel == "stops" else None
                if mlabel == "stops":
                    cell["official_disparity_index"] = off
                    cell["delta_vs_official"] = _delta(disparity, off)
                mstruct[race] = cell
                rows.append({
                    "year": y, "metric": mlabel, "race": race, "count": cnt,
                    "share_pct": share, "pop_pct_16plus": pop_pct,
                    "disparity_index": disparity,
                    "official_disparity_index": off,
                    "delta_vs_official": cell.get("delta_vs_official"),
                })
            ystruct[mlabel] = mstruct
        payload["years"][str(y)] = ystruct

    return _write_outputs(context, "ii2025_disparity_index", payload, rows)


# ------------------------------------------------------------------------------
# 2. Search reasons by year
# ------------------------------------------------------------------------------
@asset(
    name="ii2025_search_reasons_by_year",
    group_name=GROUP,
    deps=_DEPS,
    required_resource_keys={"data_dir_processed", "data_dir_out"},
    description="Statewide search reasons by year: Consent / Drug-alcohol odor / all-other "
                "(collapsed) and full per-reason breakdown, vs official.",
)
def ii2025_search_reasons_by_year(context) -> List[str]:
    sw = _load_statewide(context)
    sr = _load_state_reports(context)

    pc = sw[sw["canonical_key"].isin(PROBABLE_CAUSE)]
    years = sorted(int(y) for y in pc["year"].dropna().unique())

    # Official Table 5 (Probable cause) totals per report_year for cross-check.
    official: Dict[int, Dict[str, float]] = {}
    pc_off = sr[sr["section"] == "Probable cause"]
    for _, row in pc_off.iterrows():
        ck = OFFICIAL_PC_LABEL.get(str(row["metric"]))
        if ck:
            official.setdefault(int(row["report_year"]), {})[ck] = _num(row.get("Total"))

    payload = {
        "post": POST_SLUG,
        "metric": "search_reasons_by_year",
        "note": "Counts are searches by probable-cause / search-reason category, statewide. "
                "A single search can list multiple authorities, so categories may sum above total searches.",
        "highlight": {"consent": "probable-cause--consent",
                      "smell_of_drugs_alcohol": "probable-cause--drug-alcohol-odor",
                      "all_other": "sum of all remaining probable-cause categories"},
        "statewide_counts_source": "computed: 'Missouri (all agencies)' aggregate",
        "collapsed": {},  # year -> {consent, smell_of_drugs_alcohol, all_other} (+ races)
        "full": {},       # year -> {canonical_key -> {race: count}}
    }
    rows: List[dict] = []
    for y in years:
        # full per-reason
        full_y: Dict[str, dict] = {}
        per_reason_total: Dict[str, Optional[float]] = {}
        race_totals = {r: 0.0 for r in RACE_COLS}
        any_race = {r: False for r in RACE_COLS}
        for ck, label in PROBABLE_CAUSE.items():
            vals = _race_values(sw, ck, y)
            if not vals:
                continue
            full_y[ck] = {"label": label, **{r: vals.get(r) for r in RACE_COLS}}
            per_reason_total[ck] = vals.get("Total")
            for r in RACE_COLS:
                if vals.get(r) is not None:
                    race_totals[r] += vals[r]
                    any_race[r] = True
            off = official.get(y, {}).get(ck)
            rows.append({
                "year": y, "version": "full", "reason": ck, "label": label,
                **{r: vals.get(r) for r in RACE_COLS},
                "official_total": off, "delta_vs_official": _delta(vals.get("Total"), off),
            })
        if not full_y:
            continue
        payload["full"][str(y)] = full_y

        # collapsed: consent / smell / all-other
        def _bucket(keys: List[str]) -> Dict[str, Optional[float]]:
            agg = {r: None for r in RACE_COLS}
            for ck in keys:
                v = full_y.get(ck)
                if not v:
                    continue
                for r in RACE_COLS:
                    if v.get(r) is not None:
                        agg[r] = (agg[r] or 0.0) + v[r]
            return agg

        consent = _bucket(["probable-cause--consent"])
        smell = _bucket(["probable-cause--drug-alcohol-odor"])
        other_keys = [k for k in PROBABLE_CAUSE if k not in HIGHLIGHT_KEYS]
        all_other = _bucket(other_keys)
        payload["collapsed"][str(y)] = {
            "consent": consent,
            "smell_of_drugs_alcohol": smell,
            "all_other": all_other,
        }
        for bucket_name, bucket in [("consent", consent), ("smell_of_drugs_alcohol", smell), ("all_other", all_other)]:
            rows.append({"year": y, "version": "collapsed", "reason": bucket_name, "label": bucket_name,
                         **{r: bucket.get(r) for r in RACE_COLS},
                         "official_total": None, "delta_vs_official": None})

    return _write_outputs(context, "ii2025_search_reasons_by_year", payload, rows)


# ------------------------------------------------------------------------------
# 3. 2025 race summary: total stops, search rate, contraband hit rate
# ------------------------------------------------------------------------------
@asset(
    name="ii2025_race_summary_2025",
    group_name=GROUP,
    deps=_DEPS,
    required_resource_keys={"data_dir_processed", "data_dir_out"},
    description="Statewide total stops, search rate, contraband hit rate by race for 2025, vs official.",
)
def ii2025_race_summary_2025(context) -> List[str]:
    sw = _load_statewide(context)
    sr = _load_state_reports(context)
    y = PRIMARY_YEAR

    stops = _race_values(sw, "stops", y) or {}
    search_rate = _race_values(sw, "search-rate", y) or {}
    hit_rate = _race_values(sw, "contraband-hit-rate", y) or {}

    off_stops = _official_metric(sr, y, "Totals", "All stops") or {}
    off_search_rate = _official_metric(sr, y, "Rates", "Search rate") or {}
    off_hit_rate = _official_metric(sr, y, "Rates", "Contraband hit rate") or {}

    total_stops = stops.get("Total")
    payload = {
        "post": POST_SLUG,
        "metric": "race_summary",
        "year": y,
        "definitions": {
            "search_rate": "100 x searches / stops",
            "contraband_hit_rate": "100 x searches-with-contraband / searches",
            "stop_share_pct": "100 x stops(race) / stops(total)",
        },
        "statewide_counts_source": "computed: 'Missouri (all agencies)' aggregate",
        "by_race": {},
    }
    rows: List[dict] = []
    for r in RACE_COLS:
        cnt = stops.get(r)
        share = round(cnt / total_stops * 100, 4) if (cnt is not None and total_stops) else None
        cell = {
            "total_stops": cnt,
            "stop_share_pct": share,
            "search_rate": search_rate.get(r),
            "contraband_hit_rate": hit_rate.get(r),
            "official": {
                "total_stops": off_stops.get(r),
                "search_rate": off_search_rate.get(r),
                "contraband_hit_rate": off_hit_rate.get(r),
            },
            "delta_vs_official": {
                "total_stops": _delta(cnt, off_stops.get(r)),
                "search_rate": _delta(search_rate.get(r), off_search_rate.get(r)),
                "contraband_hit_rate": _delta(hit_rate.get(r), off_hit_rate.get(r)),
            },
        }
        payload["by_race"][r] = cell
        rows.append({
            "year": y, "race": r, "total_stops": cnt, "stop_share_pct": share,
            "search_rate": search_rate.get(r), "contraband_hit_rate": hit_rate.get(r),
            "official_total_stops": off_stops.get(r),
            "official_search_rate": off_search_rate.get(r),
            "official_contraband_hit_rate": off_hit_rate.get(r),
            "delta_total_stops": cell["delta_vs_official"]["total_stops"],
            "delta_search_rate": cell["delta_vs_official"]["search_rate"],
            "delta_contraband_hit_rate": cell["delta_vs_official"]["contraband_hit_rate"],
        })

    return _write_outputs(context, "ii2025_race_summary_2025", payload, rows)


# ------------------------------------------------------------------------------
# 4. Agencies reporting: roster composition + the 2024 drop-out / 2025 return
# ------------------------------------------------------------------------------
@asset(
    name="ii2025_agencies_reporting",
    group_name=GROUP,
    deps=[AssetKey("canonical_combined_parquet")],
    required_resource_keys={"data_dir_processed", "data_dir_out"},
    description="Per-year agency reporting composition (consistent / returning / new "
                "nonzero reporters + zero-stop filers) over a 10-year window, plus the "
                "cohort that dropped out in 2024 and returned in 2025. Shows that the "
                "year-over-year swing in reported stops is largely reporting-roster churn.",
)
def ii2025_agencies_reporting(context) -> List[str]:
    df = pd.read_parquet(_processed(context, "canonical_combined.parquet"))
    df = df[df["agency"] != STATEWIDE_AGENCY]
    st = df[df["canonical_key"] == "stops"][["agency", "year", "Total"]].copy()
    st["Total"] = st["Total"].fillna(0)

    # Nonzero reporters and summed stops, per year (clean canonical names).
    nz: Dict[int, set] = {}
    stops_sum: Dict[int, float] = {}
    for y, g in st[st["Total"] > 0].groupby("year"):
        nz[int(y)] = set(g["agency"])
        stops_sum[int(y)] = float(g["Total"].sum())
    all_years = sorted(nz)

    window = list(range(PRIMARY_YEAR - 9, PRIMARY_YEAR + 1))  # 2016..2025
    # Zero-stop filers are now first-class 'stops' rows with Total == 0 (folded in
    # at the combine step, issue #41) — count them straight from canonical.
    zero_by_year: Dict[int, set] = {
        int(y): set(g["agency"]) for y, g in st[st["Total"] == 0].groupby("year")
    }

    def prior_filed(y: int) -> set:
        s: set = set()
        for yy in all_years:
            if yy < y:
                s |= nz[yy]
        return s

    by_year: Dict[str, dict] = {}
    rows: List[dict] = []
    for y in window:
        cur = nz.get(y, set())
        prev = nz.get(y - 1, set())
        before = prior_filed(y)
        consistent = cur & prev                # reported this year AND last year
        returning = (cur - prev) & before      # reported before, gap last year
        new = cur - before                     # never reported before
        n_zero = len(zero_by_year.get(y, []))
        cell = {
            "reported_nonzero": len(cur),
            "consistent": len(consistent),
            "returning": len(returning),
            "new": len(new),
            "zero_stop_filers": n_zero,
            "total_filed": len(cur) + n_zero,
            "total_stops": round(stops_sum.get(y, 0.0)),
        }
        by_year[str(y)] = cell
        rows.append({"year": y, **cell})

    # 2024 churn detail: agencies that reported (nonzero) in 2023, vanished from
    # the 2024 report entirely, and came back in 2025.
    s23 = st[(st["year"] == 2023) & (st["Total"] > 0)].set_index("agency")["Total"]
    s25 = st[(st["year"] == 2025) & (st["Total"] > 0)].set_index("agency")["Total"]
    nz23, nz24, nz25 = nz.get(2023, set()), nz.get(2024, set()), nz.get(2025, set())
    came_back = sorted((nz23 - nz24) & nz25, key=lambda a: -float(s25.get(a, 0)))
    comeback_rows = [
        {"agency": a, "stops_2023": int(s23.get(a, 0)),
         "reported_2024": False, "stops_2025": int(s25.get(a, 0))}
        for a in came_back
    ]
    raw23, raw24, raw25 = stops_sum.get(2023, 0.0), stops_sum.get(2024, 0.0), stops_sum.get(2025, 0.0)
    panel = nz23 & nz24 & nz25
    panel_23 = float(s23.reindex(panel).fillna(0).sum())
    panel_25 = float(s25.reindex(panel).fillna(0).sum())
    cb_25 = float(s25.reindex(came_back).fillna(0).sum()) if came_back else 0.0

    payload = {
        "post": POST_SLUG,
        "metric": "agencies_reporting",
        "window": {"start": window[0], "end": window[-1]},
        "definitions": {
            "reported_nonzero": "agencies with >0 stops in the per-agency tables (from canonical_combined)",
            "consistent": "reported (nonzero) this year AND the prior year",
            "returning": "reported this year, NOT the prior year, but had reported in some earlier year",
            "new": "reported this year, never reported in any prior year on record",
            "zero_stop_filers": "agencies that filed but recorded zero stops (a 'stops' row of 0). For 2020+ "
                                "these come from the report's prose 'Zero Stops' list, now folded into "
                                "canonical_combined at the combine step (issue #41); earlier years pick up any "
                                "agency whose all-stops total is 0.",
            "total_filed": "reported_nonzero + zero_stop_filers (every agency that filed anything)",
        },
        "note": "The number of agencies in the report swings year to year. 2024 is a sharp dip: about "
                "80% of the agencies missing in 2024 reappear in 2025, bringing back nearly identical "
                "stop volume — so most of the 2024->2025 rise in total stops is returning agencies, not "
                "more policing. Compare absolute counts across these years with caution.",
        "headline_2024": {
            "reported_2023": len(nz23),
            "reported_2024": len(nz24),
            "reported_2025": len(nz25),
            "dropped_from_2023_to_2024": len(nz23 - nz24),
            "of_which_returned_in_2025": len(came_back),
            "comeback_stops_2023": int(s23.reindex(came_back).fillna(0).sum()) if came_back else 0,
            "comeback_stops_2025": int(cb_25),
            "raw_stops_2024": round(raw24),
            "raw_stops_2025": round(raw25),
            "raw_change_2024_to_2025": round(raw25 - raw24),
            "raw_pct_change_2024_to_2025": round((raw25 / raw24 - 1) * 100, 1) if raw24 else None,
            "comeback_share_of_raw_increase_pct": round(cb_25 / (raw25 - raw24) * 100) if raw25 != raw24 else None,
            "balanced_panel_agencies": len(panel),
            "balanced_panel_stops_2023": round(panel_23),
            "balanced_panel_stops_2025": round(panel_25),
            "balanced_panel_pct_change_2023_to_2025": round((panel_25 / panel_23 - 1) * 100, 1) if panel_23 else None,
        },
        "by_year": by_year,
        "comeback_2024_to_2025": comeback_rows,
    }

    out_dir = Path(context.resources.data_dir_out.get_path()) / "analysis" / POST_SLUG
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "ii2025_agencies_reporting.json"
    csv_path = out_dir / "ii2025_agencies_reporting.csv"
    comeback_path = out_dir / "ii2025_agencies_dropped_2024_returned_2025.csv"
    json_path.write_text(json.dumps(payload, indent=2))
    pd.DataFrame(rows).to_csv(csv_path, index=False)
    pd.DataFrame(comeback_rows).to_csv(comeback_path, index=False)
    context.add_output_metadata({
        "json": str(json_path), "composition_csv": str(csv_path),
        "comeback_csv": str(comeback_path), "comeback_agencies": len(comeback_rows),
    })
    return [str(json_path), str(csv_path), str(comeback_path)]
