from __future__ import annotations

import json
import os
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import List

import shutil
import tempfile

import duckdb
import pandas as pd
from slugify import slugify

from dagster import AssetIn, AssetKey, Out, asset, graph_asset, op

from missouri_vsr.assets.extract import PIVOT_VALUE_COLUMNS, YEAR_URLS, _slugify_simple
from missouri_vsr.cli.crosswalk import _normalize_name
from missouri_vsr.assets.s3_utils import (
    upload_paths,
    s3_uri_for_dir,
    s3_uri_for_path,
    upload_file_to_s3,
)

# ------------------------------------------------------------------------------
# Pivoted outputs & per-agency JSON exports
# ------------------------------------------------------------------------------
MSHP_DEPARTMENT_NAME = "Missouri State Highway Patrol"
DOWNLOAD_PREFIX = f"missouri_vsr_{min(YEAR_URLS)}_{max(YEAR_URLS)}_"
# Row_keys excluded from metric_year_subset (not user-facing in the homepage picker).
# Anything else present in the canonical combined parquet will be emitted.
METRIC_YEAR_SUBSET_EXCLUDE_PREFIXES = ("rates-population",)

# Cross-rate pairings (e.g. "stop-rate-residents--arrest-rate") are scatter-chart
# inputs only — they're kept in metric_year_subset/{key}.json so the scatter
# components can read them by name, but excluded from manifest.canonical_metrics
# so they don't surface in the homepage picker.
_RATE_HALVES = {
    "search-rate",
    "arrest-rate",
    "contraband-hit-rate",
    "stop-rate",
    "citation-rate",
    "stop-rate-residents",
}


def _is_cross_rate_pairing(key: str) -> bool:
    """True iff key is a top-level pairing of two rate-shaped halves (X--Y)."""
    if "--" not in key:
        return False
    parts = key.split("--")
    if len(parts) != 2:
        return False
    return all(p in _RATE_HALVES or p.endswith("-rate") for p in parts)
STATEWIDE_SUMS_SUBSET_KEYS = [
    # Canonical keys (post-collapse row_key = canonical_key)
    "stops",
    "probable-cause--consent",
    "arrests",
    "citations",
    "stop-outcome--warning",
    "stop-outcome--no-action",
]

HOMEPAGE_STATS_YEAR = max(YEAR_URLS)
HOMEPAGE_STATS_METRICS = {
    # Canonical keys (post-collapse row_key = canonical_key)
    "all_stops": "stops",
    "searches": "searches",
    "contraband": "contraband-total",
    "citations": "citations",
    "arrests": "arrests",
    "warnings": "stop-outcome--warning",
}
STATEWIDE_RATE_SPECS = [
    {"row_key": "citation-rate",       "numerator": "citations",                  "denominator": "stops"},
    {"row_key": "search-rate",         "numerator": "searches",                   "denominator": "stops"},
    {"row_key": "arrest-rate",         "numerator": "arrests",                    "denominator": "stops"},
    {"row_key": "contraband-hit-rate", "numerator": "contraband-total",           "denominator": "searches"},
    {"row_key": "resident-stop-rate",  "numerator": "resident-stops",             "denominator": "stops"},
    {"row_key": "equipment-stop-rate", "numerator": "reason-for-stop--equipment", "denominator": "stops"},
    {"row_key": "license-stop-rate",   "numerator": "reason-for-stop--license",   "denominator": "stops"},
    {"row_key": "moving-stop-rate",    "numerator": "reason-for-stop--moving",    "denominator": "stops"},
    {"row_key": "male-driver-rate",    "numerator": "driver-gender--male",        "denominator": "stops"},
    {"row_key": "warning-rate",        "numerator": "stop-outcome--warning",      "denominator": "stops"},
    # stop-rate omitted: ACS population denominator key changes each year (era-specific).
]

STATEWIDE_AGENCY_NAME = "Missouri (all agencies)"
STATEWIDE_AGENCY_SLUG = "missouri-all-agencies"

# Minimum year included in dist outputs (downloads keep full history)
DIST_MIN_YEAR = 2001

# Issue #4: in-group percentage column names (one per non-Total race column)
RACE_PCT_COLUMNS = [
    f"{race} Pct"
    for race in ["White", "Black", "Hispanic", "Native American", "Asian", "Other"]
]
# Issue #5: non-white to white ratio column name
RATIO_COLUMN = "Nonwhite to White Ratio"


def _build_rank_percentile_frame(
    base: pd.DataFrame,
    metric_df: pd.DataFrame,
    value_cols: List[str],
    suffix: str,
) -> pd.DataFrame:
    frame = base[["agency", "year", "row_key"]].copy()
    frame["row_key"] = frame["row_key"].astype(str) + suffix
    for col in value_cols:
        frame[col] = metric_df[col]
    for col in base.columns:
        if col not in frame.columns:
            frame[col] = pd.NA
    return frame[base.columns]


def _add_ingroup_pcts_and_ratios(df: pd.DataFrame, value_cols: List[str]) -> None:
    """Add in-group percentage and ratio columns to non-rate rows in-place.

    For each non-Total race column adds ``{race} Pct`` = race / Total.
    Also adds ``Nonwhite to White Ratio`` = (Total - White) / White.
    Rate rows and rows where the denominator is zero receive NaN.
    """
    if "metric" in df.columns:
        rate_mask = (
            df["metric"]
            .astype(str)
            .str.contains(r"\brate\b", case=False, regex=True, na=False)
        )
    else:
        rate_mask = pd.Series(False, index=df.index)

    total = (
        pd.to_numeric(df["Total"], errors="coerce")
        if "Total" in df.columns
        else pd.Series(dtype=float, index=df.index)
    )
    valid = ~rate_mask & (total > 0)

    race_cols = [c for c in value_cols if c != "Total" and c in df.columns]
    for race in race_cols:
        pct_col = f"{race} Pct"
        race_vals = pd.to_numeric(df[race], errors="coerce")
        df[pct_col] = (race_vals / total).where(valid)

    if "White" in df.columns:
        white = pd.to_numeric(df["White"], errors="coerce")
        df[RATIO_COLUMN] = ((total - white) / white).where(valid & (white > 0))


def _compute_statewide_rates(grouped: pd.DataFrame, value_cols: List[str]) -> pd.DataFrame:
    """Compute derived rate rows from statewide grouped sums.

    Extracted so it can be used both by ``write_statewide_year_sums_json``
    and the new statewide agency JSON writer.
    """
    if grouped.empty:
        return pd.DataFrame()
    lookup = grouped.set_index(["year", "row_key"])[value_cols]
    derived_rows: List[dict] = []
    for year in grouped["year"].dropna().unique().tolist():
        for spec in STATEWIDE_RATE_SPECS:
            num_key = spec["numerator"]
            den_key = spec["denominator"]
            if (year, num_key) not in lookup.index or (year, den_key) not in lookup.index:
                continue
            num = lookup.loc[(year, num_key)]
            den = lookup.loc[(year, den_key)]
            rates = num / den
            rates = rates.where(den != 0)
            record: dict = {"year": year, "row_key": spec["row_key"]}
            for col in value_cols:
                record[col] = rates.get(col)
            derived_rows.append(record)
    return pd.DataFrame(derived_rows)


def _build_statewide_canonical_rows(canonical: pd.DataFrame) -> pd.DataFrame:
    """Build statewide-aggregate rows ('Missouri (all agencies)') matching the canonical schema.

    Rolls up counts across all real agencies per (year, row_key), re-derives rate
    rows via STATEWIDE_RATE_SPECS, and returns a frame whose columns are a strict
    superset-compatible reindex of ``canonical.columns``. Skips population-normalized
    rates — naive aggregation of ACS denominators is misleading because traffic
    through a place is not who lives there.
    """
    if canonical.empty:
        return pd.DataFrame(columns=canonical.columns)

    value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in canonical.columns]
    if not value_cols:
        return pd.DataFrame(columns=canonical.columns)

    base = canonical[canonical["agency"] != STATEWIDE_AGENCY_NAME].copy()
    derived_mask = base["row_key"].astype(str).str.contains(
        r"-(?:rank|percentile|percentage)$", regex=True, na=False
    )
    base = base[~derived_mask]

    # Exclude rate rows and population rows from aggregation source.
    # Rates are re-derived from totals; population aggregation is misleading.
    exclude_mask = (
        base["row_key"].astype(str).str.endswith("-rate", na=False)
        | base["row_key"].astype(str).str.startswith("rates-by-race--population", na=False)
    )
    count_base = base[~exclude_mask].copy()
    for col in value_cols:
        count_base[col] = pd.to_numeric(count_base[col], errors="coerce")

    grouped = (
        count_base.groupby(["year", "row_key"], dropna=True)[value_cols]
        .sum(min_count=1)
        .reset_index()
    )
    if grouped.empty:
        return pd.DataFrame(columns=canonical.columns)

    derived_rates = _compute_statewide_rates(grouped, value_cols)
    all_rows = (
        pd.concat([grouped, derived_rates], ignore_index=True)
        if not derived_rates.empty
        else grouped
    )

    meta_cols = ["table", "table_id", "section", "section_id", "metric", "metric_id"]
    present_meta = [c for c in meta_cols if c in canonical.columns]
    if present_meta:
        meta_lookup = (
            base[["row_key", *present_meta]]
            .drop_duplicates("row_key", keep="first")
            .set_index("row_key")
        )
        for col in present_meta:
            all_rows[col] = all_rows["row_key"].map(meta_lookup[col])

    all_rows["agency"] = STATEWIDE_AGENCY_NAME
    if "canonical_key" in canonical.columns:
        all_rows["canonical_key"] = all_rows["row_key"]
    if "year" in all_rows.columns:
        year_strs = all_rows["year"].apply(
            lambda y: str(int(y)) if pd.notna(y) else ""
        )
        all_rows["row_id"] = (
            year_strs + f"-{STATEWIDE_AGENCY_SLUG}-" + all_rows["row_key"].astype(str)
        )

    for col in canonical.columns:
        if col not in all_rows.columns:
            all_rows[col] = pd.NA

    return all_rows[canonical.columns]


def _build_percentage_frame(
    base: pd.DataFrame,
    value_cols: List[str],
    *,
    metric_col: str,
) -> pd.DataFrame:
    if metric_col not in base.columns:
        return pd.DataFrame(columns=base.columns)

    metric_series = base[metric_col].astype(str)
    non_rate_mask = ~metric_series.str.contains(r"\brate\b", case=False, regex=True, na=False)
    percentage_base = base.loc[non_rate_mask].copy()
    if percentage_base.empty:
        return pd.DataFrame(columns=base.columns)

    total = pd.to_numeric(percentage_base["Total"], errors="coerce")
    percentages = percentage_base[value_cols].div(total, axis=0)
    percentages = percentages.where(total > 0)
    percentage_base[value_cols] = percentages
    percentage_base["row_key"] = percentage_base["row_key"].astype(str) + "-percentage"
    return percentage_base


def _open_canonical_db(parquet_path: str) -> duckdb.DuckDBPyConnection:
    """
    Open an in-memory DuckDB connection with a 'canonical_combined' view that
    applies the canonical collapse directly from the combined Parquet file.

    The view:
    - Deduplicates canonical_key rows to one per (agency, year, canonical_key),
      preferring computed--rates-- row_keys (uniform methodology).
    - Replaces row_key with canonical_key for era-independent grouping.
    - Passes era-specific (null canonical_key) rows through unchanged.

    Downstream ops query this view rather than loading the full DataFrame.
    """
    # DuckDB needs double-quoted identifiers for column names with spaces/dots.
    race_cols = [
        '"Total"', '"White"', '"Black"', '"Hispanic"',
        '"Native American"', '"Asian"', '"Other"',
    ]
    race_sql = ", ".join(race_cols)

    # Use explicit column lists (not SELECT *) to avoid DuckDB type-binding
    # errors with window functions on mixed-type CTEs.
    meta_cols = "agency, CAST(year AS BIGINT) AS year, table_id, \"table\", section_id, section, metric_id, metric, row_id"

    con = duckdb.connect()
    con.execute(f"""
        CREATE VIEW canonical_combined AS
        WITH base AS (
            SELECT
                {meta_cols},
                row_key, canonical_key,
                {race_sql},
                (row_key LIKE 'computed--rates--%')::INTEGER AS _priority
            FROM read_parquet('{parquet_path}')
        ),
        best_canonical AS (
            SELECT
                {meta_cols},
                canonical_key AS row_key, canonical_key,
                {race_sql}
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY agency, year, canonical_key
                        ORDER BY _priority DESC
                    ) AS _rn
                FROM base WHERE canonical_key IS NOT NULL
            ) sub WHERE _rn = 1
        ),
        era_specific AS (
            SELECT {meta_cols}, row_key, canonical_key, {race_sql}
            FROM base WHERE canonical_key IS NULL
        )
        SELECT * FROM best_canonical
        UNION ALL
        SELECT * FROM era_specific
    """)
    return con


def _collapse_to_canonical(df: pd.DataFrame) -> pd.DataFrame:
    """
    Produce the canonical dist view from the full combined DataFrame.

    Rules:
    - Rows with canonical_key=null (era-specific, no cross-era equivalent) pass through unchanged.
    - Rows with canonical_key: deduplicate to ONE per (agency, year, canonical_key),
      preferring computed--rates-- row_keys (uniform methodology) over PDF pre-computed.
    - The row_key of kept canonical rows is replaced with the canonical_key value so that
      all downstream grouping (ranks, JSON keys) is era-independent.
    - row_id is rebuilt after the substitution.

    The full multi-row_key data remains available in the layer-2 combined Parquet; this
    collapse is only applied for layer-3 dist outputs.
    """
    if "canonical_key" not in df.columns or df.empty:
        return df

    null_mask = df["canonical_key"].isna()
    era_specific = df[null_mask].copy()
    canonical = df[~null_mask].copy()

    if canonical.empty:
        return df

    # Sort so computed--rates-- rows sort first within each group (priority = 1 > 0)
    canonical["_priority"] = canonical["row_key"].str.startswith("computed--rates--", na=False).astype(int)
    canonical = canonical.sort_values(
        ["agency", "year", "canonical_key", "_priority"],
        ascending=[True, True, True, False],
    )
    canonical = canonical.drop_duplicates(subset=["agency", "year", "canonical_key"], keep="first")
    canonical = canonical.drop(columns=["_priority"])

    # Replace row_key with canonical_key so downstream grouping is era-independent
    canonical["row_key"] = canonical["canonical_key"]

    result = pd.concat([era_specific, canonical], ignore_index=True)
    return _rebuild_row_ids(result)


def _rebuild_row_ids(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"year", "agency", "row_key"}
    if not required.issubset(frame.columns):
        return frame

    def _format_year(value) -> str | None:
        if pd.isna(value):
            return None
        try:
            return str(int(value))
        except (TypeError, ValueError):
            return str(value)

    def _row_id(row: pd.Series) -> str:
        parts = [
            _format_year(row["year"]),
            _slugify_simple(str(row["agency"])),
            str(row["row_key"]),
        ]
        return "-".join(part for part in parts if part)

    updated = frame.copy()
    updated["row_id"] = updated.apply(_row_id, axis=1)
    return updated


def _rank_and_percentile(
    base: pd.DataFrame,
    value_cols: List[str],
    *,
    exclude_mask: pd.Series | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if exclude_mask is not None:
        working = base.loc[~exclude_mask]
    else:
        working = base

    if working.empty:
        blank = pd.DataFrame(index=base.index, columns=value_cols, dtype="float")
        return blank.copy(), blank.copy()

    grouped = working.groupby(["year", "row_key"])[value_cols]
    rank_df = grouped.rank(method="dense", ascending=False)
    percentile_df = grouped.rank(method="max", ascending=True, pct=True) * 100

    if exclude_mask is None:
        return rank_df, percentile_df

    rank_full = pd.DataFrame(index=base.index, columns=value_cols, dtype="float")
    percentile_full = pd.DataFrame(index=base.index, columns=value_cols, dtype="float")
    rank_full.loc[working.index, :] = rank_df
    percentile_full.loc[working.index, :] = percentile_df
    return rank_full, percentile_full


def _rank_dense_counts(
    base: pd.DataFrame,
    *,
    value_col: str = "Total",
    exclude_mask: pd.Series | None = None,
) -> tuple[pd.Series, pd.Series]:
    if value_col not in base.columns:
        blank = pd.Series(index=base.index, dtype="float")
        return blank.copy(), blank.copy()

    working = base.loc[~exclude_mask] if exclude_mask is not None else base
    if working.empty:
        blank = pd.Series(index=base.index, dtype="float")
        return blank.copy(), blank.copy()

    values = pd.to_numeric(working[value_col], errors="coerce")
    working = working.copy()
    working["_rank_value"] = values
    working = working[pd.notna(working["_rank_value"])]

    if working.empty:
        blank = pd.Series(index=base.index, dtype="float")
        return blank.copy(), blank.copy()

    grouped = working.groupby(["year", "row_key"])["_rank_value"]
    rank_series = grouped.rank(method="dense", ascending=False)
    count_series = grouped.transform("count")

    rank_full = pd.Series(index=base.index, dtype="float")
    count_full = pd.Series(index=base.index, dtype="float")
    rank_full.loc[working.index] = rank_series
    count_full.loc[working.index] = count_series
    return rank_full, count_full


@op(out=Out(pd.DataFrame), required_resource_keys={"data_dir_processed", "s3"})
def add_rank_percentile_rows(context, combined: pd.DataFrame) -> pd.DataFrame:
    """Add rank/percentile rows per row_key/year across agencies."""
    if combined.empty:
        context.log.warning("Combined report DataFrame is empty; skipping rank/percentile.")
        return combined

    combined = _collapse_to_canonical(combined)
    context.log.info("Collapsed to canonical view: %d rows", len(combined))

    required_cols = {"agency", "year", "row_key"}
    missing = required_cols - set(combined.columns)
    if missing:
        raise ValueError(f"Cannot add ranks/percentiles – missing columns: {sorted(missing)}")

    value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in combined.columns]
    if not value_cols:
        raise ValueError("Cannot add ranks/percentiles – no numeric value columns were found.")

    # Process year-by-year and write each chunk to a temp Parquet immediately.
    # This avoids holding all 25 year-chunks in RAM simultaneously (which OOM-kills
    # on the final pd.concat).  DuckDB unions the temp files into the final Parquet.
    years = sorted(combined["year"].dropna().unique())
    metric_col = "metric"
    total_base = 0
    total_out = 0

    tmp_dir = Path(tempfile.mkdtemp(prefix="vsr_rank_"))
    try:
        chunk_columns: list[str] | None = None
        for yr in years:
            base = combined[combined["year"] == yr].copy()
            if base.empty:
                continue

            percentage_rows = _build_percentage_frame(base, value_cols, metric_col=metric_col)
            rank_all, percentile_all = _rank_and_percentile(base, value_cols)
            rank_dense, rank_count = _rank_dense_counts(base, value_col="Total")

            base["rank_dense"] = rank_dense
            base["rank_count"] = rank_count
            base["rank_method"] = pd.NA
            base.loc[base["rank_dense"].notna(), "rank_method"] = "dense"
            try:
                base["rank_dense"] = base["rank_dense"].astype("Int64")
                base["rank_count"] = base["rank_count"].astype("Int64")
            except Exception:
                pass

            derived = [
                percentage_rows,
                _build_rank_percentile_frame(base, rank_all, value_cols, "-rank"),
                _build_rank_percentile_frame(base, percentile_all, value_cols, "-percentile"),
            ]
            _add_ingroup_pcts_and_ratios(base, value_cols)

            chunk = _rebuild_row_ids(pd.concat([base, *derived], ignore_index=True))
            if chunk_columns is None:
                chunk_columns = list(chunk.columns)
            chunk_path = tmp_dir / f"chunk_{yr}.parquet"
            chunk.to_parquet(chunk_path, index=False, engine="pyarrow")
            total_base += len(base)
            total_out += len(chunk)
            context.log.debug("Wrote year %d chunk: %d rows", yr, len(chunk))

        # Inject statewide base rows (no rank/percentile/percentage variants).
        # Built from the canonical-collapsed real-agency frame so the rollup is
        # consistent with canonical_combined.parquet.
        if chunk_columns is not None:
            template = pd.DataFrame(columns=chunk_columns)
            statewide_source_cols = [c for c in template.columns if c in combined.columns]
            statewide_source = combined[statewide_source_cols].copy()
            for col in chunk_columns:
                if col not in statewide_source.columns:
                    statewide_source[col] = pd.NA
            statewide_source = statewide_source[chunk_columns]
            statewide_rows = _build_statewide_canonical_rows(statewide_source)
            if not statewide_rows.empty:
                statewide_path = tmp_dir / "chunk_statewide.parquet"
                statewide_rows.to_parquet(statewide_path, index=False, engine="pyarrow")
                total_out += len(statewide_rows)
                context.log.info(
                    "Appended %d statewide base rows to reports_with_rank_percentile",
                    len(statewide_rows),
                )

        out_dir = Path(context.resources.data_dir_processed.get_path())
        out_path = out_dir / "reports_with_rank_percentile.parquet"
        con = duckdb.connect()
        con.execute(
            f"COPY (SELECT * FROM read_parquet('{tmp_dir}/chunk_*.parquet')) "
            f"TO '{out_path}' (FORMAT PARQUET)"
        )
        con.close()
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    context.log.info(
        "Added rank/percentile/percentage rows: %d base rows → %d total rows across %d years",
        total_base, total_out, len(years),
    )

    out_dir = Path(context.resources.data_dir_processed.get_path())
    out_path = out_dir / "reports_with_rank_percentile.parquet"
    context.log.info("Wrote rank/percentile Parquet → %s (%d rows)", out_path, total_out)

    augmented = pd.read_parquet(out_path)  # read back for return value + metadata
    meta = {"local_path": str(out_path), "row_count": total_out}
    try:
        s3_meta = upload_file_to_s3(
            context,
            out_path,
            f"downloads/{DOWNLOAD_PREFIX}vsr_statistics.parquet",
            content_type="application/vnd.apache.parquet",
        )
        if s3_meta:
            meta.update(s3_meta)
    except Exception as exc:
        context.log.exception("S3 handling encountered an exception for rank/percentile Parquet: %s", exc)

    try:
        context.add_output_metadata(meta)
    except Exception:
        pass
    return augmented


@graph_asset(
    name="reports_with_rank_percentile",
    group_name="processed",
    ins={"combine_all_reports": AssetIn(key=AssetKey("combine_all_reports"))},
    description="Add rank/percentile rows per row_key/year across agencies.",
)
def reports_with_rank_percentile(combine_all_reports: pd.DataFrame) -> pd.DataFrame:
    return add_rank_percentile_rows(combine_all_reports)


@op(out=Out(pd.DataFrame), required_resource_keys={"data_dir_processed", "s3"})
def compute_statewide_slug_baselines(context, combined: pd.DataFrame) -> pd.DataFrame:
    """Compute statewide mean/median baselines per year/row_key/metric."""
    combined = _collapse_to_canonical(combined)
    baseline_columns = [
        "year",
        "row_key",
        "metric",
        "count",
        "mean",
        "median",
        "count__no_mshp",
        "mean__no_mshp",
        "median__no_mshp",
    ]
    if combined.empty:
        context.log.warning("Combined report DataFrame is empty; no baselines computed.")
        return pd.DataFrame(columns=baseline_columns)

    required_cols = {"agency", "year", "row_key"}
    missing = required_cols - set(combined.columns)
    if missing:
        raise ValueError(f"Cannot compute baselines – missing columns: {sorted(missing)}")

    value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in combined.columns]
    if not value_cols:
        raise ValueError("Cannot compute baselines – no numeric value columns were found.")

    base = combined[["agency", "year", "row_key", *value_cols]].copy()
    base["agency"] = base["agency"].astype(str).str.strip()

    def _build_baseline(df: pd.DataFrame, scope: str) -> pd.DataFrame:
        melted = df.melt(
            id_vars=["year", "row_key"],
            value_vars=value_cols,
            var_name="metric",
            value_name="value",
        )
        melted = melted.dropna(subset=["value"])
        if melted.empty:
            return pd.DataFrame(columns=["year", "row_key", "metric", "count", "mean", "median"])
        grouped = (
            melted.groupby(["year", "row_key", "metric"])["value"]
            .agg(count="count", mean="mean", median="median")
            .reset_index()
        )
        if scope == "no_mshp":
            grouped = grouped.rename(
                columns={
                    "count": "count__no_mshp",
                    "mean": "mean__no_mshp",
                    "median": "median__no_mshp",
                }
            )
        return grouped

    all_baselines = _build_baseline(base, "all")
    no_mshp_baselines = _build_baseline(base.loc[base["agency"].ne(MSHP_DEPARTMENT_NAME)], "no_mshp")
    if all_baselines.empty and no_mshp_baselines.empty:
        baselines = pd.DataFrame(columns=baseline_columns)
    else:
        baselines = all_baselines.merge(
            no_mshp_baselines,
            on=["year", "row_key", "metric"],
            how="outer",
        )
        baselines = baselines[baseline_columns]

    out_dir = Path(context.resources.data_dir_processed.get_path())
    out_path = out_dir / "statewide_slug_baselines.parquet"
    baselines.to_parquet(out_path, index=False, engine="pyarrow")
    context.log.info("Wrote statewide baselines Parquet → %s (%d rows)", out_path, len(baselines))

    meta = {"local_path": str(out_path), "row_count": len(baselines)}
    try:
        s3_meta = upload_file_to_s3(
            context,
            out_path,
            f"downloads/{DOWNLOAD_PREFIX}statewide_slug_baselines.parquet",
            content_type="application/vnd.apache.parquet",
        )
        if s3_meta:
            meta.update(s3_meta)
    except Exception as exc:
        context.log.exception("S3 handling encountered an exception for statewide baselines Parquet: %s", exc)

    try:
        context.add_output_metadata(meta)
    except Exception:
        pass
    return baselines


@graph_asset(
    name="statewide_slug_baselines",
    group_name="processed",
    ins={"combine_all_reports": AssetIn(key=AssetKey("combine_all_reports"))},
    description="Compute statewide mean/median baselines per year/row_key/metric.",
)
def statewide_slug_baselines(combine_all_reports: pd.DataFrame) -> pd.DataFrame:
    return compute_statewide_slug_baselines(combine_all_reports)


def pivot_reports_by_slug_op(context, combined: pd.DataFrame) -> pd.DataFrame:
    """Pivot combined report data so each Agency+Year row contains row_key-based columns."""
    if combined.empty:
        context.log.warning("Combined report DataFrame is empty; returning empty pivot result.")
        return pd.DataFrame(columns=["agency", "year"])

    required_cols = {"agency", "year", "row_key"}
    missing = required_cols - set(combined.columns)
    if missing:
        raise ValueError(f"Cannot pivot – missing columns: {sorted(missing)}")

    value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in combined.columns]
    if not value_cols:
        raise ValueError("Cannot pivot – no numeric value columns were found.")

    base = combined[["agency", "year", "row_key", *value_cols]].copy()
    dup_mask = base.duplicated(subset=["agency", "year", "row_key"], keep=False)
    if dup_mask.any():
        dupe_preview = (
            base.loc[dup_mask, ["agency", "year", "row_key"]]
            .drop_duplicates()
            .head(5)
            .to_dict(orient="records")
        )
        context.log.warning(
            "Found %d duplicate row_key rows after grouping; keeping first occurrence. Examples: %s",
            dup_mask.sum(),
            dupe_preview,
        )

    pivoted = base.pivot_table(
        index=["agency", "year"],
        columns="row_key",
        values=value_cols,
        aggfunc="first",
    )

    if pivoted.empty:
        return pivoted.reset_index()

    if isinstance(pivoted.columns, pd.MultiIndex):
        pivoted = pivoted.sort_index(axis=1, level=0, sort_remaining=True)
        pivoted.columns = pivoted.columns.swaplevel(0, 1)
        pivoted.columns = [
            f"{row_key}__{metric}"
            for row_key, metric in pivoted.columns
        ]
    else:
        pivoted.columns = [str(c) for c in pivoted.columns]

    pivoted = pivoted.reset_index()
    pivoted["year"] = pivoted["year"].astype("Int64", copy=False)
    return pivoted


_WIDE_RACE_SLUGS = {
    "Total": "total",
    "White": "white",
    "Black": "black",
    "Hispanic": "hispanic",
    "Native American": "native_american",
    "Asian": "asian",
    "Other": "other",
}


def _flatten_to_wide_columns(pivoted: pd.DataFrame) -> pd.DataFrame:
    """Rename columns from `{row_key}__{Race}` to `{flat_row_key}_{race_slug}`.

    `flat_row_key` replaces row_key's `--` hierarchy markers and inner `-`s with
    `_`. Race names are lowercased and spaces become underscores. Leaves the
    `agency` and `year` index columns untouched.
    """
    def rename(col: str) -> str:
        if col in ("agency", "year"):
            return col
        rk, _, race = col.rpartition("__")
        if not rk:
            return col
        rk_flat = rk.replace("--", "_").replace("-", "_").lower()
        race_slug = _WIDE_RACE_SLUGS.get(race, race.lower().replace(" ", "_"))
        return f"{rk_flat}_{race_slug}"

    return pivoted.rename(columns=rename)


@asset(
    name="pivot_reports_by_slug",
    group_name="processed",
    deps=[AssetKey("reports_with_rank_percentile")],
    required_resource_keys={"data_dir_processed"},
    description="Pivot combined report data so each agency/year row has row_key-derived columns.",
)
def pivot_reports_by_slug(context) -> pd.DataFrame:
    processed_dir = Path(context.resources.data_dir_processed.get_path())
    combined = pd.read_parquet(processed_dir / "reports_with_rank_percentile.parquet")
    return pivot_reports_by_slug_op(context, combined)


def _is_null(value) -> bool:
    try:
        result = pd.isna(value)
    except Exception:
        return False
    if isinstance(result, bool):
        return result
    try:
        return bool(result)
    except Exception:
        return False


def _json_safe_value(value):
    if _is_null(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return item()
        except Exception:
            pass
    return value


def _series_to_json_dict(series: pd.Series) -> dict:
    data = {}
    for col, value in series.items():
        if _is_null(value):
            continue
        if col.endswith("_response") and isinstance(value, str):
            try:
                data[col] = json.loads(value)
                continue
            except ValueError:
                pass
        data[col] = _json_safe_value(value)
    return data


def _agency_slug(value) -> str:
    if value is None:
        return "agency"
    text = str(value).replace("'", "").strip()
    if not text or text.lower() == "nan":
        return "agency"
    slug = slugify(text, lowercase=True)
    return slug or "agency"


def _json_safe_record(record: dict) -> dict:
    cleaned = {}
    for key, value in record.items():
        if _is_null(value):
            cleaned[key] = None
        else:
            cleaned[key] = _json_safe_value(value)
    return cleaned


def _extract_jurisdiction_geoid(row: pd.Series) -> str | None:
    """Return the Census GEOID for an agency's best-guess jurisdictional boundary.

    Municipal agencies → 7-digit place GEOID; county agencies → 5-digit county GEOID.
    Returns None when the agency type is unclassified or geocoding data is absent.
    """
    agency_type = re.sub(r"\s+", " ", str(row.get("AgencyType") or "").strip().lower())
    resp_raw = row.get("geocode_jurisdiction_response")
    if not resp_raw:
        return None
    try:
        resp = json.loads(resp_raw) if isinstance(resp_raw, str) else resp_raw
    except (json.JSONDecodeError, TypeError):
        return None
    results = resp.get("results") if isinstance(resp, dict) else None
    if not isinstance(results, list) or not results:
        return None
    fields = results[0].get("fields")
    if not isinstance(fields, dict):
        return None
    census = fields.get("census")
    if not isinstance(census, dict):
        return None
    years = [int(k) for k in census if str(k).isdigit()]
    if not years:
        return None
    record = census[str(max(years))]
    if not isinstance(record, dict):
        return None
    state_fips = str(record.get("state_fips") or "29").strip().zfill(2)
    if "county" in agency_type:
        county_fips = str(record.get("county_fips") or "").strip()
        if county_fips.isdigit() and len(county_fips) == 3:
            return f"{state_fips}{county_fips}"
        if county_fips.isdigit() and len(county_fips) == 5 and county_fips.startswith(state_fips):
            return county_fips
        return None
    if "municipal" in agency_type:
        place_raw = record.get("place")
        if isinstance(place_raw, dict):
            place_raw = place_raw.get("fips") or place_raw.get("geoid") or place_raw.get("place")
        place = str(place_raw or "").strip()
        if place.isdigit() and len(place) == 5:
            return f"{state_fips}{place}"
        if place.isdigit() and len(place) == 7 and place.startswith(state_fips):
            return place
        return None
    return None


def pq_column_names(parquet_path: Path) -> list[str]:
    """Return a parquet file's column names without reading any row data."""
    import pyarrow.parquet as pq

    return list(pq.ParquetFile(str(parquet_path)).schema.names)


def _canonical_name_lookup(agency_reference_geocoded: pd.DataFrame) -> dict[str, str]:
    """{normalized_agency_name: canonical_name} from the agency reference."""
    lookup: dict[str, str] = {}
    if agency_reference_geocoded is None or agency_reference_geocoded.empty:
        return lookup
    for _, row in agency_reference_geocoded.iterrows():
        normalized = _first_non_empty(row, ["Normalized"]) or ""
        canonical = _first_non_empty(row, ["Department", "Canonical"]) or ""
        if normalized and canonical:
            lookup[normalized] = canonical
    return lookup


def _apply_canonical_names(reports: pd.DataFrame, lookup: dict[str, str]) -> pd.DataFrame:
    """Add a canonical_name column to reports using a prebuilt normalized->canonical lookup."""
    reports["canonical_name"] = reports["agency"].apply(
        lambda a: lookup.get(_normalize_name(str(a))) if pd.notna(a) else None
    )
    return reports


def _add_canonical_names(
    reports: pd.DataFrame,
    agency_reference_geocoded: pd.DataFrame,
) -> pd.DataFrame:
    """Add a canonical_name column to reports by joining against the agency reference."""
    if reports.empty or agency_reference_geocoded.empty:
        return reports
    lookup = _canonical_name_lookup(agency_reference_geocoded)
    if not lookup:
        return reports
    return _apply_canonical_names(reports.copy(), lookup)


def _load_program_287g_by_canonical(processed_dir: Path) -> dict[str, dict]:
    """Build {canonical_agency_name: program_287g_payload} from ice_287g_latest.parquet.

    Returns {} when no snapshot has been materialized.
    """
    path = processed_dir / "ice_287g_latest.parquet"
    if not path.exists():
        return {}
    df = pd.read_parquet(path)
    if df.empty or "agency_canonical" not in df.columns:
        return {}
    df = df[df["agency_canonical"].notna()].copy()
    out: dict[str, dict] = {}
    for canonical, group in df.groupby("agency_canonical", sort=False):
        agreements = []
        for _, r in group.sort_values("signed_date", na_position="last").iterrows():
            agreements.append(
                {
                    "support_type": r.get("support_type"),
                    "signed_date": r.get("signed_date"),
                    "moa_status": r.get("moa_status"),
                    "moa_url": r.get("moa_url"),
                }
            )
        first = group.iloc[0]
        out[str(canonical)] = {
            "snapshot_date": first.get("snapshot_date"),
            "snapshot_filename": first.get("snapshot_filename"),
            "agreements": agreements,
        }
    return out


def build_agency_index_records(
    pivoted: pd.DataFrame,
    agency_reference_geocoded: pd.DataFrame,
    combined: pd.DataFrame | None = None,
    years_by_agency: dict[str, list[int]] | None = None,
    program_287g_by_canonical: dict[str, dict] | None = None,
) -> list[dict]:
    """Build agency index records with optional filtering for agencies with data."""
    names_by_key: dict[str, dict] = {}
    name_cols = [
        "Department",
        "Canonical",
        "AgencyName",
        "Agency Name",
        "Agency",
        "DepartmentName",
        "Name",
        "Raw",
    ]
    city_cols = ["AddressCity", "City"]
    zip_cols = ["AddressZip", "Zip", "ZipCode"]
    phone_cols = ["Phone", "PhoneNumber", "Phone #"]
    county_cols = ["geocode_jurisdiction_county", "geocode_address_county", "County"]
    metric_row_key = "stops"
    metric_col = f"{metric_row_key}__Total"
    metric_alias = "all_stops_total"

    if not agency_reference_geocoded.empty:
        for _, row in agency_reference_geocoded.iterrows():
            display_name = _first_non_empty(row, name_cols)
            if not display_name:
                continue
            canonical = _first_non_empty(row, ["Department", "Canonical"])
            key = canonical or display_name
            entry = names_by_key.get(key)
            if entry is None:
                agency_type = str(row.get("AgencyType") or "").strip() or None
                entry = {
                    "agency_slug": _agency_slug(canonical or display_name),
                    "canonical_name": canonical,
                    "names": [],
                    "city": None,
                    "zip": None,
                    "phone": None,
                    "county": None,
                    "agency_type": agency_type,
                    "census_geoid": _extract_jurisdiction_geoid(row),
                    metric_row_key: None,
                    metric_alias: None,
                }
                names_by_key[key] = entry
            elif not entry.get("census_geoid"):
                entry["census_geoid"] = _extract_jurisdiction_geoid(row)

            new_names = _collect_names(row, name_cols)
            for name in new_names:
                if name not in entry["names"]:
                    entry["names"].append(name)
            if not entry["city"]:
                entry["city"] = _first_non_empty(row, city_cols)
            if not entry["zip"]:
                entry["zip"] = _first_non_empty(row, zip_cols)
            if not entry["phone"]:
                entry["phone"] = _first_non_empty(row, phone_cols)
            if not entry["county"]:
                entry["county"] = _first_non_empty(row, county_cols)

    if not pivoted.empty and "agency" in pivoted.columns:
        for agency in (
            pivoted["agency"].dropna().astype(str).str.strip().replace("", pd.NA).dropna().unique()
        ):
            entry = names_by_key.get(agency)
            if entry is None:
                entry = {
                    "agency_slug": _agency_slug(agency),
                    "canonical_name": agency,
                    "names": [agency],
                    "city": None,
                    "zip": None,
                    "phone": None,
                    "county": None,
                    "agency_type": None,
                    "census_geoid": None,
                    metric_row_key: None,
                    metric_alias: None,
                }
                names_by_key[agency] = entry
            else:
                if agency not in entry["names"]:
                    entry["names"].append(agency)

    if not pivoted.empty and "agency" in pivoted.columns and metric_col in pivoted.columns:
        subset = pivoted[["agency", "year", metric_col]].copy()
        subset = subset[pd.notna(subset["agency"])]
        subset["agency"] = subset["agency"].astype(str).str.strip()
        subset = subset[subset["agency"].ne("")]
        subset["year"] = pd.to_numeric(subset["year"], errors="coerce")
        subset = subset.sort_values(["agency", "year"], ascending=[True, False])
        for agency, group in subset.groupby("agency", sort=False):
            value = None
            for item in group[metric_col].tolist():
                if _is_null(item):
                    continue
                value = _json_safe_value(item)
                break
            if value is None:
                continue
            entry = names_by_key.get(agency)
            if entry is None:
                entry = {
                    "agency_slug": _agency_slug(agency),
                    "canonical_name": agency,
                    "names": [agency],
                    "city": None,
                    "zip": None,
                    "phone": None,
                    "county": None,
                    "agency_type": None,
                    "census_geoid": None,
                    metric_row_key: None,
                    metric_alias: None,
                }
                names_by_key[agency] = entry
            entry[metric_row_key] = value
            entry[metric_alias] = value

    # Issue #6: always include the statewide aggregate entry when there is data.
    if combined is not None and not combined.empty:
        if STATEWIDE_AGENCY_NAME not in names_by_key:
            names_by_key[STATEWIDE_AGENCY_NAME] = {
                "agency_slug": STATEWIDE_AGENCY_SLUG,
                "canonical_name": STATEWIDE_AGENCY_NAME,
                "names": [STATEWIDE_AGENCY_NAME],
                "city": None,
                "zip": None,
                "phone": None,
                "county": None,
                "agency_type": None,
                "census_geoid": None,
                metric_row_key: None,
                metric_alias: None,
            }

    allowed_keys: set[str] | None = None
    if combined is not None and not combined.empty:
        value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in combined.columns]
        if value_cols and "agency" in combined.columns:
            mask = combined[value_cols].notna().any(axis=1)
            agencies = combined.loc[mask, "agency"].dropna().astype(str)
            allowed_keys = {_normalize_agency_key(a) for a in agencies if a.strip()}
            # Always keep the statewide aggregate entry even though it is synthetic.
            allowed_keys.add(_normalize_agency_key(STATEWIDE_AGENCY_NAME))

    # Stamp years_with_data / latest_year_with_data onto each entry.
    if years_by_agency:
        for entry in names_by_key.values():
            all_names = list(entry.get("names") or [])
            canonical = entry.get("canonical_name")
            if canonical and canonical not in all_names:
                all_names.append(canonical)
            years: set[int] = set()
            for name in all_names:
                for yr in years_by_agency.get(name) or []:
                    years.add(yr)
            sorted_years = sorted(years)
            entry["years_with_data"] = sorted_years
            entry["latest_year_with_data"] = sorted_years[-1] if sorted_years else None

    if program_287g_by_canonical:
        for entry in names_by_key.values():
            payload = None
            for candidate in [entry.get("canonical_name"), *(entry.get("names") or [])]:
                if candidate and candidate in program_287g_by_canonical:
                    payload = program_287g_by_canonical[candidate]
                    break
            if payload is not None:
                entry["program_287g"] = payload

    records = sorted(names_by_key.values(), key=lambda item: item["agency_slug"])
    if allowed_keys is None:
        return records
    filtered: list[dict] = []
    for record in records:
        names = record.get("names") or []
        keys = {_normalize_agency_key(record.get("canonical_name") or "")}
        keys.update({_normalize_agency_key(name) for name in names})
        if any(key and key in allowed_keys for key in keys):
            filtered.append(record)
    return filtered


def _normalize_agency_key(value: str | None) -> str:
    if value is None:
        return ""
    text = (
        str(value)
        .replace("’", "'")
        .replace("‘", "'")
        .replace("`", "'")
    )
    text = text.strip()
    text = re.sub(r"\s+'s\b", "'s", text)
    text = re.sub(r"\s{2,}", " ", text)
    return text.lower()


def write_agency_year_json(
    context,
    combined: pd.DataFrame,
    agency_reference_geocoded: pd.DataFrame,
    agency_comments: pd.DataFrame,
) -> List[str]:
    """Write one JSON file per agency per year under agency_year/{slug}/{year}.json.

    Reads rank/percentile rows from reports_with_rank_percentile.parquet (already
    canonical-key-collapsed).  One file per (agency, year); ~30–50 KB each.
    """
    del agency_reference_geocoded
    if combined.empty:
        context.log.warning("Combined DataFrame empty; no JSON outputs created.")
        return []

    out_root = Path(context.resources.data_dir_out.get_path()) / "agency_year"
    out_root.mkdir(parents=True, exist_ok=True)

    agency_meta_lookup: dict[str, pd.Series] = {}
    comments_lookup: dict[str, list[dict]] = {}
    processed_dir = Path(context.resources.data_dir_processed.get_path())
    reference_path = processed_dir / "agency_reference_geocoded.parquet"
    if not reference_path.exists():
        reference_path = processed_dir / "agency_reference.parquet"

    if reference_path.exists():
        try:
            reference_df = pd.read_parquet(reference_path)
            join_col = next(
                (c for c in ["Canonical", "Department", "Agency", "Name"] if c in reference_df.columns),
                None,
            )
            if not join_col:
                context.log.warning(
                    "agency_reference.parquet missing expected join column (Canonical/Department/Agency/Name); skipping metadata"
                )
            else:
                reference_df = reference_df.copy()
                reference_df = reference_df[pd.notna(reference_df[join_col])]
                reference_df[join_col] = reference_df[join_col].astype(str).str.strip()
                reference_df = reference_df[reference_df[join_col].ne("")]
                dup_mask = reference_df.duplicated(subset=[join_col], keep=False)
                if dup_mask.any():
                    dupe_sample = (
                        reference_df.loc[dup_mask, join_col]
                        .drop_duplicates()
                        .head(5)
                        .tolist()
                    )
                    context.log.warning(
                        "agency_reference has duplicate %s values; keeping first. Sample: %s",
                        join_col,
                        dupe_sample,
                    )
                reference_df = reference_df.drop_duplicates(subset=[join_col], keep="first")
                agency_meta_lookup = {
                    str(key): row
                    for key, row in reference_df.set_index(join_col).iterrows()
                }
                context.log.info(
                    "Loaded agency reference metadata for %d agencies from %s",
                    len(agency_meta_lookup),
                    reference_path,
                )
        except Exception as e:
            context.log.exception("Failed to load agency reference metadata: %s", e)
    else:
        context.log.info("Agency reference metadata not found at %s; skipping metadata", reference_path)

    if agency_comments is not None and not agency_comments.empty:
        try:
            for _, row in agency_comments.iterrows():
                agency = row.get("agency")
                key = _normalize_agency_key(agency)
                if not key:
                    continue
                year_val = row.get("year")
                year = int(year_val) if pd.notna(year_val) else None
                entry = {
                    "year": year,
                    "comment": _json_safe_value(row.get("comment")),
                    "has_comment": bool(row.get("has_comment")),
                    "source_url": _json_safe_value(row.get("source_url")),
                }
                comments_lookup.setdefault(key, []).append(entry)
            context.log.info("Loaded agency comments for %d agencies", len(comments_lookup))
        except Exception as exc:
            context.log.exception("Failed to process agency comments: %s", exc)

    required_cols = {
        "agency",
        "year",
        "row_key",
        "table",
        "table_id",
        "section",
        "section_id",
        "metric",
        "metric_id",
        "row_id",
    }
    missing = required_cols - set(combined.columns)
    if missing:
        raise ValueError(f"Cannot write agency JSON – missing columns: {sorted(missing)}")

    value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in combined.columns]
    pct_cols = [c for c in RACE_PCT_COLUMNS if c in combined.columns]
    ratio_cols = [RATIO_COLUMN] if RATIO_COLUMN in combined.columns else []
    rank_cols = [c for c in ["rank_dense", "rank_count", "rank_method"] if c in combined.columns]
    row_cols = [
        "year",
        "row_key",
        "table",
        "table_id",
        "section",
        "section_id",
        "metric",
        "metric_id",
        "row_id",
        *value_cols,
        *pct_cols,
        *ratio_cols,
        *rank_cols,
    ]
    row_cols = [col for col in row_cols if col in combined.columns]
    subset_cols = ["agency", *row_cols]
    working = combined[subset_cols].copy()

    output_paths: List[str] = []
    for agency, agency_group in working.groupby("agency", sort=False):
        agency_slug = _agency_slug(agency)
        slug_dir = out_root / agency_slug
        slug_dir.mkdir(parents=True, exist_ok=True)

        meta_row = agency_meta_lookup.get(str(agency).strip())
        agency_metadata = _series_to_json_dict(meta_row) if meta_row is not None else None
        all_agency_comments = comments_lookup.get(_normalize_agency_key(agency)) or []

        sorted_agency_group = agency_group.sort_values(["year", "row_key"], kind="mergesort")
        for year_val, year_group in sorted_agency_group.groupby("year", sort=True):
            year = int(year_val) if pd.notna(year_val) else None
            records: List[dict] = []
            for _, row in year_group.iterrows():
                payload = {col: row.get(col) for col in row_cols if col in row.index}
                yr = payload.get("year")
                payload["year"] = int(yr) if pd.notna(yr) else None
                records.append(_json_safe_record(payload))

            out_path = slug_dir / f"{year}.json"
            file_payload: dict = {"agency": agency, "year": year, "rows": records}
            if agency_metadata is not None:
                file_payload["agency_metadata"] = agency_metadata
            year_comments = [c for c in all_agency_comments if c.get("year") == year]
            if year_comments:
                file_payload["agency_comments"] = year_comments
            out_path.write_text(json.dumps(file_payload, indent=2))
            output_paths.append(str(out_path))

        context.log.info(
            "Wrote %d year files for %s → %s/", len(list(slug_dir.glob("*.json"))), agency, slug_dir
        )

    base_dir = Path(context.resources.data_dir_out.get_path())
    uploaded = upload_paths(
        context,
        [Path(path) for path in output_paths],
        base_dir=base_dir,
    )
    if uploaded:
        context.log.info("Uploaded %d agency-year JSON files to S3", len(uploaded))

    try:
        metadata = {"output_count": len(output_paths)}
        s3_folder = s3_uri_for_dir(context, out_root, base_dir)
        if s3_folder:
            metadata["s3_folder"] = s3_folder
        if uploaded:
            metadata["s3_paths"] = uploaded
        context.add_output_metadata(metadata)
    except Exception:
        pass

    return output_paths


def write_metric_year_json(context, combined: pd.DataFrame) -> List[str]:
    """Write one JSON file per row_key with agency/year and race columns."""
    combined = _collapse_to_canonical(combined)
    if combined.empty:
        context.log.warning("Combined DataFrame empty; no metric-year JSON outputs created.")
        return []

    required_cols = {"agency", "year", "row_key"}
    missing = required_cols - set(combined.columns)
    if missing:
        raise ValueError(f"Cannot write metric-year JSON – missing columns: {sorted(missing)}")

    value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in combined.columns]
    if not value_cols:
        raise ValueError("Cannot write metric-year JSON – no numeric value columns were found.")

    out_root = Path(context.resources.data_dir_out.get_path()) / "metric_year"
    out_root.mkdir(parents=True, exist_ok=True)

    output_paths: List[str] = []
    row_key_count = 0
    row_count = 0

    for row_key, group in combined.groupby("row_key"):
        if pd.isna(row_key) or str(row_key).strip() == "":
            continue
        row_key_count += 1
        records: List[dict] = []
        sorted_group = group.sort_values(["agency", "year"], na_position="last")
        for _, row in sorted_group.iterrows():
            agency = row.get("agency")
            year_val = row.get("year")
            year = int(year_val) if pd.notna(year_val) else None
            payload = {
                "agency": _json_safe_value(agency),
                "year": year,
            }
            for col in value_cols:
                payload[col] = _json_safe_value(row.get(col))
            records.append(payload)
        row_count += len(records)
        payload = {"row_key": str(row_key), "rows": records}
        out_path = out_root / f"{row_key}.json"
        out_path.write_text(json.dumps(payload, indent=2))
        output_paths.append(str(out_path))
        context.log.info("Wrote metric-year JSON → %s (%d rows)", out_path, len(records))

    try:
        context.add_output_metadata({
            "output_count": len(output_paths),
            "row_key_count": row_key_count,
            "row_count": row_count,
        })
    except Exception:
        pass

    return output_paths


def write_metric_year_subset_json(context, combined: pd.DataFrame) -> str:
    """Write a compact JSON file with selected row_keys across agencies/years."""
    combined = _collapse_to_canonical(combined)
    if combined.empty:
        context.log.warning("Combined DataFrame empty; no metric-year subset JSON created.")
        return ""

    required_cols = {"agency", "year", "row_key"}
    missing = required_cols - set(combined.columns)
    if missing:
        raise ValueError(f"Cannot write metric-year subset JSON – missing columns: {sorted(missing)}")

    value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in combined.columns]
    if not value_cols:
        raise ValueError("Cannot write metric-year subset JSON – no numeric value columns were found.")

    out_root = Path(context.resources.data_dir_out.get_path())
    out_root.mkdir(parents=True, exist_ok=True)
    out_path = out_root / "metric_year_subset.json"

    # Only emit canonical metrics — rows whose canonical_key is null are
    # era-specific or pre-canonical leftovers (OCR-mangled slugs, deprecated
    # row_keys) and never appear in manifest.canonical_metrics.
    canonical_only = combined
    if "canonical_key" in combined.columns:
        canonical_only = combined[combined["canonical_key"].notna()]
    all_keys = (
        canonical_only["row_key"]
        .dropna()
        .astype(str)
        .loc[lambda s: s.str.strip() != ""]
        .unique()
        .tolist()
    )
    subset_keys = sorted(
        k for k in all_keys
        if not k.startswith(METRIC_YEAR_SUBSET_EXCLUDE_PREFIXES)
    )

    subset = combined[combined["row_key"].isin(subset_keys)].copy()
    subset = subset.sort_values(["row_key", "agency", "year"], na_position="last")

    agencies = (
        subset["agency"]
        .dropna()
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )
    years = (
        pd.to_numeric(subset["year"], errors="coerce")
        .dropna()
        .astype(int)
        .unique()
        .tolist()
    )
    agencies = sorted(agencies)
    years = sorted(years)
    agency_index = {name: idx for idx, name in enumerate(agencies)}
    year_index = {year: idx for idx, year in enumerate(years)}

    rows_by_key: dict[str, list[list]] = {}
    total_rows = 0
    for key in subset_keys:
        group = subset[subset["row_key"] == key]
        rows: List[list] = []
        for _, row in group.iterrows():
            agency = row.get("agency")
            agency_key = str(agency).strip() if pd.notna(agency) else None
            if not agency_key or agency_key not in agency_index:
                continue
            year_val = row.get("year")
            if pd.isna(year_val):
                continue
            try:
                year_val = int(year_val)
            except (TypeError, ValueError):
                continue
            if year_val not in year_index:
                continue
            entry = [agency_index[agency_key], year_index[year_val]]
            for col in value_cols:
                entry.append(_json_safe_value(row.get(col)))
            rows.append(entry)
        total_rows += len(rows)
        rows_by_key[key] = rows

    columns = ["agency_idx", "year_idx", *value_cols]
    payload = {
        "agencies": agencies,
        "years": years,
        "columns": columns,
        "rows": rows_by_key,
    }
    out_path.write_text(json.dumps(payload, separators=(",", ":")))
    context.log.info("Wrote metric-year subset JSON → %s (%d rows)", out_path, total_rows)

    split_dir = out_root / "metric_year_subset"
    split_dir.mkdir(parents=True, exist_ok=True)
    # Remove any stale per-key files from a prior run so deletions propagate.
    for stale in split_dir.glob("*.json"):
        stale.unlink()
    index_payload = {
        "agencies": agencies,
        "years": years,
        "columns": columns,
        "row_keys": list(subset_keys),
    }
    (split_dir / "_index.json").write_text(json.dumps(index_payload, separators=(",", ":")))
    for key, rows in rows_by_key.items():
        per_key_payload = {"row_key": key, "rows": rows}
        (split_dir / f"{key}.json").write_text(json.dumps(per_key_payload, separators=(",", ":")))
    context.log.info(
        "Wrote metric-year subset split files → %s (%d per-key files)",
        split_dir,
        len(rows_by_key),
    )

    # Per-year combined slice for the scatter use-case: one file per year with
    # all metrics rolled up. agency_idx is col 0; year_idx is implicit.
    by_year_dir = split_dir / "by_year"
    by_year_dir.mkdir(parents=True, exist_ok=True)
    for stale in by_year_dir.glob("*.json"):
        stale.unlink()
    by_year_columns = ["agency_idx", *value_cols]
    rows_by_year_key: dict[int, dict[str, list[list]]] = {
        year: {key: [] for key in subset_keys} for year in years
    }
    for key, rows in rows_by_key.items():
        for entry in rows:
            year = years[entry[1]]
            rows_by_year_key[year][key].append([entry[0], *entry[2:]])
    for year in years:
        per_year_payload = {
            "year": year,
            "agencies": agencies,
            "columns": by_year_columns,
            "rows": rows_by_year_key[year],
        }
        (by_year_dir / f"{year}.json").write_text(json.dumps(per_year_payload, separators=(",", ":")))
    context.log.info(
        "Wrote metric-year subset by-year files → %s (%d year files)",
        by_year_dir,
        len(years),
    )

    base_dir = Path(context.resources.data_dir_out.get_path())
    uploaded = upload_paths(
        context,
        [out_path],
        base_dir=base_dir,
    )
    if uploaded:
        context.log.info("Uploaded metric-year subset JSON to S3")

    try:
        metadata = {
            "local_path": str(out_path),
            "row_key_count": len(subset_keys),
            "row_count": total_rows,
        }
        s3_path = s3_uri_for_path(context, out_path, base_dir)
        if s3_path:
            metadata["s3_path"] = s3_path
        context.add_output_metadata(metadata)
    except Exception:
        pass

    return str(out_path)


def _collect_names(row: pd.Series, name_cols: list[str]) -> list[str]:
    seen = set()
    names: list[str] = []
    for col in name_cols:
        if col not in row:
            continue
        value = row.get(col)
        if _is_null(value):
            continue
        text = str(value).strip()
        if not text or text.lower() == "nan":
            continue
        if text not in seen:
            seen.add(text)
            names.append(text)
    return names


def _first_non_empty(row: pd.Series, columns: list[str]) -> str | None:
    for col in columns:
        if col not in row:
            continue
        value = row.get(col)
        if _is_null(value):
            continue
        text = str(value).strip()
        if text and text.lower() != "nan":
            return text
    return None


def write_agency_index_json(
    context,
    pivoted: pd.DataFrame,
    agency_reference_geocoded: pd.DataFrame,
    combined: pd.DataFrame | None = None,
    years_by_agency: dict[str, list[int]] | None = None,
    program_287g_by_canonical: dict[str, dict] | None = None,
) -> str:
    """Write a JSON index of agencies for search/discovery."""
    out_root = Path(context.resources.data_dir_out.get_path())
    out_root.mkdir(parents=True, exist_ok=True)
    out_path = out_root / "agency_index.json"

    records = build_agency_index_records(
        pivoted,
        agency_reference_geocoded,
        combined=combined,
        years_by_agency=years_by_agency,
        program_287g_by_canonical=program_287g_by_canonical,
    )
    out_path.write_text(json.dumps(records, indent=2))
    context.log.info("Wrote agency index JSON → %s (%d rows)", out_path, len(records))
    base_dir = Path(context.resources.data_dir_out.get_path())
    uploaded = upload_paths(
        context,
        [out_path],
        base_dir=base_dir,
    )
    if uploaded:
        context.log.info("Uploaded agency index JSON to S3")
    try:
        metadata = {"local_path": str(out_path), "row_count": len(records)}
        s3_path = s3_uri_for_path(context, out_path, base_dir)
        if s3_path:
            metadata["s3_path"] = s3_path
        context.add_output_metadata(metadata)
    except Exception:
        pass
    return str(out_path)


def write_statewide_baselines_json(context, baselines: pd.DataFrame) -> str:
    """Write statewide baselines to JSON for downstream consumers."""
    out_root = Path(context.resources.data_dir_out.get_path())
    out_root.mkdir(parents=True, exist_ok=True)
    out_path = out_root / "statewide_slug_baselines.json"

    records = [_json_safe_record(item) for item in baselines.to_dict(orient="records")]
    out_path.write_text(json.dumps(records, indent=2))
    context.log.info("Wrote statewide baselines JSON → %s (%d rows)", out_path, len(records))
    base_dir = Path(context.resources.data_dir_out.get_path())
    uploaded = upload_paths(
        context,
        [out_path],
        base_dir=base_dir,
    )
    if uploaded:
        context.log.info("Uploaded statewide baselines JSON to S3")
    try:
        metadata = {"local_path": str(out_path), "row_count": len(records)}
        s3_path = s3_uri_for_path(context, out_path, base_dir)
        if s3_path:
            metadata["s3_path"] = s3_path
        context.add_output_metadata(metadata)
    except Exception:
        pass
    return str(out_path)


def _write_download_bundle(
    context,
    df: pd.DataFrame,
    *,
    base_name: str,
    out_dir: Path,
    prefix_override: str | None = None,
) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    prefix = prefix_override if prefix_override is not None else DOWNLOAD_PREFIX
    parquet_path = out_dir / f"{prefix}{base_name}.parquet"
    csv_path = out_dir / f"{prefix}{base_name}.csv"
    legacy_json = out_dir / f"{prefix}{base_name}.json"

    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    df.to_csv(csv_path, index=False)
    if legacy_json.exists():
        try:
            legacy_json.unlink()
        except OSError:
            pass

    s3_meta = {}
    try:
        s3_meta = upload_paths(
            context,
            [parquet_path, csv_path],
            base_dir=out_dir,
            prefix_override="downloads",
        )
    except Exception:
        pass

    return {
        "parquet_path": str(parquet_path),
        "csv_path": str(csv_path),
        "s3_paths": s3_meta or [],
    }


def _combine_download_parquet(named_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for name, df in named_frames.items():
        if df.empty:
            continue
        frame = df.copy()
        frame["dataset"] = name
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def write_statewide_year_sums_json(context, combined: pd.DataFrame) -> str:
    """Write statewide per-year sums for each row_key and race column."""
    combined = _collapse_to_canonical(combined)
    if combined.empty:
        context.log.warning("Combined DataFrame empty; no statewide sums JSON created.")
        return ""

    required_cols = {"year", "row_key"}
    missing = required_cols - set(combined.columns)
    if missing:
        raise ValueError(f"Cannot write statewide sums JSON – missing columns: {sorted(missing)}")

    value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in combined.columns]
    if not value_cols:
        raise ValueError("Cannot write statewide sums JSON – no numeric value columns were found.")

    subset_all = combined[["year", "row_key", "agency", *value_cols]].copy()
    for col in value_cols:
        subset_all[col] = pd.to_numeric(subset_all[col], errors="coerce")

    grouped_all = (
        subset_all.groupby(["year", "row_key"], dropna=True)[value_cols]
        .sum(min_count=1)
        .reset_index()
    )
    subset_no_mshp_all = subset_all[subset_all["agency"].ne(MSHP_DEPARTMENT_NAME)].copy()
    grouped_no_mshp = (
        subset_no_mshp_all.groupby(["year", "row_key"], dropna=True)[value_cols]
        .sum(min_count=1)
        .reset_index()
    )

    subset = grouped_all.copy()
    if "row_key" in subset.columns:
        rate_mask = subset["row_key"].astype(str).str.endswith("-rate", na=False)
        if rate_mask.any():
            subset = subset.loc[~rate_mask].copy()
        pop_mask = subset["row_key"].astype(str).str.startswith(
            "rates-by-race--population", na=False
        )
        if pop_mask.any():
            subset = subset.loc[~pop_mask].copy()

    derived = _compute_statewide_rates(grouped_all, value_cols)
    if not derived.empty:
        subset = pd.concat([subset, derived], ignore_index=True)

    subset_no_mshp = grouped_no_mshp.copy()
    if "row_key" in subset_no_mshp.columns:
        rate_mask = subset_no_mshp["row_key"].astype(str).str.endswith("-rate", na=False)
        if rate_mask.any():
            subset_no_mshp = subset_no_mshp.loc[~rate_mask].copy()
        pop_mask = subset_no_mshp["row_key"].astype(str).str.startswith(
            "rates-by-race--population", na=False
        )
        if pop_mask.any():
            subset_no_mshp = subset_no_mshp.loc[~pop_mask].copy()
    derived_no_mshp = _compute_statewide_rates(grouped_no_mshp, value_cols)
    if not derived_no_mshp.empty:
        subset_no_mshp = pd.concat([subset_no_mshp, derived_no_mshp], ignore_index=True)
    if not subset_no_mshp.empty:
        subset_no_mshp["row_key"] = "no-mshp--" + subset_no_mshp["row_key"].astype(str)

    subset_avg_no_mshp = subset_no_mshp_all.copy()
    if "row_key" in subset_avg_no_mshp.columns:
        rate_mask = subset_avg_no_mshp["row_key"].astype(str).str.endswith("-rate", na=False)
        if rate_mask.any():
            subset_avg_no_mshp = subset_avg_no_mshp.loc[~rate_mask].copy()
        pop_mask = subset_avg_no_mshp["row_key"].astype(str).str.startswith(
            "rates-by-race--population", na=False
        )
        if pop_mask.any():
            subset_avg_no_mshp = subset_avg_no_mshp.loc[~pop_mask].copy()
    avg_no_mshp = (
        subset_avg_no_mshp.groupby(["year", "row_key"], dropna=True)[value_cols]
        .mean()
        .reset_index()
    )

    def _derived_rates_avg_from_agency(
        agency_df: pd.DataFrame,
    ) -> pd.DataFrame:
        derived_rows: list[dict] = []
        for spec in STATEWIDE_RATE_SPECS:
            num_key = spec["numerator"]
            den_key = spec["denominator"]
            num_df = agency_df[agency_df["row_key"] == num_key][
                ["agency", "year", *value_cols]
            ]
            den_df = agency_df[agency_df["row_key"] == den_key][
                ["agency", "year", *value_cols]
            ]
            if num_df.empty or den_df.empty:
                continue
            merged = num_df.merge(den_df, on=["agency", "year"], suffixes=("_num", "_den"))
            if merged.empty:
                continue
            rate_records: list[dict] = []
            for _, row in merged.iterrows():
                record = {"year": row["year"]}
                for col in value_cols:
                    num = row.get(f"{col}_num")
                    den = row.get(f"{col}_den")
                    if pd.isna(num) or pd.isna(den) or den == 0:
                        record[col] = pd.NA
                    else:
                        record[col] = num / den
                rate_records.append(record)
            rates_df = pd.DataFrame(rate_records)
            if rates_df.empty:
                continue
            grouped = rates_df.groupby("year")[value_cols].mean().reset_index()
            grouped["row_key"] = spec["row_key"]
            derived_rows.append(grouped)
        if not derived_rows:
            return pd.DataFrame(columns=["year", "row_key", *value_cols])
        return pd.concat(derived_rows, ignore_index=True)

    derived_avg_no_mshp = _derived_rates_avg_from_agency(subset_no_mshp_all)
    if not derived_avg_no_mshp.empty:
        avg_no_mshp = pd.concat([avg_no_mshp, derived_avg_no_mshp], ignore_index=True)
    if not avg_no_mshp.empty:
        avg_no_mshp["row_key"] = "avg-no-mshp--" + avg_no_mshp["row_key"].astype(str)

    if not subset_no_mshp.empty:
        subset = pd.concat([subset, subset_no_mshp], ignore_index=True)
    if not avg_no_mshp.empty:
        subset = pd.concat([subset, avg_no_mshp], ignore_index=True)

    subset = subset.sort_values(["year", "row_key"], na_position="last")

    records = [_json_safe_record(item) for item in subset.to_dict(orient="records")]

    out_root = Path(context.resources.data_dir_out.get_path())
    out_root.mkdir(parents=True, exist_ok=True)
    out_path = out_root / "statewide_year_sums.json"
    out_path.write_text(json.dumps(records, indent=2))
    context.log.info("Wrote statewide sums JSON → %s (%d rows)", out_path, len(records))

    base_dir = Path(context.resources.data_dir_out.get_path())
    uploaded = upload_paths(context, [out_path], base_dir=base_dir)
    if uploaded:
        context.log.info("Uploaded statewide sums JSON to S3")

    try:
        metadata = {
            "local_path": str(out_path),
            "row_count": len(records),
            "row_key_count": int(subset["row_key"].nunique(dropna=True)),
            "year_count": int(subset["year"].nunique(dropna=True)),
        }
        s3_path = s3_uri_for_path(context, out_path, base_dir)
        if s3_path:
            metadata["s3_path"] = s3_path
        context.add_output_metadata(metadata)
    except Exception:
        pass

    return str(out_path)


def write_statewide_year_sums_subset_json(context, combined: pd.DataFrame) -> str:
    """Write a slimmed-down statewide sums JSON for selected row_keys."""
    combined = _collapse_to_canonical(combined)
    if combined.empty:
        context.log.warning("Combined DataFrame empty; no statewide sums subset created.")
        return ""

    required_cols = {"year", "row_key"}
    missing = required_cols - set(combined.columns)
    if missing:
        raise ValueError(f"Cannot write statewide sums subset – missing columns: {sorted(missing)}")

    value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in combined.columns]
    if not value_cols:
        raise ValueError("Cannot write statewide sums subset – no numeric value columns were found.")

    subset = combined[
        combined["row_key"].isin(STATEWIDE_SUMS_SUBSET_KEYS)
    ][["year", "row_key", *value_cols]].copy()

    if subset.empty:
        context.log.warning("No rows found for statewide sums subset; skipping output.")
        return ""

    for col in value_cols:
        subset[col] = pd.to_numeric(subset[col], errors="coerce")

    grouped = (
        subset.groupby(["year", "row_key"], dropna=True)[value_cols]
        .sum(min_count=1)
        .reset_index()
    )

    records = [_json_safe_record(item) for item in grouped.to_dict(orient="records")]
    out_root = Path(context.resources.data_dir_out.get_path())
    out_root.mkdir(parents=True, exist_ok=True)
    out_path = out_root / "statewide_year_sums_subset.json"
    out_path.write_text(json.dumps(records, indent=2))
    context.log.info(
        "Wrote statewide sums subset JSON → %s (%d rows)", out_path, len(records)
    )

    base_dir = Path(context.resources.data_dir_out.get_path())
    uploaded = upload_paths(context, [out_path], base_dir=base_dir)
    if uploaded:
        context.log.info("Uploaded statewide sums subset JSON to S3")

    try:
        metadata = {
            "local_path": str(out_path),
            "row_count": len(records),
            "row_key_count": int(grouped["row_key"].nunique(dropna=True)),
            "year_count": int(grouped["year"].nunique(dropna=True)),
        }
        s3_path = s3_uri_for_path(context, out_path, base_dir)
        if s3_path:
            metadata["s3_path"] = s3_path
        context.add_output_metadata(metadata)
    except Exception:
        pass
    return str(out_path)


def write_homepage_stats_json(context, combined: pd.DataFrame) -> str:
    """Write homepage stats for the latest report year."""
    combined = _collapse_to_canonical(combined)
    if combined.empty:
        context.log.warning("Combined DataFrame empty; no homepage stats created.")
        return ""

    value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in combined.columns]
    if not value_cols:
        raise ValueError("Cannot write homepage stats – no numeric value columns were found.")

    subset = combined[
        (combined["year"] == HOMEPAGE_STATS_YEAR)
        & (combined["row_key"].isin(HOMEPAGE_STATS_METRICS.values()))
    ].copy()
    if subset.empty:
        context.log.warning(
            "No rows found for homepage stats (year=%s).",
            HOMEPAGE_STATS_YEAR,
        )
        return ""

    for col in value_cols:
        subset[col] = pd.to_numeric(subset[col], errors="coerce")

    year_data = {name: {race: 0.0 for race in value_cols} for name in HOMEPAGE_STATS_METRICS}
    agency_count = 0
    all_stops_key = HOMEPAGE_STATS_METRICS["all_stops"]
    all_stops_rows = subset[subset["row_key"] == all_stops_key]
    if not all_stops_rows.empty:
        agency_count = all_stops_rows[pd.notna(all_stops_rows["Total"])]["agency"].nunique()

    for metric_name, row_key in HOMEPAGE_STATS_METRICS.items():
        metric_rows = subset[subset["row_key"] == row_key]
        if metric_rows.empty:
            continue
        sums = metric_rows[value_cols].sum(axis=0, skipna=True)
        for race in value_cols:
            year_data[metric_name][race] = float(sums.get(race) or 0.0)

    total_stops = year_data["all_stops"].get("Total", 0.0) or 0.0
    total_searches = year_data["searches"].get("Total", 0.0) or 0.0
    total_contraband = year_data["contraband"].get("Total", 0.0) or 0.0
    total_citations = year_data["citations"].get("Total", 0.0) or 0.0
    total_arrests = year_data["arrests"].get("Total", 0.0) or 0.0
    total_warnings = year_data["warnings"].get("Total", 0.0) or 0.0

    def _rate(numer: float, denom: float) -> float:
        if denom <= 0:
            return 0.0
        return float(numer / denom * 100.0)

    output_data = {
        "year": HOMEPAGE_STATS_YEAR,
        "total_stops": total_stops,
        "agency_count": agency_count,
        "by_race": {key: dict(values) for key, values in year_data.items()},
        "summary": {
            "search_rate": _rate(total_searches, total_stops),
            "hit_rate": _rate(total_contraband, total_searches),
            "citation_rate": _rate(total_citations, total_stops),
            "arrest_rate": _rate(total_arrests, total_stops),
            "warning_rate": _rate(total_warnings, total_stops),
        },
    }

    out_path = Path(context.resources.data_dir_out.get_path()) / f"homepage_{HOMEPAGE_STATS_YEAR}_stats.json"
    out_path.write_text(json.dumps(output_data, indent=2))
    context.log.info("Wrote homepage stats JSON → %s", out_path)

    base_dir = Path(context.resources.data_dir_out.get_path())
    uploaded = upload_paths(context, [out_path], base_dir=base_dir)
    if uploaded:
        context.log.info("Uploaded homepage stats JSON to S3")

    try:
        metadata = {
            "local_path": str(out_path),
            "year": HOMEPAGE_STATS_YEAR,
            "agency_count": agency_count,
        }
        s3_path = s3_uri_for_path(context, out_path, base_dir)
        if s3_path:
            metadata["s3_path"] = s3_path
        context.add_output_metadata(metadata)
    except Exception:
        pass
    return str(out_path)


def write_report_dimension_index_json(context, combined: pd.DataFrame) -> str:
    """Write unique table/section/metric identifiers to JSON for translations."""
    combined = _collapse_to_canonical(combined)
    out_root = Path(context.resources.data_dir_out.get_path())
    out_root.mkdir(parents=True, exist_ok=True)
    out_path = out_root / "report_dimensions.json"

    if combined.empty:
        payload = {"table_ids": [], "section_ids": [], "metric_ids": []}
    else:
        table_ids = (
            combined["table_id"]
            .dropna()
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .dropna()
            .unique()
            .tolist()
        )
        section_ids = (
            combined["section_id"]
            .dropna()
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .dropna()
            .unique()
            .tolist()
        )
        metric_ids = (
            combined["metric_id"]
            .dropna()
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .dropna()
            .unique()
            .tolist()
        )
        payload = {
            "table_ids": sorted(table_ids),
            "section_ids": sorted(section_ids),
            "metric_ids": sorted(metric_ids),
        }

    out_path.write_text(json.dumps(payload, indent=2))
    context.log.info(
        "Wrote report dimension index JSON → %s (tables=%d sections=%d metrics=%d)",
        out_path,
        len(payload["table_ids"]),
        len(payload["section_ids"]),
        len(payload["metric_ids"]),
    )
    base_dir = Path(context.resources.data_dir_out.get_path())
    uploaded = upload_paths(
        context,
        [out_path],
        base_dir=base_dir,
    )
    if uploaded:
        context.log.info("Uploaded report dimension index JSON to S3")
    try:
        metadata = {
            "local_path": str(out_path),
            "table_id_count": len(payload["table_ids"]),
            "section_id_count": len(payload["section_ids"]),
            "metric_id_count": len(payload["metric_ids"]),
        }
        s3_path = s3_uri_for_path(context, out_path, base_dir)
        if s3_path:
            metadata["s3_path"] = s3_path
        context.add_output_metadata(metadata)
    except Exception:
        pass
    return str(out_path)


def _write_agency_year_for_year(
    yr: int,
    parquet_path: str,
    out_root: Path,
    row_cols: list,
    agency_meta_lookup: dict,
    comments_lookup: dict,
) -> tuple[int, list[str]]:
    """Write all per-agency JSON files for a single year. Streams tuples from DuckDB — no pandas."""
    # Select only needed columns; ORDER BY agency so we can stream agency groups.
    needed = ["agency"] + [c for c in row_cols if c != "agency"]
    col_sql = ", ".join(f'"{c}"' for c in needed)
    row_cols_set = set(row_cols)

    con = duckdb.connect()
    cur = con.execute(
        f"SELECT {col_sql} FROM read_parquet('{parquet_path}') "
        f"WHERE year = {yr} ORDER BY agency, row_key"
    )
    col_names = [desc[0] for desc in cur.description]
    agency_idx = col_names.index("agency")

    def _flush(agency: str, rows: list) -> str:
        agency_slug = _agency_slug(agency)
        slug_dir = out_root / agency_slug
        records = []
        for row in rows:
            rec = {}
            for col, val in zip(col_names, row):
                if col == "agency" or col not in row_cols_set:
                    continue
                rec[col] = _json_safe_value(val)
            records.append(rec)
        payload: dict = {"agency": agency, "year": yr, "rows": records}
        meta_row = agency_meta_lookup.get(str(agency).strip())
        if meta_row is not None:
            payload["agency_metadata"] = _series_to_json_dict(meta_row)
        year_comments = [
            c for c in (comments_lookup.get(_normalize_agency_key(agency)) or [])
            if c.get("year") == yr
        ]
        if year_comments:
            payload["agency_comments"] = year_comments
        out_path = slug_dir / f"{yr}.json"
        out_path.write_text(json.dumps(payload, separators=(",", ":")))
        return str(out_path)

    paths: list[str] = []
    current_agency: str | None = None
    current_rows: list = []
    while True:
        batch = cur.fetchmany(2000)
        if not batch:
            if current_agency is not None:
                paths.append(_flush(current_agency, current_rows))
            break
        for row in batch:
            agency = row[agency_idx]
            if agency != current_agency:
                if current_agency is not None:
                    paths.append(_flush(current_agency, current_rows))
                current_agency = agency
                current_rows = []
            current_rows.append(row)

    con.close()
    return yr, paths


@asset(
    name="agency_year_json_exports",
    group_name="dist",
    deps=[AssetKey("reports_with_rank_percentile")],
    required_resource_keys={"data_dir_processed", "data_dir_out", "s3"},
    description="Generate per-agency per-year JSON files (dist/agency_year/{slug}/{year}.json) from rank/percentile Parquet.",
)
def agency_year_json_exports(context) -> List[str]:
    """Process years in parallel (ThreadPoolExecutor); each thread owns its DuckDB connection."""
    processed_dir = Path(context.resources.data_dir_processed.get_path())
    parquet_path = str(processed_dir / "reports_with_rank_percentile.parquet")

    # Load small reference data once (shared read-only across threads).
    agency_meta_lookup: dict = {}
    for ref_name in ("agency_reference_geocoded.parquet", "agency_reference.parquet"):
        ref_path = processed_dir / ref_name
        if ref_path.exists():
            try:
                ref_df = pd.read_parquet(ref_path)
                join_col = next(
                    (c for c in ["Canonical", "Department", "Agency", "Name"] if c in ref_df.columns),
                    None,
                )
                if join_col:
                    ref_df = ref_df[pd.notna(ref_df[join_col])].copy()
                    ref_df[join_col] = ref_df[join_col].astype(str).str.strip()
                    ref_df = ref_df.drop_duplicates(subset=[join_col], keep="first")
                    agency_meta_lookup = {
                        str(k): row for k, row in ref_df.set_index(join_col).iterrows()
                    }
            except Exception as exc:
                context.log.exception("Failed to load agency reference: %s", exc)
            break

    comments_lookup: dict = {}
    comments_path = processed_dir / "agency_comments.parquet"
    if comments_path.exists():
        try:
            for _, row in pd.read_parquet(comments_path).iterrows():
                key = _normalize_agency_key(row.get("agency"))
                if not key:
                    continue
                yr_val = row.get("year")
                comments_lookup.setdefault(key, []).append({
                    "year": int(yr_val) if pd.notna(yr_val) else None,
                    "comment": _json_safe_value(row.get("comment")),
                    "has_comment": bool(row.get("has_comment")),
                    "source_url": _json_safe_value(row.get("source_url")),
                })
        except Exception as exc:
            context.log.exception("Failed to load agency comments: %s", exc)

    # Determine column layout and enumerate years + agencies in one DuckDB pass.
    con = duckdb.connect()
    schema_df = con.execute(f"SELECT * FROM read_parquet('{parquet_path}') LIMIT 0").df()
    years = sorted(
        int(r[0]) for r in
        con.execute(f"SELECT DISTINCT year FROM read_parquet('{parquet_path}') WHERE year IS NOT NULL AND year >= {DIST_MIN_YEAR}").fetchall()
    )
    agency_slugs = [
        _agency_slug(r[0]) for r in
        con.execute(f"SELECT DISTINCT agency FROM read_parquet('{parquet_path}') WHERE agency IS NOT NULL").fetchall()
    ]
    con.close()

    value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in schema_df.columns]
    pct_cols = [c for c in RACE_PCT_COLUMNS if c in schema_df.columns]
    ratio_cols = [RATIO_COLUMN] if RATIO_COLUMN in schema_df.columns else []
    rank_cols = [c for c in ["rank_dense", "rank_count", "rank_method"] if c in schema_df.columns]
    row_cols = [
        c for c in [
            "year", "row_key", "table", "table_id", "section", "section_id",
            "metric", "metric_id", "row_id",
            *value_cols, *pct_cols, *ratio_cols, *rank_cols,
        ] if c in schema_df.columns
    ]

    out_root = Path(context.resources.data_dir_out.get_path()) / "agency_year"
    out_root.mkdir(parents=True, exist_ok=True)

    # Pre-create all per-agency slug directories before the thread pool starts.
    # This removes 897 mkdir syscalls from the per-thread hot path and eliminates
    # any race between concurrent threads trying to create the same directory.
    for slug in agency_slugs:
        (out_root / slug).mkdir(exist_ok=True)
    context.log.info("Pre-created %d agency slug directories", len(agency_slugs))

    # Each thread streams tuples (no pandas), so memory per worker is minimal.
    workers = min(len(years), os.cpu_count() or 4)
    output_paths: List[str] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _write_agency_year_for_year,
                yr, parquet_path, out_root, row_cols, agency_meta_lookup, comments_lookup,
            ): yr
            for yr in years
        }
        for future in as_completed(futures):
            yr, year_paths = future.result()
            output_paths.extend(year_paths)
            context.log.info("Wrote year %d: %d agency files", yr, len(year_paths))

    try:
        context.add_output_metadata({"output_count": len(output_paths)})
    except Exception:
        pass
    return output_paths


def write_statewide_agency_json(context, combined: pd.DataFrame) -> str:
    """Write an agency-year-format JSON for the Missouri (all agencies) aggregate."""
    combined = _collapse_to_canonical(combined)
    if combined.empty:
        context.log.warning("Combined DataFrame is empty; skipping statewide agency JSON.")
        return ""

    value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in combined.columns]
    if not value_cols:
        raise ValueError("Cannot write statewide agency JSON – no numeric value columns found.")

    # Strip any derived rows (rank/percentile/percentage) that may be present.
    base_mask = ~combined["row_key"].astype(str).str.contains(
        r"-(?:rank|percentile|percentage)$", regex=True, na=False
    )
    base = combined.loc[base_mask].copy()
    for col in value_cols:
        base[col] = pd.to_numeric(base[col], errors="coerce")

    # Exclude rows that are themselves computed rates or population denominators;
    # rates will be re-derived from the statewide sums.
    exclude_mask = base["row_key"].astype(str).str.endswith("-rate", na=False) | base[
        "row_key"
    ].astype(str).str.startswith("rates-by-race--population", na=False)
    count_base = base.loc[~exclude_mask]

    grouped = (
        count_base.groupby(["year", "row_key"], dropna=True)[value_cols]
        .sum(min_count=1)
        .reset_index()
    )

    derived_rates = _compute_statewide_rates(grouped, value_cols)
    all_rows = (
        pd.concat([grouped, derived_rates], ignore_index=True)
        if not derived_rates.empty
        else grouped
    )

    # Metadata lookup: table/section/metric etc. are the same across all agencies.
    meta_cols = ["row_key", "table", "table_id", "section", "section_id", "metric", "metric_id"]
    present_meta_cols = [c for c in meta_cols if c in combined.columns]
    meta_lookup: dict = {}
    if len(present_meta_cols) > 1:
        for _, row in (
            base[present_meta_cols].drop_duplicates("row_key", keep="first").iterrows()
        ):
            rk = str(row["row_key"])
            meta_lookup[rk] = {
                c: _json_safe_value(row.get(c)) for c in present_meta_cols if c != "row_key"
            }

    records: List[dict] = []
    for _, row in all_rows.sort_values(["year", "row_key"]).iterrows():
        rk = str(row["row_key"])
        year_val = row.get("year")
        year = int(year_val) if pd.notna(year_val) else None
        record: dict = {"year": year, "row_key": rk}
        for mc, mv in (meta_lookup.get(rk) or {}).items():
            record[mc] = mv
        year_str = str(year) if year is not None else ""
        record["row_id"] = f"{year_str}-{STATEWIDE_AGENCY_SLUG}-{rk}"
        for col in value_cols:
            record[col] = _json_safe_value(row.get(col))
        records.append(record)

    out_root = Path(context.resources.data_dir_out.get_path()) / "agency_year"
    out_root.mkdir(parents=True, exist_ok=True)
    out_path = out_root / f"{STATEWIDE_AGENCY_SLUG}.json"
    out_path.write_text(json.dumps({"agency": STATEWIDE_AGENCY_NAME, "rows": records}, indent=2))
    context.log.info("Wrote statewide agency JSON → %s (%d rows)", out_path, len(records))

    base_dir = Path(context.resources.data_dir_out.get_path())
    uploaded = upload_paths(context, [out_path], base_dir=base_dir)
    try:
        meta: dict = {"local_path": str(out_path), "row_count": len(records)}
        s3_path = s3_uri_for_path(context, out_path, base_dir)
        if s3_path:
            meta["s3_path"] = s3_path
        if uploaded:
            meta["s3_paths"] = uploaded
        context.add_output_metadata(meta)
    except Exception:
        pass

    return str(out_path)


@asset(
    name="canonical_combined_parquet",
    group_name="processed",
    deps=[AssetKey("combine_all_reports")],
    required_resource_keys={"data_dir_processed"},
    description="Run canonical deduplication window function once; write result to parquet for dist assets.",
)
def canonical_combined_parquet(context) -> str:
    processed_dir = Path(context.resources.data_dir_processed.get_path())
    out_path = processed_dir / "canonical_combined.parquet"
    con = _open_canonical_db(str(processed_dir / "all_combined_output.parquet"))
    con.execute(f"COPY (SELECT * FROM canonical_combined WHERE year >= {DIST_MIN_YEAR}) TO '{out_path}' (FORMAT PARQUET)")
    con.close()

    df = pd.read_parquet(out_path)

    statewide_rows = _build_statewide_canonical_rows(df)
    if not statewide_rows.empty:
        df = pd.concat([df, statewide_rows], ignore_index=True)
        context.log.info("Appended %d statewide rows to canonical combined", len(statewide_rows))

    # Clip rate values to 100% for dist outputs (bad source data can produce >100% rates).
    # Downloads keep the raw computed values.
    race_cols = [c for c in PIVOT_VALUE_COLUMNS if c != "Total"]
    all_val_cols = ["Total"] + race_cols
    rate_mask = df["canonical_key"].str.endswith("-rate", na=False)
    for col in all_val_cols:
        if col in df.columns:
            df.loc[rate_mask, col] = pd.to_numeric(df.loc[rate_mask, col], errors="coerce").clip(upper=100)
    df.to_parquet(out_path, index=False)

    row_count = len(df)
    context.log.info("Wrote canonical combined Parquet → %s (%d rows)", out_path, row_count)
    try:
        context.add_output_metadata({"local_path": str(out_path), "row_count": int(row_count)})
    except Exception:
        pass
    return str(out_path)


@asset(
    name="statewide_agency_json_export",
    group_name="dist",
    ins={"canonical_combined_parquet": AssetIn(key=AssetKey("canonical_combined_parquet"))},
    required_resource_keys={"data_dir_out", "s3"},
    description="Per-agency-format JSON for Missouri (all agencies) statewide aggregate.",
)
def statewide_agency_json_export(context, canonical_combined_parquet: str) -> str:
    return write_statewide_agency_json(context, pd.read_parquet(canonical_combined_parquet))


@asset(
    name="metric_year_json_exports",
    group_name="dist",
    ins={"canonical_combined_parquet": AssetIn(key=AssetKey("canonical_combined_parquet"))},
    required_resource_keys={"data_dir_out", "s3"},
    description="Generate per-row_key JSON files with agency/year/race values.",
)
def metric_year_json_exports(context, canonical_combined_parquet: str) -> List[str]:
    return write_metric_year_json(context, pd.read_parquet(canonical_combined_parquet))


@asset(
    name="metric_year_subset_json",
    group_name="dist",
    ins={"canonical_combined_parquet": AssetIn(key=AssetKey("canonical_combined_parquet"))},
    required_resource_keys={"data_dir_out", "s3"},
    description="Write a compact JSON file with selected row_keys across agencies/years.",
)
def metric_year_subset_json(context, canonical_combined_parquet: str) -> str:
    return write_metric_year_subset_json(context, pd.read_parquet(canonical_combined_parquet))


@asset(
    name="agency_index_json",
    group_name="dist",
    deps=[
        AssetKey("combine_all_reports"),
        AssetKey("agency_reference_geocoded"),
        AssetKey("ice_287g_latest"),
    ],
    required_resource_keys={"data_dir_processed", "data_dir_out", "s3"},
    description="Generate an agency index JSON for search and lookup.",
)
def agency_index_json(context) -> str:
    processed_dir = Path(context.resources.data_dir_processed.get_path())
    parquet_path = str(processed_dir / "all_combined_output.parquet")

    # Use DuckDB to avoid loading the full combined DataFrame into memory.
    con = duckdb.connect()
    # stops rows: just what build_agency_index_records needs for the metric column
    stops_rows = con.execute(
        f"SELECT agency, CAST(year AS BIGINT) AS year, \"Total\" "
        f"FROM read_parquet('{parquet_path}') "
        f"WHERE (row_key = 'stops' OR canonical_key = 'stops') AND \"Total\" IS NOT NULL"
    ).df()
    stops_rows = stops_rows.rename(columns={"Total": "stops__Total"})

    # Distinct agencies with any data — passed as 'combined' so build_agency_index_records
    # can build the allowed_keys filter without loading all rows.
    agencies_with_data = con.execute(
        f"SELECT DISTINCT agency, 1 AS \"Total\" "
        f"FROM read_parquet('{parquet_path}') "
        f"WHERE agency IS NOT NULL AND \"Total\" IS NOT NULL"
    ).df()

    # Per-agency year coverage: which years have at least one non-null value row.
    years_rows = con.execute(
        f"SELECT agency, CAST(year AS BIGINT) AS year "
        f"FROM read_parquet('{parquet_path}') "
        f"WHERE agency IS NOT NULL AND \"Total\" IS NOT NULL "
        f"GROUP BY agency, year "
        f"ORDER BY agency, year"
    ).df()
    years_by_agency: dict[str, list[int]] = {}
    for agency, group in years_rows.groupby("agency"):
        years_by_agency[str(agency)] = sorted(int(y) for y in group["year"].dropna().unique())

    con.close()

    # Load geocoded agency reference.
    agency_reference_geocoded = pd.DataFrame()
    for ref_name in ("agency_reference_geocoded.parquet", "agency_reference.parquet"):
        ref_path = processed_dir / ref_name
        if ref_path.exists():
            agency_reference_geocoded = pd.read_parquet(ref_path)
            break

    program_287g = _load_program_287g_by_canonical(processed_dir)
    if program_287g:
        context.log.info("Loaded program_287g for %d agencies", len(program_287g))

    return write_agency_index_json(
        context,
        stops_rows,
        agency_reference_geocoded,
        combined=agencies_with_data,
        years_by_agency=years_by_agency,
        program_287g_by_canonical=program_287g,
    )


@asset(
    name="statewide_slug_baselines_json",
    group_name="dist",
    deps=[AssetKey("statewide_slug_baselines")],
    required_resource_keys={"data_dir_processed", "data_dir_out", "s3"},
    description="Generate statewide baselines JSON for downstream consumers.",
)
def statewide_slug_baselines_json(context) -> str:
    processed_dir = Path(context.resources.data_dir_processed.get_path())
    baselines = pd.read_parquet(processed_dir / "statewide_slug_baselines.parquet")
    return write_statewide_baselines_json(context, baselines)


@asset(
    name="statewide_year_sums_json",
    group_name="dist",
    ins={"canonical_combined_parquet": AssetIn(key=AssetKey("canonical_combined_parquet"))},
    required_resource_keys={"data_dir_out", "s3"},
    description="Write statewide per-year sums for each row_key and race column.",
)
def statewide_year_sums_json(context, canonical_combined_parquet: str) -> str:
    return write_statewide_year_sums_json(context, pd.read_parquet(canonical_combined_parquet))


@asset(
    name="statewide_year_sums_subset_json",
    group_name="dist",
    ins={"canonical_combined_parquet": AssetIn(key=AssetKey("canonical_combined_parquet"))},
    required_resource_keys={"data_dir_out", "s3"},
    description="Write a slim statewide sums JSON for selected metrics.",
)
def statewide_year_sums_subset_json(context, canonical_combined_parquet: str) -> str:
    return write_statewide_year_sums_subset_json(context, pd.read_parquet(canonical_combined_parquet))


@asset(
    name="report_dimension_index_json",
    group_name="dist",
    ins={"canonical_combined_parquet": AssetIn(key=AssetKey("canonical_combined_parquet"))},
    required_resource_keys={"data_dir_out", "s3"},
    description="Write unique table/section/metric identifiers for translations.",
)
def report_dimension_index_json(context, canonical_combined_parquet: str) -> str:
    return write_report_dimension_index_json(context, pd.read_parquet(canonical_combined_parquet))


@asset(
    name="homepage_stats_json",
    group_name="dist",
    ins={"canonical_combined_parquet": AssetIn(key=AssetKey("canonical_combined_parquet"))},
    required_resource_keys={"data_dir_out", "s3"},
    description="Generate homepage stats JSON for the latest report year.",
)
def homepage_stats_json(context, canonical_combined_parquet: str) -> str:
    return write_homepage_stats_json(context, pd.read_parquet(canonical_combined_parquet))


@asset(
    name="dist_manifest_json",
    group_name="dist",
    deps=[AssetKey("combine_all_reports"), AssetKey("reports_with_rank_percentile")],
    required_resource_keys={"data_dir_processed", "data_dir_out", "s3"},
    description="Write releases/v2/manifest.json describing the v2 data release.",
)
def dist_manifest_json(context) -> str:
    processed_dir = Path(context.resources.data_dir_processed.get_path())

    con = duckdb.connect()
    years_rows = con.execute(
        f"SELECT DISTINCT year FROM read_parquet('{processed_dir}/all_combined_output.parquet') "
        f"WHERE year IS NOT NULL AND year >= {DIST_MIN_YEAR} ORDER BY year"
    ).fetchall()
    canonical_rows = con.execute(
        f"SELECT DISTINCT canonical_key FROM read_parquet('{processed_dir}/all_combined_output.parquet') "
        f"WHERE canonical_key IS NOT NULL AND year >= {DIST_MIN_YEAR} ORDER BY canonical_key"
    ).fetchall()
    con.close()

    years = [int(r[0]) for r in years_rows]
    canonical_metrics = [r[0] for r in canonical_rows if not _is_cross_rate_pairing(r[0])]

    manifest = {
        "version": "2.0",
        "released": date.today().isoformat(),
        "years": years,
        "schema_version": "2.0",
        "canonical_metrics": canonical_metrics,
        "changelog": (
            f"v2.0: Added {DIST_MIN_YEAR}–2019 pre-2020 format data with canonical_key normalization. "
            "row_key in all dist outputs is now canonical_key (era-independent). "
            "Agency JSON partitioned by year (dist/agency_year/{slug}/{year}.json). "
            f"Years before {DIST_MIN_YEAR} excluded from dist outputs (sparse/unreliable coverage)."
        ),
    }

    out_dir = Path(context.resources.data_dir_out.get_path())
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "manifest.json"
    out_path.write_text(json.dumps(manifest, indent=2))
    context.log.info(
        "Wrote manifest.json: %d years, %d canonical metrics → %s",
        len(years), len(canonical_metrics), out_path,
    )

    meta: dict = {"local_path": str(out_path), "years": len(years), "canonical_metrics": len(canonical_metrics)}
    try:
        s3_meta = upload_file_to_s3(context, out_path, "manifest.json", content_type="application/json")
        if s3_meta:
            meta.update(s3_meta)
    except Exception as exc:
        context.log.exception("Failed to upload manifest.json to S3: %s", exc)
    try:
        context.add_output_metadata(meta)
    except Exception:
        pass
    return str(out_path)


def write_download_vsr_statistics(
    context,
    reports: pd.DataFrame,
    agency_reference_geocoded: pd.DataFrame,
) -> dict:
    out_dir = Path(context.resources.data_dir_out.get_path()) / "downloads"
    if reports.empty:
        context.log.warning("Rank/percentile DataFrame empty; no download outputs created.")
        return {}
    enriched = _add_canonical_names(reports, agency_reference_geocoded)
    return _write_download_bundle(
        context,
        enriched,
        base_name="vsr_statistics",
        out_dir=out_dir,
    )


def write_download_agency_index(
    context,
    pivoted: pd.DataFrame,
    agency_reference_geocoded: pd.DataFrame,
    combined: pd.DataFrame,
) -> dict:
    out_dir = Path(context.resources.data_dir_out.get_path()) / "downloads"
    processed_dir = Path(context.resources.data_dir_processed.get_path())
    program_287g = _load_program_287g_by_canonical(processed_dir)
    records = build_agency_index_records(
        pivoted,
        agency_reference_geocoded,
        combined=combined,
        program_287g_by_canonical=program_287g,
    )
    if not records:
        context.log.warning("No agency index records; no download outputs created.")
        return {}
    df = pd.DataFrame(records)
    return _write_download_bundle(
        context,
        df,
        base_name="agency_index",
        out_dir=out_dir,
    )


def write_download_agency_comments(context, agency_comments: pd.DataFrame) -> dict:
    out_dir = Path(context.resources.data_dir_out.get_path()) / "downloads"
    if agency_comments.empty:
        context.log.warning("Agency comments empty; no download outputs created.")
        return {}
    return _write_download_bundle(
        context,
        agency_comments,
        base_name="agency_comments",
        out_dir=out_dir,
    )


def write_downloads_combined(
    context,
    vsr_statistics_path: Path,
    pivoted: pd.DataFrame,
    agency_reference_geocoded: pd.DataFrame,
    combined: pd.DataFrame,
    agency_comments: pd.DataFrame,
    *,
    batch_size: int = 100_000,
) -> dict:
    """Write the combined downloads bundle, streaming the large vsr_statistics dataset.

    vsr_statistics is read from its parquet in row batches and serialized record-by-record
    so the full reports table never lives in memory at once — this keeps peak RSS low
    enough to run on small (4GB) cloud workers. The small agency_index / agency_comments
    datasets are still built in memory.
    """
    import pyarrow.parquet as pq

    out_dir = Path(context.resources.data_dir_out.get_path()) / "downloads"
    out_dir.mkdir(parents=True, exist_ok=True)

    processed_dir = Path(context.resources.data_dir_processed.get_path())
    program_287g = _load_program_287g_by_canonical(processed_dir)
    agency_index_records = build_agency_index_records(
        pivoted,
        agency_reference_geocoded,
        combined=combined,
        program_287g_by_canonical=program_287g,
    )
    agency_index_df = pd.DataFrame(agency_index_records)
    agency_index_payload = (
        agency_index_df.to_dict(orient="records") if not agency_index_df.empty else []
    )
    agency_comments_payload = (
        agency_comments.to_dict(orient="records") if not agency_comments.empty else []
    )

    canonical_lookup = _canonical_name_lookup(agency_reference_geocoded)
    sep = (",", ":")
    combined_json = out_dir / f"{DOWNLOAD_PREFIX}downloads.json"

    vsr_rows = 0
    with combined_json.open("w") as f:
        f.write('{"vsr_statistics":[')
        first = True
        vsr_statistics_path = Path(vsr_statistics_path)
        if vsr_statistics_path.exists():
            parquet_file = pq.ParquetFile(str(vsr_statistics_path))
            for batch in parquet_file.iter_batches(batch_size=batch_size):
                chunk = batch.to_pandas()
                if canonical_lookup:
                    _apply_canonical_names(chunk, canonical_lookup)
                for record in chunk.to_dict(orient="records"):
                    f.write("," if not first else "")
                    first = False
                    f.write(json.dumps(record, separators=sep))
                vsr_rows += len(chunk)
                del chunk
        f.write('],"agency_index":')
        f.write(json.dumps(agency_index_payload, separators=sep))
        f.write(',"agency_comments":')
        f.write(json.dumps(agency_comments_payload, separators=sep))
        f.write("}")

    context.log.info(
        "Wrote %s (vsr_statistics=%d rows, agency_index=%d, agency_comments=%d)",
        combined_json, vsr_rows, len(agency_index_payload), len(agency_comments_payload),
    )

    uploaded = upload_paths(
        context,
        [combined_json],
        base_dir=out_dir,
        prefix_override="downloads",
    )

    return {
        "json_path": str(combined_json),
        "s3_paths": uploaded or [],
    }


@asset(
    name="downloads_vsr_statistics",
    group_name="downloads",
    deps=[AssetKey("reports_with_rank_percentile"), AssetKey("agency_reference_geocoded")],
    required_resource_keys={"data_dir_processed", "data_dir_out", "s3"},
    description="Download bundle for VSR statistics (parquet/json/csv).",
)
def downloads_vsr_statistics(context) -> dict:
    processed_dir = Path(context.resources.data_dir_processed.get_path())
    reports = pd.read_parquet(processed_dir / "reports_with_rank_percentile.parquet")
    agency_ref = pd.read_parquet(processed_dir / "agency_reference_geocoded.parquet")
    return write_download_vsr_statistics(context, reports, agency_ref)


@asset(
    name="downloads_agency_index",
    group_name="downloads",
    deps=[
        AssetKey("reports_with_rank_percentile"),
        AssetKey("agency_reference_geocoded"),
        AssetKey("ice_287g_latest"),
    ],
    required_resource_keys={"data_dir_processed", "data_dir_out", "s3"},
    description="Download bundle for agency_index (parquet/json/csv).",
)
def downloads_agency_index(context) -> dict:
    processed_dir = Path(context.resources.data_dir_processed.get_path())
    reports = pd.read_parquet(processed_dir / "reports_with_rank_percentile.parquet")
    stops_rows = reports[reports["row_key"] == "stops"][["agency", "year", "Total"]].copy()
    stops_rows = stops_rows.rename(columns={"Total": "stops__Total"})
    agency_ref = pd.read_parquet(processed_dir / "agency_reference_geocoded.parquet")
    return write_download_agency_index(context, stops_rows, agency_ref, reports)


@asset(
    name="downloads_agency_comments",
    group_name="downloads",
    deps=[AssetKey("agency_comments")],
    required_resource_keys={"data_dir_processed", "data_dir_out", "s3"},
    description="Download bundle for agency comments (parquet/json/csv).",
)
def downloads_agency_comments(context) -> dict:
    processed_dir = Path(context.resources.data_dir_processed.get_path())
    agency_comments = pd.read_parquet(processed_dir / "agency_comments.parquet")
    return write_download_agency_comments(context, agency_comments)


@asset(
    name="downloads_combined",
    group_name="downloads",
    deps=[
        AssetKey("reports_with_rank_percentile"),
        AssetKey("agency_reference_geocoded"),
        AssetKey("agency_comments"),
        AssetKey("ice_287g_latest"),
    ],
    required_resource_keys={"data_dir_processed", "data_dir_out", "s3"},
    description="Combined downloads bundle (json + parquet).",
)
def downloads_combined(context) -> dict:
    processed_dir = Path(context.resources.data_dir_processed.get_path())
    reports_path = processed_dir / "reports_with_rank_percentile.parquet"
    # Derive the small frames via DuckDB so the full reports table is never loaded into
    # memory — write_downloads_combined streams vsr_statistics straight from the parquet.
    value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in pq_column_names(reports_path)]
    any_value = " OR ".join(f'"{c}" IS NOT NULL' for c in value_cols) or "TRUE"
    con = duckdb.connect()
    stops_rows = con.execute(
        f"SELECT agency, year, \"Total\" AS stops__Total "
        f"FROM read_parquet('{reports_path}') WHERE row_key = 'stops'"
    ).df()
    # `combined` only drives the statewide entry + the allowed-keys filter in
    # build_agency_index_records: any agency with a non-null value column. Mirror that
    # exactly without loading the table.
    agencies_with_data = con.execute(
        f"SELECT DISTINCT agency, 1 AS \"Total\" FROM read_parquet('{reports_path}') "
        f"WHERE agency IS NOT NULL AND ({any_value})"
    ).df()
    con.close()
    agency_ref = pd.read_parquet(processed_dir / "agency_reference_geocoded.parquet")
    agency_comments = pd.read_parquet(processed_dir / "agency_comments.parquet")
    return write_downloads_combined(
        context, reports_path, stops_rows, agency_ref, agencies_with_data, agency_comments
    )


@asset(
    name="downloads_wide",
    group_name="downloads",
    deps=[AssetKey("combine_all_reports")],
    required_resource_keys={"data_dir_processed", "data_dir_out", "s3"},
    description=(
        "Wide-format download bundle: one row per agency-year with columns "
        "flattened as `<row_key>_<race>` (e.g. `stops_black`, "
        "`rates_by_race_population_density_white`). Emits an all-years CSV+parquet "
        "plus one per-year CSV+parquet slice."
    ),
)
def downloads_wide(context) -> dict:
    processed_dir = Path(context.resources.data_dir_processed.get_path())
    out_dir = Path(context.resources.data_dir_out.get_path()) / "downloads"
    combined = pd.read_parquet(processed_dir / "all_combined_output.parquet")
    pivoted = pivot_reports_by_slug_op(context, combined)
    wide = _flatten_to_wide_columns(pivoted)
    context.log.info("Wide pivot shape: %s rows × %s cols", *wide.shape)

    results: dict = {
        "all_years": _write_download_bundle(
            context, wide, base_name="wide", out_dir=out_dir
        )
    }
    for year in sorted(wide["year"].dropna().unique()):
        year_int = int(year)
        year_slice = wide[wide["year"] == year].drop(columns=["year"])
        results[f"year_{year_int}"] = _write_download_bundle(
            context,
            year_slice,
            base_name="wide",
            out_dir=out_dir,
            prefix_override=f"missouri_vsr_{year_int}_",
        )
    return results


_WIDE_FILENAME_RE = re.compile(
    r"^missouri_vsr_(?P<start>\d{4})(?:_(?P<end>\d{4}))?_wide\.(?:csv|parquet)$"
)


@asset(
    name="downloads_manifest",
    group_name="downloads",
    required_resource_keys={"data_dir_out", "s3"},
    deps=[
        AssetKey("downloads_vsr_statistics"),
        AssetKey("downloads_agency_index"),
        AssetKey("downloads_agency_comments"),
        AssetKey("downloads_combined"),
        AssetKey("downloads_wide"),
    ],
    description="Manifest of download bundle files with sizes.",
)
def write_downloads_manifest(context) -> str:
    downloads_dir = Path(context.resources.data_dir_out.get_path()) / "downloads"
    downloads_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = downloads_dir / f"{DOWNLOAD_PREFIX}downloads_manifest.json"

    entries = []
    for path in sorted(downloads_dir.rglob("*")):
        if not path.is_file():
            continue
        if path.name == manifest_path.name:
            continue
        if path.suffix.lower() == ".json" and path.name != f"{DOWNLOAD_PREFIX}downloads.json":
            continue
        rel = path.relative_to(downloads_dir).as_posix()
        try:
            size = path.stat().st_size
        except OSError:
            size = None
        suffix = path.suffix.lower().lstrip(".")
        entry = {"path": rel, "size_bytes": size, "group": suffix or "unknown"}

        wide_match = _WIDE_FILENAME_RE.match(path.name)
        if wide_match:
            entry["format"] = "wide"
            # Per-year files: only `start` is present. All-years: both `start` and `end`.
            if wide_match.group("end") is None:
                entry["year"] = int(wide_match.group("start"))

        entries.append(entry)

    entries.sort(
        key=lambda item: (item["size_bytes"] is None, -(item["size_bytes"] or 0))
    )

    payload = {
        "prefix": DOWNLOAD_PREFIX,
        "base_dir": "data/out/downloads",
        "file_count": len(entries),
        "files": entries,
    }
    manifest_path.write_text(json.dumps(payload, indent=2))

    uploaded = upload_paths(
        context,
        [manifest_path],
        base_dir=downloads_dir,
        prefix_override="downloads",
    )

    try:
        metadata = {
            "local_path": str(manifest_path),
            "file_count": len(entries),
        }
        if uploaded:
            metadata["s3_paths"] = uploaded
        context.add_output_metadata(metadata)
    except Exception:
        pass
    return str(manifest_path)


@asset(
    name="dist_sync",
    group_name="dist",
    deps=[
        AssetKey("agency_year_json_exports"),
        AssetKey("metric_year_json_exports"),
        AssetKey("metric_year_subset_json"),
        AssetKey("agency_index_json"),
        AssetKey("statewide_slug_baselines_json"),
        AssetKey("statewide_year_sums_json"),
        AssetKey("statewide_year_sums_subset_json"),
        AssetKey("report_dimension_index_json"),
        AssetKey("homepage_stats_json"),
        AssetKey("dist_manifest_json"),
        AssetKey("statewide_agency_json_export"),
        AssetKey("agency_relationships"),
        AssetKey("agency_boundaries_index"),
        AssetKey("mo_jurisdictions_manifest"),
    ],
    required_resource_keys={"data_dir_out", "s3"},
    description=(
        "Sync all locally generated dist outputs to S3 using aws s3 sync. "
        "Runs after all dist generation assets. No-op when S3 is not configured."
    ),
)
def dist_sync(context) -> dict:
    """Push data/out/ to S3 using aws s3 sync (parallelized by the AWS CLI)."""
    s3_res = getattr(context.resources, "s3", None)
    bucket = s3_res.resolved_bucket() if s3_res else None
    if not bucket:
        context.log.info("S3 not configured; skipping dist_sync.")
        try:
            context.add_output_metadata({"skipped": True, "reason": "S3 not configured"})
        except Exception:
            pass
        return {"skipped": True}

    prefix = s3_res.resolved_prefix()
    out_dir = Path(context.resources.data_dir_out.get_path())

    # dist: everything under data_dir_out except downloads/
    dist_s3 = f"s3://{bucket}/{prefix}/dist/" if prefix else f"s3://{bucket}/dist/"
    downloads_s3 = f"s3://{bucket}/{prefix}/downloads/" if prefix else f"s3://{bucket}/downloads/"

    def _run_sync(src: str, dst: str, extra_args: list[str] | None = None) -> dict:
        cmd = ["aws", "s3", "sync", src, dst, "--no-progress"]
        if extra_args:
            cmd.extend(extra_args)
        context.log.info("Running: %s", " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            context.log.error("aws s3 sync failed:\n%s", result.stderr)
            raise RuntimeError(f"aws s3 sync failed (exit {result.returncode}): {result.stderr[:500]}")
        lines = [l for l in result.stdout.splitlines() if l.strip()]
        context.log.info("sync %s → %s: %d lines output", src, dst, len(lines))
        return {"src": src, "dst": dst, "lines": len(lines)}

    results = []
    # Sync dist outputs (exclude the downloads/ subdir — it has its own S3 prefix)
    results.append(_run_sync(
        str(out_dir) + "/",
        dist_s3,
        ["--exclude", "downloads/*", "--delete"],
    ))
    # Sync downloads separately under the downloads/ prefix
    downloads_dir = out_dir / "downloads"
    if downloads_dir.exists():
        results.append(_run_sync(str(downloads_dir) + "/", downloads_s3, ["--delete"]))

    meta = {"sync_results": results}
    try:
        context.add_output_metadata(meta)
    except Exception:
        pass
    return meta
