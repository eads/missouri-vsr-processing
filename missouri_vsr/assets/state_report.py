"""Statewide VSR annual report ingest.

The Missouri AG publishes a *statewide* annual Vehicle Stops Report (authored by
the University of Missouri) as a separate PDF from the per-agency reports parsed
in ``extract.py``. Each statewide report is a handful of clean tables:

  - Rates by Race      — population (ACS 16+ and Decennial 18+), totals, rates
  - Disparity Index    — the AG's official disparity index (some years only)
  - Disparity Index time series (2000..report_year) — some years only
  - Number of Stops by Race — stops, reasons, outcomes, violations, etc.
  - Search statistics  — probable-cause/search-reason, what searched, contraband

Two reasons we ingest it:

1. It carries the **official ACS 16+ population by race** used as the disparity
   denominator for that report year — the authoritative population baseline.
2. It is an independent **check on every statewide figure we compute** by summing
   the per-agency reports (see ``analysis`` assets).

Tables are numbered differently across years (search statistics is Table 5 in
2025 but Table 3 in 2023) and the disparity tables are absent in some years, so
the parser keys on table *title*, not number. Extraction stays close to source
(raw labels preserved, incl. the AG's "Equpiment" typo); canonical mapping and
normalization happen downstream.
"""
from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests
from slugify import slugify

from dagster import AssetIn, asset

# ------------------------------------------------------------------------------
# Year -> statewide report URL. The AG's filenames are idiosyncratic (cf.
# YEAR_URLS in extract.py); confirmed entries only. Add years as URLs are found.
# ------------------------------------------------------------------------------
STATE_REPORT_URLS: Dict[int, str] = {
    2025: "https://ago.mo.gov/wp-content/uploads/2025-VSR-Annual-State-Report.pdf",
    2023: "https://ago.mo.gov/wp-content/uploads/VSRstatereport2023.pdf",
}

RACE_COLS = ["Total", "White", "Black", "Hispanic", "Native American", "Asian", "Other"]
# The disparity-index *time series* table omits the Total column.
RACE_COLS_NO_TOTAL = RACE_COLS[1:]

# A value token: integer (with thousands commas), decimal, leading-dot decimal,
# or a bare "." meaning null. Negative allowed (the AG occasionally files them).
_VALUE_RE = re.compile(r"^-?(?:\d[\d,]*(?:\.\d+)?|\.\d+)$")
_TABLE_HDR_RE = re.compile(r"Table\s+(\d+):\s*(.+?)\s+for Missouri", re.I)
_FIGURE_HDR_RE = re.compile(r"Figure\s+\d+:", re.I)
_TIMESERIES_TITLE_RE = re.compile(r"disparity index from", re.I)
_YEAR_ROW_RE = re.compile(r"^\s*((?:19|20)\d{2})\s+(.+)$")


def _pdftotext_layout(path: str | Path) -> List[str]:
    out = subprocess.run(
        ["pdftotext", "-layout", str(path), "-"],
        capture_output=True, text=True, check=True,
    )
    return out.stdout.splitlines()


def _is_value(tok: str) -> bool:
    return tok == "." or bool(_VALUE_RE.match(tok))


def _to_num(tok: str) -> Optional[float]:
    if tok == ".":
        return None
    return float(tok.replace(",", ""))


def _split_label_values(line: str, n: int = 7):
    """If *line* ends in exactly *n* value-like tokens, return (label, [values]).

    Race rows render as ``<label> v1 v2 ... v7`` in pdftotext -layout output.
    Section-header rows are ``<label> . . . . . . .`` (all null). Labels may
    themselves contain numbers ("17 and under", "0-15 minutes", "2024 ACS pop.")
    so we take the *trailing* n value tokens, not every numeric token.
    """
    toks = line.split()
    if len(toks) < n:
        return None
    tail = toks[-n:]
    if not all(_is_value(t) for t in tail):
        return None
    label = " ".join(toks[:-n]).strip()
    if not label:
        return None
    return label, [_to_num(t) for t in tail]


def parse_state_report(path: str | Path, report_year: int) -> pd.DataFrame:
    """Parse one statewide report PDF into close-to-source long-by-race rows.

    Returns columns: report_year, table_title, section, metric, series_year,
    Total..Other (one row per metric). Time-series rows (disparity 2000..N) set
    series_year and leave Total null.
    """
    lines = _pdftotext_layout(path)
    records: List[dict] = []
    table_title: Optional[str] = None
    section: Optional[str] = None
    in_table = False
    in_timeseries = False

    for raw in lines:
        line = raw.rstrip()
        if not line.strip():
            continue

        m = _TABLE_HDR_RE.search(line)
        if m:
            table_title = m.group(2).strip()
            section = None
            in_table = True
            in_timeseries = bool(_TIMESERIES_TITLE_RE.search(table_title))
            continue

        # A figure ends the table region; stop collecting until the next table so
        # chart axis ticks don't leak in as fake metrics.
        if _FIGURE_HDR_RE.search(line):
            in_table = False
            in_timeseries = False
            section = None
            continue

        if not in_table:
            continue

        if in_timeseries:
            ym = _YEAR_ROW_RE.match(line.strip())
            if not ym:
                continue
            rest = ym.group(2).split()
            if len(rest) != 6 or not all(_is_value(t) for t in rest):
                continue
            rec = {
                "report_year": report_year, "table_title": table_title,
                "section": "disparity-index-timeseries",
                "metric": ym.group(1), "series_year": int(ym.group(1)),
                "Total": None,
            }
            for col, tok in zip(RACE_COLS_NO_TOTAL, rest):
                rec[col] = _to_num(tok)
            records.append(rec)
            continue

        parsed = _split_label_values(line, 7)
        if parsed is None:
            continue
        label, vals = parsed
        if all(v is None for v in vals):
            section = label  # all-null row is a section header
            continue
        rec = {
            "report_year": report_year, "table_title": table_title,
            "section": section, "metric": label, "series_year": None,
        }
        for col, v in zip(RACE_COLS, vals):
            rec[col] = v
        records.append(rec)

    df = pd.DataFrame.from_records(records)
    if df.empty:
        return df
    df["table_id"] = df["table_title"].map(lambda s: slugify(s) if pd.notna(s) else None)
    df["section_id"] = df["section"].map(lambda s: slugify(s) if pd.notna(s) else None)
    df["metric_id"] = df["metric"].map(lambda s: slugify(str(s)) if pd.notna(s) else None)
    ordered = [
        "report_year", "table_title", "table_id", "section", "section_id",
        "metric", "metric_id", "series_year", *RACE_COLS,
    ]
    return df[ordered]


# ------------------------------------------------------------------------------
# Assets
# ------------------------------------------------------------------------------
@asset(
    group_name="state_report",
    required_resource_keys={"data_dir_report_pdfs"},
    description="Download statewide VSR annual report PDFs (one per configured year).",
)
def download_state_reports(context) -> Dict[str, str]:
    """Fetch each configured statewide report to disk; return {year: path}."""
    out_dir = Path(context.resources.data_dir_report_pdfs.get_path())
    out_dir.mkdir(parents=True, exist_ok=True)
    paths: Dict[str, str] = {}
    for year, url in STATE_REPORT_URLS.items():
        out_path = out_dir / f"statewide_VSRreport{year}.pdf"
        if not out_path.exists():
            context.log.info("Downloading statewide %s -> %s", url, out_path)
            resp = requests.get(url, timeout=(10, 300))
            resp.raise_for_status()
            out_path.write_bytes(resp.content)
        else:
            context.log.debug("Using cached %s", out_path)
        paths[str(year)] = str(out_path)
    return paths


@asset(
    group_name="state_report",
    ins={"download_state_reports": AssetIn()},
    required_resource_keys={"data_dir_processed"},
    description="Parse statewide report PDFs into one tidy long-by-race table; write parquet.",
)
def state_reports_combined(context, download_state_reports: Dict[str, str]) -> pd.DataFrame:
    frames = []
    for year_str, path in sorted(download_state_reports.items()):
        df = parse_state_report(path, int(year_str))
        context.log.info("Parsed statewide %s: %d metric rows", year_str, len(df))
        if not df.empty:
            frames.append(df)
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    out_dir = Path(context.resources.data_dir_processed.get_path())
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "state_reports_combined.parquet"
    combined.to_parquet(out_path, index=False)
    context.add_output_metadata({
        "rows": len(combined),
        "years": sorted(combined["report_year"].unique().tolist()) if not combined.empty else [],
        "path": str(out_path),
    })
    return combined
