"""Ingest ICE 287(g) participating-agencies snapshots.

Source data is manually downloaded from
https://www.ice.gov/identify-and-arrest/287g (the "View participating
agencies" link, currently https://www.ice.gov/file-download/download/public/185939).

Drops are placed in data/src/287g/ as YYYY-MM-DD[-label].xlsx so future
snapshots stack alongside one another. The asset reads the newest snapshot
on the filesystem; "current" is whatever's lexically latest.

This first pass is MO-only extract → parquet. Crosswalk to agency_reference
and injection into per-agency dist metadata are deferred to a follow-up
(see issue #19).
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from dagster import AssetOut, Output, multi_asset


SNAPSHOT_SUBDIR = "287g"
PROCESSED_FILENAME = "287g_mo_snapshot.parquet"
FILENAME_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})(?:-(.+))?\.xlsx$", re.IGNORECASE)
SAFELINKS_URL_RE = re.compile(r"safelinks\.protection\.outlook\.com.*?[?&]url=([^&]+)", re.IGNORECASE)


def _parse_filename(name: str) -> tuple[date, str | None]:
    m = FILENAME_DATE_RE.match(name)
    if not m:
        raise ValueError(
            f"287g snapshot filename must match YYYY-MM-DD[-label].xlsx, got {name!r}"
        )
    yr, mo, day, label = m.groups()
    return date(int(yr), int(mo), int(day)), label


def _unwrap_safelink(url: str | None) -> str | None:
    if not url:
        return url
    m = SAFELINKS_URL_RE.search(url)
    if not m:
        return url
    from urllib.parse import unquote

    return unquote(m.group(1))


def _read_snapshot_with_links(xlsx_path: Path) -> pd.DataFrame:
    from openpyxl import load_workbook

    wb = load_workbook(xlsx_path, read_only=False, data_only=True)
    ws = wb.active
    rows = ws.iter_rows(values_only=False)
    header = [cell.value for cell in next(rows)]
    header = [h for h in header if h is not None]
    moa_idx = header.index("MOA") if "MOA" in header else None

    records: list[dict[str, Any]] = []
    for row in rows:
        record: dict[str, Any] = {}
        for i, key in enumerate(header):
            cell = row[i] if i < len(row) else None
            record[key] = getattr(cell, "value", None)
        if moa_idx is not None and moa_idx < len(row):
            link = getattr(row[moa_idx], "hyperlink", None)
            record["moa_url"] = _unwrap_safelink(link.target) if link else None
        else:
            record["moa_url"] = None
        records.append(record)

    df = pd.DataFrame.from_records(records)
    df = df.dropna(how="all")
    return df


@multi_asset(
    outs={"program_287g_mo_snapshot": AssetOut()},
    group_name="extract",
    required_resource_keys={"data_dir_source", "data_dir_processed"},
    description=(
        "Latest ICE 287(g) participating-agencies snapshot, filtered to "
        "Missouri. Source: data/src/287g/YYYY-MM-DD[-label].xlsx (newest "
        "wins). MOA hyperlinks are extracted from the cell metadata."
    ),
)
def program_287g_mo_snapshot(context):
    src_dir = Path(context.resources.data_dir_source.get_path()) / SNAPSHOT_SUBDIR
    if not src_dir.exists():
        raise FileNotFoundError(
            f"No 287g snapshots directory at {src_dir}. Drop a YYYY-MM-DD[-label].xlsx in there."
        )

    candidates = sorted(src_dir.glob("*.xlsx"))
    if not candidates:
        raise FileNotFoundError(f"No .xlsx snapshots found in {src_dir}")
    snapshot_path = candidates[-1]
    snapshot_date, snapshot_label = _parse_filename(snapshot_path.name)
    context.log.info("Reading 287g snapshot: %s (date=%s, label=%s)", snapshot_path, snapshot_date, snapshot_label)

    raw = _read_snapshot_with_links(snapshot_path)

    str_cols = [c for c in raw.columns if raw[c].dtype == object]
    for col in str_cols:
        raw[col] = raw[col].map(lambda v: v.strip() if isinstance(v, str) else v)

    if "STATE" not in raw.columns:
        raise ValueError(f"Expected STATE column in {snapshot_path}, got {list(raw.columns)}")

    mo = raw[raw["STATE"].astype(str).str.upper() == "MISSOURI"].copy()
    mo.columns = [str(c).lower().replace(" ", "_") for c in mo.columns]

    if "signed" in mo.columns:
        mo["signed"] = pd.to_datetime(mo["signed"], errors="coerce").dt.date

    mo["source_date"] = snapshot_date
    mo["source_label"] = snapshot_label
    mo["source_filename"] = snapshot_path.name

    out_dir = Path(context.resources.data_dir_processed.get_path()) / SNAPSHOT_SUBDIR
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / PROCESSED_FILENAME
    mo.to_parquet(out_path, index=False)
    context.log.info(
        "Wrote MO 287g snapshot → %s (rows=%d, agencies=%d)",
        out_path, len(mo), mo["law_enforcement_agency"].nunique(),
    )

    yield Output(
        mo,
        output_name="program_287g_mo_snapshot",
        metadata={
            "source_path": str(snapshot_path),
            "source_date": str(snapshot_date),
            "row_count": len(mo),
            "unique_agencies": int(mo["law_enforcement_agency"].nunique()),
            "support_type_counts": mo["support_type"].value_counts().to_dict(),
            "moa_url_coverage": int(mo["moa_url"].notna().sum()),
            "local_path": str(out_path),
        },
    )
