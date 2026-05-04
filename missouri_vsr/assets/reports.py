from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import pandas as pd
import requests

from dagster import AssetIn, AssetKey, AssetOut, In, Out, Output, graph_asset, op, multi_asset

from missouri_vsr.assets.extract import YEAR_URLS, _ensure_pdftotext, _slugify_simple
from missouri_vsr.assets.s3_utils import upload_file_to_s3
from missouri_vsr.cli.crosswalk import _normalize_name

AGENCY_RESPONSE_URLS = {
    2024: "https://ago.mo.gov/wp-content/uploads/2024-Agency-Responses-1.pdf",
    2023: "https://ago.mo.gov/wp-content/uploads/VSRagencynotes2023.pdf",
    2022: "https://ago.mo.gov/wp-content/uploads/2022-agency-comments-ago.pdf",
    2021: "https://ago.mo.gov/wp-content/uploads/2021-VSR-Agency-Comments.pdf",
    2020: "https://ago.mo.gov/wp-content/uploads/2020-VSR-Agency-Comments.pdf",
}

DOWNLOAD_PREFIX = f"missouri_vsr_{min(YEAR_URLS)}_{max(YEAR_URLS)}_"

_COMMENT_MARKER_RE = re.compile(r"agency public comments?|public agency comments", re.IGNORECASE)


def _normalize_agency_name(value: str) -> str:
    text = unicodedata.normalize("NFKC", value)
    text = (
        text.replace("’", "'")
        .replace("‘", "'")
        .replace("`", "'")
        .replace("\u00a0", " ")
        .replace("\u200b", "")
    )
    text = re.sub(r"\s+'s\b", "'s", text)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def _is_comment_marker(line: str) -> bool:
    return bool(_COMMENT_MARKER_RE.search(line))


def _parse_agency_header(line: str) -> str | None:
    text = _normalize_agency_name(line)
    if not text:
        return None
    lower = text.lower()
    if "contents" == lower or "table of contents" in lower:
        return None
    if "agency comments" == lower or "agency responses" == lower:
        return None
    if _is_comment_marker(text):
        return None
    tokens = text.split()
    if not tokens:
        return None
    idx = 0
    while idx < len(tokens) and re.fullmatch(r"\d+[.)]?", tokens[idx]):
        idx += 1
    if idx == 0:
        return None
    tokens = tokens[idx:]
    if tokens and re.fullmatch(r"\d+", tokens[-1]):
        tokens = tokens[:-1]
    if not tokens:
        return None
    if tokens[0] and tokens[0][0].islower():
        return None
    if any(re.fullmatch(r"\d+", tok) for tok in tokens):
        return None
    name = " ".join(tokens).strip()
    if not re.search(r"[A-Za-z]", name):
        return None
    return _normalize_agency_name(name)


def _collapse_comment_lines(lines: list[str]) -> str:
    paragraphs: list[list[str]] = []
    current: list[str] = []
    for line in lines:
        text = line.strip()
        if not text:
            if current:
                paragraphs.append(current)
                current = []
            continue
        current.append(text)
    if current:
        paragraphs.append(current)
    if not paragraphs:
        return ""
    joined = [" ".join(part).strip() for part in paragraphs if part]
    text = "\n\n".join(joined).strip()
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text


def _parse_agency_comments(lines: list[str], *, year: int, pdf_name: str, url: str, log) -> list[dict]:
    normalized_lines = []
    for raw in lines:
        cleaned = raw.replace("\x0c", "")
        cleaned = _normalize_agency_name(cleaned)
        normalized_lines.append(cleaned)

    marker_mode = any(_is_comment_marker(line) for line in normalized_lines)
    records: dict[str, dict] = {}

    current_agency: str | None = None
    current_lines: list[str] = []
    pending_agency: str | None = None
    pending_age = 0
    capturing = False

    def _commit():
        nonlocal current_agency, current_lines, capturing
        if not current_agency:
            return
        comment = _collapse_comment_lines(current_lines)
        record = {
            "year": year,
            "agency": current_agency,
            "comment": comment or None,
            "has_comment": bool(comment),
            "source_pdf": pdf_name,
            "source_url": url,
        }
        existing = records.get(current_agency)
        if existing is None or (record["has_comment"] and not existing["has_comment"]):
            records[current_agency] = record
        current_agency = None
        current_lines = []
        capturing = False

    for line in normalized_lines:
        if pending_agency:
            pending_age += 1
            if pending_age > 5:
                pending_agency = None
        if not line.strip():
            if capturing:
                current_lines.append("")
            continue
        if line.strip().isdigit():
            continue

        header = _parse_agency_header(line)
        if header:
            if marker_mode:
                if capturing:
                    _commit()
                pending_agency = header
                pending_age = 0
                continue
            if current_agency:
                _commit()
            current_agency = header
            current_lines = []
            capturing = True
            continue

        if marker_mode and _is_comment_marker(line):
            if pending_agency and pending_age <= 3:
                if current_agency:
                    _commit()
                current_agency = pending_agency
                current_lines = []
                capturing = True
            pending_agency = None
            continue

        if capturing and current_agency:
            current_lines.append(line)

    if current_agency:
        _commit()

    log.info(
        "Parsed %d agency comment rows from %s (%s)",
        len(records),
        pdf_name,
        year,
    )
    return list(records.values())

# ------------------------------------------------------------------------------
# Canonical key crosswalk helpers
# ------------------------------------------------------------------------------

def _parse_year_range(yr_range: str) -> list[int]:
    """Parse "2020-2024" or "2019" into a list of years."""
    yr_range = yr_range.strip()
    if "-" in yr_range:
        parts = yr_range.split("-")
        return list(range(int(parts[0]), int(parts[-1]) + 1))
    return [int(yr_range)]


def _build_canonical_key_lookup(crosswalk_path: Path) -> dict[tuple[str, int], str]:
    """
    Build a {(row_key, year): canonical_key} lookup from the crosswalk CSV.

    Handles pipe-separated row_keys and year ranges for renamed metrics.
    Multiple pipe-separated values align 1:1; if there are more row_keys than
    year ranges (post-2020 duplicates covering the same years), the last year
    range is repeated for the extra row_keys.
    """
    lookup: dict[tuple[str, int], str] = {}
    df = pd.read_csv(crosswalk_path)

    for _, row in df.iterrows():
        canonical_key = row["canonical_key"]

        for col_key, col_years in (
            ("pre_2020_row_key", "pre_2020_years"),
            ("post_2020_row_key", "post_2020_years"),
        ):
            raw_keys = str(row.get(col_key, ""))
            raw_years = str(row.get(col_years, ""))
            if not raw_keys or raw_keys == "nan" or not raw_years or raw_years == "nan":
                continue

            keys = [k.strip() for k in raw_keys.split("|")]
            year_ranges = [y.strip() for y in raw_years.split("|")]

            for i, rk in enumerate(keys):
                yr_range = year_ranges[min(i, len(year_ranges) - 1)]
                for year in _parse_year_range(yr_range):
                    lookup[(rk, year)] = canonical_key

    return lookup


# Race columns used for per-race rate computation (includes both eras)
_RATE_RACE_COLS = ["Total", "White", "Black", "Hispanic", "Native American", "Asian", "Other"]

# Rate specifications: (output_canonical_key, numerator_ck, denominator_ck, year_limit_fn)
# year_limit_fn: None = all years; callable = filter years
_RATE_SPECS = [
    ("search-rate",                    "searches",                  "stops",    None),
    ("arrest-rate",                    "arrests",                   "stops",    None),
    ("citation-rate",                  "citations",                 "stops",    None),
    ("contraband-hit-rate",            "contraband-total",          "searches", lambda y: y >= 2020),
    ("resident-stop-rate",             "resident-stops",            "stops",    lambda y: y >= 2020),
    ("equipment-stop-rate",            "reason-for-stop--equipment","stops",    None),
    ("license-stop-rate",              "reason-for-stop--license",  "stops",    None),
    ("moving-stop-rate",               "reason-for-stop--moving",   "stops",    None),
    ("male-driver-rate",               "driver-gender--male",       "stops",    None),
    ("warning-rate",                   "stop-outcome--warning",     "stops",    None),
]


def _compute_agency_rates(combined: pd.DataFrame) -> pd.DataFrame:
    """
    Compute per-agency rate rows from raw counts using canonical_key.

    Produces rows with canonical_key = 'search-rate', 'arrest-rate', etc.
    using row_key 'computed--rates--{rate}' to distinguish from PDF pre-computed
    values.  Both pre-2020 and 2020+ use the same formula; cross-era consistency
    is guaranteed because canonical_key selects the right raw count row per era.

    Contraband-hit-rate is 2020+-only: pre-2020 has no single-row contraband total.
    """
    if "canonical_key" not in combined.columns or combined.empty:
        return pd.DataFrame()

    race_cols = [c for c in _RATE_RACE_COLS if c in combined.columns]
    id_cols = ["agency", "year"]

    # Needed canonical keys for numerators/denominators
    needed_cks = {num for _, num, _, _ in _RATE_SPECS} | {den for _, _, den, _ in _RATE_SPECS}
    base = combined[combined["canonical_key"].isin(needed_cks)][id_cols + race_cols + ["canonical_key"]].copy()

    # Drop duplicate rows per (agency, year, canonical_key) — 2020+ has two row_keys
    # for the same canonical (primary + duplicate); values are identical so keep first.
    base = base.drop_duplicates(subset=[*id_cols, "canonical_key"])

    computed_frames = []
    for rate_ck, num_ck, den_ck, year_filter in _RATE_SPECS:
        num_df = base[base["canonical_key"] == num_ck][id_cols + race_cols].copy()
        den_df = base[base["canonical_key"] == den_ck][id_cols + race_cols].copy()
        if num_df.empty or den_df.empty:
            continue

        merged = num_df.merge(
            den_df, on=id_cols, suffixes=("_num", "_den"), how="inner"
        )
        if year_filter:
            merged = merged[merged["year"].apply(year_filter)]
        if merged.empty:
            continue

        rate_frame = merged[id_cols].copy()
        for col in race_cols:
            num_vals = pd.to_numeric(merged[f"{col}_num"], errors="coerce")
            den_vals = pd.to_numeric(merged[f"{col}_den"], errors="coerce")
            rate_frame[col] = (num_vals / den_vals * 100).where(den_vals > 0)

        rate_frame["canonical_key"] = rate_ck
        rate_frame["row_key"] = f"computed--rates--{rate_ck}"
        rate_frame["table_id"] = "computed-rates"
        rate_frame["table"] = "Computed Rates"
        rate_frame["section_id"] = "rates"
        rate_frame["section"] = "Rates"
        rate_frame["metric_id"] = rate_ck
        rate_frame["metric"] = rate_ck.replace("-", " ").title()
        rate_frame["row_id"] = [
            f"{yr}-{_slugify_simple(ag)}-computed--rates--{rate_ck}"
            for ag, yr in zip(rate_frame["agency"], rate_frame["year"])
        ]
        computed_frames.append(rate_frame)

    if not computed_frames:
        return pd.DataFrame()
    return pd.concat(computed_frames, ignore_index=True)


# ------------------------------------------------------------------------------
# Combine all extracted DataFrame assets into one JSON and DataFrame
# ------------------------------------------------------------------------------
def _normalize_acs_population_rows(combined: pd.DataFrame, log) -> pd.DataFrame:
    if "row_key" not in combined.columns:
        return combined

    row_key_series = combined["row_key"].astype(str)
    pattern = r"^.+--population--\d{4}-(?:acs-)?pop(?:-pct)?$"
    mask = row_key_series.str.match(pattern, na=False)
    if not mask.any():
        return combined

    old_keys = row_key_series[mask]
    new_keys = old_keys.str.replace(r"--\d{4}-(?:acs-)?pop", "--acs-pop", regex=True)
    combined.loc[mask, "row_key"] = new_keys.values

    if "metric_id" in combined.columns:
        combined.loc[mask, "metric_id"] = (
            combined.loc[mask, "metric_id"]
            .astype(str)
            .str.replace(r"^\d{4}-(?:acs-)?pop", "acs-pop", regex=True)
        )

    if "metric" in combined.columns:
        combined.loc[mask, "metric"] = (
            combined.loc[mask, "metric"]
            .astype(str)
            .str.replace(r"^\d{4}\s+(?:ACS\s+)?pop\s*%", "ACS pop %", regex=True)
            .str.replace(r"^\d{4}\s+(?:ACS\s+)?pop", "ACS pop", regex=True)
        )

    if "row_id" in combined.columns:
        row_ids = combined.loc[mask, "row_id"].astype(str)
        combined.loc[mask, "row_id"] = [
            row_id.replace(old_key, new_key) if old_key in row_id else row_id
            for row_id, old_key, new_key in zip(row_ids, old_keys, new_keys)
        ]

    log.info("Normalized ACS population row_key values (%d rows).", int(mask.sum()))
    return combined


def _build_agency_name_lookup(crosswalk_path: Path) -> dict[str, str]:
    """Build a {normalized_name: canonical_name} lookup from the agency crosswalk CSV."""
    try:
        df = pd.read_csv(crosswalk_path)
    except Exception:
        return {}
    lookup: dict[str, str] = {}
    for _, row in df.iterrows():
        normalized = str(row.get("Normalized", "") or "").strip()
        canonical = str(row.get("Canonical", "") or "").strip()
        if normalized and canonical and canonical not in ("None", "nan"):
            lookup[normalized] = canonical
    return lookup


def _normalize_agency_names(combined: pd.DataFrame, crosswalk_path: Path, log) -> pd.DataFrame:
    """Replace raw agency name variants with canonical names using the agency crosswalk."""
    if "agency" not in combined.columns or combined.empty:
        return combined
    lookup = _build_agency_name_lookup(crosswalk_path)
    if not lookup:
        return combined
    original = combined["agency"].copy()
    combined["agency"] = combined["agency"].apply(
        lambda a: lookup.get(_normalize_name(str(a)), a) if pd.notna(a) else a
    )
    updates = int((combined["agency"] != original).sum())
    if updates:
        log.info("Agency name normalization: %d rows updated via crosswalk", updates)
    return combined


@op(
    ins={f"extract_pdf_data_{year}": In(pd.DataFrame) for year in YEAR_URLS},
    out=Out(pd.DataFrame),
    required_resource_keys={"data_dir_processed", "s3"},
)
def combine_reports(context, **extracted_reports: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Concatenate all extract_pdf_data_* assets into a single DataFrame and write combined Parquet."""
    # Merge all DataFrames
    dfs = [df for df in extracted_reports.values() if not df.empty]
    if not dfs:
        raise ValueError("No extracted tables found to combine.")
    combined = pd.concat(dfs, ignore_index=True)
    legacy_cols = ["Key", "Department", "Table name", "Measurement"]
    drop_cols = [col for col in legacy_cols if col in combined.columns]
    if drop_cols:
        context.log.info("Dropping legacy columns from combined output: %s", drop_cols)
        combined = combined.drop(columns=drop_cols)
    if "row_key" not in combined.columns and "slug" in combined.columns:
        context.log.info("Renaming legacy slug column to row_key in combined output.")
        combined = combined.rename(columns={"slug": "row_key"})
    if "slug" in combined.columns:
        combined = combined.drop(columns=["slug"])

    # Collapse pre-2020 'Am. Indian' race column into the 2020+ 'Native American'
    # column so downstream layers only see one. Pre-2020 reports use 'Am. Indian';
    # 2020+ reports use 'Native American'. The two never both populate a row.
    if "Am. Indian" in combined.columns:
        if "Native American" not in combined.columns:
            combined["Native American"] = combined["Am. Indian"]
        else:
            combined["Native American"] = combined["Native American"].combine_first(combined["Am. Indian"])
        combined = combined.drop(columns=["Am. Indian"])

    # Normalize agency names to canonical forms using the agency crosswalk
    agency_crosswalk_path = Path("data/src/agency_crosswalk.csv")
    combined = _normalize_agency_names(combined, agency_crosswalk_path, context.log)

    # Add canonical_key column from crosswalk BEFORE ACS normalization so that
    # year-specific population row_keys (e.g. rates-by-race--population--2019-population)
    # still match the crosswalk entries before they are stripped to --acs-pop.
    crosswalk_path = Path("data/src/canonical_crosswalk.csv")
    if crosswalk_path.exists():
        ck_lookup = _build_canonical_key_lookup(crosswalk_path)
        combined["canonical_key"] = [
            ck_lookup.get((rk, yr))
            for rk, yr in zip(combined["row_key"], combined["year"])
        ]
        mapped = combined["canonical_key"].notna().sum()
        context.log.info(
            "canonical_key: %d/%d rows mapped (%.1f%%)",
            int(mapped), len(combined), 100 * mapped / len(combined) if len(combined) else 0,
        )
    else:
        combined["canonical_key"] = None
        context.log.warning("canonical_crosswalk.csv not found at %s; canonical_key set to None", crosswalk_path)

    combined = _normalize_acs_population_rows(combined, context.log)

    # Compute per-agency rates from raw counts (uniform methodology across eras)
    computed = _compute_agency_rates(combined)
    if not computed.empty:
        combined = pd.concat([combined, computed], ignore_index=True)
        context.log.info(
            "Appended %d computed rate rows (%s)",
            len(computed),
            computed["canonical_key"].value_counts().to_dict(),
        )

    # Write combined Parquet
    processed_dir = Path(context.resources.data_dir_processed.get_path())
    out_file = processed_dir / "all_combined_output.parquet"
    combined.to_parquet(out_file, index=False, engine="pyarrow")
    context.log.info("Wrote combined Parquet: %d rows → %s", len(combined), out_file)

    # Attach local path and optional S3 metadata
    meta = {"local_path": str(out_file)}
    try:
        s3_meta = upload_file_to_s3(
            context,
            out_file,
            f"downloads/{DOWNLOAD_PREFIX}all_combined_output.parquet",
            content_type="application/vnd.apache.parquet",
        )
        if s3_meta:
            meta.update(s3_meta)
            if "s3_uri" in s3_meta:
                context.log.info("Combined Parquet available at %s", s3_meta["s3_uri"])
    except Exception as e:
        context.log.exception("S3 handling encountered an exception for combined Parquet: %s", e)

    # Add the same row-count/uniques metadata as per-year extracts.
    try:
        meta["row_count"] = int(len(combined))
        if "agency" in combined.columns:
            meta["unique_agencies"] = int(combined["agency"].nunique(dropna=True))
        if "row_key" in combined.columns:
            meta["unique_row_keys"] = int(combined["row_key"].nunique(dropna=True))
        context.add_output_metadata(meta)
        context.log.info("combine_all_reports metadata: %s", meta)
    except Exception:
        pass

    return combined


@graph_asset(
    name="combine_all_reports",
    group_name="processed",
    ins={
        f"extract_pdf_data_{year}": AssetIn(key=AssetKey(f"extract_pdf_data_{year}"))
        for year in YEAR_URLS
    },
    description="Combine all per-year extract_pdf_data_* assets into a single JSON and DataFrame."
)
def combine_all_reports(**extracted_reports: pd.DataFrame) -> pd.DataFrame:
    return combine_reports(**extracted_reports)


@multi_asset(
    outs={
        f"agency_response_{year}": AssetOut(metadata={"url": url})
        for year, url in AGENCY_RESPONSE_URLS.items()
    },
    group_name="responses",
    can_subset=True,
    required_resource_keys={"data_dir_source"},
    description="Download VSR agency response PDFs for each year.",
)
def download_agency_responses(context):
    selected = (
        set(context.selected_output_names)
        if context.selected_output_names
        else set(f"agency_response_{year}" for year in AGENCY_RESPONSE_URLS)
    )
    out_dir = Path(context.resources.data_dir_source.get_path()) / "agency_comments"
    out_dir.mkdir(parents=True, exist_ok=True)

    for year, url in AGENCY_RESPONSE_URLS.items():
        output_name = f"agency_response_{year}"
        if output_name not in selected:
            context.log.debug("Skipping %s (not selected)", output_name)
            continue
        pdf_path = out_dir / f"VSR_agency_comments_{year}.pdf"
        if not pdf_path.exists():
            context.log.info("Downloading agency responses %s → %s", url, pdf_path)
            resp = requests.get(url, timeout=(10, 300))
            resp.raise_for_status()
            pdf_path.write_bytes(resp.content)
        else:
            context.log.debug("Using cached %s", pdf_path)
        yield Output(
            str(pdf_path),
            output_name=output_name,
            metadata={"url": url, "local_path": str(pdf_path), "year": year},
        )


@op(
    out=Out(pd.DataFrame),
    ins={f"agency_response_{year}": In(str) for year in AGENCY_RESPONSE_URLS},
    required_resource_keys={"data_dir_processed"},
)
def parse_agency_comments(context, **response_paths: dict[str, str]) -> pd.DataFrame:
    records: list[dict] = []
    for year, url in AGENCY_RESPONSE_URLS.items():
        key = f"agency_response_{year}"
        path_str = response_paths.get(key)
        if not path_str:
            context.log.warning("Missing agency response path for %s", year)
            continue
        pdf_path = Path(path_str)
        text_path = _ensure_pdftotext(pdf_path, context.log)
        if text_path is None:
            context.log.error("Skipping agency comments for %s (pdftotext failed)", year)
            continue
        try:
            lines = text_path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except Exception as exc:
            context.log.error("Failed reading %s: %s", text_path, exc)
            continue
        records.extend(
            _parse_agency_comments(
                lines,
                year=year,
                pdf_name=pdf_path.name,
                url=url,
                log=context.log,
            )
        )

    comments = pd.DataFrame(records)
    processed_dir = Path(context.resources.data_dir_processed.get_path())
    out_path = processed_dir / "agency_comments.parquet"
    comments.to_parquet(out_path, index=False, engine="pyarrow")
    context.log.info("Wrote agency comments Parquet → %s (%d rows)", out_path, len(comments))

    try:
        meta = {"local_path": str(out_path), "row_count": len(comments)}
        if not comments.empty:
            meta["agency_count"] = int(comments["agency"].nunique(dropna=True))
            meta["year_count"] = int(comments["year"].nunique(dropna=True))
        context.add_output_metadata(meta)
    except Exception:
        pass
    return comments


@graph_asset(
    name="agency_comments",
    group_name="processed",
    ins={f"agency_response_{year}": AssetIn(key=AssetKey(f"agency_response_{year}")) for year in AGENCY_RESPONSE_URLS},
    description="Parse agency response PDFs into a per-agency comment table.",
)
def agency_comments(**response_paths: str) -> pd.DataFrame:
    return parse_agency_comments(**response_paths)
