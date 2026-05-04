from __future__ import annotations

import os
import re
import subprocess
import unicodedata
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests
from PyPDF2 import PdfReader
from slugify import slugify

from dagster import (
    AssetIn,
    AssetOut,
    AssetKey,
    In,
    Out,
    DynamicOut,
    DynamicOutput,
    Output,
    graph_asset,
    multi_asset,
    op,
)

# ------------------------------------------------------------------------------
# Configure the years you want Dagster to ingest
# ------------------------------------------------------------------------------
YEAR_URLS: Dict[int, str] = {
    2024: "https://ago.mo.gov/wp-content/uploads/2024-VSR-Agency-Specific-Reports.pdf",
    2023: "https://ago.mo.gov/wp-content/uploads/VSRreport2023.pdf",
    2022: "https://ago.mo.gov/wp-content/uploads/vsrreport2022.pdf",
    2021: "https://ago.mo.gov/wp-content/uploads/2021-VSR-Agency-Specific-Report.pdf",
    2020: "https://ago.mo.gov/wp-content/uploads/2020-VSR-Agency-Specific-Report.pdf",
    # Pre-2020 format (different layout — see _parse_pre2020_pdftotext_lines)
    2019: "https://ago.mo.gov/wp-content/uploads/2019agencyreports.pdf",
    2018: "https://ago.mo.gov/wp-content/uploads/2018agencyreports.pdf",
    2017: "https://ago.mo.gov/wp-content/uploads/2017agencyreports.pdf",
    2016: "https://ago.mo.gov/wp-content/uploads/2016agencyreports.pdf",
    2015: "https://ago.mo.gov/wp-content/uploads/2015agencyreports.pdf",
    2014: "https://ago.mo.gov/wp-content/uploads/2014agencyreports.pdf",
    2013: "https://ago.mo.gov/wp-content/uploads/2013-agency-reports.pdf",
    2012: "https://ago.mo.gov/wp-content/uploads/2012agencyreport.pdf",
    2011: "https://ago.mo.gov/wp-content/uploads/2011agencyreport.pdf",
    2010: "https://ago.mo.gov/wp-content/uploads/2010agencyreports.pdf",
    2009: "https://ago.mo.gov/wp-content/uploads/2009agencyreports.pdf",
    2008: "https://ago.mo.gov/wp-content/uploads/2008agencyreports.pdf",
    2007: "https://ago.mo.gov/wp-content/uploads/2007agencyreports.pdf",
    2006: "https://ago.mo.gov/wp-content/uploads/2006agencyreports.pdf",
    2005: "https://ago.mo.gov/wp-content/uploads/2005agencyreports.pdf",
    2004: "https://ago.mo.gov/wp-content/uploads/2004agencyreports.pdf",
    2003: "https://ago.mo.gov/wp-content/uploads/2003agencyreports.pdf",
    2002: "https://ago.mo.gov/wp-content/uploads/2002agencyreports.pdf",
    2001: "https://ago.mo.gov/wp-content/uploads/2001agencyreports.pdf",
    2000: "https://ago.mo.gov/wp-content/uploads/2000agencyreports.pdf",
}

# ------------------------------------------------------------------------------
# Download PDFs (one logical asset per year)
# ------------------------------------------------------------------------------
@multi_asset(
    outs={str(year): AssetOut(metadata={"url": url}, is_required=False) for year, url in YEAR_URLS.items()},
    group_name="reports",
    can_subset=True,
    required_resource_keys={"data_dir_report_pdfs"},
    description="Download VSR PDFs for every configured year.",
)
def download_reports(context):
    # figure out which outputs we should actually produce
    selected = (
        set(context.selected_output_names)
        if context.selected_output_names
        else set(str(y) for y in YEAR_URLS)
    )

    out_dir = Path(context.resources.data_dir_report_pdfs.get_path())

    for year, url in YEAR_URLS.items():
        name = str(year)
        if name not in selected:
            context.log.debug("Skipping %s (not selected)", name)
            continue

        # If the file already exists locally, re-use it and skip the download
        # Otherwise, download to the reports directory.

        out_path = out_dir / f"VSRreport{year}.pdf"

        # only hit the network if we haven't already downloaded it
        if not out_path.exists():
            context.log.info("Downloading %s → %s", url, out_path)
            # (10 s to connect, 300 s to read)
            resp = requests.get(url, timeout=(10, 300))
            resp.raise_for_status()
            out_path.write_bytes(resp.content)
        else:
            context.log.debug("Using cached %s", out_path)

        yield Output(
            str(out_path),
            output_name=name,
            metadata={"mode": "reports", "url": url, "local_path": str(out_path)},
        )

# ------------------------------------------------------------------------------
# PDF parsing helpers
# ------------------------------------------------------------------------------

def _env_positive_int(name: str, default: int) -> int:
    """Parse a positive int from env var *name*, falling back to *default*."""
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        val = int(raw)
        return val if val > 0 else default
    except ValueError:
        return default


PAGE_CHUNK_SIZE = _env_positive_int("PAGE_CHUNK_SIZE", 5)  # tune as needed

FINAL_COLUMNS = [
    "Key",
    "Total",
    "White",
    "Black",
    "Hispanic",
    "Native American",
    "Asian",
    "Other",
    "Department",
    "Table name",
    "Measurement",
]
NUMERIC_COLS = FINAL_COLUMNS[1:8]
PIVOT_VALUE_COLUMNS = NUMERIC_COLS
EXTRACT_COLUMNS = [
    "Total",
    "White",
    "Black",
    "Hispanic",
    "Native American",
    "Asian",
    "Other",
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

TABLE_SLUG_LOOKUP = {
    "Rates by Race": "rates",
    "Number of Stops by Race": "stops",
    "Search statistics": "search",
}

RACE_COLUMNS = [
    "Total",
    "White",
    "Black",
    "Hispanic",
    "Native American",
    "Asian",
    "Other",
]

# Pre-2020 PDFs use "Am. Indian" and put Asian before Am. Indian
RACE_COLUMNS_PRE2020 = [
    "Total",
    "White",
    "Black",
    "Hispanic",
    "Asian",
    "Am. Indian",
    "Other",
]

EXTRACT_COLUMNS_PRE2020 = [
    "Total",
    "White",
    "Black",
    "Hispanic",
    "Asian",
    "Am. Indian",
    "Other",
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

def _normalize_text(text: str) -> str:
    """Strip ligatures & smart quotes to plain ASCII."""
    text = unicodedata.normalize("NFKC", text)
    return text.replace("ﬀ", "ff").replace("’", "'").replace(".", "")


# Accept numbers like 10, 10.5, 1,234.56, and leading-dot decimals like .27, with optional trailing %
_NUM_RE = re.compile(r"^(?:\d[\d,]*\.?\d*|\.\d+)%?$")
_DOTS_RE = re.compile(r"^[\.·]{2,}$")
_NUM_TOKEN_RE = re.compile(r"^(?:\d+\.\d+|\d+|\.\d+|\.)$")


def _is_numeric(tok: str) -> bool:
    tok = tok.strip()
    # Treat only real numbers (with optional commas/decimals) as numeric.
    # Dot leaders like "." are NOT considered numeric to avoid false positives.
    return bool(_NUM_RE.match(tok))


def _clean_numeric_str(tok: str) -> str:
    if tok is None or (isinstance(tok, float) and pd.isna(tok)):
        return ""
    t = str(tok).strip().replace(",", "")
    return t[:-1] if t.endswith("%") else t


def _slugify_simple(value: str) -> str:
    text = value.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-{2,}", "-", text)
    return text.strip("-")


def _slugify_metric(value: str) -> str:
    return _slugify_simple(value.replace("%", "pct"))


def _build_metric_id(metric: str) -> str:
    return _slugify_metric(metric)


def _build_section_id(section: str | None) -> str | None:
    if not section:
        return None
    return _slugify_metric(section)


def _build_row_key(table_id: str, section_id: str | None, metric_id: str) -> str:
    parts = [table_id]
    if section_id:
        parts.append(section_id)
    parts.append(metric_id)
    return "--".join(parts)


def _build_row_id(
    year: int | None,
    agency: str,
    table_id: str,
    section_id: str | None,
    metric_id: str,
) -> str:
    row_key = _build_row_key(table_id, section_id, metric_id)
    parts = [str(year)] if year is not None else []
    parts.append(_slugify_simple(agency))
    parts.append(row_key)
    return "-".join(parts)


def _normalize_line(line: str) -> str:
    text = unicodedata.normalize("NFKC", line)
    text = text.replace("’", "'").replace("“", '"').replace("”", '"')
    return text.replace("\x0c", "").strip()


def _clean_agency_name(name: str) -> str:
    text = _normalize_text(name)
    text = text.replace(" 's", "'s").strip()
    text = re.sub(r"\s+", " ", text)
    return text


def _is_page_number(line: str) -> bool:
    stripped = line.strip()
    return stripped.isdigit()


def _is_race_header(line: str) -> bool:
    tokens = [t.strip() for t in line.split() if t.strip()]
    target = ["total", "white", "black", "hispanic", "native", "american", "asian", "other"]
    idx = 0
    for tok in tokens:
        if idx < len(target) and tok.lower() == target[idx]:
            idx += 1
            if idx == len(target):
                return True
    return False


def _normalize_numeric_token(tok: str, *, allow_dot: bool) -> str | None:
    cleaned = tok.strip().replace(",", "")
    if cleaned.endswith("%"):
        cleaned = cleaned[:-1]
    if not allow_dot and cleaned == ".":
        return None
    return cleaned if _NUM_TOKEN_RE.match(cleaned) else None


def _parse_section_header(line: str) -> str | None:
    tokens = [t for t in line.split() if t]
    if not tokens:
        return None
    if any(_normalize_numeric_token(tok, allow_dot=False) for tok in tokens):
        return None
    if not any(_DOTS_RE.match(tok) or tok == "." for tok in tokens):
        return None
    label_tokens = [tok for tok in tokens if not (_DOTS_RE.match(tok) or tok == ".")]
    label = " ".join(label_tokens).strip()
    return label or None


def _parse_metric_line(line: str) -> tuple[str, list[str | None]] | None:
    tokens = [t for t in line.split() if t]
    if not tokens:
        return None

    numeric_values: list[str | None] = []
    first_numeric_idx: int | None = None
    for idx in range(len(tokens) - 1, -1, -1):
        token = tokens[idx]
        cleaned = _normalize_numeric_token(token, allow_dot=True)
        if cleaned is None:
            continue
        numeric_values.append(None if cleaned == "." else cleaned)
        first_numeric_idx = idx
        if len(numeric_values) == 7:
            break

    if len(numeric_values) < 7 or first_numeric_idx is None:
        return None

    numeric_values = list(reversed(numeric_values))
    label_tokens = [
        tok for tok in tokens[:first_numeric_idx] if not (_DOTS_RE.match(tok) or tok == ".")
    ]
    label = " ".join(label_tokens).strip()
    if not label:
        return None
    return label, numeric_values


def _should_end_table(line: str) -> bool:
    lowered = line.lower()
    return lowered.startswith("notes:") or lowered.startswith("agency notes:") or lowered.startswith("figure")


def _match_column_header(line: str) -> str | None:
    normalized = re.sub(r"\s+", " ", line).strip().lower()
    for col in RACE_COLUMNS:
        if normalized == col.lower():
            return col
    return None


_KNOWN_SECTIONS = {
    "population",
    "totals",
    "rates",
    "reason for stop",
    "stop outcome",
    "citation/warning violation",
    "arrest violation",
    "officer assignment",
    "location of stop",
    "driver gender",
    "driver age",
    "stops",
    "disparity index",
}


def _parse_plain_section_header(line: str) -> str | None:
    if any(_normalize_numeric_token(tok, allow_dot=False) for tok in line.split()):
        return None
    normalized = re.sub(r"\s+", " ", line).strip()
    if not normalized:
        return None
    if normalized.lower() in _KNOWN_SECTIONS:
        return normalized
    return None


# ------------------------------------------------------------------------------
# Pre-2020 parser helpers
# ------------------------------------------------------------------------------

# Null placeholder tokens that appear in numeric columns (treat as None slots)
_LEGACY_NULL_RE = re.compile(r"^(N/A|#Num!|#NUM!|#Error|#ERROR|#VALUE!|#Div/0!|#DIV/0!)$", re.IGNORECASE)

# Banner lines that signal the start of a new agency block
_PRE2020_BANNER_RE = re.compile(
    r"^(RACIAL PROFILING DATA[/\s]\d{4}|VEHICLE STOPS DATA\s+\d{4})\s*$",
    re.IGNORECASE,
)

# Agency name line after the banner: optionally prefixed with "Agency:"
# and suffixed with population info. Strip both.
_PRE2020_AGENCY_RE = re.compile(
    r"^(?:Agency:\s*)?(.+?)\s+(?:\d{4}\s+)?Population:.*$",
    re.IGNORECASE,
)

# Page-continuation header like "Adair County Sheriff's Dept.: page 2"
_PRE2020_PAGE_CONT_RE = re.compile(r"^(.+?):\s+page\s+\d+\s*$", re.IGNORECASE)

# Table title keywords (all-caps headers in pre-2020 PDFs)
_PRE2020_TABLES = {
    "KEY INDICATORS": ("Key Indicators", "key-indicators"),
    "VEHICLE STOP STATS": ("Vehicle Stop Stats", "vehicle-stop-stats"),
    "SEARCH STATS": ("Search Stats", "search-stats"),
}

# Section triggers scoped to each table type.
# KEY INDICATORS has no sub-sections — never apply triggers there.
_STOP_STATS_TRIGGERS: Dict[str, str | None] = {
    "reason": "Reason for stop",
    "stop": "Stop outcome",
    "location": "Location of stop",
    "driver": None,  # disambiguated by metric below
}

_SEARCH_STATS_TRIGGERS: Dict[str, str | None] = {
    "probable": "Probable cause/authority to search",
    "what": "What searched",
    "search": "Search duration",
    "contra-": "Contraband found",
    "arrest": "Arrest charge",
}

# "Probable cause/authority to search" and "Contraband found" span 3 PDF lines;
# the secondary label fragments appear on the same line as the next metric.
# Strip these tokens when they lead the label so they don't pollute metric names.
_SEARCH_STATS_SECONDARY_FRAGMENTS = {"authority", "band", "found"}

# Second words of multi-line section headers that appear on a no-value line AFTER the
# trigger word. When seen while a pending_metric_label is set, the remainder of the
# line should be appended to pending_metric_label rather than replacing it.
# e.g. 2000 format: "Arrest Outstanding\ncharge warrant\n[values]"
#   → section trigger "arrest" fires, pending = "Outstanding"
#   → "charge warrant" → "charge" is a header continuation → append "warrant" → pending = "Outstanding warrant"
_SEARCH_STATS_HEADER_CONTINUATIONS = {"charge"}

_PRE2020_DRIVER_GENDER_METRICS = {"male", "female"}


def _is_pre2020_race_header(line: str) -> bool:
    """Detect the pre-2020 race column header (Asian before Am. Indian)."""
    tokens = [t.strip().lower() for t in line.split() if t.strip()]
    target = ["total", "white", "black", "hispanic", "asian", "am.", "indian", "other"]
    idx = 0
    for tok in tokens:
        if idx < len(target) and tok == target[idx]:
            idx += 1
            if idx == len(target):
                return True
    return False


def _parse_legacy_metric_line(
    line: str,
) -> tuple[str, list[str | None]] | None:
    """
    Like _parse_metric_line but treats N/A and #Num! as null slots.

    Accepts rows with exactly 6 numeric/null tokens when the PDF wraps a value
    to the next line.  Heuristic for padding to 7 columns:
    - If the leftmost found value is a null token (N/A) → Total is present but
      Am. Indian (position 5) is blank → insert None at position 5.
    - Otherwise → Total is missing (wrapped to next line) → insert None at
      position 0 (Total).
    """
    tokens = [t for t in line.split() if t]
    if not tokens:
        return None

    values: list[str | None] = []
    first_value_idx: int | None = None

    for idx in range(len(tokens) - 1, -1, -1):
        tok = tokens[idx]
        if _LEGACY_NULL_RE.match(tok):
            values.append(None)
            first_value_idx = idx
        else:
            cleaned = _normalize_numeric_token(tok, allow_dot=True)
            if cleaned is None:
                break
            values.append(None if cleaned == "." else cleaned)
            first_value_idx = idx
        if len(values) == 7:
            break

    if len(values) < 6 or first_value_idx is None:
        return None

    values = list(reversed(values))

    if len(values) == 6:
        # Heuristic: was the leftmost value a null token (e.g. N/A)?
        first_tok = tokens[first_value_idx]
        if _LEGACY_NULL_RE.match(first_tok):
            # Total present (N/A), Am. Indian column blank → pad at index 5
            values.insert(5, None)
        else:
            # Total wrapped to next line → pad at index 0
            values.insert(0, None)

    label_tokens = [
        t for t in tokens[:first_value_idx]
        if not (_DOTS_RE.match(t) or t == ".")
    ]
    label = " ".join(label_tokens).strip()
    if not label:
        return None
    return label, values


def _split_pre2020_section_metric(
    label: str,
    current_section: str | None,
    table_id: str | None,
) -> tuple[str, str | None]:
    """
    Given a raw label from a pre-2020 data row, split off any leading section
    trigger word and return (metric, new_section).

    Rules vary by table:
    - key-indicators: no sub-sections; return unchanged.
    - vehicle-stop-stats: apply _STOP_STATS_TRIGGERS.
    - search-stats: apply _SEARCH_STATS_TRIGGERS; also strip secondary
      fragments ("authority", "band", "found") that bleed from multi-line
      section labels onto adjacent metric lines.
    """
    if table_id == "key-indicators":
        return label, current_section

    tokens = label.split()
    if not tokens:
        return label, current_section

    trigger = tokens[0].lower()

    if table_id == "vehicle-stop-stats":
        trigger_map = _STOP_STATS_TRIGGERS
        secondary_frags: set[str] = set()
    elif table_id == "search-stats":
        trigger_map = _SEARCH_STATS_TRIGGERS
        secondary_frags = _SEARCH_STATS_SECONDARY_FRAGMENTS
    else:
        return label, current_section

    # Strip secondary section-label fragments (e.g. "authority", "found")
    # Also handle two-word fragment "to search" (4th line of "Probable cause/authority to search")
    if trigger in secondary_frags:
        metric = " ".join(tokens[1:]).strip()
        return metric or label, current_section
    if (
        table_id == "search-stats"
        and len(tokens) >= 2
        and tokens[0].lower() == "to"
        and tokens[1].lower() == "search"
    ):
        metric = " ".join(tokens[2:]).strip()
        return metric or label, current_section

    if trigger not in trigger_map:
        return label, current_section

    full_section = trigger_map[trigger]
    metric = " ".join(tokens[1:]).strip()

    if full_section is None:
        # "driver" — disambiguate by first metric token
        metric_lower = metric.lower()
        if any(metric_lower.startswith(g) for g in _PRE2020_DRIVER_GENDER_METRICS):
            full_section = "Driver gender"
        else:
            full_section = "Driver age"

    return metric, full_section


def _extract_values_only(line: str) -> list[str | None] | None:
    """
    If the line contains ONLY numeric/null tokens (no text label), return the
    rightmost 7 values; otherwise return None.

    Used to handle the "Reasonable suspicion-weapon" pattern where the data
    appears on a line between two label-fragment lines.
    """
    tokens = [t for t in line.split() if t]
    if not tokens:
        return None
    values: list[str | None] = []
    for tok in tokens:
        if _LEGACY_NULL_RE.match(tok):
            values.append(None)
        else:
            cleaned = _normalize_numeric_token(tok, allow_dot=True)
            if cleaned is None:
                return None  # text found — not a values-only line
            values.append(None if cleaned == "." else cleaned)
    return values[-7:] if len(values) >= 7 else None


def _parse_pre2020_pdftotext_lines(
    lines: list[str], log, *, year: int | None, pdf_name: str
) -> pd.DataFrame:
    """
    State-machine parser for pre-2020 VSR PDFs (2014–2019 layout).

    Key differences from the 2020+ parser:
    - Agency detected via banner line (RACIAL PROFILING DATA / VEHICLE STOPS DATA)
      followed by an "Agency: name  Population: ..." line.
    - Tables identified by all-caps keywords (KEY INDICATORS, VEHICLE STOP STATS,
      SEARCH STATS) rather than "Table N: … for …".
    - Race column order: Total White Black Hispanic Asian Am. Indian Other.
    - Section labels are inline: trigger word on the same line as the first metric row,
      continuation text (e.g. "for stop") on the next line with no data.
    - N/A and #Num! are null placeholders in numeric columns.
    - Table ends on "Notes:" or "Agency response".
    """
    # Parser state
    expect_agency_name = False
    current_agency: str | None = None
    current_table_title: str | None = None
    current_table_id: str | None = None
    current_section: str | None = None
    has_columns = False
    tables_allowed = False  # True only after banner + confirmed agency; blocks appendix tables
    pending_metric_label: str | None = None  # for multi-line labels (e.g. "Reasonable / [data] / suspicion-weapon")

    records: list[dict] = []
    unparsed_counts: dict[tuple[str | None, str | None], int] = {}
    record_counts: dict[tuple[str | None, str | None], int] = {}
    stats = {
        "total_lines": 0,
        "blank_lines": 0,
        "banner_lines": 0,
        "agency_headers": 0,
        "table_headers": 0,
        "race_headers": 0,
        "section_continuations": 0,
        "metric_rows": 0,
        "notes_lines": 0,
        "unparsed_lines": 0,
    }

    def _emit(metric_label: str, values: list[str | None], section: str | None) -> None:
        if not current_table_id or not current_agency:
            return
        section_id = _build_section_id(section)
        metric_id = _build_metric_id(metric_label)
        records.append({
            "Total": values[0],
            "White": values[1],
            "Black": values[2],
            "Hispanic": values[3],
            "Asian": values[4],
            "Am. Indian": values[5],
            "Other": values[6],
            "year": year,
            "row_key": _build_row_key(current_table_id, section_id, metric_id),
            "agency": current_agency,
            "table": current_table_title,
            "table_id": current_table_id,
            "section": section,
            "section_id": section_id,
            "metric": metric_label,
            "metric_id": metric_id,
            "row_id": _build_row_id(year, current_agency, current_table_id, section_id, metric_id),
        })
        key = (current_agency, current_table_id)
        record_counts[key] = record_counts.get(key, 0) + 1
        stats["metric_rows"] += 1

    for raw in lines:
        stats["total_lines"] += 1
        line = _normalize_line(raw)

        if not line:
            stats["blank_lines"] += 1
            continue

        # Lone page-number lines (digits only) — skip everywhere
        if _is_page_number(line):
            continue

        # ── Banner line: signals a new agency block follows ──────────────────
        if _PRE2020_BANNER_RE.match(line):
            stats["banner_lines"] += 1
            expect_agency_name = True
            tables_allowed = False  # wait for agency confirmation
            current_table_title = None
            current_table_id = None
            current_section = None
            has_columns = False
            continue

        # ── Agency name line (immediately after banner) ───────────────────────
        if expect_agency_name:
            # Skip page-continuation headers ("Dept.: page 2")
            if _PRE2020_PAGE_CONT_RE.match(line):
                continue
            m = _PRE2020_AGENCY_RE.match(line)
            if m:
                current_agency = _clean_agency_name(m.group(1))
                tables_allowed = True
                stats["agency_headers"] += 1
                log.debug("Agency: %s (%s)", current_agency, pdf_name)
                expect_agency_name = False
                continue
            # If it doesn't match the agency pattern, still stop expecting
            expect_agency_name = False

        # Skip until we have an agency
        if not current_agency:
            continue

        # ── Page-continuation header (page 2, 3 …) ───────────────────────────
        if _PRE2020_PAGE_CONT_RE.match(line):
            # Keep current agency but reset table state
            current_table_title = None
            current_table_id = None
            current_section = None
            has_columns = False
            continue

        # ── Table header ──────────────────────────────────────────────────────
        # Table headers have the race columns on the same line, so match by prefix.
        upper = line.strip().upper()
        matched_table = False
        for key, (title, tid) in _PRE2020_TABLES.items():
            if upper == key or upper.startswith(key + " ") or upper.startswith(key + "\t"):
                if tables_allowed:
                    current_table_title = title
                    current_table_id = tid
                    current_section = None
                    has_columns = True  # race columns are on the same line as the header
                    stats["table_headers"] += 1
                matched_table = True
                break

        if matched_table:
            continue

        # Skip until we have a table
        if not current_table_title:
            continue

        # ── End-of-table markers ──────────────────────────────────────────────
        low = line.lower()
        if low.startswith("notes:") or low.startswith("agency response"):
            stats["notes_lines"] += 1
            if low.startswith("agency response"):
                tables_allowed = False  # this agency's block is complete
            current_table_title = None
            current_table_id = None
            current_section = None
            has_columns = False
            pending_metric_label = None
            continue

        # ── Race column header ────────────────────────────────────────────────
        if _is_pre2020_race_header(line):
            has_columns = True
            stats["race_headers"] += 1
            continue

        if not has_columns:
            continue

        # ── Data row ──────────────────────────────────────────────────────────
        parsed = _parse_legacy_metric_line(line)
        if parsed:
            raw_label, values = parsed
            if not raw_label.strip() and pending_metric_label:
                raw_label = pending_metric_label
            pending_metric_label = None
            metric, new_section = _split_pre2020_section_metric(raw_label, current_section, current_table_id)
            if new_section != current_section:
                current_section = new_section
            if metric:
                _emit(metric, values, current_section)
            continue

        # ── Values-only line (label appears on a different line) ──────────────
        # Handles "Reasonable\n  [data line]\nsuspicion-weapon" pattern.
        if pending_metric_label:
            values_only = _extract_values_only(line)
            if values_only is not None:
                metric, new_section = _split_pre2020_section_metric(
                    pending_metric_label, current_section, current_table_id
                )
                pending_metric_label = None
                if new_section != current_section:
                    current_section = new_section
                if metric:
                    _emit(metric, values_only, current_section)
                continue

        # ── No-number lines: section continuation or pending metric label ─────
        # Save the stripped text as a candidate label.  If the next data line
        # has no label of its own (values-only), this becomes the metric name.
        # This handles e.g. "Reasonable\n  [data]\nsuspicion-weapon".
        # Also fires section triggers that appear on no-value lines (e.g. 2000
        # "Arrest Outstanding" / "charge warrant" / "[values]" pattern).
        tokens = [t for t in line.split() if t]
        if tokens and not any(_is_numeric(t) for t in tokens):
            stats["section_continuations"] += 1
            trigger = tokens[0].lower()
            # Fire section triggers even when the line has no values
            if current_table_id == "search-stats" and trigger in _SEARCH_STATS_TRIGGERS:
                new_section = _SEARCH_STATS_TRIGGERS[trigger]
                if new_section is not None:
                    current_section = new_section
                remaining = " ".join(tokens[1:]).strip()
                pending_metric_label = remaining if remaining else None
                continue
            # Append to pending label when this is a multi-line section-header continuation
            # (e.g. "charge" as the second word of "Arrest charge")
            if (
                current_table_id == "search-stats"
                and pending_metric_label is not None
                and trigger in _SEARCH_STATS_HEADER_CONTINUATIONS
            ):
                remaining = " ".join(tokens[1:]).strip()
                if remaining:
                    pending_metric_label = pending_metric_label + " " + remaining
                continue
            pending_metric_label = line  # already stripped by _normalize_line
            continue

        # ── Unparsed ──────────────────────────────────────────────────────────
        key = (current_agency, current_table_id)
        count = unparsed_counts.get(key, 0)
        if count < 5:
            log.warning(
                "Pre-2020 unparsed line (%s, %s, %s): %s",
                pdf_name, current_agency, current_table_id, line,
            )
        unparsed_counts[key] = count + 1
        stats["unparsed_lines"] += 1

    if record_counts:
        sample = sorted(record_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        log.info("Pre-2020 parsed records by agency/table (top 5): %s", sample)
    if unparsed_counts:
        worst = sorted(unparsed_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        log.warning("Pre-2020 unparsed line counts (top 5): %s", worst)
    log.info("Pre-2020 parser summary (%s): %s", pdf_name, {k: v for k, v in stats.items() if v})

    df = pd.DataFrame(records)
    if df.empty:
        return pd.DataFrame(columns=EXTRACT_COLUMNS_PRE2020)

    df = df.reindex(columns=EXTRACT_COLUMNS_PRE2020)
    numeric_cols = ["Total", "White", "Black", "Hispanic", "Asian", "Am. Indian", "Other"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col].map(_clean_numeric_str), errors="coerce")
    return df


# ------------------------------------------------------------------------------
def _ensure_pdftotext(pdf_path: Path, log) -> Path | None:
    txt_path = pdf_path.with_suffix(".layout.txt")
    try:
        if txt_path.exists() and txt_path.stat().st_mtime >= pdf_path.stat().st_mtime:
            log.debug("Using cached pdftotext output → %s", txt_path)
            return txt_path
    except Exception:
        pass

    cmd = ["pdftotext", "-layout", "-enc", "UTF-8", str(pdf_path), str(txt_path)]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        if result.stderr:
            log.debug("pdftotext stderr: %s", result.stderr.strip())
        return txt_path
    except FileNotFoundError:
        log.error("pdftotext not found on PATH; install poppler-utils to enable text parsing.")
    except subprocess.CalledProcessError as exc:
        log.error("pdftotext failed: %s", exc.stderr.strip() if exc.stderr else exc)
    return None


def _parse_pdftotext_lines(lines: list[str], log, *, year: int | None, pdf_name: str) -> pd.DataFrame:
    agency_re = re.compile(r"^\d+\.\d+\s+(.*\S)\s*$")
    table_re = re.compile(r"^(Table\s+\d+:\s+.+?)\s+for\s+(.+?)\s*$")

    armed = False
    current_agency: str | None = None
    current_table_title: str | None = None
    current_table_id: str | None = None
    current_section: str | None = None
    current_columns: list[str] | None = None
    pending_table_line: str | None = None
    column_mode = False
    column_row_labels: list[tuple[str, str | None]] = []
    column_data: dict[str, list[str | None]] = {}
    column_current: str | None = None

    records: list[dict] = []
    unparsed_counts: dict[tuple[str | None, str | None], int] = {}
    record_counts: dict[tuple[str | None, str | None], int] = {}
    stats = {
        "total_lines": 0,
        "blank_lines": 0,
        "page_number_lines": 0,
        "front_matter_lines": 0,
        "armed_lines": 0,
        "agency_headers": 0,
        "table_headers": 0,
        "section_headers": 0,
        "metric_rows": 0,
        "unparsed_lines": 0,
        "notes_lines": 0,
        "figure_lines": 0,
    }

    def _flush_columnar_records() -> None:
        nonlocal column_mode, column_row_labels, column_data, column_current
        if not column_row_labels or not column_data:
            column_mode = False
            column_row_labels = []
            column_data = {}
            column_current = None
            return
        if not current_table_id or not current_agency:
            column_mode = False
            column_row_labels = []
            column_data = {}
            column_current = None
            return
        for idx, (metric_label, section_label) in enumerate(column_row_labels):
            values = []
            for col in RACE_COLUMNS:
                col_values = column_data.get(col, [])
                if idx < len(col_values):
                    values.append(col_values[idx])
                else:
                    values.append(None)
            section_id = _build_section_id(section_label)
            metric_id = _build_metric_id(metric_label)
            record = {
                "Total": values[0],
                "White": values[1],
                "Black": values[2],
                "Hispanic": values[3],
                "Native American": values[4],
                "Asian": values[5],
                "Other": values[6],
                "year": year,
                "row_key": _build_row_key(current_table_id, section_id, metric_id),
                "agency": current_agency,
                "table": current_table_title,
                "table_id": current_table_id,
                "section": section_label,
                "section_id": section_id,
                "metric": metric_label,
                "metric_id": metric_id,
                "row_id": _build_row_id(
                    year,
                    current_agency,
                    current_table_id,
                    section_id,
                    metric_id,
                ),
            }
            records.append(record)
            key = (current_agency, current_table_id)
            record_counts[key] = record_counts.get(key, 0) + 1
            stats["metric_rows"] += 1
        column_mode = False
        column_row_labels = []
        column_data = {}
        column_current = None

    for raw in lines:
        stats["total_lines"] += 1
        line = _normalize_line(raw)
        if not line:
            stats["blank_lines"] += 1
            continue
        if _is_page_number(line) and current_table_title is None and not column_mode:
            stats["page_number_lines"] += 1
            continue

        if not armed:
            if "agency results" in line.lower():
                armed = True
                log.info("Parser armed at Agency Results (%s)", pdf_name)
                continue
            agency_match = agency_re.match(line)
            if agency_match:
                armed = True
                current_agency = _clean_agency_name(agency_match.group(1))
                stats["agency_headers"] += 1
                log.info("Parser armed at agency header (%s): %s", pdf_name, current_agency)
                current_table_title = None
                current_table_id = None
                current_section = None
                current_columns = None
                continue
            stats["front_matter_lines"] += 1
            continue
        else:
            stats["armed_lines"] += 1

        agency_match = agency_re.match(line)
        if agency_match:
            _flush_columnar_records()
            current_agency = _clean_agency_name(agency_match.group(1))
            stats["agency_headers"] += 1
            current_table_title = None
            current_table_id = None
            current_section = None
            current_columns = None
            pending_table_line = None
            continue

        table_line = line
        if pending_table_line:
            table_line = f"{pending_table_line} {line}".strip()
        table_match = table_re.match(table_line)
        if table_match:
            _flush_columnar_records()
            full_title = table_match.group(1)
            raw_title = re.sub(r"^Table\s+\d+:\s*", "", full_title).strip()
            raw_agency = _clean_agency_name(table_match.group(2))
            current_table_title = raw_title
            current_table_id = _slugify_simple(raw_title)
            current_section = None
            current_columns = None
            pending_table_line = None
            stats["table_headers"] += 1

            if current_agency and raw_agency and raw_agency != current_agency:
                log.warning(
                    "Table agency mismatch (%s): header=%s table=%s",
                    pdf_name,
                    current_agency,
                    raw_agency,
                )
            if not current_agency and raw_agency:
                current_agency = raw_agency
            continue
        if line.startswith("Table ") and " for " not in line:
            pending_table_line = line
            continue
        pending_table_line = None

        if not current_table_title:
            continue

        if _should_end_table(line):
            _flush_columnar_records()
            if line.lower().startswith("figure"):
                stats["figure_lines"] += 1
            else:
                stats["notes_lines"] += 1
            current_table_title = None
            current_table_id = None
            current_section = None
            current_columns = None
            continue

        if _is_race_header(line):
            current_columns = RACE_COLUMNS
            continue

        column_header = _match_column_header(line)
        if column_header and column_row_labels:
            column_mode = True
            column_current = column_header
            column_data.setdefault(column_header, [])
            continue

        if column_mode:
            column_header = _match_column_header(line)
            if column_header:
                column_current = column_header
                column_data.setdefault(column_header, [])
                continue
            if column_current:
                token = None
                for raw_token in line.split():
                    cleaned = _normalize_numeric_token(raw_token, allow_dot=True)
                    if cleaned is not None:
                        token = None if cleaned == "." else cleaned
                        break
                if token is None and line.strip() == ".":
                    token = None
                if token is not None or line.strip() == ".":
                    column_data[column_current].append(token)
                    continue

        section_label = _parse_section_header(line) or _parse_plain_section_header(line)
        if section_label:
            current_section = section_label
            stats["section_headers"] += 1
            continue

        if current_columns is None and not column_mode:
            column_row_labels.append((line, current_section))
            continue

        parsed = _parse_metric_line(line)
        if parsed and current_table_id and current_agency:
            metric_label, values = parsed
            section_id = _build_section_id(current_section)
            metric_id = _build_metric_id(metric_label)
            record = {
                "Total": values[0],
                "White": values[1],
                "Black": values[2],
                "Hispanic": values[3],
                "Native American": values[4],
                "Asian": values[5],
                "Other": values[6],
                "year": year,
                "row_key": _build_row_key(current_table_id, section_id, metric_id),
                "agency": current_agency,
                "table": current_table_title,
                "table_id": current_table_id,
                "section": current_section,
                "section_id": section_id,
                "metric": metric_label,
                "metric_id": metric_id,
                "row_id": _build_row_id(
                    year,
                    current_agency,
                    current_table_id,
                    section_id,
                    metric_id,
                ),
            }
            records.append(record)
            key = (current_agency, current_table_id)
            record_counts[key] = record_counts.get(key, 0) + 1
            stats["metric_rows"] += 1
            continue

        warn_key = (current_agency, current_table_id)
        count = unparsed_counts.get(warn_key, 0)
        if count < 5:
            log.warning(
                "Unparsed line (%s, %s, %s): %s",
                pdf_name,
                current_agency,
                current_table_id,
                line,
            )
        unparsed_counts[warn_key] = count + 1
        stats["unparsed_lines"] += 1

    _flush_columnar_records()

    if record_counts:
        sample = sorted(record_counts.items(), key=lambda item: item[1], reverse=True)[:5]
        log.info("Parsed records by agency/table (top 5): %s", sample)
    if unparsed_counts:
        worst = sorted(unparsed_counts.items(), key=lambda item: item[1], reverse=True)[:5]
        log.warning("Unparsed line counts (top 5): %s", worst)
    log.info(
        "Parser summary (%s): %s",
        pdf_name,
        {k: v for k, v in stats.items() if v},
    )

    df = pd.DataFrame(records)
    if df.empty:
        return pd.DataFrame(columns=EXTRACT_COLUMNS)

    df = df.reindex(columns=EXTRACT_COLUMNS)
    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col].map(_clean_numeric_str), errors="coerce")
    return df


def _add_extract_metadata(context, df: pd.DataFrame, label: str) -> None:
    if df is None:
        return
    meta = {
        "row_count": int(len(df)),
    }
    if not df.empty:
        if "agency" in df.columns:
            meta["unique_agencies"] = int(df["agency"].nunique(dropna=True))
        if "row_key" in df.columns:
            meta["unique_row_keys"] = int(df["row_key"].nunique(dropna=True))
    try:
        context.add_output_metadata(meta)
        context.log.info("%s metadata: %s", label, meta)
    except Exception:
        pass

def normalize_row_tokens(row: list[str], dept_name: str, table_slug: str, log) -> list[str]:
    log.debug("Raw row tokens: %s / %s %s", list(row), table_slug, dept_name)
    row = [str(c).strip() for c in row if str(c).strip()]

    # Merge split decimals before numeric sniffing (e.g., '2 . 89' → '2.89', '. 27' → '0.27')
    merged: list[str] = []
    i = 0
    while i < len(row):
        tok = row[i]
        # Only merge when the right-hand numeric token has at least 2 digits to avoid fusing '0 . 0'
        if tok == '.' and i + 1 < len(row) and row[i + 1].isdigit() and len(row[i + 1]) >= 2:
            merged.append(f"0.{row[i + 1]}")
            i += 2
            continue
        if (
            i + 2 < len(row)
            and row[i + 1] == '.'
            and row[i].isdigit()
            and row[i + 2].isdigit()
            and len(row[i + 2]) >= 2
        ):
            merged.append(f"{row[i]}.{row[i + 2]}")
            i += 3
            continue
        merged.append(tok)
        i += 1
    # Remove dot-leader filler tokens (.., ..., ···) after merging
    row = [t for t in merged if not _DOTS_RE.match(t)]

    # Normalise tokens for numeric detection only (keep original tokens for key)
    def _norm_num_token(t: str) -> str:
        s = t.strip()
        # Fix common OCR-ish confusions
        if s.lower() == 'o':
            s = '0'
        # Strip trailing paired punctuation that can cling to numbers
        s = s.rstrip(')]')
        return s
    row_num = [_norm_num_token(t) for t in row]

    # Pick the rightmost 7 slots that are numeric OR a lone '.' placeholder
    # Treat a single '.' as an explicit missing slot to preserve column alignment.
    numeric_positions = [
        idx
        for idx, tok in enumerate(row_num)
        if _is_numeric(tok) or tok == "."
    ]
    picked_positions = numeric_positions[-7:]
    numeric = [row_num[idx] for idx in picked_positions]

    # Build key from non-picked tokens, preserving original order
    picked_set = set(picked_positions)
    key_tokens = [tok for idx, tok in enumerate(row) if idx not in picked_set]

    if len(numeric) < 7:
        log.warning("Found only %d numeric cols: %s", len(numeric), numeric)
        # Pad on the RIGHT so present values align to early columns (Total, White, …)
        numeric = numeric + [""] * (7 - len(numeric))
    elif len(numeric) > 7:
        log.debug("Extra numeric cols %s – keeping rightmost 7", numeric[:-7])
        numeric = numeric[-7:]

    key = " ".join(key_tokens).strip() or "(blank key)"

    # Convert any '.' placeholders into explicit blanks and leave numeric tokens as-is.
    # Light touch: do not scale integers heuristically (preserve source values for auditing).
    numeric = ["" if v == "." else v for v in numeric]

    return [key] + numeric


def _clean_camelot_table(table, log, *, year: int) -> pd.DataFrame | None:
    """Return a tidy DataFrame for one Camelot table (with ``year`` column)."""
    df = table.df.copy()
    log.debug("Raw Camelot table shape: %s", df.shape)
    df = df.dropna(axis=1, how="all")

    metadata_row_idx = metadata_line = None
    for i, row in df.iterrows():
        line = " ".join(row.dropna())
        if re.search(r"Table\s*\d+:", line):
            metadata_row_idx, metadata_line = i, line
            break
    if metadata_row_idx is None:
        log.warning("Missing metadata row – skipping table")
        return None

    m = re.search(r"Table\s*(\d+):\s*(.*?)\s*for\s*(.+)", metadata_line)
    if not m:
        log.warning("Could not parse metadata line: %s", metadata_line)
        return None

    raw_table_name, raw_dept = m.group(2).strip(), m.group(3).strip()
    dept_name = _normalize_text(raw_dept)
    table_slug = TABLE_SLUG_LOOKUP.get(raw_table_name, slugify(raw_table_name, lowercase=True))

    # Slice away the metadata rows ("Table N: …") and a possible blank spacer row
    df = df.iloc[metadata_row_idx + 2 :].reset_index(drop=True)
    if pd.isna(df.iloc[0, 0]) or str(df.iloc[0, 0]).strip() == "":
        df = df.iloc[:, 1:]

    # Detect indentation per original Camelot row using cell x-coordinates
    # Align Camelot cells to the sliced DataFrame rows
    start_row_idx = int(metadata_row_idx) + 2
    aligned_cell_rows = table.cells[start_row_idx : start_row_idx + len(df)]

    left_x: list[float] = []
    for r in aligned_cell_rows:
        xs = [float(c.x1) for c in r if str(c.text).strip()]
        left_x.append(min(xs) if xs else float("inf"))
    finite_lefts = [x for x in left_x if x != float("inf")]
    min_left = min(finite_lefts) if finite_lefts else float("inf")
    # Treat rows as indented if their left edge is meaningfully to the right
    indent_threshold = 1.5  # PDF coordinate units; small but effective
    is_indented_row = [(lx - min_left) > indent_threshold for lx in left_x]

    # Build normalized token rows from text-only df; keep a flag for numeric presence
    has_numeric_flags: List[bool] = []
    normalized_rows: List[List[str]] = []
    for _, row in df.iterrows():
        toks = [str(c).strip() for c in row if str(c).strip()]
        has_numeric = any(_is_numeric(t) for t in toks)
        has_numeric_flags.append(has_numeric)
        normalized_rows.append(
            normalize_row_tokens(list(toks), dept_name, table_slug, log)
        )
    df = pd.DataFrame(normalized_rows, columns=FINAL_COLUMNS[:8])

    df["Key"] = (
        df["Key"]
        .astype(str)
        .str.replace(r"\s*\n\s*", " ", regex=True)
        .str.strip()
        .str.lower()
        .apply(_normalize_text)
    )

    # Attach sections: rows without numbers AND without indentation are section headers, then ffill
    df["Measurement"] = None
    header_mask_list: list[bool] = [
        (not has_numeric_flags[i]) and (not is_indented_row[i]) for i in range(len(df))
    ]
    for i, is_header in enumerate(header_mask_list):
        if is_header:
            df.loc[i, "Measurement"] = df.loc[i, "Key"].strip()
    section_header_mask = pd.Series(header_mask_list, index=df.index)

    df["Measurement"] = df["Measurement"].ffill()

    # Remove blank rows, notes, and the section-header rows themselves
    mask_blank_key = df["Key"].str.strip().eq("") | df["Key"].isna()
    mask_notes = df["Key"].str.contains(r"^notes?\s*:\s*", case=False, na=False)
    df = df[~(mask_blank_key | mask_notes | section_header_mask)].copy()
    if df.empty:
        log.warning("All rows removed after cleanup – skipping table")
        return None

    # Convert numerics. Keep NaN for blanks.
    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col].map(_clean_numeric_str), errors="coerce")

    # Drop rows that contain no numeric data at all (filters leftovers)
    mask_all_na_numeric = df[NUMERIC_COLS].isna().all(axis=1)
    if mask_all_na_numeric.any():
        log.debug("Dropping %d rows with no numeric values", int(mask_all_na_numeric.sum()))
    df = df[~mask_all_na_numeric].copy()
    if df.empty:
        log.warning("Only non-numeric rows found – skipping table")
        return None

    df["agency"] = dept_name
    df["table"] = raw_table_name
    df["table_id"] = _slugify_simple(raw_table_name)
    df["section"] = df["Measurement"]
    df["metric"] = df["Key"]
    df["section_id"] = df["section"].apply(_build_section_id)
    df["metric_id"] = df["metric"].apply(_build_metric_id)
    df["row_key"] = df.apply(
        lambda row: _build_row_key(row["table_id"], row["section_id"], row["metric_id"]),
        axis=1,
    )
    df["row_id"] = df.apply(
        lambda row: _build_row_id(year, dept_name, row["table_id"], row["section_id"], row["metric_id"]),
        axis=1,
    )
    df["year"] = year
    df = df.drop(columns=["Key", "Measurement", "Department", "Table name"], errors="ignore")
    return df.reindex(columns=EXTRACT_COLUMNS)

# ------------------------------------------------------------------------------
# Page-range fan-out / fan-in ops
# ------------------------------------------------------------------------------
@op(out=DynamicOut(str))
def calculate_page_ranges(context, pdf_path: str):
    """Yield a single page-range string for full-document text parsing."""
    total_pages = len(PdfReader(pdf_path).pages)
    page_range = f"1-{total_pages}"
    yield DynamicOutput(page_range, mapping_key="all_pages")


@op(out=Out(pd.DataFrame))
def parse_page_range(context, pdf_path: str, page_range: str) -> pd.DataFrame:
    """Extract every metric row from the PDF using pdftotext -layout."""
    del page_range
    pdf_file = Path(pdf_path)
    text_path = _ensure_pdftotext(pdf_file, context.log)
    if not text_path:
        return pd.DataFrame()

    try:
        text = text_path.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:
        context.log.error("Failed reading pdftotext output %s: %s", text_path, exc)
        return pd.DataFrame()

    year_match = re.search(r"(\d{4})", pdf_file.name)
    year = int(year_match.group(1)) if year_match else None
    lines = text.splitlines()
    if year is not None and year < 2020:
        df = _parse_pre2020_pdftotext_lines(lines, context.log, year=year, pdf_name=pdf_file.name)
    else:
        df = _parse_pdftotext_lines(lines, context.log, year=year, pdf_name=pdf_file.name)
    _add_extract_metadata(context, df, f"extract_page_range_{pdf_file.name}")
    return df


@op(out=Out(pd.DataFrame), required_resource_keys={"data_dir_processed"})
def concat_and_write_parquet(context, chunks: List[pd.DataFrame], pdf_path: str) -> pd.DataFrame:
    """Merge page chunks, write Parquet, return combined DataFrame."""
    non_empty = [c for c in chunks if not c.empty]
    if not non_empty:
        raise ValueError("No tables were extracted from the PDF.")
    combined = pd.concat(non_empty, ignore_index=True)

    year_match = re.search(r"(\d{4})", Path(pdf_path).name)
    year = year_match.group(1) if year_match else "unknown"

    out_parquet = context.resources.data_dir_processed.get_path() / f"combined_output_{year}.parquet"
    combined.to_parquet(out_parquet, index=False, engine="pyarrow")
    context.log.info("Wrote %d rows (Parquet) → %s", len(combined), out_parquet)

    # Attach local path and optional S3 metadata to the asset materialization
    meta = {"local_path": str(out_parquet)}
    try:
        s3_res = getattr(context.resources, "s3", None)
        if s3_res is not None and hasattr(s3_res, "upload_file"):
            bucket = getattr(s3_res, "bucket", None)
            s3_prefix = getattr(s3_res, "s3_prefix", "") or ""
            key_prefix = (s3_prefix.strip("/") + "/" if s3_prefix else "") + "missouri-vsr/"
            key = f"{key_prefix}{year}/combined_output_{year}.parquet"
            presigned = s3_res.upload_file(str(out_parquet), key)
            uri = f"s3://{bucket}/{key}"
            meta.update({"s3_uri": uri})
            if presigned:
                meta.update({"presigned_url": presigned})
            context.log.info("Uploaded per-year Parquet to %s", uri)
    except Exception as e:
        context.log.warning("S3 upload skipped/failed: %s", e)

    try:
        context.add_output_metadata(meta)
    except Exception:
        pass

    _add_extract_metadata(context, combined, f"extract_parquet_{year}")

    return combined

# ------------------------------------------------------------------------------
# Extract graph assets factory
# ------------------------------------------------------------------------------
def make_extract_asset(year: int):
    @graph_asset(
        name=f"extract_pdf_data_{year}",
        group_name="extract",
        ins={"pdf_path": AssetIn(key=AssetKey(str(year)))},
        description=f"Extract tabular data from the {year} VSR (parallelised per page range).",
    )
    def extract_for_year(pdf_path: str) -> pd.DataFrame:
        page_ranges = calculate_page_ranges(pdf_path)
        processed = page_ranges.map(lambda pr: parse_page_range(pdf_path, pr))
        return concat_and_write_parquet(processed.collect(), pdf_path)

    extract_for_year.__name__ = f"extract_pdf_data_{year}"
    return extract_for_year


# Instantiate one extract asset per year
for yr in YEAR_URLS:
    globals()[f"extract_pdf_data_{yr}"] = make_extract_asset(yr)
