# Missouri VSR — Architecture & Agent Guide

## Overview

This project extracts, normalizes, and publishes Missouri's annual Vehicle Stops Report (VSR). Agencies are obligated by a statute that went into effect in 2000 to report aggregate information about a number of metrics relating to the reason and outcomes of stops, broken down by the driver's race. They report their data to Missouri Attorney General's office, who work with researchers at the University of Missouri to generate the stops report.

Recovered Factory, using code originally developed at The Marshall Project, is extracting, combining, enriching, and publishing this data.

The pipeline covers 2014–present across two distinct PDF formats (pre-2020 and 2020+), ~600 law enforcement agencies. Primary outputs are versioned, structured data files consumed by a separate frontend application and perhaps more importantly, available for journalists, researchers, policy makers, and concerned citizens to download and analyze.

**Design principles**

- *Extract close to source.* Raw parsing preserves the original report's structure, metric names, and values without interpretation. Normalization is a separate, explicit layer.
- *Reproducible by re-running.* Every output can be deterministically regenerated from source PDFs and config files in this repo.
- *Checks at each stage.* Parser regressions and data quality issues are caught at the per-year extract layer before downstream assets run and rely _heavily_ on Dagster asset checks; downstream asset checks (like data transformations) are more focused and use a mix of asset checks and unit tests.  
- *Stable contracts.* The frontend pins to a versioned data release. Pipeline changes that affect outputs increment the release version rather than silently breaking consumers.
- *Agent-friendly.* The repo is structured so that AI coding agents working on the pipeline and on the frontend can operate independently against a shared data contract, minimizing coordination overhead.
- *Entirely backend.* APIs, microservices, and anything other than the most basic file outputs should live elsewhere.

**Tech stack**

- Dagster 1.x — orchestration (no schedules; batch on-demand)
- pandas / pyarrow — data wrangling
- pdftotext -layout (poppler-utils) — primary PDF extraction
- DuckDB — query layer for data exploration and (planned) API
- GeoPandas + tippecanoe — GIS / PMTiles
- S3 — versioned output distribution

**Repo layout (essentials)**

- `missouri_vsr/assets/` — Asset modules (`extract.py`, `reports.py`, `processed.py`, `audit.py`, `agency_reference.py`, `gis.py`)
- `missouri_vsr/asset_checks/` — Per-year and combined asset checks
- `missouri_vsr/definitions/` — Dagster `Definitions`; registers assets and resources
- `data_checks/` — CSV-driven row sanity checks per year
- `run_configs/` — Example Dagster run configs
- `data/` — Local inputs/outputs (reports, processed parquet, JSON exports)

**Setup**

- Python 3.12+; `uv sync` to install deps
- `uv run dagster dev` to start the Dagster UI
- Copy `.env.example` to `.env` and fill in AWS credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, `MISSOURI_VSR_BUCKET_NAME`, `MISSOURI_VSR_S3_PREFIX`)

---

## Architecture

### Three-layer pipeline

Data moves through three conceptually distinct layers. Keeping them separate is what makes the system reproducible, testable, and safe to evolve.

```
Layer 1 — Raw / parsed
  Source PDFs → pdftotext → per-year DataFrames
  Assets: download_reports, extract_pdf_data_{year}
  Output: combined_output_{year}.parquet
  Row keys: era-specific (pre-2020 uses key-indicators/vehicle-stop-stats/search-stats;
            2020+ uses rates-by-race/number-of-stops-by-race/search-statistics)
  Invariant: no interpretation, no normalization; values match the source PDF exactly

Layer 2 — Normalized / processed
  Combined across years → canonical metrics → ranks, percentiles, baselines
  Assets: combine_all_reports, reports_with_rank_percentile,
          statewide_slug_baselines, agency_reference, gis
  Output: all_combined_output.parquet, reports_with_rank_percentile.parquet,
          per-agency JSON under data/out/agency_year/
  Row keys: both era-specific keys preserved AND a canonical_key column (planned)
            that maps equivalent metrics across eras for cross-year analysis
  Invariant: layer 1 outputs are never modified; normalization adds columns / rows

Layer 3 — Released
  Versioned snapshot promoted to S3 for frontend consumption
  Assets: dist group (JSON/GeoJSON/PMTiles exports, manifests)
  Output: releases/vN/ on S3 with a manifest.json describing schema version,
          years covered, canonical metric definitions
  Invariant: a released version is immutable; frontend pins to a version
```

**The era-coherence problem** (current active design question): Pre-2020 and 2020+ reports use different metric names for equivalent concepts. The planned solution is a `canonical_key` column populated at layer 2 from a crosswalk config — e.g., both `key-indicators--stops` (2014–2019) and `rates-by-race--totals--all-stops` (2020+) map to canonical key `stops`. Layer 1 is untouched; layer 2 gains cross-year query capability; layer 3 exposes only canonical metrics to the frontend.

### Release workflow

No formal staging environment. During active v2 development, outputs are published incrementally to `releases/v2/` on S3. The frontend stays pinned to v1 via the top-level `manifest.json` until a deliberate cutover. The version number is the signal; there's no separate staging path.

A release is cut by:
1. All asset checks passing on main
2. Updating the top-level `manifest.json` to point at the new version

Patch releases (vN.x) are backward-compatible: new years added, bug fixes that don't change existing values. Major releases require frontend coordination before the manifest is flipped.

### Testing strategy

Asset checks are the primary regression gate. They run at two levels:

**Per-year extract checks** (`extract_pdf_data_{year}`):
- `schema` — DataFrame columns match the expected set for that era
- `no_duplicate_row_keys` — no `(agency, row_key)` collisions within a year
- `numeric_columns_parse` — all race columns contain parseable numbers
- `row_expectations` — golden-file CSV (`data_checks/row_sanity_checks_{year}.csv`) with spot-checked agency/metric values verified against source PDFs

**Combined asset checks** (`combine_all_reports`):
- Schema, duplicate row_key/row_id checks across the full multi-year frame

**Adding sanity check rows**: pick an agency, read its PDF page, verify the values, append to `data_checks/row_sanity_checks_{year}.csv`. The `checked` column is for human review tracking; all rows run regardless.

Unit tests (`pytest`) cover normalization ops and processing helpers but not the PDF parser directly — parser regressions are caught via the golden-file checks above.

---

## Data + Release

### Release / version strategy

Data releases follow semantic versioning (vMAJOR.MINOR):

- **Major** — breaking schema change: columns renamed/removed, row_key structure changed, canonical metric definitions revised. Requires frontend coordination and a deprecation window for the prior major version.
- **Minor** — backward-compatible addition: new years added, new canonical metrics added, new derived columns added. Frontend can adopt at its own pace.

No formal staging environment. During active development, v2 is published incrementally to `releases/v2/` until it's ready — the version number is the signal. The frontend stays pinned to v1 until explicitly cut over.

S3 layout:

```
s3://{bucket}/
  releases/
    v1/          ← frozen (2020–2024, era-specific row_keys)
    v2/          ← live development target; incremented until ready
  manifest.json  ← points to the current frontend-facing release
```

Each release directory contains a `manifest.json`:

```json
{
  "version": "2.0",
  "released": "YYYY-MM-DD",
  "years": [2014, 2015, ..., 2024],
  "schema_version": "2.0",
  "canonical_metrics": ["stops", "searches", "arrests", ...],
  "changelog": "Added 2000–2019 pre-2020 format data with canonical_key normalization."
}
```

The top-level `manifest.json` points to the current frontend-facing version. Updating it is the release act.

### Major migrations + milestones

**v1.0 — current** (2020–2024, 2020+ format only)
- Per-year extract assets with asset checks
- Era-specific row_keys (`rates-by-race--*`, `number-of-stops-by-race--*`, `search-statistics--*`)
- S3 flat-file outputs, per-agency JSON

**v2.0 — planned** (2000–2024, cross-era normalization)
- Pre-2020 data (2000–2019) added to pipeline. **Year-range notes from verification:**
  - 2000: fully parsed; key-indicators + search-stats only (no vehicle-stop-stats, no arrest-rate/contraband-hit-rate)
  - 2001–2003: **partially parsed** — ~50% of agencies have data. Root cause: PDFs omit blank race columns entirely instead of printing "0", but the parser expects ≥6 numeric tokens per row. Tracked as a known parser limitation; fix deferred.
  - 2002+: arrest-charge table added; 2004+: vehicle-stop-stats table added; 2009: some row_key renames (reason-for-stop--for-moving→moving, driver-age--age-17-and-under→17-and-under, arrest-charge--offense-against→off-against-person)
  - 2014–2019: full parse, stable schema
- `canonical_key` column added at layer 2 mapping both eras to shared concept names
- Layer 3 dist output collapses on `canonical_key`: where a canonical equivalent exists, the era-specific source row_key is dropped. This matters because 2020+ already has duplicate coverage — the pre-computed rates and the raw counts both appear under different row_keys for the same concept. The dist output should be clean and canonical-only.
- Era-specific row_keys remain available in the layer 2 processed Parquet for audit/research use
- **The biggest single task: thorough canonical mapping.** The draft crosswalk table in this doc covers a handful of rows; the full mapping spans all ~50+ row_keys across both eras, including deciding which pre-2020 rows have no 2020+ equivalent and vice versa. This mapping process should be done carefully and verified against source PDFs before any code is written.
- Requires: canonical metric crosswalk config, processed.py era-aware subset lists, frontend schema update

**v3.0 — future** (TBD)
- May include: additional years as AG publishes, revised ACS population vintages, expanded GIS outputs

### Data contract

The data contract defines what the frontend can rely on across releases. It lives in this file and is the coordination point when pipeline and frontend agents work in parallel.

**Stable across all versions:**
- Core tidy row schema: `year`, `agency`, `row_key`, `row_id`, `table_id`, `section_id`, `metric_id`, plus race count columns
- Race columns (2020+): `Total`, `White`, `Black`, `Hispanic`, `Native American`, `Asian`, `Other`
- Race columns (pre-2020): `Total`, `White`, `Black`, `Hispanic`, `Asian`, `Am. Indian`, `Other`
- `row_key` pattern: `{table_id}--{section_id}--{metric_id}`
- `row_id` pattern: `{year}-{agency_slug}-{row_key}`

**Added in v2:**
- `canonical_key` — era-independent concept name (e.g., `stops`, `searches`, `arrests`, `moving-violation`, `consent-search`). Null for metrics with no cross-era equivalent.

**Canonical metric crosswalk (draft — v2 design):**

| canonical_key | pre-2020 row_key | 2020+ row_key |
|---|---|---|
| `stops` | `key-indicators--stops` | `rates-by-race--totals--all-stops` |
| `searches` | `key-indicators--searches` | `rates-by-race--totals--searches` |
| `arrests` | `key-indicators--arrests` | `number-of-stops-by-race--stop-outcome--arrests` |
| `moving-violation` | `vehicle-stop-stats--reason-for-stop--moving` | `number-of-stops-by-race--reason-for-stop--moving` |
| `consent-search` | `search-stats--probable-cause-authority-to-search--consent` | `search-statistics--probable-cause--consent` |

**On arrests:** `key-indicators--arrests` (pre-2020) and `number-of-stops-by-race--stop-outcome--arrests` (2020+) measure the same thing and can be treated as equivalent in cross-era analysis.

**On rates:** Both eras pre-compute per-agency rates directly in the PDF. The eras differ in which rates are present and what population source is used.

Pre-2020 rates (from key-indicators, using 2010 decennial census population):

| rate | formula |
|---|---|
| Disparity index | (proportion of stops / proportion of population) — value of 1 = no disparity |
| Search rate | searches / stops × 100 |
| Contraband hit rate | searches with contraband / total searches × 100 |
| Arrest rate | arrests / stops × 100 |

Note: pre-2020 reports include a "% of population" row per agency (statewide and local), which means the implied census population figures can be back-calculated from the data if needed.

2020+ rates (from rates-by-race--rates--, using previous-year ACS population for the stop-rate denominators):

| rate | formula |
|---|---|
| Stop rate | stops / ACS population × 100 |
| Resident stop rate | resident stops / ACS population × 100 |
| Search rate | searches / stops × 100 |
| Contraband hit rate | searches with contraband / total searches × 100 |
| Arrest rate | arrests / stops × 100 |
| Citation rate | citations / stops × 100 |

**Design decision — compute rates uniformly at layer 2**

For v2, rates will be computed from raw counts at layer 2 for all years rather than using the pre-computed values from the PDFs. This gives a single consistent methodology across 2014–2024.

The University of Missouri researchers who build the report get the pre-computed rates right in both eras. This means our computed rates should match the PDF values exactly — which doubles as a validation check on our raw count extraction. Any mismatch signals a parsing error in the underlying counts.

Computing from raw counts for pre-2020 also lets us substitute ACS population for 2010 census population once those figures are sourced (a future task), and gives us citation rate for free even though the AG didn't surface it.

Stop rate and resident stop rate are 2020+-only; pre-2020 ACS population is not a blocker for v2.

**Editorial note on population-based rates:** Unlike crime statistics, traffic stop rates divided by residential population are methodologically limited — traffic through a place is not the same as who lives there. Population-based rates (stop rate, resident stop rate) are included because the reports provide them and they have analytical uses, but they should not be treated as the primary disparity measure. The resident vs. non-resident split in 2020+ is useful precisely because it partially addresses this, but it doesn't exist pre-2020 and there's no clean equivalent.

The crosswalk config (to be implemented in v2) must flag which rates are available per year range so the frontend can conditionally show or suppress rate comparisons by year.

The crosswalk config will be the authoritative source; this table is a planning aid.

---

## Data for Frontend

### Query layer

The query layer — Lambda + DuckDB serving the frontend — lives in the frontend repo, not here. Query patterns change when the frontend changes; those two things should evolve together. This repo's job ends at producing well-structured, versioned Parquet, CSV, JSON, and PMTiles files on S3.

For local data exploration, DuckDB against `data/processed/*.parquet` is the expected tool. It handles the full dataset comfortably and requires no infrastructure. Anyone doing serious pipeline work will have the parquets locally already. A local MCP server pointed at those parquets is a one-liner and doesn't need to live in either repo.

Key design choice for layer 3 outputs: **partition Parquet by year**. DuckDB's partition pruning means a query for a single agency's 2024 data only scans the 2024 partition, keeping the frontend Lambda fast even as the year range grows.

Lambda cold starts are not a meaningful concern here: the function will be kept warm with a cron ping, responses are heavily cached, and the data doesn't change between pipeline runs. There's no need for short cache TTLs or aggressive Lambda optimization.

### v2 dist file contract

**row_key in all v2 dist outputs = `canonical_key`**, not the era-specific PDF row_key. The frontend must update any hardcoded row_key strings:

| v1 row_key | v2 row_key (canonical) |
|---|---|
| `rates-by-race--totals--all-stops` | `stops` |
| `rates-by-race--totals--searches` | `searches` |
| `rates-by-race--totals--arrests` | `arrests` |
| `rates-by-race--totals--citations` | `citations` |
| `rates-by-race--totals--contraband` | `contraband-total` |
| `rates-by-race--rates--search-rate` | `search-rate` |
| `number-of-stops-by-race--stop-outcome--warning` | `stop-outcome--warning` |

Full canonical key list: `data/src/canonical_crosswalk.csv`.

**Race columns:** `Native American` is null for pre-2020 rows; `Am. Indian` is null for 2020+ rows. Both appear in the layer-2 Parquet. The dist JSON uses the standard 7-column set (`Total`, `White`, `Black`, `Hispanic`, `Native American`, `Asian`, `Other`); pre-2020 `Native American` values will be null.

**Year range:** v2 covers 2000–2024. 2001–2003 have partial coverage (~50% of agencies) due to a known parser limitation (blank race columns). Years 2004–2024 have full coverage.

**Dist JSON design principle:** raw counts + canonical keys only. No rank/percentile/percentage derived rows. The frontend computes any display-layer derived values it needs, OR defers to the download Parquet.

**Download Parquet** (`reports_with_rank_percentile.parquet`): includes pre-computed rank, percentile, and in-group percentage rows as convenience derivations for data consumers. These also serve as a data quality signal — computed values should match PDF pre-computed rates.

### Agency year JSON — per-year partition (v2 target)

Agency JSON files should be **partitioned by year**:

```
dist/agency_year/{agency-slug}/{year}.json   ← one file per agency per year
```

**Why:** The frontend loads only the most recent year on initial page render; historical years are fetched on demand. With 25 years of data, a monolithic per-agency file would be 700 KB–1.4 MB. Per-year files are ~30–50 KB each (including rank/percentile rows).

**Rank/percentile must be in the per-year file.** The frontend cannot compute rank without all agencies' data, and the N (number of reporting agencies) differs by year. Pre-computed rank is required.

**Not yet implemented** — current pipeline still writes monolithic per-agency files. The next step is to restructure `agency_year_json_exports` to write `{slug}/{year}.json`, reading from `reports_with_rank_percentile.parquet` (which has canonical keys + ranks).

**Current file sizes (monolithic, no ranks):**
- Average: 500 KB per agency, 436 MB total, 892 agencies
- Expected with per-year partition + ranks: ~30–50 KB per file, ~22k total files

**Observed sizes for other dist files (v2):**
- `metric_year_subset.json`: 4.6 MB (9 canonical keys × 884 agencies × 25 years)
- `statewide_year_sums.json`: 1.1 MB
- `statewide_year_sums_subset.json`: 30 KB

### Data-derived frontend artifacts

The pipeline can generate frontend-facing content directly from data and schema, reducing manual maintenance:

**"About the data" section** — auto-generated from the canonical metric crosswalk and schema definitions. Lists which metrics are available for which year ranges, what the race columns represent, and data caveats (ACS vintage, population denominator methodology). Emitted as a JSON blob or markdown at layer 3.

**Versioned download links** — the manifest.json + S3 layout enables the frontend to render a "download the data" section that lists every available release, its year range, and a direct link, without any manual upkeep.

**Agency year index** — a lightweight JSON index of `{agency, years_available, canonical_metrics_available}` that lets the frontend quickly determine what to show before fetching the full agency data.

**Schema changelog** — each manifest.json entry includes a human-readable changelog that the frontend can surface in a "what changed" UI, linking data version to editorial context.

---

## Development

### Multi-agent setup

The pipeline repo and frontend repo are kept separate — different toolchains, different deployment cadences. The coordination point between agents working on each side is the **data contract** section of this file and the `manifest.json` in each release.

**Running two agents in parallel:**

- Open one Claude Code session in the pipeline repo, one in the frontend repo
- Give both sessions the relevant section of this file as context (paste or reference `agents.md`)
- Define the change in terms of the data contract first — agree on what layer 3 will emit before either agent writes code
- Pipeline agent works against `staging/vN/`; frontend agent works against the same staging path
- Neither agent needs to understand the other repo's internals — only the contract matters

**CLAUDE.md and memory:** Each repo has its own `CLAUDE.md` and per-project memory. The pipeline memory (`.claude/projects/.../memory/`) captures parser quirks, feedback, and project state that would otherwise be re-derived every session. Frontend repo should have equivalent memory for its own domain.

**Branch and PR conventions:**
- Feature branches are named `issue-{N}-{short-description}`
- PRs close the issue they're tied to; commit messages reference the agent co-author
- `agents.md` changes that affect the data contract should be committed and pushed before the implementing PR is opened, so both repos can reference the same contract version

**Operational notes for agents:**
- `pdftotext -layout` outputs are cached next to PDFs as `*.layout.txt`; delete to force re-extraction
- S3 uploads happen only when the `s3` resource is configured; local runs skip upload silently
- `row_key` encodes `{table_id}--{section_id}--{metric_id}`; `row_id` is `{year}-{agency_slug}-{row_key}`
- Pre-2020 parser uses `_parse_pre2020_pdftotext_lines`; 2020+ uses the main state machine in `extract.py`
- Sample PDF for fast dev iteration: `data/src/examples/VSRreport2023.pdf` (pages 1694–1745); use `run_configs/example_2023_sample.yaml` with `--select extract_pdf_data_2023`
- Agency crosswalk CLI: `uv run python -m missouri_vsr.cli.crosswalk` — interactive fuzzy-match tool for resolving agency name variations; progress autosaves to `.state.json`
