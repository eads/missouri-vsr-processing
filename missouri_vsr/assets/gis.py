from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd
import requests
from slugify import slugify

from dagster import AssetIn, AssetKey, AssetOut, Output, asset, graph_asset, multi_asset, op, Out

from missouri_vsr.assets.extract import PIVOT_VALUE_COLUMNS
from missouri_vsr.assets.s3_utils import upload_paths, s3_uri_for_dir, s3_uri_for_path
GENZ_GPKG_URL = "https://www2.census.gov/geo/tiger/GENZ2024/gpkg/cb_2024_us_all_500k.zip"

COUNTY_PARQUET_NAME = "cb2024_mo_counties.parquet"
PLACE_PARQUET_NAME = "cb2024_mo_places.parquet"
BOUNDARY_MATCHES_NAME = "agency_boundary_matches.parquet"
RELATIONSHIPS_NAME = "agency_relationships.parquet"
PMTILES_NAME = "mo_jurisdictions_2024_500k.pmtiles"
MANIFEST_NAME = "mo_jurisdictions_2024_500k.manifest.json"

TARGET_ROW_KEY = "rates-by-race--totals--all-stops"


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y"}


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _processed_gis_dir(context) -> Path:
    return Path(context.resources.data_dir_processed.get_path()) / "gis"


def _out_gis_dir(context) -> Path:
    return Path(context.resources.data_dir_out.get_path()) / "gis"


def _out_tiles_dir(context) -> Path:
    return Path(context.resources.data_dir_out.get_path()) / "tiles"


def _out_agency_boundaries_dir(context) -> Path:
    return Path(context.resources.data_dir_out.get_path()) / "agency_boundaries"


def _zip_has_gpkg(path: Path) -> bool:
    try:
        with zipfile.ZipFile(path) as zf:
            return any(name.lower().endswith(".gpkg") for name in zf.namelist())
    except Exception:
        return False


def _download_file(url: str, dest: Path, log, *, force: bool = False) -> None:
    if dest.exists() and not force:
        log.info("Using cached %s", dest)
        return
    log.info("Downloading %s → %s", url, dest)
    resp = requests.get(url, timeout=(10, 300))
    resp.raise_for_status()
    dest.write_bytes(resp.content)


def _require_geopandas():
    try:
        import geopandas as gpd  # type: ignore
    except ImportError as exc:
        raise ImportError("geopandas is required for GIS assets; install geopandas + shapely.") from exc
    return gpd


def _list_gpkg_layers(path: Path) -> List[str]:
    try:
        import pyogrio  # type: ignore

        return [name for name, _ in pyogrio.list_layers(path)]
    except Exception:
        pass
    try:
        import fiona  # type: ignore

        return list(fiona.listlayers(path))
    except Exception as exc:  # pragma: no cover - missing GIS stack
        raise RuntimeError(f"Unable to list layers for {path}") from exc


def _select_layer(layers: List[str], kind: str, log) -> str:
    preferred = {
        "county": "cb_2024_us_county_500k",
        "place": "cb_2024_us_place_500k",
    }
    target = preferred.get(kind)
    if target and target in layers:
        return target

    pattern = re.compile(rf".*{kind}.*500k.*", re.IGNORECASE)
    matches = [layer for layer in layers if pattern.match(layer)]
    if not matches:
        raise ValueError(f"No {kind} layer found in {layers}")
    if len(matches) > 1:
        log.warning("Multiple %s layers found; using %s", kind, matches[0])
    return sorted(matches)[0]


def _read_gpkg_layer(gpkg_path: Path, layer: str, log):
    gpd = _require_geopandas()
    engine = None
    try:
        import pyogrio  # noqa: F401
        engine = "pyogrio"
    except ImportError:
        engine = None
    log.info("Reading geopackage %s (layer=%s, engine=%s)", gpkg_path, layer, engine or "default")
    gdf = gpd.read_file(gpkg_path, layer=layer, engine=engine)
    if gdf.crs is None or str(gdf.crs).lower() != "epsg:4326":
        gdf = gdf.to_crs("EPSG:4326")
    return gdf


def _extract_gpkg(zip_path: Path, log) -> Path:
    with zipfile.ZipFile(zip_path) as zf:
        gpkg_names = [name for name in zf.namelist() if name.lower().endswith(".gpkg")]
        if not gpkg_names:
            raise ValueError(f"No .gpkg found in {zip_path}")
        gpkg_name = gpkg_names[0]
        target_path = zip_path.parent / Path(gpkg_name).name

        if target_path.exists():
            try:
                if target_path.stat().st_mtime >= zip_path.stat().st_mtime:
                    return target_path
            except Exception:
                pass

        log.info("Extracting %s from %s", gpkg_name, zip_path)
        zf.extract(gpkg_name, path=zip_path.parent)
        extracted_path = zip_path.parent / gpkg_name
        if extracted_path != target_path:
            if target_path.exists():
                target_path.unlink()
            shutil.move(str(extracted_path), str(target_path))
        return target_path


def _filter_missouri(gdf: pd.DataFrame) -> pd.DataFrame:
    if "STATEFP" in gdf.columns:
        state_series = gdf["STATEFP"].astype(str).str.zfill(2)
    elif "statefp" in gdf.columns:
        state_series = gdf["statefp"].astype(str).str.zfill(2)
    else:
        raise ValueError("Missing STATEFP column for Missouri filter.")
    return gdf[state_series.eq("29")].copy()


def _fix_invalid_geometries(gdf, log) -> float:
    valid_mask = gdf.geometry.is_valid
    invalid_count = int((~valid_mask).sum())
    if invalid_count == 0:
        return 1.0

    log.info("Fixing %d invalid geometries", invalid_count)
    try:
        from shapely import make_valid  # type: ignore

        gdf.loc[~valid_mask, "geometry"] = gdf.loc[~valid_mask, "geometry"].apply(make_valid)
    except Exception:
        gdf.loc[~valid_mask, "geometry"] = gdf.loc[~valid_mask, "geometry"].buffer(0)

    valid_mask = gdf.geometry.is_valid
    return float(valid_mask.mean()) if len(valid_mask) else 0.0


def _normalize_columns(gdf, *, boundary_type: str):
    rename = {
        "GEOID": "geoid",
        "NAME": "name",
        "NAMELSAD": "namelsad",
        "STATEFP": "statefp",
        "COUNTYFP": "countyfp",
        "PLACEFP": "placefp",
    }
    gdf = gdf.rename(columns={k: v for k, v in rename.items() if k in gdf.columns}).copy()

    base_cols = ["geoid", "name", "namelsad", "statefp"]
    if boundary_type == "county":
        base_cols.append("countyfp")
        gdf["geoid"] = gdf["geoid"].astype(str).str.zfill(5)
        gdf["statefp"] = gdf["statefp"].astype(str).str.zfill(2)
        gdf["countyfp"] = gdf["countyfp"].astype(str).str.zfill(3)
    else:
        base_cols.append("placefp")
        gdf["geoid"] = gdf["geoid"].astype(str).str.zfill(7)
        gdf["statefp"] = gdf["statefp"].astype(str).str.zfill(2)
        gdf["placefp"] = gdf["placefp"].astype(str).str.zfill(5)

    if "namelsad" not in gdf.columns:
        gdf["namelsad"] = pd.NA

    gdf = gdf[base_cols + ["geometry"]].copy()
    gdf["boundary_type"] = boundary_type
    return gdf


def normalize_statefp(value: Any) -> str:
    raw = str(value or "").strip()
    if raw.isdigit() and len(raw) == 2:
        return raw
    return "29"


def normalize_county_geoid(state_fips: str, county_fips: Any) -> str | None:
    if county_fips is None:
        return None
    raw = str(county_fips).strip()
    if not raw:
        return None
    if raw.isdigit() and len(raw) == 5 and raw.startswith(state_fips):
        return raw
    if raw.isdigit() and len(raw) == 3:
        return f"{state_fips}{raw.zfill(3)}"
    return None


def normalize_place_geoid(state_fips: str, place_fips_or_geoid: Any) -> str | None:
    if place_fips_or_geoid is None:
        return None
    raw = str(place_fips_or_geoid).strip()
    if not raw:
        return None
    if raw.isdigit() and len(raw) == 7 and raw.startswith(state_fips):
        return raw
    if raw.isdigit() and len(raw) == 5:
        return f"{state_fips}{raw.zfill(5)}"
    return None


def _parse_json(value: Any) -> Dict[str, Any] | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return None


def _latest_census_record(census: Dict[str, Any]) -> Dict[str, Any] | None:
    if not census:
        return None
    years = [int(key) for key in census.keys() if str(key).isdigit()]
    if not years:
        return None
    latest = str(max(years))
    record = census.get(latest)
    return record if isinstance(record, dict) else None


def _extract_census_identifiers(row: pd.Series) -> Dict[str, Any]:
    raw = _parse_json(row.get("census_identifiers"))
    if raw:
        return raw

    resp = _parse_json(row.get("geocode_jurisdiction_response"))
    if resp is None:
        return {}
    results = resp.get("results")
    if not isinstance(results, list) or not results:
        return {}
    fields = results[0].get("fields")
    if not isinstance(fields, dict):
        return {}
    census = fields.get("census")
    if not isinstance(census, dict):
        return {}
    record = _latest_census_record(census)
    if not record:
        return {}

    place = record.get("place")
    if isinstance(place, dict):
        place_value = place.get("fips") or place.get("geoid") or place.get("place")
    else:
        place_value = place

    return {
        "state_fips": record.get("state_fips"),
        "county_fips": record.get("county_fips"),
        "place": place_value,
    }


def _normalize_agency_type(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _pick_agency_id(row: pd.Series) -> str | None:
    for col in ("agency_id", "AgencyID", "agency_slug"):
        if col in row and pd.notna(row[col]):
            value = str(row[col]).strip()
            if value and value.lower() != "nan":
                return value
    for col in ("Department", "Canonical", "AgencyName"):
        if col in row and pd.notna(row[col]):
            value = str(row[col]).strip()
            if value and value.lower() != "nan":
                return _agency_slug(value)
    return None


def _pick_agency_name(row: pd.Series) -> str | None:
    for col in ("Department", "Canonical", "AgencyName", "Agency Name", "Agency"):
        if col in row and pd.notna(row[col]):
            value = str(row[col]).strip()
            if value and value.lower() != "nan":
                return value
    return None


def _agency_slug(value: str) -> str:
    text = str(value or "").replace("'", "").strip()
    if not text or text.lower() == "nan":
        return "agency"
    return slugify(text, lowercase=True) or "agency"


def _normalize_agency_key(value: Any) -> str:
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


def _collect_agency_names(row: pd.Series) -> list[str]:
    names: list[str] = []
    for col in ("Department", "Canonical", "AgencyName", "Agency Name", "Agency"):
        if col in row and pd.notna(row[col]):
            value = str(row[col]).strip()
            if value and value.lower() != "nan":
                names.append(value)
    return names


def agency_to_boundary(row: pd.Series) -> Tuple[str | None, str | None]:
    agency_type = _normalize_agency_type(row.get("AgencyType") or row.get("agency_type"))
    census = _extract_census_identifiers(row)
    statefp = normalize_statefp(census.get("state_fips"))

    if "county" in agency_type:
        geoid = normalize_county_geoid(statefp, census.get("county_fips"))
        return ("county", geoid) if geoid else (None, None)

    if "municipal" in agency_type:
        geoid = normalize_place_geoid(statefp, census.get("place"))
        return ("place", geoid) if geoid else (None, None)

    return (None, None)


def _build_total_stops_lookup(combined: pd.DataFrame) -> Dict[str, int]:
    if combined.empty or "row_key" not in combined.columns:
        return {}
    subset = combined[combined["row_key"] == TARGET_ROW_KEY].copy()
    if subset.empty:
        return {}
    subset = subset[["agency", "year", "Total"]].copy()
    subset = subset[pd.notna(subset["agency"])]
    subset["agency"] = subset["agency"].astype(str).str.strip()
    subset["year"] = pd.to_numeric(subset["year"], errors="coerce")
    subset = subset.sort_values(["agency", "year"], ascending=[True, False])

    lookup: Dict[str, int] = {}
    for agency, group in subset.groupby("agency"):
        for value in group["Total"].tolist():
            if pd.isna(value):
                continue
            try:
                lookup[agency] = int(value)
            except (TypeError, ValueError):
                continue
            break
    return lookup


def _prepare_tile_layer(gdf: pd.DataFrame, boundary_type: str):
    gpd = _require_geopandas()
    cols = ["geoid", "name", "namelsad", "statefp", "agency_id", "agency_name", "total_stops", "geometry"]
    if boundary_type == "county":
        cols.insert(4, "countyfp")
    else:
        cols.insert(4, "placefp")
    missing = [col for col in cols if col not in gdf.columns]
    if missing:
        raise ValueError(f"Missing required columns for tiles: {missing}")
    layer = gpd.GeoDataFrame(gdf[cols].copy(), geometry="geometry", crs="EPSG:4326")
    return layer


def _extract_address_point(row: pd.Series):
    """Return (lat, lng) from geocode_address_response, or (None, None)."""
    resp = _parse_json(row.get("geocode_address_response"))
    if resp is None:
        return None, None
    results = resp.get("results")
    if not isinstance(results, list) or not results:
        return None, None
    loc = results[0].get("location")
    if not isinstance(loc, dict):
        return None, None
    return loc.get("lat"), loc.get("lng")


def _prepare_centroid_layer(
    counties_gdf: pd.DataFrame,
    places_gdf: pd.DataFrame,
    agency_reference_geocoded: pd.DataFrame,
):
    gpd = _require_geopandas()
    from shapely.geometry import Point  # type: ignore

    cols = ["geoid", "name", "agency_id", "agency_name", "total_stops", "boundary_type", "geometry"]

    # Matched agencies: centroid derived from polygon boundary
    county_matched = counties_gdf[pd.notna(counties_gdf["agency_id"])].copy()
    place_matched = places_gdf[pd.notna(places_gdf["agency_id"])].copy()
    county_matched["geometry"] = county_matched.geometry.centroid
    place_matched["geometry"] = place_matched.geometry.centroid

    matched_ids = set(
        pd.concat(
            [county_matched["agency_id"], place_matched["agency_id"]], ignore_index=True
        ).dropna().astype(str)
    )

    # Unmatched agencies: fall back to geocoded address point
    fallback_rows = []
    for _, row in agency_reference_geocoded.iterrows():
        agency_id = _pick_agency_id(row)
        if agency_id is None or str(agency_id) in matched_ids:
            continue
        lat, lng = _extract_address_point(row)
        if lat is None or lng is None:
            continue
        fallback_rows.append({
            "geoid": None,
            "name": _pick_agency_name(row),
            "agency_id": agency_id,
            "agency_name": _pick_agency_name(row),
            "total_stops": None,
            "boundary_type": "point",
            "geometry": Point(lng, lat),
        })

    parts: list[pd.DataFrame] = [
        p[cols] for p in [county_matched, place_matched] if not p.empty
    ]
    if fallback_rows:
        parts.append(
            gpd.GeoDataFrame(fallback_rows, geometry="geometry", crs="EPSG:4326")[cols]
        )

    if not parts:
        return gpd.GeoDataFrame(columns=cols, geometry="geometry", crs="EPSG:4326")
    combined = pd.concat(parts, ignore_index=True)
    return gpd.GeoDataFrame(combined, geometry="geometry", crs="EPSG:4326")


def _run_command(cmd: List[str], log) -> None:
    log.info("Running command: %s", " ".join(cmd))
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        if result.stdout:
            log.debug("Command stdout: %s", result.stdout.strip())
        if result.stderr:
            # Suppress tippecanoe percentage-progress lines (start with a digit)
            non_progress = [
                line for line in result.stderr.splitlines()
                if not line.strip()[:1].isdigit()
            ]
            if non_progress:
                log.debug("Command stderr: %s", "\n".join(non_progress))
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr or str(exc)
        # Strip progress lines; surface last 50 non-progress lines as the error
        error_lines = [
            line for line in stderr.splitlines()
            if not line.strip()[:1].isdigit()
        ]
        raise RuntimeError(
            f"Command failed: {' '.join(cmd)}\n" + "\n".join(error_lines[-50:])
        ) from exc


@multi_asset(
    outs={
        "us_gpkg": AssetOut(),
    },
    group_name="gis",
    can_subset=True,
    required_resource_keys={"data_dir_source", "data_dir_processed"},
    description="Download and cache the GENZ2024 national GeoPackage (500k) for Missouri filtering.",
)
def download_mo_gis_zips(context):
    src_dir = Path(context.resources.data_dir_source.get_path()) / "gis"
    _ensure_dir(src_dir)
    force = _env_flag("FORCE_GIS_DOWNLOAD", False)

    zip_path = src_dir / "cb_2024_us_all_500k.zip"
    _download_file(GENZ_GPKG_URL, zip_path, context.log, force=force)
    if not _zip_has_gpkg(zip_path):
        raise RuntimeError(f"Downloaded ZIP does not contain a .gpkg: {zip_path}")
    gpkg_path = _extract_gpkg(zip_path, context.log)

    yield Output(
        str(gpkg_path),
        output_name="us_gpkg",
        metadata={"local_path": str(gpkg_path), "zip_path": str(zip_path), "url": GENZ_GPKG_URL},
    )


@multi_asset(
    ins={
        "us_gpkg": AssetIn(key=AssetKey("us_gpkg")),
    },
    outs={
        "mo_counties": AssetOut(),
        "mo_places": AssetOut(),
    },
    group_name="gis",
    required_resource_keys={"data_dir_processed"},
    description="Load Missouri counties/places from the GENZ2024 GeoPackage and write geoparquet.",
)
def load_mo_gis_layers(context, us_gpkg: str):
    gis_dir = _processed_gis_dir(context)
    _ensure_dir(gis_dir)
    gpkg_path = Path(us_gpkg)
    layers = _list_gpkg_layers(gpkg_path)
    county_layer = _select_layer(layers, "county", context.log)
    place_layer = _select_layer(layers, "place", context.log)

    county_gdf = _read_gpkg_layer(gpkg_path, county_layer, context.log)
    place_gdf = _read_gpkg_layer(gpkg_path, place_layer, context.log)

    county_gdf = _filter_missouri(county_gdf)
    place_gdf = _filter_missouri(place_gdf)

    county_gdf = _normalize_columns(county_gdf, boundary_type="county")
    place_gdf = _normalize_columns(place_gdf, boundary_type="place")

    county_valid_ratio = _fix_invalid_geometries(county_gdf, context.log)
    place_valid_ratio = _fix_invalid_geometries(place_gdf, context.log)

    county_path = gis_dir / COUNTY_PARQUET_NAME
    place_path = gis_dir / PLACE_PARQUET_NAME
    county_gdf.to_parquet(county_path, index=False)
    place_gdf.to_parquet(place_path, index=False)

    context.log.info("Wrote counties geoparquet → %s (%d rows)", county_path, len(county_gdf))
    context.log.info("Wrote places geoparquet → %s (%d rows)", place_path, len(place_gdf))

    yield Output(
        county_gdf,
        output_name="mo_counties",
        metadata={
            "row_count": len(county_gdf),
            "valid_ratio": county_valid_ratio,
            "local_path": str(county_path),
        },
    )
    yield Output(
        place_gdf,
        output_name="mo_places",
        metadata={
            "row_count": len(place_gdf),
            "valid_ratio": place_valid_ratio,
            "local_path": str(place_path),
        },
    )


@multi_asset(
    ins={
        "mo_counties": AssetIn(key=AssetKey("mo_counties")),
        "mo_places": AssetIn(key=AssetKey("mo_places")),
        "agency_reference_geocoded": AssetIn(key=AssetKey("agency_reference_geocoded")),
        "combine_all_reports": AssetIn(key=AssetKey("combine_all_reports")),
    },
    outs={
        "mo_counties_enriched": AssetOut(),
        "mo_places_enriched": AssetOut(),
        "agency_boundary_matches": AssetOut(),
    },
    group_name="gis",
    required_resource_keys={"data_dir_processed"},
    description="Join agency identifiers to county/place boundaries and enrich with agency metadata.",
)
def map_agencies_to_boundaries(
    context,
    mo_counties: pd.DataFrame,
    mo_places: pd.DataFrame,
    agency_reference_geocoded: pd.DataFrame,
    combine_all_reports: pd.DataFrame,
):
    gpd = _require_geopandas()
    mo_counties = gpd.GeoDataFrame(mo_counties, geometry="geometry", crs="EPSG:4326")
    mo_places = gpd.GeoDataFrame(mo_places, geometry="geometry", crs="EPSG:4326")

    total_stops_lookup = _build_total_stops_lookup(combine_all_reports)

    allowed_names: set[str] | None = None
    if not combine_all_reports.empty and "agency" in combine_all_reports.columns:
        value_cols = [c for c in PIVOT_VALUE_COLUMNS if c in combine_all_reports.columns]
        if value_cols:
            mask = combine_all_reports[value_cols].notna().any(axis=1)
            agencies = combine_all_reports.loc[mask, "agency"].dropna().astype(str)
            allowed_names = {_normalize_agency_key(a) for a in agencies if a.strip()}

    records: List[Dict[str, Any]] = []
    for _, row in agency_reference_geocoded.iterrows():
        agency_id = _pick_agency_id(row)
        agency_name = _pick_agency_name(row)
        agency_type = str(row.get("AgencyType") or "")
        boundary_type, boundary_geoid = agency_to_boundary(row)
        total_stops = total_stops_lookup.get(agency_name or "")
        has_data = None
        if allowed_names is not None:
            names = _collect_agency_names(row)
            keys = {_normalize_agency_key(name) for name in names}
            has_data = any(key and key in allowed_names for key in keys)
        records.append(
            {
                "agency_id": agency_id,
                "agency_name": agency_name,
                "agency_type": agency_type,
                "boundary_type": boundary_type,
                "boundary_geoid": boundary_geoid,
                "total_stops": total_stops,
                "matched": boundary_type is not None and boundary_geoid is not None,
                "has_data": has_data,
            }
        )

    matches = pd.DataFrame.from_records(records)
    if matches.empty:
        matches = pd.DataFrame(
            columns=[
                "agency_id",
                "agency_name",
                "agency_type",
                "boundary_type",
                "boundary_geoid",
                "total_stops",
                "matched",
            ]
        )

    unmatched = matches[~matches["matched"]]
    context.log.info("Unmatched agencies (no boundary geoid): %d", len(unmatched))

    county_matches = matches[matches["boundary_type"] == "county"].dropna(subset=["boundary_geoid"])
    place_matches = matches[matches["boundary_type"] == "place"].dropna(subset=["boundary_geoid"])

    def _select_highest_volume(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        working = df.copy()
        working["total_stops_num"] = pd.to_numeric(working["total_stops"], errors="coerce").fillna(-1)
        idx = working.groupby("boundary_geoid")["total_stops_num"].idxmax()
        return working.loc[idx].drop(columns=["total_stops_num"], errors="ignore")

    dup_counties = county_matches[county_matches.duplicated(subset=["boundary_geoid"], keep=False)]
    if not dup_counties.empty:
        context.log.warning(
            "Duplicate county boundary GEOIDs in agency matches; using highest total_stops. Examples: %s",
            dup_counties["boundary_geoid"].drop_duplicates().head(5).tolist(),
        )
    dup_places = place_matches[place_matches.duplicated(subset=["boundary_geoid"], keep=False)]
    if not dup_places.empty:
        context.log.warning(
            "Duplicate place boundary GEOIDs in agency matches; using highest total_stops. Examples: %s",
            dup_places["boundary_geoid"].drop_duplicates().head(5).tolist(),
        )

    county_matches = _select_highest_volume(county_matches)
    place_matches = _select_highest_volume(place_matches)

    county_enriched = mo_counties.merge(
        county_matches,
        how="left",
        left_on="geoid",
        right_on="boundary_geoid",
    )
    place_enriched = mo_places.merge(
        place_matches,
        how="left",
        left_on="geoid",
        right_on="boundary_geoid",
    )
    county_enriched = gpd.GeoDataFrame(county_enriched, geometry="geometry", crs="EPSG:4326")
    place_enriched = gpd.GeoDataFrame(place_enriched, geometry="geometry", crs="EPSG:4326")

    county_enriched["boundary_type"] = "county"
    place_enriched["boundary_type"] = "place"

    keep_cols = ["geoid", "name", "namelsad", "statefp", "agency_id", "agency_name", "total_stops", "boundary_type", "geometry"]
    county_cols = keep_cols[:4] + ["countyfp"] + keep_cols[4:]
    place_cols = keep_cols[:4] + ["placefp"] + keep_cols[4:]

    county_enriched = county_enriched[county_cols].copy()
    place_enriched = place_enriched[place_cols].copy()

    county_total = len(
        matches[matches["agency_type"].astype(str).str.contains("county", case=False, na=False)]
    )
    county_matched = len(county_matches)
    county_match_rate = (county_matched / county_total) if county_total else 0.0

    context.log.info(
        "County agency match rate: %d/%d (%.2f)",
        county_matched,
        county_total,
        county_match_rate,
    )

    gis_dir = _processed_gis_dir(context)
    _ensure_dir(gis_dir)
    boundary_path = gis_dir / BOUNDARY_MATCHES_NAME
    matches.to_parquet(boundary_path, index=False)
    context.log.info("Wrote agency boundary matches → %s (%d rows)", boundary_path, len(matches))

    yield Output(
        county_enriched,
        output_name="mo_counties_enriched",
        metadata={"row_count": len(county_enriched)},
    )
    yield Output(
        place_enriched,
        output_name="mo_places_enriched",
        metadata={"row_count": len(place_enriched)},
    )
    yield Output(
        matches,
        output_name="agency_boundary_matches",
        metadata={
            "row_count": len(matches),
            "county_match_rate": county_match_rate,
            "local_path": str(boundary_path),
        },
    )


@multi_asset(
    ins={
        "mo_counties_enriched": AssetIn(key=AssetKey("mo_counties_enriched")),
        "mo_places_enriched": AssetIn(key=AssetKey("mo_places_enriched")),
        "agency_reference_geocoded": AssetIn(key=AssetKey("agency_reference_geocoded")),
    },
    outs={
        "mo_jurisdictions_pmtiles": AssetOut(),
        "mo_jurisdictions_manifest": AssetOut(),
    },
    group_name="dist",
    required_resource_keys={"data_dir_processed", "data_dir_out", "s3"},
    description="Build PMTiles for Missouri jurisdictions and write a manifest.",
)
def build_mo_jurisdiction_tiles(
    context,
    mo_counties_enriched: pd.DataFrame,
    mo_places_enriched: pd.DataFrame,
    agency_reference_geocoded: pd.DataFrame,
):
    tiles_dir = _out_tiles_dir(context)
    _ensure_dir(tiles_dir)
    tmp_dir = _processed_gis_dir(context) / "tiles_tmp"
    _ensure_dir(tmp_dir)

    counties_layer = _prepare_tile_layer(mo_counties_enriched, boundary_type="county")
    places_layer = _prepare_tile_layer(mo_places_enriched, boundary_type="place")
    centroids_layer = _prepare_centroid_layer(mo_counties_enriched, mo_places_enriched, agency_reference_geocoded)

    counties_geojson = tmp_dir / "counties.geojson"
    places_geojson = tmp_dir / "places.geojson"
    centroids_geojson = tmp_dir / "centroids.geojson"
    counties_layer.to_file(counties_geojson, driver="GeoJSON", index=False)
    places_layer.to_file(places_geojson, driver="GeoJSON", index=False)
    centroids_layer.to_file(centroids_geojson, driver="GeoJSON", index=False)

    tippecanoe = shutil.which("tippecanoe")
    tile_join = shutil.which("tile-join")
    pmtiles = shutil.which("pmtiles")
    if not tippecanoe or not tile_join:
        raise RuntimeError("tippecanoe and tile-join are required to build PMTiles.")

    counties_mbtiles = tmp_dir / "counties.mbtiles"
    places_mbtiles = tmp_dir / "places.mbtiles"
    centroids_mbtiles = tmp_dir / "centroids.mbtiles"
    combined_mbtiles = tmp_dir / "mo_jurisdictions.mbtiles"

    _run_command(
        [
            tippecanoe,
            "-o",
            str(counties_mbtiles),
            "-l",
            "counties",
            "-Z0",
            "-z10",
            "--force",
            str(counties_geojson),
        ],
        context.log,
    )
    _run_command(
        [
            tippecanoe,
            "-o",
            str(places_mbtiles),
            "-l",
            "places",
            "-Z0",
            "-z12",
            "--force",
            str(places_geojson),
        ],
        context.log,
    )
    _run_command(
        [
            tippecanoe,
            "-o",
            str(centroids_mbtiles),
            "-l",
            "centroids",
            "-Z0",
            "-z12",
            "-B0",
            "--force",
            str(centroids_geojson),
        ],
        context.log,
    )
    _run_command(
        [
            tile_join,
            "-o",
            str(combined_mbtiles),
            "--force",
            str(counties_mbtiles),
            str(places_mbtiles),
            str(centroids_mbtiles),
        ],
        context.log,
    )

    pmtiles_path = tiles_dir / PMTILES_NAME
    manifest_path = tiles_dir / MANIFEST_NAME
    if pmtiles:
        _run_command([pmtiles, "convert", str(combined_mbtiles), str(pmtiles_path)], context.log)
    else:
        _run_command(
            [
                tippecanoe,
                "-o",
                str(pmtiles_path),
                "-Z0",
                "-z12",
                "-B0",
                "--force",
                "-L",
                f"counties:{counties_geojson}",
                "-L",
                f"places:{places_geojson}",
                "-L",
                f"centroids:{centroids_geojson}",
            ],
            context.log,
        )

    pmtiles_size = pmtiles_path.stat().st_size if pmtiles_path.exists() else 0

    manifest = {
        "layers": {
            "counties": {
                "row_count": int(len(counties_layer)),
                "bounds": [float(v) for v in counties_layer.total_bounds],
                "schema": {col: str(counties_layer[col].dtype) for col in counties_layer.columns if col != "geometry"},
            },
            "places": {
                "row_count": int(len(places_layer)),
                "bounds": [float(v) for v in places_layer.total_bounds],
                "schema": {col: str(places_layer[col].dtype) for col in places_layer.columns if col != "geometry"},
            },
            "centroids": {
                "row_count": int(len(centroids_layer)),
                "bounds": [float(v) for v in centroids_layer.total_bounds] if not centroids_layer.empty else [],
                "schema": {col: str(centroids_layer[col].dtype) for col in centroids_layer.columns if col != "geometry"},
                "note": "Point layer: polygon centroid for county/municipal agencies; geocoded address point for others.",
            },
        },
        "pmtiles": {
            "path": str(pmtiles_path),
            "size_bytes": pmtiles_size,
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2))
    context.log.info("Wrote PMTiles → %s (%.2f MB)", pmtiles_path, pmtiles_size / (1024 * 1024))
    context.log.info("Wrote manifest → %s", manifest_path)

    base_dir = Path(context.resources.data_dir_out.get_path())
    uploaded = upload_paths(
        context,
        [pmtiles_path, manifest_path],
        base_dir=base_dir,
    )
    if uploaded:
        context.log.info("Uploaded %d jurisdiction tile artifacts to S3", len(uploaded))

    pmtiles_meta = {"size_bytes": pmtiles_size}
    pmtiles_s3 = s3_uri_for_path(context, pmtiles_path, base_dir)
    if pmtiles_s3:
        pmtiles_meta["s3_path"] = pmtiles_s3
    manifest_meta = {"local_path": str(manifest_path)}
    manifest_s3 = s3_uri_for_path(context, manifest_path, base_dir)
    if manifest_s3:
        manifest_meta["s3_path"] = manifest_s3

    yield Output(str(pmtiles_path), output_name="mo_jurisdictions_pmtiles", metadata=pmtiles_meta)
    yield Output(str(manifest_path), output_name="mo_jurisdictions_manifest", metadata=manifest_meta)


def _build_touching_map(gdf) -> Dict[str, List[str]]:
    touch_map: Dict[str, List[str]] = {}
    if gdf.empty:
        return touch_map

    gdf = gdf.reset_index(drop=True)
    sindex = gdf.sindex

    for idx, geom in enumerate(gdf.geometry):
        agency_id = gdf.loc[idx, "agency_id"]
        if not agency_id or geom is None or geom.is_empty:
            touch_map[str(agency_id)] = []
            continue
        candidates = list(sindex.intersection(geom.bounds))
        neighbors: List[str] = []
        for cand in candidates:
            if cand == idx:
                continue
            other_geom = gdf.geometry.iloc[cand]
            if other_geom is None or other_geom.is_empty:
                continue
            if geom.touches(other_geom):
                other_id = gdf.loc[cand, "agency_id"]
                if other_id and other_id != agency_id:
                    neighbors.append(str(other_id))
        touch_map[str(agency_id)] = sorted(set(neighbors))
    return touch_map


def _build_contained_map(counties_gdf, places_gdf) -> Dict[str, List[str]]:
    contained: Dict[str, List[str]] = {}
    if counties_gdf.empty or places_gdf.empty:
        return contained
    places_gdf = places_gdf.reset_index(drop=True)
    place_index = places_gdf.sindex

    for idx, geom in enumerate(counties_gdf.geometry):
        agency_id = counties_gdf.loc[idx, "agency_id"]
        if not agency_id or geom is None or geom.is_empty:
            contained[str(agency_id)] = []
            continue
        candidates = list(place_index.intersection(geom.bounds))
        inside: List[str] = []
        for cand in candidates:
            place_geom = places_gdf.geometry.iloc[cand]
            if place_geom is None or place_geom.is_empty:
                continue
            if place_geom.within(geom):
                place_id = places_gdf.loc[cand, "agency_id"]
                if place_id:
                    inside.append(str(place_id))
        contained[str(agency_id)] = sorted(set(inside))
    return contained


@op(out=Out(pd.DataFrame), required_resource_keys={"data_dir_out", "s3"})
def build_agency_relationships(
    context,
    agency_boundary_matches: pd.DataFrame,
    mo_counties: pd.DataFrame,
    mo_places: pd.DataFrame,
) -> pd.DataFrame:
    out_dir = _out_gis_dir(context)
    boundaries_dir = _out_agency_boundaries_dir(context)
    _ensure_dir(out_dir)
    _ensure_dir(boundaries_dir)

    gpd = _require_geopandas()
    counties = gpd.GeoDataFrame(mo_counties, geometry="geometry", crs="EPSG:4326")
    places = gpd.GeoDataFrame(mo_places, geometry="geometry", crs="EPSG:4326")

    matches = agency_boundary_matches.copy()
    matched_flag = matches["matched"] if "matched" in matches.columns else True
    matches = matches[matched_flag == True]  # noqa: E712
    if "has_data" in matches.columns:
        matches = matches[matches["has_data"] != False]  # noqa: E712
    matches = matches.dropna(subset=["boundary_geoid", "boundary_type", "agency_id"])

    county_matches = matches[matches["boundary_type"] == "county"].copy()
    place_matches = matches[matches["boundary_type"] == "place"].copy()

    counties_join = county_matches.merge(
        counties,
        how="left",
        left_on="boundary_geoid",
        right_on="geoid",
    )
    places_join = place_matches.merge(
        places,
        how="left",
        left_on="boundary_geoid",
        right_on="geoid",
    )

    counties_join["boundary_type"] = "county"
    places_join["boundary_type"] = "place"

    geoms = pd.concat([counties_join, places_join], ignore_index=True)
    geoms = geoms[pd.notna(geoms["geometry"])].copy()
    geoms = gpd.GeoDataFrame(geoms, geometry="geometry", crs="EPSG:4326")

    touching = _build_touching_map(geoms)
    contained = _build_contained_map(
        geoms[geoms["boundary_type"].eq("county")],
        geoms[geoms["boundary_type"].eq("place")],
    )

    agency_lookup: Dict[str, Dict[str, str]] = {}
    for _, row in geoms.iterrows():
        agency_id = row.get("agency_id")
        if not agency_id:
            continue
        agency_name = row.get("agency_name") or row.get("name")
        agency_slug = _agency_slug(agency_name or agency_id)
        agency_lookup[str(agency_id)] = {
            "agency_id": str(agency_id),
            "agency_slug": agency_slug,
            "agency_name": str(agency_name) if agency_name is not None else None,
        }

    def _expand_ids(ids: List[str]) -> List[Dict[str, str]]:
        return [agency_lookup[item] for item in ids if item in agency_lookup]

    rows = []
    for agency_id in geoms["agency_id"].dropna().astype(str).unique():
        rows.append(
            {
                "agency_id": agency_id,
                "touching_ids": touching.get(agency_id, []),
                "contained_ids": contained.get(agency_id, []),
            }
        )

    relationships = pd.DataFrame(rows)
    relationships_path = out_dir / RELATIONSHIPS_NAME
    relationships.to_parquet(relationships_path, index=False)
    context.log.info("Wrote agency relationships → %s (%d rows)", relationships_path, len(relationships))

    try:
        from shapely.geometry import mapping  # type: ignore
    except Exception:
        mapping = None

    geojson_paths: list[Path] = []
    if mapping is not None:
        for _, row in geoms.iterrows():
            agency_id = row.get("agency_id")
            if not agency_id:
                continue
            geom = row.get("geometry")
            if geom is None or getattr(geom, "is_empty", False):
                continue
            feature = {
                "type": "Feature",
                "geometry": mapping(geom),
                "properties": {
                    "geoid": row.get("geoid"),
                    "name": row.get("name"),
                    "boundary_type": row.get("boundary_type"),
                    "agency_id": agency_id,
                    "agency_slug": agency_lookup.get(str(agency_id), {}).get("agency_slug"),
                    "touching_ids": touching.get(str(agency_id), []),
                    "contained_ids": contained.get(str(agency_id), []),
                    "touching_agencies": _expand_ids(touching.get(str(agency_id), [])),
                    "contained_agencies": _expand_ids(contained.get(str(agency_id), [])),
                },
            }
            payload = {"type": "FeatureCollection", "features": [feature]}
            out_path = boundaries_dir / f"{agency_id}.geojson"
            out_path.write_text(json.dumps(payload))
            geojson_paths.append(out_path)

    context.log.info(
        "Wrote %d agency boundary GeoJSON files → %s/ (use dist_sync to upload)",
        len(geojson_paths), boundaries_dir,
    )
    try:
        base_dir = Path(context.resources.data_dir_out.get_path())
        metadata = {"local_path": str(relationships_path), "row_count": len(relationships)}
        s3_folder = s3_uri_for_dir(context, boundaries_dir, base_dir)
        if s3_folder:
            metadata["s3_folder"] = s3_folder
        context.add_output_metadata(metadata)
    except Exception:
        pass
    return relationships


@graph_asset(
    name="agency_relationships",
    group_name="dist",
    ins={
        "agency_boundary_matches": AssetIn(key=AssetKey("agency_boundary_matches")),
        "mo_counties": AssetIn(key=AssetKey("mo_counties")),
        "mo_places": AssetIn(key=AssetKey("mo_places")),
    },
    description="Compute touching/contained relationships and write per-agency boundary GeoJSON.",
)
def agency_relationships(
    agency_boundary_matches: pd.DataFrame,
    mo_counties: pd.DataFrame,
    mo_places: pd.DataFrame,
) -> pd.DataFrame:
    return build_agency_relationships(agency_boundary_matches, mo_counties, mo_places)


@asset(
    name="agency_boundaries_index",
    group_name="dist",
    deps=[AssetKey("agency_relationships")],
    required_resource_keys={"data_dir_out", "s3"},
    description="Index of available agency boundary GeoJSON files for the frontend.",
)
def agency_boundaries_index(context) -> str:
    boundaries_dir = _out_agency_boundaries_dir(context)
    if not boundaries_dir.exists():
        context.log.warning("Agency boundaries dir not found at %s", boundaries_dir)
        slugs: list[str] = []
    else:
        slugs = sorted([path.stem for path in boundaries_dir.glob("*.geojson")])

    payload = {"count": len(slugs), "slugs": slugs}

    out_path = Path(context.resources.data_dir_out.get_path()) / "agency_boundaries_index.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2))

    base_dir = Path(context.resources.data_dir_out.get_path())
    uploaded = upload_paths(context, [out_path], base_dir=base_dir)
    if uploaded:
        context.log.info("Uploaded agency boundaries index to S3")

    try:
        metadata = {"local_path": str(out_path), "count": len(slugs)}
        s3_path = s3_uri_for_path(context, out_path, base_dir)
        if s3_path:
            metadata["s3_path"] = s3_path
        context.add_output_metadata(metadata)
    except Exception:
        pass

    return str(out_path)


LOCATOR_SVG_NAME = "mo_locator.svg"
LOCATOR_SIMPLIFY_TOLERANCE = 0.003  # degrees; ~330m at MO latitudes
LOCATOR_COORD_PRECISION = 4


def _format_coord(value: float) -> str:
    text = f"{value:.{LOCATOR_COORD_PRECISION}f}"
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _ring_to_path_d(coords, project) -> str:
    parts: list[str] = []
    for idx, (lng, lat) in enumerate(coords):
        x, y = project(lng, lat)
        cmd = "M" if idx == 0 else "L"
        parts.append(f"{cmd}{_format_coord(x)} {_format_coord(y)}")
    parts.append("Z")
    return "".join(parts)


def _geometry_to_path_d(geom, project) -> str:
    from shapely.geometry import MultiPolygon, Polygon  # type: ignore

    pieces: list[str] = []

    def _add_polygon(poly: "Polygon") -> None:
        if poly.is_empty:
            return
        pieces.append(_ring_to_path_d(list(poly.exterior.coords), project))
        for interior in poly.interiors:
            pieces.append(_ring_to_path_d(list(interior.coords), project))

    if isinstance(geom, Polygon):
        _add_polygon(geom)
    elif isinstance(geom, MultiPolygon):
        for poly in geom.geoms:
            _add_polygon(poly)
    return "".join(pieces)


@asset(
    name="mo_locator_svg",
    group_name="dist",
    deps=[AssetKey("agency_relationships")],
    ins={"mo_counties": AssetIn(key=AssetKey("mo_counties"))},
    required_resource_keys={"data_dir_out", "s3"},
    description=(
        "Static MO locator SVG with per-agency <path id=\"agency-{slug}\"> for "
        "frontend cards (issue #25). Inline once and <use> per card; CSS handles "
        "highlighting."
    ),
)
def mo_locator_svg(context, mo_counties: pd.DataFrame) -> str:
    import math

    from shapely.geometry import shape  # type: ignore
    from shapely.ops import unary_union  # type: ignore

    boundaries_dir = _out_agency_boundaries_dir(context)
    if not boundaries_dir.exists():
        raise RuntimeError(
            f"Expected agency boundaries at {boundaries_dir}; materialize agency_relationships first."
        )

    geojson_paths = sorted(boundaries_dir.glob("*.geojson"))
    if not geojson_paths:
        raise RuntimeError(f"No agency boundary GeoJSONs found in {boundaries_dir}")

    state_geom = unary_union([g for g in mo_counties["geometry"] if g is not None and not g.is_empty])
    if state_geom.is_empty:
        raise RuntimeError("Empty MO state geometry from mo_counties")

    min_lng, min_lat, max_lng, max_lat = state_geom.bounds
    mid_lat = (min_lat + max_lat) / 2.0
    cos_mid = math.cos(math.radians(mid_lat))

    def project(lng: float, lat: float) -> tuple[float, float]:
        return lng * cos_mid, -lat

    proj_xs = [min_lng * cos_mid, max_lng * cos_mid]
    proj_ys = [-max_lat, -min_lat]
    vb_x = min(proj_xs)
    vb_y = min(proj_ys)
    vb_w = max(proj_xs) - vb_x
    vb_h = max(proj_ys) - vb_y

    state_simplified = state_geom.simplify(LOCATOR_SIMPLIFY_TOLERANCE, preserve_topology=True)
    state_d = _geometry_to_path_d(state_simplified, project)

    agency_paths: list[str] = []
    centroid_circles: list[str] = []
    seen_slugs: set[str] = set()
    skipped = 0

    for path in geojson_paths:
        try:
            payload = json.loads(path.read_text())
        except Exception as exc:
            context.log.warning("Could not read %s: %s", path, exc)
            skipped += 1
            continue
        features = payload.get("features") or []
        if not features:
            skipped += 1
            continue
        feat = features[0]
        props = feat.get("properties") or {}
        slug = props.get("agency_slug") or _agency_slug(path.stem)
        if slug in seen_slugs:
            slug = f"{slug}-{path.stem}"
        seen_slugs.add(slug)

        try:
            geom = shape(feat["geometry"])
        except Exception as exc:
            context.log.warning("Bad geometry in %s: %s", path, exc)
            skipped += 1
            continue

        geom = geom.simplify(LOCATOR_SIMPLIFY_TOLERANCE, preserve_topology=True)
        if geom.is_empty:
            skipped += 1
            continue

        d = _geometry_to_path_d(geom, project)
        if not d:
            skipped += 1
            continue
        agency_paths.append(f'<path id="agency-{slug}" class="agency" d="{d}"/>')

        try:
            centroid = geom.representative_point()
            cx, cy = project(centroid.x, centroid.y)
            centroid_circles.append(
                f'<circle class="centroid" data-slug="{slug}" '
                f'cx="{_format_coord(cx)}" cy="{_format_coord(cy)}" r="0.015"/>'
            )
        except Exception:
            pass

    viewbox = (
        f"{_format_coord(vb_x)} {_format_coord(vb_y)} "
        f"{_format_coord(vb_w)} {_format_coord(vb_h)}"
    )

    svg_parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{viewbox}" '
        'preserveAspectRatio="xMidYMid meet">',
        f'<path class="state" d="{state_d}"/>',
        '<g class="agencies">',
        *agency_paths,
        '</g>',
        '<g class="centroids">',
        *centroid_circles,
        '</g>',
        '</svg>',
    ]
    svg_text = "\n".join(svg_parts) + "\n"

    out_dir = Path(context.resources.data_dir_out.get_path())
    _ensure_dir(out_dir)
    out_path = out_dir / LOCATOR_SVG_NAME
    out_path.write_text(svg_text)

    base_dir = Path(context.resources.data_dir_out.get_path())
    upload_paths(context, [out_path], base_dir=base_dir)

    metadata: Dict[str, Any] = {
        "local_path": str(out_path),
        "agency_count": len(agency_paths),
        "skipped": skipped,
        "size_bytes": out_path.stat().st_size,
        "viewbox": viewbox,
    }
    s3_path = s3_uri_for_path(context, out_path, base_dir)
    if s3_path:
        metadata["s3_path"] = s3_path
    context.add_output_metadata(metadata)

    context.log.info(
        "Wrote MO locator SVG → %s (%d agencies, %d bytes, skipped=%d)",
        out_path,
        len(agency_paths),
        out_path.stat().st_size,
        skipped,
    )
    return str(out_path)
