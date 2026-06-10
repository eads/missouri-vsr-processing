# definitions.py
from pathlib import Path

from dagster import Definitions, load_assets_from_modules

from missouri_vsr.asset_checks import asset_checks
from missouri_vsr.assets import agency_reference, audit, extract, gis, processed, program_287g, reports, state_report
from missouri_vsr.assets.analysis import initial_impressions_2025
from missouri_vsr.resources import LocalDirectoryResource, S3Resource

DATA_DIR_SOURCE=Path("data/src")
DATA_DIR_REPORT_PDFS=Path("data/src/reports")
DATA_DIR_PROCESSED=Path("data/processed")
DATA_DIR_OUT=Path("data/out")

# Automatically load assets from the assets modules.
assets_loaded = load_assets_from_modules(
    [extract, reports, processed, agency_reference, audit, gis, program_287g, state_report,
     initial_impressions_2025]
)

defs = Definitions(
    assets=assets_loaded,
    asset_checks=asset_checks,
    resources={
        "data_dir_source": LocalDirectoryResource(path=str(DATA_DIR_SOURCE)),
        "data_dir_report_pdfs": LocalDirectoryResource(path=str(DATA_DIR_REPORT_PDFS)),
        "data_dir_processed": LocalDirectoryResource(path=str(DATA_DIR_PROCESSED)),
        "data_dir_out": LocalDirectoryResource(path=str(DATA_DIR_OUT)),
        "s3": S3Resource.configure_at_launch(),
    },
)
