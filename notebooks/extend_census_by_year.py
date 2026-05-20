"""
Pull ACS 5-year estimates (2014-2019) for Missouri counties and places and
append to census_population_by_year.csv so the Observable per-capita selector
can render 11 years of data instead of 5.

Uses table B03002 (Hispanic or Latino by Race):
  B03002_001E = Total
  B03002_003E = White alone, not Hispanic
  B03002_004E = Black or African American alone, not Hispanic
  B03002_012E = Hispanic or Latino

The existing file already contains rows for 2020-2024 at county + place level
for Missouri. This script adds 2014-2019 for both geo types in the same schema.
"""

import csv
import sys
import time
import urllib.request
import json
from pathlib import Path

CSV_PATH = Path(__file__).parent / "observable" / "src" / "data" / "census_population_by_year.csv"

FIELDS = ["B03002_001E", "B03002_003E", "B03002_004E", "B03002_012E"]
STATE_FIPS = "29"  # Missouri
YEARS = [2014, 2015, 2016, 2017, 2018, 2019]

BASE_URL = "https://api.census.gov/data/{year}/acs/acs5"


def fetch(url):
    with urllib.request.urlopen(url, timeout=60) as r:
        return json.loads(r.read().decode("utf-8"))


def pull_counties(year):
    url = f"{BASE_URL.format(year=year)}?get=NAME,{','.join(FIELDS)}&for=county:*&in=state:{STATE_FIPS}"
    data = fetch(url)
    header = data[0]
    rows = []
    for row in data[1:]:
        rec = dict(zip(header, row))
        rows.append({
            "year": year,
            "source": "acs5",
            "geo_type": "county",
            "name": rec["NAME"],
            "Total": rec["B03002_001E"],
            "White": rec["B03002_003E"],
            "Black": rec["B03002_004E"],
            "Hispanic": rec["B03002_012E"],
            "state_fips": rec["state"],
            "county_fips": rec["county"],
            "place_fips": "",
        })
    return rows


def pull_places(year):
    url = f"{BASE_URL.format(year=year)}?get=NAME,{','.join(FIELDS)}&for=place:*&in=state:{STATE_FIPS}"
    data = fetch(url)
    header = data[0]
    rows = []
    for row in data[1:]:
        rec = dict(zip(header, row))
        rows.append({
            "year": year,
            "source": "acs5",
            "geo_type": "place",
            "name": rec["NAME"],
            "Total": rec["B03002_001E"],
            "White": rec["B03002_003E"],
            "Black": rec["B03002_004E"],
            "Hispanic": rec["B03002_012E"],
            "state_fips": rec["state"],
            "county_fips": "",
            "place_fips": rec["place"],
        })
    return rows


def main():
    assert CSV_PATH.exists(), f"missing {CSV_PATH}"

    with CSV_PATH.open() as f:
        reader = csv.DictReader(f)
        existing_rows = list(reader)
        fieldnames = reader.fieldnames

    existing_keys = {
        (r["year"], r["geo_type"], r["name"]) for r in existing_rows
    }
    print(f"existing rows: {len(existing_rows)}")

    new_rows = []
    for year in YEARS:
        print(f"year {year}: fetching counties...", end=" ", flush=True)
        c = pull_counties(year)
        print(f"{len(c)} counties", end=" / ", flush=True)
        time.sleep(0.2)
        print("places...", end=" ", flush=True)
        p = pull_places(year)
        print(f"{len(p)} places")
        time.sleep(0.2)
        for row in c + p:
            key = (str(row["year"]), row["geo_type"], row["name"])
            if key in existing_keys:
                continue
            new_rows.append(row)

    print(f"new rows to append: {len(new_rows)}")

    all_rows = existing_rows + new_rows
    all_rows.sort(key=lambda r: (int(r["year"]), r["geo_type"], r["name"]))

    with CSV_PATH.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"wrote {len(all_rows)} total rows to {CSV_PATH}")


if __name__ == "__main__":
    main()
