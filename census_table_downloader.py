## Census ACS Table Downloader
# Last update: 2025-01-03

"""Download ACS tables from the U.S. Census Bureau using the Data API."""

# Erase all declared global variables
globals().clear()

import os
from pathlib import Path
import pandas as pd
import requests


# Settings
if pd.__version__ >= '1.5.0' and pd.__version__ < '3.0.0':
    pd.options.mode.copy_on_write = True


def fetch_acs_table_names(*, year=2023, dataset="acs/acs5", api_key=None):
    """Return a list of ACS table names available for the specified dataset."""
    url = f"https://api.census.gov/data/{year}/{dataset}/groups.json"
    params = {}
    if api_key:
        params["key"] = api_key
    response = requests.get(url=url, params=params, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5, verify=True)
    response.raise_for_status()
    groups = response.json()
    return [group["name"] for group in groups]


def download_acs_tables(*, year=2023, dataset="acs/acs5", geography="us:*", output_directory=".", api_key=None):
    """Download all ACS tables and save each one as a CSV file."""
    if api_key is None:
        api_key = os.getenv("CENSUS_API_KEY")
    tables = fetch_acs_table_names(year=year, dataset=dataset, api_key=api_key)
    output_dir = Path(output_directory)
    output_dir.mkdir(parents=True, exist_ok=True)
    for table in tables:
        url = f"https://api.census.gov/data/{year}/{dataset}"
        params = {
            "get": "NAME",
            "for": geography,
            "group": table,
        }
        if api_key:
            params["key"] = api_key
        response = requests.get(url=url, params=params, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5, verify=True)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data[1:], columns=data[0])
        df.to_csv(output_dir / f"{table}.csv", index=False, encoding="utf-8")

