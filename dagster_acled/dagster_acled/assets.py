import os
from typing import Literal

from config.secrets_config import SecretManager
import dagster as dg
from dagster import AssetIn, AssetOut, MetadataValue, TableRecord
import polars as pl
import requests

sm = SecretManager(region_name="eu-north-1")
API_KEY = sm.get_secret('ACLED-API')
EMAIL = "s.a.bojilov@umail.leidenuniv.nl"

ACLED_API_BASE = "https://api.acleddata.com"
ACLED_EVENT_ENDPOINT = "/acled/read"


def fetch_acled_data(
    api_key: str,
    email: str,
    limit: int = 5000,
    max_pages: int = 2,
    query_params: dict | None = None,
) -> pl.DataFrame:
    all_rows = []
    page = 1
    while page <= max_pages:
        params = {"key": api_key, "email": email, "limit": limit, "page": page}
        if query_params:
            params.update(query_params)

        url = ACLED_API_BASE + ACLED_EVENT_ENDPOINT
        resp = requests.get(url, params=params)
        resp.raise_for_status()

        payload = resp.json()
        rows = payload.get("data", [])
        if not rows:
            break

        all_rows.extend(rows)
        if len(rows) < limit:
            break
        page += 1

    return pl.DataFrame(all_rows)


@dg.asset
def acled_events():
    """
    Dagster asset to ingest ACLED event data.
    """
    query_params = {
        "iso": 100,
        "year": 2024,
    }

    df = fetch_acled_data(
        api_key=API_KEY,
        email=EMAIL,
        limit=2000,
        max_pages=3,
        query_params=query_params,
    )
    records = [TableRecord(row) for row in df.head(5).to_dicts()]

    yield dg.Output(
        df,
        metadata={
        "preview": dg.MetadataValue.table(records),
        "row_count": dg.MetadataValue.md(f"**Rows:** {len(df)}"),
        },
    )