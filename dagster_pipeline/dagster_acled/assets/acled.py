from datetime import date, datetime, timedelta
import os

import aiohttp
from config.folder_config import DataFolder
from config.secrets_config import SecretManager
import dagster as dg
from dagster import TableRecord
from dagster_acled.partitions import weekly_partition
from dagster_acled.utils import fetch_page
from dagster_pipeline.dagster_acled.acled_client import AcledConfig
import polars as pl

sm = SecretManager(region_name=os.environ['REGION_NAME'])
API_KEY = sm.get_secret('ACLED-API')


@dg.asset(
    partitions_def=weekly_partition
)
async def acled_request(
    context: dg.AssetExecutionContext,
    config: AcledConfig,
) -> dg.MaterializeResult:
    """
    Fetch ACLED events for this week’s partition,
    verify full coverage of event_date, and log an error if data is missing.
    """
    # derive week bounds from the partition time window
    week_start: date = context.partition_time_window.start.date()
    week_end: date = context.partition_time_window.end.date()

    url = f"{config.base_url.rstrip('/')}/{config.endpoint}"
    all_rows = []
    page = 1

    async with aiohttp.ClientSession() as session:
        day = week_start
        while day <= week_end:
            while True:
                params = config.build_params()
                params.update({
                    "limit": config.max_pages,
                    "page": page,
                    "event_date": day.isoformat(),
                    "event_date_where": "=",
                })
                chunk = await fetch_page(session, url, params)
                if not chunk:
                    break

                all_rows.extend(chunk)
                if len(chunk) < config.max_pages:
                    break
                page += 1
            day += timedelta(days=1)

    # TODO: upsert to database instead of writing to CSV
    df = pl.DataFrame(all_rows)
    if df.is_empty():
        context.log.error(f"No data returned for week {week_start}–{week_end}.")
        return dg.MaterializeResult(metadata={"number_of_records": 0})
    else: 
        df.write_csv(file = DataFolder.from_proj_root().raw / f"acled_{week_start}_{week_end}", 
                    include_header=True)

        # verify that event_date covers entire week
        if not df.is_empty():
            dates = df.select(pl.col("event_date").cast(pl.Date)).to_series().sort()
            first, last = dates[0], dates[-1]
            if first > week_start or last < week_end:
                context.log.error(
                    f"Missing data for partition [{week_start} to {week_end}]: "
                    f"fetched event_date from {first} to {last}."
                )
        else:
            context.log.error(f"No data returned for week {week_start}–{week_end}.")
        
        first_ts = datetime.combine(first, datetime.min.time()).timestamp()
        last_ts = datetime.combine(last, datetime.min.time()).timestamp()

        event_type_df = (df.select("event_type")
                         .to_series()
                         .value_counts()
                         .sort("count", descending=True))

        schema = dg.TableSchema(
            columns=[
                dg.TableColumn(name="event_type"),
                dg.TableColumn(name="count", type="int"),
            ]
        )

        return dg.MaterializeResult(
            metadata={
                "start_event_date": dg.TimestampMetadataValue(first_ts),
                "end_event_date": dg.TimestampMetadataValue(last_ts),
                "number_of_records": len(df), 
                "event_type_distribution": dg.TableMetadataValue(records=[TableRecord(record) for record in event_type_df.to_dicts()], schema=schema), 
            }
        )




