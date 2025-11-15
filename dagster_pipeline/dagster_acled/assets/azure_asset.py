from datetime import date, datetime

import aiohttp
import dagster as dg
from dagster_acled.acled_request_config import AcledConfig
from dagster_acled.partitions import daily_partition
from dagster_acled.resources.resources import ResourceConfig
from dagster_acled.utils import fetch_page
import polars as pl


@dg.asset(
    name="acled_azure_daily_data", 
    partitions_def=daily_partition,
    description="Fetch ACLED events for the current day and store in Azure Blob Storage.",
    group_name="acled",
)
async def acled_azure_request_daily(
    context: dg.AssetExecutionContext,
    config: AcledConfig,
) -> None:  
    """
    Fetch ACLED events for this day's partition.
    Uploads directly to Azure Blob Storage using Polars.
    """
    day: date = context.partition_time_window.start.date()
    
    url = f"{config.base_url.rstrip('/')}/{config.endpoint}"
    all_rows = []
    page = 1
    
    # Get OAuth authentication parameters and headers
    base_params, headers = await config.build_params()
    
    async with aiohttp.ClientSession() as session:
        while True:
            # Start with base params and add pagination/date filters
            params = base_params.copy()
            params.update({
                "limit": config.max_pages,
                "page": page,
                "event_date": day.isoformat(),
                "event_date_where": "=",
            })
            
            # Pass headers to fetch_page function
            chunk = await fetch_page(session, url, params, headers=headers)
            if not chunk:
                context.log.warning(f'Maximum limit of {config.max_pages} pages requests exceeded!')
                break
            
            all_rows.extend(chunk)
            
            if len(chunk) < config.max_pages:
                break
                
            page += 1
    
    df = pl.DataFrame(all_rows)
    
    if len(df) == 0:
        context.log.error(f"No data returned for day {day}.")
        raise dg.DagsterInvalidSubsetError()
    
    if not df.is_empty():
        dates = df.select(pl.col("event_date").cast(pl.Date)).to_series().unique().sort()
        if len(dates) > 1 or (len(dates) == 1 and dates[0] != day):
            context.log.warning(
                f"Unexpected event_date values for partition {day}: "
                f"found dates {dates.to_list()}"
            )
    
    context.log.info(f"Fetched {len(df)} records for {day}")
    
    event_type_counts = (df.select("event_type")
                        .to_series()
                        .value_counts()
                        .sort("count", descending=True))
    
    resource_dict = ResourceConfig.load_resource_config()

    df = df.with_columns([
            pl.col("event_date").str.strptime(pl.Date, "%Y-%m-%d", strict=False),
            pl.col("year").cast(pl.Int16, strict=False),
            pl.col("time_precision").cast(pl.Int16, strict=False),
            pl.col("iso").cast(pl.Int16, strict=False),
            pl.col("geo_precision").cast(pl.Int16, strict=False),
            pl.col("fatalities").cast(pl.Int16, strict=False),
            pl.col("latitude").cast(pl.Float64, strict=False),
            pl.col("longitude").cast(pl.Float64, strict=False),
            pl.col("timestamp").cast(pl.Int64).cast(pl.Datetime("ms"), strict=False),
        ])

    df.write_parquet(
        file=f"az://{resource_dict['storage_account']['container']}/daily_data/africa/event_date={day}/0.parquet",
        storage_options={'account_name': resource_dict['storage_account']['name']}, 
        credential_provider=ResourceConfig.blob_credential_provider,
    )

    
    # Add metadata
    context.add_output_metadata({
        "event_date": dg.TimestampMetadataValue(
            datetime.combine(day, datetime.min.time()).timestamp()
        ),
        "number_of_records": len(df),
        "event_type_distribution": dg.TableMetadataValue(
            records=[dg.TableRecord(record) for record in event_type_counts.to_dicts()],
            schema=dg.TableSchema(columns=[
                dg.TableColumn(name="event_type"),
                dg.TableColumn(name="count", type="int"),
            ])
        )
    })


@dg.asset(
    name="jnim_events_consolidated",
    description="Consolidated Parquet file containing all JNIM-related events from ACLED data.",
    group_name="acled",
    deps=["acled_azure_daily_data"],
    automation_condition=(
        dg.AutomationCondition.any_deps_updated()
        & ~dg.AutomationCondition.any_deps_in_progress()
    ),
)
async def jnim_events_consolidated(
    context: dg.AssetExecutionContext,
) -> None:
    """
    Read all daily ACLED data from Azure Blob Storage, filter for JNIM events,
    and write a single consolidated Parquet file.
    
    This asset automatically materializes whenever acled_azure_daily_data is updated.
    """
    resource_dict = ResourceConfig.load_resource_config()
    container = resource_dict['storage_account']['container']
    account_name = resource_dict['storage_account']['name']

    context.log.info("Reading daily data from Azure Blob Storage...")
    df = pl.scan_parquet(
        f"az://{container}/daily_data/africa/**/*.parquet",
        storage_options={'account_name': account_name},
        credential_provider=ResourceConfig.blob_credential_provider,
        extra_columns='ignore', 
        cast_options=pl.ScanCastOptions(integer_cast='upcast')
    )
    
    # Filter for JNIM events
    jnim_pattern = r"(?i)jnim|jama'?at nusrat|nusrat al-islam"
    
    jnim_df = df.filter(
        pl.col("actor1").str.contains(jnim_pattern, literal=False) |
        pl.col("actor2").str.contains(jnim_pattern, literal=False) |
        pl.col("assoc_actor_1").str.contains(jnim_pattern, literal=False) |
        pl.col("assoc_actor_2").str.contains(jnim_pattern, literal=False)
    ).collect(engine='streaming')

    
    context.log.info(f"JNIM events found: {len(jnim_df)}")
    
    if jnim_df.is_empty():
        context.log.warning("No JNIM events found in the data.")
        return
    
    # Sort by event date for better organization
    jnim_df = jnim_df.sort("event_date")
    
    # Write consolidated file
    output_path = f"az://{container}/consolidated/jnim_events.parquet"
    jnim_df.write_parquet(
        file=output_path,
        storage_options={'account_name': account_name},
        credential_provider=ResourceConfig.blob_credential_provider,
    )
    
    context.log.info(f"Written consolidated JNIM events to {output_path}")
    
    # Generate metadata
    date_range = jnim_df.select([
        pl.col("event_date").min().alias("earliest"),
        pl.col("event_date").max().alias("latest")
    ]).to_dicts()[0]
    
    event_type_counts = (
        jnim_df.select("event_type")
        .to_series()
        .value_counts()
        .sort("count", descending=True)
    )
    
    country_counts = (
        jnim_df.select("country")
        .to_series()
        .value_counts()
        .sort("count", descending=True)
    )
    
    context.add_output_metadata({
        "total_jnim_events": len(jnim_df),
        "earliest_event_date": dg.TimestampMetadataValue(
            datetime.combine(date_range["earliest"], datetime.min.time()).timestamp()
        ),
        "latest_event_date": dg.TimestampMetadataValue(
            datetime.combine(date_range["latest"], datetime.min.time()).timestamp()
        ),
        "event_type_distribution": dg.TableMetadataValue(
            records=[dg.TableRecord(record) for record in event_type_counts.to_dicts()],
            schema=dg.TableSchema(columns=[
                dg.TableColumn(name="event_type"),
                dg.TableColumn(name="count", type="int"),
            ])
        ),
        "country_distribution": dg.TableMetadataValue(
            records=[dg.TableRecord(record) for record in country_counts.to_dicts()],
            schema=dg.TableSchema(columns=[
                dg.TableColumn(name="country"),
                dg.TableColumn(name="count", type="int"),
            ])
        ),
    })