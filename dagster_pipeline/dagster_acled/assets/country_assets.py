from dagster import asset, AssetExecutionContext, DailyPartitionsDefinition
import polars as pl
from datetime import date, datetime
import aiohttp
import dagster as dg
from dagster_acled.partitions import daily_partition
from dagster_acled.utils import fetch_page
from dagster_acled.acled_request_config import AcledConfig


REGIONS = {
    "Africa": ['1', '2', '3', '4', '5'], 
    "Middle_East": ['11'], 
}

def create_region_asset(region_name: str, region_codes: list[str]):
    """Factory function that creates an asset for a specific region"""
    
    @dg.asset(
        name=f"acled_{region_name.lower()}",
        partitions_def=daily_partition,
        description=f"Fetch ACLED events for {region_name.replace('_', ' ').title()}",
        group_name="acled",
        io_manager_key="s3_io_manager",
    )
    async def region_acled_data(
        context: AssetExecutionContext,
        config: AcledConfig,
    ) -> pl.DataFrame:
        day: date = context.partition_time_window.start.date()
        
        url = f"{config.base_url.rstrip('/')}/{config.endpoint}"
        all_rows = []

        async with aiohttp.ClientSession() as session:

            for region_code in region_codes:
                context.log.info(f"Fetching data for region code: {region_code}")
                config.region = region_code
                
                base_params, headers = await config.build_params()
                page = 1
                
                while True:
                    params = base_params.copy()
                    params.update({
                        "limit": config.max_pages,
                        "page": page,
                        "event_date": day.isoformat(),
                        "event_date_where": "=",
                    })

                    if page == 1:
                        context.log.debug(f"Request params for region {region_code}: {params}")

                    chunk = await fetch_page(session, url, params, headers=headers)
                    
                    if not chunk:
                        context.log.debug(f'No more data for region {region_code}, page {page}')
                        break

                    all_rows.extend(chunk)
                    context.log.debug(f"Fetched {len(chunk)} records from region {region_code}, page {page}")

                    if len(chunk) < config.max_pages:
                        break
                        
                    page += 1
                    
                    if page > config.max_pages:
                        context.log.error(f'Exceeded maximum page limit for region {region_code}')
                        break

        df = pl.DataFrame(all_rows)

        context.log.info(f'DataFrame: {list(df.to_dict().items())[:1]}')
        
        if len(df) == 0:
            context.log.warning(f"No data returned for {region_name} on {day}.")
            return pl.DataFrame()
        
        context.log.info(f"Fetched {len(df):,} total records for {region_name} on {day}")
        
        # Add metadata
        event_type_counts = (
            df.select("event_type")
            .to_series()
            .value_counts()
            .sort("count", descending=True)
        )
        
        # Count records per region code
        if "region" in df.columns:
            region_distribution = (
                df.select("region")
                .to_series()
                .value_counts()
                .sort("count", descending=True)
            )
        
        context.add_output_metadata({
            "region_name": region_name,
            "region_codes": ", ".join(region_codes),
            "event_date": dg.TimestampMetadataValue(
                datetime.combine(day, datetime.min.time()).timestamp()
            ),
            "number_of_records": len(df),
            "number_of_actors": df.select('actor1').unique().shape[0],
            "event_type_distribution": dg.TableMetadataValue(
                records=[dg.TableRecord(record) for record in event_type_counts.to_dicts()],
                schema=dg.TableSchema(columns=[
                    dg.TableColumn(name="event_type", type="string"),
                    dg.TableColumn(name="count", type="int"),
                ])
            )
        })
        
        return df
    
    return region_acled_data


region_assets = [
    create_region_asset(region_name, region_codes)
    for region_name, region_codes in REGIONS.items()
]