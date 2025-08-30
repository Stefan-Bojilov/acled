import asyncio
from datetime import date, datetime, timedelta

import aiohttp
import dagster as dg
from dagster_acled.acled_request_config import AcledConfig

from dagster_acled.jobs import acled_update_job
from dagster_acled.utils import fetch_page


@dg.sensor(
    job=acled_update_job,
    minimum_interval_seconds=172800,  
    description="Monitor ACLED API for new data and trigger backfill runs", 
)
def acled_sensor(context: dg.SensorEvaluationContext) -> list[dg.RunRequest]:
    """
    Sensor that checks ACLED API for available data and triggers runs for missing partitions.
    Works backwards from today to find the most recent available data.
    """
    config = AcledConfig()
    
    async def check_data_availability(session: aiohttp.ClientSession, check_date: date) -> bool:
        """Check if data is available for a specific date"""
        url = f"{config.base_url.rstrip('/')}/{config.endpoint}"
        params = config.build_params()
        params.update({
            "limit": 1,  # Just need to check if any data exists
            "page": 1,
            "event_date": check_date.isoformat(),
            "event_date_where": "=",
        })
        
        try:
            chunk = await fetch_page(session, url, params)
            return len(chunk) > 0
        except Exception as e:
            context.log.warning(f"Error checking data for {check_date}: {e}")
            return False

    async def find_available_dates(days_back: int = 14) -> list[date]:
        """Find available dates by checking backwards from today"""
        available_dates = []
        today = date.today()
        
        async with aiohttp.ClientSession() as session:
            # Check the last N days
            for i in range(days_back):
                check_date = today - timedelta(days=i)
                if await check_data_availability(session, check_date):
                    available_dates.append(check_date)
                    context.log.info(f"Data available for {check_date}")
                else:
                    context.log.debug(f"No data available for {check_date}")
        
        return sorted(available_dates)

    # Get materialized partitions to avoid re-running successful ones
    materialized_partitions = set()
    if context.instance:
        # Get the asset key
        asset_key = dg.AssetKey("ACLED_daily_to_PostgreSQL")
        
        # Get materialized partitions for this asset
        materializations = context.instance.get_event_records(
            dg.EventRecordsFilter(
                event_type=dg.DagsterEventType.ASSET_MATERIALIZATION,
                asset_key=asset_key
            ),
            limit=100  # Adjust based on your needs
        )
        
        for record in materializations:
            if record.event_log_entry.dagster_event.partition:
                partition_date = datetime.fromisoformat(
                    record.event_log_entry.dagster_event.partition
                ).date()
                materialized_partitions.add(partition_date)

    context.log.info(f"Found {len(materialized_partitions)} already materialized partitions")

    # Find available dates
    available_dates = asyncio.run(find_available_dates())
    
    if not available_dates:
        context.log.info("No new data available")
        return []

    # Filter out already materialized partitions
    dates_to_run = [d for d in available_dates if d not in materialized_partitions]
    
    if not dates_to_run:
        context.log.info("All available data already materialized")
        return []

    # Create run requests for missing dates
    run_requests = []
    for partition_date in dates_to_run:
        partition_key = partition_date.strftime("%Y-%m-%d")
        run_requests.append(
            dg.RunRequest(
                run_key=f"acled_backfill_{partition_key}",
                partition_key=partition_key,
                tags={
                    "dagster/backfill": "acled_sensor",
                    "partition_date": partition_key,
                    "sensor_timestamp": datetime.now().isoformat()
                }
            )
        )

    context.log.info(f"Triggering {len(run_requests)} runs for dates: {[d.isoformat() for d in dates_to_run]}")
    
    # Store cursor to track what we've seen
    latest_date = max(available_dates) if available_dates else date.today()
    context.update_cursor(latest_date.isoformat())
    
    return run_requests
