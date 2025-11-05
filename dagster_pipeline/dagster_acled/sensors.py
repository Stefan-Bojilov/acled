import asyncio
from datetime import date, datetime, timedelta

import aiohttp
import dagster as dg
from dagster_acled.acled_request_config import AcledConfig

from dagster_acled.jobs import acled_update_job
from dagster_acled.utils import fetch_page
from dagster_acled.resources.resources import load_resource_config
from dagster_aws.s3 import S3Resource


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
    
    async def check_data_availability(session: aiohttp.ClientSession, check_date: date, headers: dict) -> bool:
        """Check if data is available for a specific date"""
        url = f"{config.base_url.rstrip('/')}/{config.endpoint}"
        
        try:
            # Use the OAuth-compatible build_params method
            base_params, _ = await config.build_params()  # Headers already from outer scope
            base_params.update({
                "limit": 1,  # Just need to check if any data exists
                "page": 1,
                "event_date": check_date.isoformat(),
                "event_date_where": "=",
            })
            
            context.log.info(f"Checking {url} with params: {base_params}")
            context.log.info(f"Using headers: {list(headers.keys())}")  # Don't log token value
            
            chunk = await fetch_page(session, url, base_params, headers=headers)
            has_data = len(chunk) > 0 if chunk else False
            context.log.info(f"Data check for {check_date}: {has_data} (found {len(chunk) if chunk else 0} records)")
            return has_data
            
        except Exception as e:
            context.log.error(f"Error checking data for {check_date}: {str(e)}")
            import traceback
            context.log.error(f"Full traceback: {traceback.format_exc()}")
            return False

    async def find_available_dates(days_back: int = 10) -> list[date]:  
        """Find available dates by checking backwards from today"""
        available_dates = []
        today = date.today()
        
        # Get OAuth authentication once for all requests
        try:
            context.log.info("Getting OAuth token...")
            _, headers = await config.build_params()
            context.log.info("OAuth token obtained successfully")
        except Exception as e:
            context.log.error(f"Failed to get OAuth token: {str(e)}")
            import traceback
            context.log.error(f"OAuth error traceback: {traceback.format_exc()}")
            return []
        
        async with aiohttp.ClientSession() as session:
            # Check the last N days
            for i in range(days_back):
                check_date = today - timedelta(days=i + 1)  # Start from yesterday, not today
                context.log.info(f"Checking date: {check_date}")
                
                if await check_data_availability(session, check_date, headers):
                    available_dates.append(check_date)
                    context.log.info(f"✓ Data available for {check_date}")
                else:
                    context.log.info(f"✗ No data available for {check_date}")
        
        return sorted(available_dates)

    # Get materialized partitions using the newer API
    materialized_partitions = set()
    if context.instance:
        # Use the correct asset key
        asset_key = dg.AssetKey("acled_daily_data")  
        
        try:
            # Use the newer fetch_materializations method
            materializations = context.instance.fetch_materializations(
                asset_key,
                limit=100
            )
            
            for materialization in materializations.records:
                if materialization.partition_key:
                    partition_date = datetime.fromisoformat(materialization.partition_key).date()
                    materialized_partitions.add(partition_date)
                    
            context.log.info(f"Found {len(materialized_partitions)} already materialized partitions: {sorted(materialized_partitions)}")
                    
        except Exception as e:
            context.log.warning(f"Could not fetch materializations: {e}")
            # Fallback to empty set if fetching fails

    # Find available dates
    try:
        context.log.info("Starting to check for available dates...")
        available_dates = asyncio.run(find_available_dates())
        context.log.info(f"Available dates found: {available_dates}")
    except Exception as e:
        context.log.error(f"Failed to check for available dates: {str(e)}")
        import traceback
        context.log.error(f"Available dates error traceback: {traceback.format_exc()}")
        return []
    
    if not available_dates:
        context.log.info("No new data available - this could mean:")
        context.log.info("1. No data exists for the checked date range")
        context.log.info("2. Authentication is failing")
        context.log.info("3. API parameters are incorrect")
        return []

    # Filter out already materialized partitions
    dates_to_run = [d for d in available_dates if d not in materialized_partitions]
    
    context.log.info(f"Available dates: {[d.isoformat() for d in available_dates]}")
    context.log.info(f"Already materialized: {[d.isoformat() for d in materialized_partitions]}")
    context.log.info(f"Dates to run: {[d.isoformat() for d in dates_to_run]}")
    
    if not dates_to_run:
        context.log.info("All available data already materialized")
        return []

    # Create run requests for missing dates
    run_requests = []
    for partition_date in dates_to_run:
        partition_key = partition_date.strftime("%Y-%m-%d")
        run_requests.append(
            dg.RunRequest(
                run_key=f"acled_backfill_{partition_key}_{int(datetime.now().timestamp())}",  # Added timestamp for uniqueness
                partition_key=partition_key,
                tags={
                    "dagster/backfill": "acled_sensor",
                    "partition_date": partition_key,
                    "sensor_timestamp": datetime.now().isoformat()
                }
            )
        )

    context.log.info(f"Creating {len(run_requests)} run requests for dates: {[d.isoformat() for d in dates_to_run]}")
    
    # Store cursor to track what we've seen
    latest_date = max(available_dates) if available_dates else date.today()
    context.update_cursor(latest_date.isoformat())
    
    return run_requests


