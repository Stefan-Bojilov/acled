import os

from dagster import (
    Definitions,
    load_asset_checks_from_modules,
    load_assets_from_modules,
    multiprocess_executor,
)
from dotenv import load_dotenv

from dagster_acled.asset_checks import acled_checks
from dagster_acled.assets import base_assets, ml, report
from dagster_acled.jobs import acled_update_job
from dagster_acled.resources.io_manager import (
    reports_s3_io_manager,
    s3_io_manager,
    s3_pickle_io_manager,
)
from dagster_acled.resources.resources import ResourceConfig
from dagster_acled.schedules import daily_schedule
from dagster_acled.secrets_config import SecretManager
from dagster_acled.sensors import acled_sensor

load_dotenv()

sm = SecretManager(region_name=os.environ['REGION_NAME'])
resources = ResourceConfig.from_secrets(sm = sm, s3_secret_name="acled_bucket", pg_secret_name="acled_postgres")
resources['s3_io_manager'] = s3_io_manager
resources['reports_s3_io_manager'] = reports_s3_io_manager #TODO unpack through io manager data model 
resources['s3_pickle_io_manager'] = s3_pickle_io_manager


all_jobs = [acled_update_job]
all_assets = load_assets_from_modules([base_assets, report, ml])
all_asset_checks = load_asset_checks_from_modules([acled_checks])
all_sensors = [acled_sensor]

multiprocess_executor_def = multiprocess_executor.configured({
    "max_concurrent": 4,
})

defs = Definitions(
    assets=all_assets, 
    asset_checks=all_asset_checks,
    resources=resources,
    jobs = all_jobs,
    sensors=all_sensors, 
    schedules=[daily_schedule],
    executor=multiprocess_executor_def
)