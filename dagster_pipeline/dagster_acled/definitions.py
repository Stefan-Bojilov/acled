import os

from dagster import Definitions, load_asset_checks_from_modules, load_assets_from_modules
from dagster_aws.s3 import S3Resource
from dagster_pipeline.dagster_acled.assets import base_assets
from dagster_pipeline.dagster_acled.resources.resources import ResourceConfig
from dotenv import load_dotenv

from dagster_acled.asset_checks import acled_checks
from dagster_acled.jobs import acled_update_job
from dagster_acled.secrets_config import SecretManager
from dagster_acled.sensors import acled_sensor

load_dotenv()

sm = SecretManager(region_name=os.environ['REGION_NAME'])
resources = ResourceConfig.from_secrets(sm = sm, s3_secret_name="acled_bucket", pg_secret_name="acled_postgres")


all_jobs = [acled_update_job]
all_assets = load_assets_from_modules([base_assets])
all_asset_checks = load_asset_checks_from_modules([acled_checks])
all_sensors = [acled_sensor]

defs = Definitions(
    assets=all_assets, 
    asset_checks=all_asset_checks,
    resources=resources,
    jobs = all_jobs,
    sensors=all_sensors,
)