import os

from dagster import Definitions, load_assets_from_modules
from dagster_aws.s3 import S3Resource
from dotenv import load_dotenv

from dagster_acled.asset_checks.acled_checks import acled_daily_data_quality_check
from dagster_acled.assets import acled
from dagster_acled.jobs import acled_update_job
from dagster_acled.resources import PostgreSQLResource
from dagster_acled.secrets_config import SecretManager
from dagster_acled.sensors import acled_sensor

load_dotenv()

sm = SecretManager(region_name=os.environ['REGION_NAME'])
POSTGRES = sm.get_secret('rds!db-7c327c65-ef21-4850-ae84-ee40cda7f9f9')

all_jobs = [acled_update_job]
all_assets = load_assets_from_modules([acled])
all_asset_checks = [acled_daily_data_quality_check]
all_sensors = [acled_sensor]

defs = Definitions(
    assets=all_assets, 
    asset_checks=all_asset_checks,
    resources={'s3': S3Resource(region_name=os.environ['REGION_NAME']), 
               'postgres': PostgreSQLResource(
                    host=os.environ['POSTGRES_HOST'],  
                    dbname="postgres",
                    user=POSTGRES['username'],
                    password=POSTGRES['password'] 
                )},
    jobs = all_jobs,
    sensors=all_sensors,
)