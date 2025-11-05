import os

from dagster import (
    Definitions,
    load_assets_from_modules,
    multiprocess_executor,
)
from dagster_azure.blob import AzureBlobStorageDefaultCredential, AzureBlobStorageResource
from dotenv import load_dotenv

from dagster_acled.assets import azure_asset
from dagster_acled.jobs import acled_update_job
from dagster_acled.resources.resources import  load_resource_config
from dagster_acled.sensors import acled_sensor

load_dotenv()

resource_config = load_resource_config()

resources = {
    "azure_blob_storage": AzureBlobStorageResource(account_url=resource_config['storage_account']['url'],
                                                credential=AzureBlobStorageDefaultCredential())
}

all_jobs = [acled_update_job]
first_asset = load_assets_from_modules([azure_asset])
all_sensors = [acled_sensor]
multiprocess_executor_def = multiprocess_executor.configured({
    "max_concurrent": 4,    
})

defs = Definitions(
    assets=first_asset, 
    resources=resources,
    jobs=all_jobs,
    sensors=all_sensors, 
    executor=multiprocess_executor_def
)
