import os

import dagster as dg
from dagster import ConfigurableResource
from dagster_aws.s3 import S3Resource
from dagster_azure.blob import AzureBlobStorageResource, AzureBlobStorageDefaultCredential
from azure.identity import DefaultAzureCredential
import psycopg
import yaml

from dagster_acled.secrets_config import SecretManager


def load_resource_config(config_path: str | None = None) -> dict[str, dict[str, str]]:
    if config_path is None:
        config_path = os.path.join(os.path.dirname(__file__), "resource_config.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)
    

class PostgreSQLResource(ConfigurableResource): 
    host: str
    dbname: str 
    username: str 
    password: str 
    table_name: str | None = None
    schema: str | None = None

    def get_connection(self): 
        return psycopg.connect(
            host=self.host,
            dbname=self.dbname,
            user=self.username,
            password=self.password
        )

class ResourceConfig(ConfigurableResource):
    s3: S3Resource
    postgres: PostgreSQLResource
    blob_storage: AzureBlobStorageResource

    @staticmethod
    def load_resource_config(config_path: str | None = None) -> dict[str, dict[str, str]]:
        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), "resource_config.yaml")
        with open(config_path, "r") as f:
            return yaml.safe_load(f)

    @staticmethod
    def from_secrets(sm: SecretManager, s3_secret_name: str, pg_secret_name: str):
        s3_conf = sm.get_secret(s3_secret_name)
        pg_conf = sm.get_secret(pg_secret_name)
        config = ResourceConfig.load_resource_config()  
        return ResourceConfig(
            s3=S3Resource(**s3_conf),
            postgres=PostgreSQLResource(**pg_conf, 
                                        table_name=config['postgres']['table_name'], 
                                        schema=config['postgres']['schema']), 
            blob_storage=AzureBlobStorageResource(account_url=config['storage_account']['url'],
                                                credential=AzureBlobStorageDefaultCredential())
        )
    
    @staticmethod
    def blob_credential_provider() -> tuple[dict, str]:
        credential = DefaultAzureCredential(exclude_managed_identity=True)
        token = credential.get_token("https://storage.azure.com/.default")
        return {'bearer_token': token.token}, token.expires_on