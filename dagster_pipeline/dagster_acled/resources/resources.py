from dagster import ConfigurableResource
from dagster_aws.s3 import S3Resource
from dagster_pipeline.dagster_acled.secrets_config import SecretManager
import psycopg


class PostgreSQLResource(ConfigurableResource): 
    host: str 
    dbname: str 
    username: str 
    password: str

    def get_connection(self): 
        return psycopg.connect(
            host=self.host,
            dbname=self.dbname,
            user=self.username,
            password=self.password
        )

class ResourceConfig(ConfigurableResource):
    #TODO: hide env in UI with dg.Env
    s3: S3Resource
    postgres: PostgreSQLResource

    @staticmethod
    def from_secrets(sm: SecretManager, s3_secret_name: str, pg_secret_name: str):
        s3_conf = sm.get_secret(s3_secret_name)
        pg_conf = sm.get_secret(pg_secret_name)
        return {
            "s3": S3Resource(**s3_conf),
            "postgres": PostgreSQLResource(**pg_conf)
        }

