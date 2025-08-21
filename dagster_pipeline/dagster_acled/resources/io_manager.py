import io
import os

import aioboto3
import dagster as dg
from dagster_aws.s3 import S3Resource
import polars as pl


class S3IOManager(dg.IOManager): 

    def __init__(self, s3_resource: S3Resource, bucket: str, prefix: str):
        self.s3_resource = s3_resource
        self.bucket = bucket
        self.prefix = prefix

    def _get_path(self, context: dg.AssetExecutionContext):
        asset_key = "/".join(context.asset_key.path)
        if context.has_partition_key:
            partition = context.partition_key
            return f"{self.prefix}/{asset_key}/partition_{partition}.parquet"
        return f"{self.prefix}/{asset_key}.parquet"

    async def handle_output(self, context: dg.AssetExecutionContext, obj: pl.DataFrame):
        session = aioboto3.Session()
        async with session.client('s3') as s3_client:
            buf = io.BytesIO()
            obj.write_parquet(buf)
            buf.seek(0)
            
            await s3_client.put_object(
                Bucket=self.bucket, 
                Key=self._get_path(context), 
                Body=buf.read()
            )

    async def load_input(self, context: dg.AssetExecutionContext) -> pl.DataFrame:
        session = aioboto3.Session()
        async with session.client('s3') as s3_client:
            response = await s3_client.get_object(
                Bucket=self.bucket, 
                Key=self._get_path(context)
            )
            body = await response['Body'].read()
            return pl.read_parquet(io.BytesIO(body))


@dg.io_manager(required_resource_keys={"s3"})
def s3_io_manager(context: dg.AssetExecutionContext): 
    return S3IOManager(
        context.resources.s3, 
        bucket=os.environ['S3_BUCKET'], #TODO: config from yaml
        prefix="acled"
    )