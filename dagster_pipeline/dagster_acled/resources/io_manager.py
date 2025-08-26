import io
import os
import boto3
import dagster as dg
import polars as pl
from pathlib import Path

from dagster_aws.s3 import S3PickleIOManager, S3Resource
from dotenv import load_dotenv

load_dotenv()

class S3IOManager(dg.IOManager):
    def __init__(self, bucket: str, prefix: str, **s3_config):
        self.bucket = bucket
        self.prefix = prefix
        self.s3_config = s3_config
        self._client = None

    @property
    def client(self):
        """Lazy-load S3 client"""
        if self._client is None:
            # Create client with the S3 config
            self._client = boto3.client('s3', **self.s3_config)
        return self._client

    def _get_path(self, context: dg.AssetExecutionContext):
        asset_key = "/".join(context.asset_key.path)
        if context.has_partition_key:
            partition = context.partition_key
            return f"{self.prefix}/{asset_key}/partition_{partition}.parquet"
        return f"{self.prefix}/{asset_key}.parquet"

    def handle_output(self, context: dg.AssetExecutionContext, obj: pl.DataFrame):
        """Store DataFrame as Parquet in S3"""
        # Serialize DataFrame to Parquet
        buf = io.BytesIO()
        obj.write_parquet(buf)
        buf.seek(0)

        # Upload to S3
        path = self._get_path(context)

        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=path,
                Body=buf.read()
            )
            context.log.info(f"Stored DataFrame with {len(obj)} rows to s3://{self.bucket}/{path}")
        except Exception as e:
            context.log.error(f"Failed to upload to S3: {str(e)}")
            raise

    def load_input(self, context: dg.AssetExecutionContext) -> pl.DataFrame:
        """Load DataFrame from S3"""
        path = self._get_path(context)

        try:
            # Download from S3
            response = self.client.get_object(Bucket=self.bucket, Key=path)
            body = response['Body'].read()

            # Parse Parquet
            buf = io.BytesIO(body)
            df = pl.read_parquet(buf)

            context.log.info(f"Loaded DataFrame with {len(df)} rows from s3://{self.bucket}/{path}")
            return df

        except self.client.exceptions.NoSuchKey:
            raise FileNotFoundError(f"No data found at s3://{self.bucket}/{path}")
        except Exception as e:
            raise RuntimeError(f"Failed to load data from s3://{self.bucket}/{path}: {str(e)}")


@dg.io_manager()  # No required_resource_keys
def s3_io_manager(context) -> S3IOManager:
    """Create S3 IO Manager instance using environment variables"""
    
    # Get S3 config from environment variables
    s3_config = {}
    if 'AWS_ACCESS_KEY_ID' in os.environ:
        s3_config['aws_access_key_id'] = os.environ['AWS_ACCESS_KEY_ID']
    if 'AWS_SECRET_ACCESS_KEY' in os.environ:
        s3_config['aws_secret_access_key'] = os.environ['AWS_SECRET_ACCESS_KEY']
    if 'AWS_REGION' in os.environ:
        s3_config['region_name'] = os.environ['AWS_REGION']
    elif 'REGION_NAME' in os.environ:
        s3_config['region_name'] = os.environ['REGION_NAME']
    
    return S3IOManager(
        bucket=os.environ['S3_BUCKET'],
        prefix="acled",
        **s3_config
    )

class ReportsS3IOManager(dg.IOManager):
    """IO Manager specifically for reports - handles PDF, PNG, etc."""
    
    def __init__(self, bucket: str, reports_prefix: str = "acled/reports", **s3_config):
        self.bucket = bucket
        self.reports_prefix = reports_prefix
        self.s3_config = s3_config
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = boto3.client('s3', **self.s3_config)
        return self._client

    def _get_s3_key(self, context: dg.AssetExecutionContext, file_extension: str):
        """Generate S3 key for report files"""
        asset_name = context.asset_key.path[-1]
        
        if context.has_partition_key:
            partition = context.partition_key
            return f"{self.reports_prefix}/{asset_name}/{asset_name}_{partition}.{file_extension}"
        else:
            timestamp = context.run_id[:8]  # Use first 8 chars of run_id as timestamp
            return f"{self.reports_prefix}/{asset_name}/{asset_name}_{timestamp}.{file_extension}"

    def handle_output(self, context: dg.AssetExecutionContext, obj: str | bytes | Path):
        """Handle different types of report outputs"""
        
        # Determine file type and content
        if isinstance(obj, (str, Path)):
            # File path - read the file
            file_path = Path(obj)
            if not file_path.exists():
                raise FileNotFoundError(f"Report file not found: {file_path}")
            
            with open(file_path, 'rb') as f:
                content = f.read()
            
            extension = file_path.suffix.lstrip('.')
            content_type = self._get_content_type(extension)
            
            # Clean up the temporary file after reading
            try:
                os.unlink(file_path)
                context.log.debug(f"Cleaned up temporary file: {file_path}")
            except Exception as e:
                context.log.warning(f"Could not clean up temporary file {file_path}: {e}")
            
        elif isinstance(obj, bytes):
            # Raw bytes - assume PDF
            content = obj
            extension = "pdf"
            content_type = "application/pdf"
        else:
            raise ValueError(f"Unsupported output type: {type(obj)}")

        # Generate S3 key based on asset and context
        s3_key = self._get_s3_key(context, extension)
        
        # Upload to S3
        self.client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=content,
            ContentType=content_type
        )
        
        s3_url = f"s3://{self.bucket}/{s3_key}"
        context.log.info(f"Report uploaded to: {s3_url}")
        
        # Store metadata for retrieval
        context.add_output_metadata({
            "s3_path": s3_url,
            "s3_key": s3_key,
            "file_size_bytes": len(content),
            "content_type": content_type
        })

    def load_input(self, context: dg.AssetExecutionContext):
        """This IO manager is write-only for reports"""
        raise NotImplementedError("Reports IO manager is write-only")

    def _get_content_type(self, extension: str) -> str:
        """Map file extensions to content types"""
        content_types = {
            'pdf': 'application/pdf',
            'png': 'image/png',
            'jpg': 'image/jpeg',
            'jpeg': 'image/jpeg',
            'svg': 'image/svg+xml',
            'html': 'text/html',
            'json': 'application/json'
        }
        return content_types.get(extension.lower(), 'application/octet-stream')


@dg.io_manager()
def reports_s3_io_manager(context) -> ReportsS3IOManager:
    """Create Reports S3 IO Manager instance"""
    s3_config = {}
    if 'AWS_ACCESS_KEY_ID' in os.environ:
        s3_config['aws_access_key_id'] = os.environ['AWS_ACCESS_KEY_ID']
    if 'AWS_SECRET_ACCESS_KEY' in os.environ:
        s3_config['aws_secret_access_key'] = os.environ['AWS_SECRET_ACCESS_KEY']
    if 'AWS_REGION' in os.environ:
        s3_config['region_name'] = os.environ['AWS_REGION']
    elif 'REGION_NAME' in os.environ:
        s3_config['region_name'] = os.environ['REGION_NAME']
    
    return ReportsS3IOManager(
        bucket=os.environ['S3_BUCKET'],
        reports_prefix="acled/reports",
        **s3_config
    )


@dg.io_manager()
def s3_pickle_io_manager(context) -> S3PickleIOManager: 
    s3_config = {}
    if 'AWS_ACCESS_KEY_ID' in os.environ:
        s3_config['aws_access_key_id'] = os.environ['AWS_ACCESS_KEY_ID']
    if 'AWS_SECRET_ACCESS_KEY' in os.environ:
        s3_config['aws_secret_access_key'] = os.environ['AWS_SECRET_ACCESS_KEY']
    if 'AWS_REGION' in os.environ:
        s3_config['region_name'] = os.environ['AWS_REGION']
    elif 'REGION_NAME' in os.environ:
        s3_config['region_name'] = os.environ['REGION_NAME']
    
    return S3PickleIOManager(
        s3_resource=S3Resource(**s3_config),
        s3_bucket=os.environ['S3_BUCKET'],
        s3_prefix="acled/models"
    )
