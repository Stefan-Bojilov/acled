import os
from pathlib import Path
from typing import Any

import dagster as dg
from dagster_acled.partitions import daily_partition
from dagster_aws.s3 import S3Resource
from dagster_pipeline.dagster_acled.acled_request_config import (
    AcledConfig,
)
from dagster_pipeline.dagster_acled.resources.models import PostgresConfig, S3Config
from dagster_pipeline.dagster_acled.resources.resources import PostgreSQLResource
import yaml


class AcledAssetFactory:
    """Factory class to create ACLED assets from YAML configuration."""
    
    def __init__(self, config_path: str = "acled_pipelines.yaml"):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.assets = []
        self.jobs = []
        self.schedules = []
    
    def _load_config(self) -> dict[str, Any]:
        """Load YAML configuration file."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _merge_config(self, pipeline_config: dict[str, Any]) -> dict[str, Any]:
        """Merge pipeline config with defaults."""
        defaults = self.config.get('defaults', {})
        merged = defaults.copy()
        
        # Merge nested configurations
        for key, value in pipeline_config.items():
            if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
                merged[key].update(value)
            else:
                merged[key] = value
        
        return merged
    
    def _create_acled_config(self, pipeline_name: str, pipeline_config: dict[str, Any]) -> AcledConfig:
        """Create AcledConfig from pipeline configuration."""
        merged_config = self._merge_config(pipeline_config)
        
        # Extract S3 configuration
        s3_config = merged_config.get('s3', {})
        s3_bucket = s3_config.get('bucket', os.environ.get('S3_BUCKET', ''))
        s3_key_prefix = s3_config.get('key_prefix', f'acled/{pipeline_name}')
        
        # Extract Postgres configuration
        postgres_config = merged_config.get('postgres', {})
        table_name = postgres_config.get('table_name', f"acled_{pipeline_name}_events")
        
        # Extract API and filter configuration
        api_config = merged_config.get('api', {})
        filter_config = merged_config.get('config', {})
        
        # Build the full configuration
        acled_config_params = {
            'base_url': api_config.get('base_url', 'https://api.acleddata.com/'),
            'endpoint': api_config.get('endpoint', 'acled/read'),
            'max_pages': api_config.get('max_pages', 5000),
            's3': S3Config(
                bucket=s3_bucket,
                key_prefix=s3_key_prefix
            ),
            'postgres': PostgresConfig(
                table_name=table_name
            )
        }
        
        # Add all filter parameters
        acled_config_params.update(filter_config)
        
        return AcledConfig(**acled_config_params)
    
    def create_asset_pair(self, pipeline_name: str, pipeline_config: dict[str, Any]) -> tuple:
        """Create a pair of assets (S3 and Postgres) for a pipeline."""
        
        merged_config = self._merge_config(pipeline_config)
        acled_config = self._create_acled_config(pipeline_name, pipeline_config)
        
        description = merged_config.get('description', f'ACLED pipeline for {pipeline_name}')
        group_name = merged_config.get('group_name', 'acled')
        
        # Create S3 asset
        @dg.asset(
            name=f"{pipeline_name}_to_s3",
            partitions_def=daily_partition,
            description=f"{description} - S3 storage",
            group_name=group_name,
            tags=merged_config.get('tags', {}),
        )
        async def s3_asset(
            context: dg.AssetExecutionContext,
            s3: S3Resource,
        ) -> dg.MaterializeResult:
            # Import the original function and call it with our config
            from dagster_pipeline.dagster_acled.assets import acled_request_daily
            return await acled_request_daily(context, acled_config, s3)
        
        # Create Postgres asset
        @dg.asset(
            name=f"{pipeline_name}_to_postgres",
            partitions_def=daily_partition,
            description=f"{description} - PostgreSQL storage",
            group_name=group_name,
            deps=[s3_asset],
            tags=merged_config.get('tags', {}),
        )
        async def postgres_asset(
            context: dg.AssetExecutionContext,
            postgres: PostgreSQLResource,
            s3: S3Resource,
        ) -> dg.MaterializeResult:
            # Import the original function and call it with our config
            from dagster_pipeline.dagster_acled.assets import acled_daily_to_postgres_from_s3
            return await acled_daily_to_postgres_from_s3(context, acled_config, postgres, s3)
        
        return s3_asset, postgres_asset
    
    def create_job(self, pipeline_name: str, s3_asset, postgres_asset) -> dg.JobDefinition:
        """Create a job for the asset pair."""
        
        @dg.job(name=f"{pipeline_name}_pipeline")
        def pipeline_job():
            postgres_asset(s3_asset())
        
        return pipeline_job
    
    def create_schedule(self, pipeline_name: str, pipeline_config: dict[str, Any], job: dg.JobDefinition) -> dg.ScheduleDefinition | None:
        """Create a schedule if specified in config."""
        
        cron_schedule = pipeline_config.get('schedule')
        if not cron_schedule:
            return None
        
        @dg.schedule(
            name=f"{pipeline_name}_schedule",
            job=job,
            cron_schedule=cron_schedule,
        )
        def pipeline_schedule(context):
            return dg.RunRequest()
        
        return pipeline_schedule
    
    def build_all_assets(self) -> dict[str, Any]:
        """Build all assets, jobs, and schedules from the configuration."""
        
        results = {
            'assets': [],
            'jobs': [],
            'schedules': []
        }
        
        pipelines = self.config.get('pipelines', {})
        
        for pipeline_name, pipeline_config in pipelines.items():
            # Create asset pair
            s3_asset, postgres_asset = self.create_asset_pair(pipeline_name, pipeline_config)
            results['assets'].extend([s3_asset, postgres_asset])
            
            # Create job
            job = self.create_job(pipeline_name, s3_asset, postgres_asset)
            results['jobs'].append(job)
            
            # Create schedule if specified
            schedule = self.create_schedule(pipeline_name, pipeline_config, job)
            if schedule:
                results['schedules'].append(schedule)
        
        return results