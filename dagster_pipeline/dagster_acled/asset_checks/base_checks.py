import yaml
from pathlib import Path
from datetime import datetime
import io
import os
from typing import Dict, List, Any, Callable

import dagster as dg
from dagster_aws.s3 import S3Resource
from dagster_acled.assets
import polars as pl


class AssetCheckFactory:
    """Factory to create asset checks from YAML configuration."""
    
    def __init__(self, config_path: str = "asset_checks.yaml"):
        self.config_path = Path(config_path)
        self.checks_config = self._load_config()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load asset checks configuration from YAML."""
        with open(self.config_path, 'r') as file:
            return yaml.safe_load(file)
    
    async def _get_dataframe(self, context: dg.AssetCheckExecutionContext, s3: S3Resource, country_name: str):
        """Helper to load country-specific data from S3."""
        day_str = context.run.tags["dagster/partition"]
        try:
            day = datetime.strptime(day_str, "%Y-%m-%d").date()
        except ValueError:
            day = datetime.fromisoformat(day_str).date()
        
        bucket = os.environ['S3_BUCKET']
        key = f"acled/{country_name}/daily_partitions/acled_{country_name}_{day}.parquet"
        
        client = s3.get_client()
        response = client.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read()
        buf = io.BytesIO(file_content)
        df = pl.read_parquet(buf)
        
        return df, day, len(file_content)
    
    def _create_file_exists_check(self, country_key: str, country_name: str, asset_func, config: Dict):
        """Create file exists check."""
        @dg.asset_check(
            asset=asset_func,
            name=config['name'].format(country=country_name),
            description=config['description'].format(country=country_key),
            blocking=config.get('blocking', False)
        )
        async def check(context: dg.AssetCheckExecutionContext, s3: S3Resource) -> dg.AssetCheckResult:
            day_str = context.run.tags["dagster/partition"]
            try:
                day = datetime.strptime(day_str, "%Y-%m-%d").date()
            except ValueError:
                day = datetime.fromisoformat(day_str).date()
            
            bucket = os.environ['S3_BUCKET']
            key = f"acled/{country_name}/daily_partitions/acled_{country_name}_{day}.parquet"
            
            try:
                client = s3.get_client()
                response = client.head_object(Bucket=bucket, Key=key)
                return dg.AssetCheckResult(
                    passed=True,
                    description=f"File exists: s3://{bucket}/{key}",
                    metadata={
                        "country": country_name,
                        "file_size_bytes": response['ContentLength'],
                        "partition_date": dg.TimestampMetadataValue(
                            datetime.combine(day, datetime.min.time()).timestamp()
                        ),
                    }
                )
            except client.exceptions.NoSuchKey:
                return dg.AssetCheckResult(
                    passed=False,
                    description=f"File not found: s3://{bucket}/{key}",
                    metadata={
                        "country": country_name,
                        "error": dg.MetadataValue.text("S3 object does not exist")
                    }
                )
        return check
    
    def _create_data_not_empty_check(self, country_key: str, country_name: str, asset_func, config: Dict):
        """Create data not empty check."""
        @dg.asset_check(
            asset=asset_func,
            name=config['name'].format(country=country_name),
            description=config['description'].format(country=country_key),
            blocking=config.get('blocking', False)
        )
        async def check(context: dg.AssetCheckExecutionContext, s3: S3Resource) -> dg.AssetCheckResult:
            try:
                df, day, file_size = await self._get_dataframe(context, s3, country_name)
                
                if df.is_empty():
                    return dg.AssetCheckResult(
                        passed=False,
                        description=f"Data is empty for {country_key}",
                        metadata={
                            "country": country_name,
                            "record_count": 0,
                            "file_size_bytes": file_size,
                        }
                    )
                else:
                    return dg.AssetCheckResult(
                        passed=True,
                        description=f"Data contains {len(df)} records for {country_key}",
                        metadata={
                            "country": country_name,
                            "record_count": len(df),
                            "file_size_bytes": file_size,
                        }
                    )
            except Exception as e:
                return dg.AssetCheckResult(
                    passed=False,
                    description=f"Check failed for {country_key}: {str(e)}",
                    metadata={"country": country_name, "error": dg.MetadataValue.text(str(e))}
                )
        return check
    
    def _create_required_columns_check(self, country_key: str, country_name: str, asset_func, config: Dict):
        """Create required columns check."""
        @dg.asset_check(
            asset=asset_func,
            name=config['name'].format(country=country_name),
            description=config['description'].format(country=country_key),
            blocking=config.get('blocking', False)
        )
        async def check(context: dg.AssetCheckExecutionContext, s3: S3Resource) -> dg.AssetCheckResult:
            try:
                df, day, file_size = await self._get_dataframe(context, s3, country_name)
                required_columns = config.get('required_columns', [])
                missing_columns = [col for col in required_columns if col not in df.columns]
                
                if missing_columns:
                    return dg.AssetCheckResult(
                        passed=False,
                        description=f"Missing columns for {country_key}: {missing_columns}",
                        metadata={
                            "country": country_name,
                            "missing_columns": dg.MetadataValue.text(", ".join(missing_columns)),
                        }
                    )
                else:
                    return dg.AssetCheckResult(
                        passed=True,
                        description=f"All required columns present for {country_key}",
                        metadata={
                            "country": country_name,
                            "required_columns": dg.MetadataValue.text(", ".join(required_columns)),
                        }
                    )
            except Exception as e:
                return dg.AssetCheckResult(
                    passed=False,
                    description=f"Check failed for {country_key}: {str(e)}",
                    metadata={"country": country_name, "error": dg.MetadataValue.text(str(e))}
                )
        return check
    
    def _create_coordinates_validation_check(self, country_key: str, country_name: str, asset_func, config: Dict):
        """Create coordinates validation check."""
        @dg.asset_check(
            asset=asset_func,
            name=config['name'].format(country=country_name),
            description=config['description'].format(country=country_key),
            blocking=config.get('blocking', False)
        )
        async def check(context: dg.AssetCheckExecutionContext, s3: S3Resource) -> dg.AssetCheckResult:
            try:
                df, day, file_size = await self._get_dataframe(context, s3, country_name)
                
                if "latitude" not in df.columns or "longitude" not in df.columns:
                    return dg.AssetCheckResult(
                        passed=True,
                        description=f"Latitude/longitude columns not present for {country_key}",
                        metadata={"country": country_name}
                    )
                
                lat_range = config.get('lat_range', [-90, 90])
                lon_range = config.get('lon_range', [-180, 180])
                
                df_coords = df.select([
                    pl.col("latitude").cast(pl.Float64, strict=False).alias("lat_numeric"),
                    pl.col("longitude").cast(pl.Float64, strict=False).alias("lon_numeric")
                ])
                
                lat_invalid = df_coords.filter(
                    pl.col("lat_numeric").is_null() | 
                    (pl.col("lat_numeric") < lat_range[0]) | 
                    (pl.col("lat_numeric") > lat_range[1])
                ).height
                
                lon_invalid = df_coords.filter(
                    pl.col("lon_numeric").is_null() | 
                    (pl.col("lon_numeric") < lon_range[0]) | 
                    (pl.col("lon_numeric") > lon_range[1])
                ).height
                
                if lat_invalid > 0 or lon_invalid > 0:
                    return dg.AssetCheckResult(
                        passed=False,
                        description=f"Invalid coordinates for {country_key}: {lat_invalid} lat, {lon_invalid} lon",
                        metadata={
                            "country": country_name,
                            "invalid_lat": lat_invalid,
                            "invalid_lon": lon_invalid,
                        }
                    )
                else:
                    return dg.AssetCheckResult(
                        passed=True,
                        description=f"All coordinates valid for {country_key}",
                        metadata={"country": country_name, "total_records": len(df)}
                    )
            except Exception as e:
                return dg.AssetCheckResult(
                    passed=False,
                    description=f"Check failed for {country_key}: {str(e)}",
                    metadata={"country": country_name, "error": dg.MetadataValue.text(str(e))}
                )
        return check
    
    def create_checks_for_country(self, country_key: str, asset_func) -> List[Callable]:
        """Create all enabled asset checks for a country."""
        country_name = country_key.lower().replace(' ', '_')
        checks = []
        
        check_creators = {
            'file_exists': self._create_file_exists_check,
            'data_not_empty': self._create_data_not_empty_check,
            'required_columns': self._create_required_columns_check,
            'coordinates_validation': self._create_coordinates_validation_check,
            # Add more check creators as needed
        }
        
        for check_type, config in self.checks_config['asset_checks'].items():
            if config.get('enabled', True) and check_type in check_creators:
                check_func = check_creators[check_type](country_key, country_name, asset_func, config)
                checks.append(check_func)
        
        return checks


