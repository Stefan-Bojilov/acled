from datetime import datetime, timedelta
import io
import os

import dagster as dg
from dagster_aws.s3 import S3Resource
from dagster_pipeline.dagster_acled.assets.base_assets import (
    acled_daily_to_postgres,
    acled_request_daily,
)
from dagster_pipeline.dagster_acled.resources.resources import PostgreSQLResource
import polars as pl


async def _get_acled_dataframe(context: dg.AssetCheckExecutionContext, s3: S3Resource):
    """Helper function to load ACLED data from S3"""
    day_str = context.run.tags["dagster/partition"]
    try:
        day = datetime.strptime(day_str, "%Y-%m-%d").date()
    except ValueError:
        try:
            day = datetime.fromisoformat(day_str).date()
        except ValueError as e:
            raise ValueError(f"Could not parse partition date '{day_str}': {e}")
    
    bucket = os.environ['S3_BUCKET']
    key = f"acled/acled_daily_data/partition_{day}.parquet"
    
    client = s3.get_client()
    response = client.get_object(Bucket=bucket, Key=key)
    file_content = response['Body'].read()
    buf = io.BytesIO(file_content)
    df = pl.read_parquet(buf)
    
    return df, day, len(file_content)


@dg.asset_check(
    asset=acled_request_daily,
    name="acled_file_exists_check",
    description="Verify ACLED daily file exists in S3"
)
async def acled_file_exists_check(
    context: dg.AssetCheckExecutionContext,
    s3: S3Resource,
) -> dg.AssetCheckResult:
    """Check if the ACLED daily file exists in S3"""
    day_str = context.run.tags["dagster/partition"]
    try:
        day = datetime.strptime(day_str, "%Y-%m-%d").date()
    except ValueError:
        try:
            day = datetime.fromisoformat(day_str).date()
        except ValueError:
            return dg.AssetCheckResult(
                passed=False,
                description=f"Invalid partition date format: {day_str}",
                metadata={"error": dg.MetadataValue.text(f"Could not parse date: {day_str}")}
            )
    
    bucket = os.environ['S3_BUCKET']
    key = f"acled/daily_partitions/acled_{day}.parquet"
    
    try:
        client = s3.get_client()
        response = client.head_object(Bucket=bucket, Key=key)
        file_size = response['ContentLength']
        
        return dg.AssetCheckResult(
            passed=True,
            description=f"File exists: s3://{bucket}/{key}",
            metadata={
                "file_size_bytes": file_size,
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
                "partition_date": dg.TimestampMetadataValue(
                    datetime.combine(day, datetime.min.time()).timestamp()
                ),
                "error": dg.MetadataValue.text("S3 object does not exist")
            }
        )


@dg.asset_check(
    asset=acled_request_daily,
    name="acled_data_not_empty_check",
    description="Verify ACLED daily data is not empty", 
    blocking = True,
)
async def acled_data_not_empty_check(
    context: dg.AssetCheckExecutionContext,
    s3: S3Resource,
) -> dg.AssetCheckResult:
    """Check that the ACLED data contains records"""
    try:
        df, day, file_size = await _get_acled_dataframe(context, s3)
        
        if df.is_empty():
            return dg.AssetCheckResult(
                passed=False,
                description="Data is empty",
                metadata={
                    "record_count": 0,
                    "file_size_bytes": file_size,
                    "partition_date": dg.TimestampMetadataValue(
                        datetime.combine(day, datetime.min.time()).timestamp()
                    ),
                }
            )
        else:
            return dg.AssetCheckResult(
                passed=True,
                description=f"Data contains {len(df)} records",
                metadata={
                    "record_count": len(df),
                    "file_size_bytes": file_size,
                    "partition_date": dg.TimestampMetadataValue(
                        datetime.combine(day, datetime.min.time()).timestamp()
                    ),
                }
            )
    except Exception as e:
        return dg.AssetCheckResult(
            passed=False,
            description=f"Check failed due to error: {str(e)}",
            metadata={"error": dg.MetadataValue.text(str(e))}
        )


@dg.asset_check(
    asset=acled_request_daily,
    name="acled_required_columns_check",
    description="Verify ACLED daily data has all required columns"
)
async def acled_required_columns_check(
    context: dg.AssetCheckExecutionContext,
    s3: S3Resource,
) -> dg.AssetCheckResult:
    """Check that all required columns are present"""
    try:
        df, day, file_size = await _get_acled_dataframe(context, s3)
        
        required_columns = [
            "event_date", "event_type", "country", "admin1", "admin2", 
            "latitude", "longitude", "fatalities"
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            return dg.AssetCheckResult(
                passed=False,
                description=f"Missing required columns: {missing_columns}",
                metadata={
                    "missing_columns": dg.MetadataValue.text(", ".join(missing_columns)),
                    "present_columns": dg.MetadataValue.text(", ".join(df.columns)),
                    "partition_date": dg.TimestampMetadataValue(
                        datetime.combine(day, datetime.min.time()).timestamp()
                    ),
                }
            )
        else:
            return dg.AssetCheckResult(
                passed=True,
                description="All required columns present",
                metadata={
                    "required_columns": dg.MetadataValue.text(", ".join(required_columns)),
                    "partition_date": dg.TimestampMetadataValue(
                        datetime.combine(day, datetime.min.time()).timestamp()
                    ),
                }
            )
    except Exception as e:
        return dg.AssetCheckResult(
            passed=False,
            description=f"Check failed due to error: {str(e)}",
            metadata={"error": dg.MetadataValue.text(str(e))}
        )


@dg.asset_check(
    asset=acled_request_daily,
    name="acled_event_date_partition_check",
    description="Verify event_date matches partition", 
    blocking = True,
)
async def acled_event_date_partition_check(
    context: dg.AssetCheckExecutionContext,
    s3: S3Resource,
) -> dg.AssetCheckResult:
    """Check that all event_date values match the partition date"""
    try:
        df, day, file_size = await _get_acled_dataframe(context, s3)
        
        if df.is_empty() or "event_date" not in df.columns:
            return dg.AssetCheckResult(
                passed=True,
                description="No data to validate or event_date column missing",
                metadata={
                    "partition_date": dg.TimestampMetadataValue(
                        datetime.combine(day, datetime.min.time()).timestamp()
                    ),
                }
            )
        
        dates = df.select(pl.col("event_date").cast(pl.Date)).to_series().unique()
        invalid_dates = [d for d in dates if d != day]
        
        if invalid_dates:
            return dg.AssetCheckResult(
                passed=False,
                description=f"Invalid event_dates found: {invalid_dates}",
                metadata={
                    "expected_date": day.isoformat(),
                    "invalid_dates": dg.MetadataValue.text(", ".join(str(d) for d in invalid_dates)),
                    "partition_date": dg.TimestampMetadataValue(
                        datetime.combine(day, datetime.min.time()).timestamp()
                    ),
                }
            )
        else:
            return dg.AssetCheckResult(
                passed=True,
                description="All event_dates match partition",
                metadata={
                    "expected_date": day.isoformat(),
                    "unique_dates_count": len(dates),
                    "partition_date": dg.TimestampMetadataValue(
                        datetime.combine(day, datetime.min.time()).timestamp()
                    ),
                }
            )
    except Exception as e:
        return dg.AssetCheckResult(
            passed=False,
            description=f"Check failed due to error: {str(e)}",
            metadata={"error": dg.MetadataValue.text(str(e))}
        )


@dg.asset_check(
    asset=acled_request_daily,
    name="acled_coordinates_validation_check",
    description="Verify latitude/longitude are numeric and within valid ranges"
)
async def acled_coordinates_validation_check(
    context: dg.AssetCheckExecutionContext,
    s3: S3Resource,
) -> dg.AssetCheckResult:
    """Check coordinate validity"""
    try:
        df, day, file_size = await _get_acled_dataframe(context, s3)
        
        if "latitude" not in df.columns or "longitude" not in df.columns:
            return dg.AssetCheckResult(
                passed=True,
                description="Latitude or longitude columns not present",
                metadata={
                    "partition_date": dg.TimestampMetadataValue(
                        datetime.combine(day, datetime.min.time()).timestamp()
                    ),
                }
            )
        
        # Ensure latitude and longitude are numeric before comparison
        df_coords = df.select([
            pl.col("latitude").cast(pl.Float64, strict=False).alias("lat_numeric"),
            pl.col("longitude").cast(pl.Float64, strict=False).alias("lon_numeric")
        ])
        
        # Check for issues
        lat_non_numeric = df_coords.filter(pl.col("lat_numeric").is_null()).height
        lon_non_numeric = df_coords.filter(pl.col("lon_numeric").is_null()).height
        lat_out_of_range = df_coords.filter(
            (pl.col("lat_numeric").is_not_null()) &
            ((pl.col("lat_numeric") < -90) | (pl.col("lat_numeric") > 90))
        ).height
        lon_out_of_range = df_coords.filter(
            (pl.col("lon_numeric").is_not_null()) &
            ((pl.col("lon_numeric") < -180) | (pl.col("lon_numeric") > 180))
        ).height
        
        issues = []
        if lat_non_numeric > 0:
            issues.append(f"{lat_non_numeric} records with non-numeric latitude")
        if lon_non_numeric > 0:
            issues.append(f"{lon_non_numeric} records with non-numeric longitude")
        if lat_out_of_range > 0:
            issues.append(f"{lat_out_of_range} records with invalid latitude range")
        if lon_out_of_range > 0:
            issues.append(f"{lon_out_of_range} records with invalid longitude range")
        
        if issues:
            return dg.AssetCheckResult(
                passed=False,
                description=f"Coordinate validation issues: {'; '.join(issues)}",
                metadata={
                    "lat_non_numeric": lat_non_numeric,
                    "lon_non_numeric": lon_non_numeric,
                    "lat_out_of_range": lat_out_of_range,
                    "lon_out_of_range": lon_out_of_range,
                    "partition_date": dg.TimestampMetadataValue(
                        datetime.combine(day, datetime.min.time()).timestamp()
                    ),
                }
            )
        else:
            return dg.AssetCheckResult(
                passed=True,
                description="All coordinates are numeric and within valid ranges",
                metadata={
                    "total_records": len(df),
                    "partition_date": dg.TimestampMetadataValue(
                        datetime.combine(day, datetime.min.time()).timestamp()
                    ),
                }
            )
    except Exception as e:
        return dg.AssetCheckResult(
            passed=False,
            description=f"Check failed due to error: {str(e)}",
            metadata={"error": dg.MetadataValue.text(str(e))}
        )


@dg.asset_check(
    asset=acled_request_daily,
    name="acled_fatalities_validation_check",
    description="Verify fatalities are numeric and non-negative"
)
async def acled_fatalities_validation_check(
    context: dg.AssetCheckExecutionContext,
    s3: S3Resource,
) -> dg.AssetCheckResult:
    """Check fatalities data validity"""
    try:
        df, day, file_size = await _get_acled_dataframe(context, s3)
        
        if "fatalities" not in df.columns:
            return dg.AssetCheckResult(
                passed=True,
                description="Fatalities column not present",
                metadata={
                    "partition_date": dg.TimestampMetadataValue(
                        datetime.combine(day, datetime.min.time()).timestamp()
                    ),
                }
            )
        
        # Ensure fatalities is numeric
        df_fatalities = df.select(pl.col("fatalities").cast(pl.Int64, strict=False).alias("fat_numeric"))
        non_numeric_fatalities = df_fatalities.filter(pl.col("fat_numeric").is_null()).height
        negative_fatalities = df_fatalities.filter(
            (pl.col("fat_numeric").is_not_null()) & (pl.col("fat_numeric") < 0)
        ).height
        
        issues = []
        if non_numeric_fatalities > 0:
            issues.append(f"{non_numeric_fatalities} records with non-numeric fatalities")
        if negative_fatalities > 0:
            issues.append(f"{negative_fatalities} records with negative fatalities")
        
        if issues:
            return dg.AssetCheckResult(
                passed=False,
                description=f"Fatalities validation issues: {'; '.join(issues)}",
                metadata={
                    "non_numeric_fatalities": non_numeric_fatalities,
                    "negative_fatalities": negative_fatalities,
                    "partition_date": dg.TimestampMetadataValue(
                        datetime.combine(day, datetime.min.time()).timestamp()
                    ),
                }
            )
        else:
            return dg.AssetCheckResult(
                passed=True,
                description="All fatalities are numeric and non-negative",
                metadata={
                    "total_records": len(df),
                    "partition_date": dg.TimestampMetadataValue(
                        datetime.combine(day, datetime.min.time()).timestamp()
                    ),
                }
            )
    except Exception as e:
        return dg.AssetCheckResult(
            passed=False,
            description=f"Check failed due to error: {str(e)}",
            metadata={"error": dg.MetadataValue.text(str(e))}
        )


@dg.asset_check(
    asset=acled_request_daily,
    name="acled_event_types_validation_check",
    description="Verify event_type values are valid"
)
async def acled_event_types_validation_check(
    context: dg.AssetCheckExecutionContext,
    s3: S3Resource,
) -> dg.AssetCheckResult:
    """Check event type validity"""
    try:
        df, day, file_size = await _get_acled_dataframe(context, s3)
        
        if "event_type" not in df.columns:
            return dg.AssetCheckResult(
                passed=True,
                description="Event_type column not present",
                metadata={
                    "partition_date": dg.TimestampMetadataValue(
                        datetime.combine(day, datetime.min.time()).timestamp()
                    ),
                }
            )
        
        valid_event_types = {
            "Violence against civilians", "Battles", "Explosions/Remote violence",
            "Riots", "Protests", "Strategic developments"
        }
        actual_event_types = set(df.select("event_type").to_series().unique())
        invalid_types = actual_event_types - valid_event_types
        
        # Create event type distribution
        event_type_counts = (
            df.select("event_type")
            .to_series()
            .value_counts()
            .sort("count", descending=True)
        )
        
        metadata = {
            "partition_date": dg.TimestampMetadataValue(
                datetime.combine(day, datetime.min.time()).timestamp()
            ),
            "event_type_distribution": dg.TableMetadataValue(
                records=[dg.TableRecord(record) for record in event_type_counts.to_dicts()],
                schema=dg.TableSchema([
                    dg.TableColumn(name="event_type"),
                    dg.TableColumn(name="count", type="int"),
                ])
            )
        }
        
        if invalid_types:
            metadata["invalid_types"] = dg.MetadataValue.text(", ".join(invalid_types))
            return dg.AssetCheckResult(
                passed=False,
                description=f"Unknown event types: {invalid_types}",
                metadata=metadata
            )
        else:
            return dg.AssetCheckResult(
                passed=True,
                description="All event types are valid",
                metadata=metadata
            )
    except Exception as e:
        return dg.AssetCheckResult(
            passed=False,
            description=f"Check failed due to error: {str(e)}",
            metadata={"error": dg.MetadataValue.text(str(e))}
        )


@dg.asset_check(
    asset=acled_request_daily,
    name="acled_duplicate_records_check",
    description="Check for duplicate records based on key fields"
)
async def acled_duplicate_records_check(
    context: dg.AssetCheckExecutionContext,
    s3: S3Resource,
) -> dg.AssetCheckResult:
    """Check for duplicate records"""
    try:
        df, day, file_size = await _get_acled_dataframe(context, s3)
        
        key_fields = ["event_id_cnty"]
        available_key_fields = [field for field in key_fields if field in df.columns]
        
        if not available_key_fields:
            return dg.AssetCheckResult(
                passed=True,
                description="No key fields available for duplicate checking",
                metadata={
                    "partition_date": dg.TimestampMetadataValue(
                        datetime.combine(day, datetime.min.time()).timestamp()
                    ),
                }
            )
        
        duplicates = df.select(available_key_fields).is_duplicated().sum()
        
        if duplicates > 0:
            return dg.AssetCheckResult(
                passed=False,
                description=f"{duplicates} duplicate records found",
                metadata={
                    "duplicate_count": duplicates,
                    "total_records": len(df),
                    "key_fields_used": dg.MetadataValue.text(", ".join(available_key_fields)),
                    "partition_date": dg.TimestampMetadataValue(
                        datetime.combine(day, datetime.min.time()).timestamp()
                    ),
                }
            )
        else:
            return dg.AssetCheckResult(
                passed=True,
                description="No duplicate records found",
                metadata={
                    "total_records": len(df),
                    "key_fields_used": dg.MetadataValue.text(", ".join(available_key_fields)),
                    "partition_date": dg.TimestampMetadataValue(
                        datetime.combine(day, datetime.min.time()).timestamp()
                    ),
                }
            )
    except Exception as e:
        return dg.AssetCheckResult(
            passed=False,
            description=f"Check failed due to error: {str(e)}",
            metadata={"error": dg.MetadataValue.text(str(e))}
        )