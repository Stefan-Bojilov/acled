from datetime import datetime, timedelta
import io
import os

import dagster as dg
from dagster_aws.s3 import S3Resource
from dagster_acled.resources.resources import load_resource_config
import polars as pl

from dagster_acled.resources.resources import PostgreSQLResource
from dagster_acled.assets.base_assets import acled_daily_to_postgres

resource_config = load_resource_config(config_path="dagster_acled/resources/resource_config.yaml")

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
        
    
    bucket = resource_config['s3']['bucket_name']
    key = f"{resource_config['s3']['data_prefix']}/partition_{day}.parquet"
    
    client = s3.get_client()
    response = client.get_object(Bucket=bucket, Key=key)
    file_content = response['Body'].read()
    buf = io.BytesIO(file_content)
    df = pl.read_parquet(buf)
    
    return df, day, len(file_content)


@dg.asset_check(
    asset="acled_request_daily",
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
    
    bucket = resource_config['s3']['bucket_name']
    key = f"{resource_config['s3']['data_prefix']}/partition_{day}.parquet"
    
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
            })


@dg.asset_check(
    asset="acled_request_daily",
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
    asset="acled_request_daily",
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
    asset="acled_request_daily",
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
    asset="acled_request_daily",
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
    asset="acled_request_daily",
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
    asset="acled_request_daily",
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
    asset="acled_request_daily",
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



@dg.asset_check(
    asset=acled_daily_to_postgres,
    name="postgres_data_completeness_check",
    description="Verify that PostgreSQL contains the expected number of records for the partition",
    blocking=True,
)
def postgres_data_completeness_check(
    context: dg.AssetCheckExecutionContext,
    postgres: PostgreSQLResource,
) -> dg.AssetCheckResult:
    """Check that the PostgreSQL table contains the expected amount of data for the partition date."""
    
    # Get partition date from context
    partition_date_str = context.run.tags.get("dagster/partition")
    if not partition_date_str:
        return dg.AssetCheckResult(
            passed=False,
            description="No partition information available",
            metadata={"error": dg.MetadataValue.text("Missing partition context")}
        )
    
    try:
        partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()
    except ValueError:
        partition_date = datetime.fromisoformat(partition_date_str).date()
    
    conn = postgres.get_connection()
    try:
        # Count records for this partition
        count_query = """
        SELECT COUNT(*) as record_count
        FROM acled_events_no_delete 
        WHERE event_date = %s
        """
        
        with conn.cursor() as cur:
            cur.execute(count_query, [partition_date])
            result = cur.fetchone()
            record_count = result[0] if result else 0
        
        # Check if we have a reasonable amount of data
        # ACLED typically has at least some events per day for active conflict zones
        min_expected_records = 1
        max_reasonable_records = 10000  # Adjust based on your data patterns
        
        if record_count < min_expected_records:
            return dg.AssetCheckResult(
                passed=False,
                description=f"Too few records found: {record_count} (expected at least {min_expected_records})",
                metadata={
                    "record_count": record_count,
                    "partition_date": partition_date.isoformat(),
                    "min_expected": min_expected_records
                }
            )
        elif record_count > max_reasonable_records:
            return dg.AssetCheckResult(
                passed=False,
                description=f"Unusually high number of records: {record_count} (max reasonable: {max_reasonable_records})",
                metadata={
                    "record_count": record_count,
                    "partition_date": partition_date.isoformat(),
                    "max_reasonable": max_reasonable_records
                }
            )
        else:
            return dg.AssetCheckResult(
                passed=True,
                description=f"Record count looks healthy: {record_count} records",
                metadata={
                    "record_count": record_count,
                    "partition_date": partition_date.isoformat(),
                }
            )
            
    except Exception as e:
        return dg.AssetCheckResult(
            passed=False,
            description=f"Failed to check data completeness: {str(e)}",
            metadata={"error": dg.MetadataValue.text(str(e))}
        )
    finally:
        conn.close()


@dg.asset_check(
    asset=acled_daily_to_postgres,
    name="postgres_critical_fields_check",
    description="Verify critical fields are not null/empty in PostgreSQL",
    blocking=False,
)
def postgres_critical_fields_check(
    context: dg.AssetCheckExecutionContext,
    postgres: PostgreSQLResource,
) -> dg.AssetCheckResult:
    """Check that critical fields like event_type, country, coordinates are populated."""
    
    partition_date_str = context.run.tags.get("dagster/partition")
    try:
        partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()
    except ValueError:
        partition_date = datetime.fromisoformat(partition_date_str).date()
    
    conn = postgres.get_connection()
    try:
        # Check critical fields for completeness
        critical_fields_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN event_type IS NULL OR event_type = '' THEN 1 END) as missing_event_type,
            COUNT(CASE WHEN country IS NULL OR country = '' THEN 1 END) as missing_country,
            COUNT(CASE WHEN admin1 IS NULL OR admin1 = '' THEN 1 END) as missing_admin1,
            COUNT(CASE WHEN latitude IS NULL THEN 1 END) as missing_latitude,
            COUNT(CASE WHEN longitude IS NULL THEN 1 END) as missing_longitude,
            COUNT(CASE WHEN fatalities IS NULL THEN 1 END) as missing_fatalities
        FROM acled_events_no_delete 
        WHERE event_date = %s
        """
        
        with conn.cursor() as cur:
            cur.execute(critical_fields_query, [partition_date])
            result = cur.fetchone()
            
            if not result or result[0] == 0:
                return dg.AssetCheckResult(
                    passed=False,
                    description="No records found for partition date",
                    metadata={"partition_date": partition_date.isoformat()}
                )
            
            total_records = result[0]
            missing_event_type = result[1]
            missing_country = result[2]
            missing_admin1 = result[3]
            missing_latitude = result[4]
            missing_longitude = result[5]
            missing_fatalities = result[6]
        
        # Calculate percentages
        event_type_completeness = round((1 - missing_event_type/total_records) * 100, 2)
        country_completeness = round((1 - missing_country/total_records) * 100, 2)
        coordinates_completeness = round((1 - max(missing_latitude, missing_longitude)/total_records) * 100, 2)
        
        # Set thresholds
        critical_threshold = 95.0  # 95% completeness required for critical fields
        issues = []
        
        if event_type_completeness < critical_threshold:
            issues.append(f"Event type completeness: {event_type_completeness}%")
        if country_completeness < critical_threshold:
            issues.append(f"Country completeness: {country_completeness}%")
        if coordinates_completeness < critical_threshold:
            issues.append(f"Coordinates completeness: {coordinates_completeness}%")
        
        metadata = {
            "total_records": total_records,
            "event_type_completeness_pct": event_type_completeness,
            "country_completeness_pct": country_completeness,
            "coordinates_completeness_pct": coordinates_completeness,
            "admin1_completeness_pct": round((1 - missing_admin1/total_records) * 100, 2),
            "fatalities_completeness_pct": round((1 - missing_fatalities/total_records) * 100, 2),
            "partition_date": partition_date.isoformat(),
        }
        
        if issues:
            return dg.AssetCheckResult(
                passed=False,
                description=f"Critical field completeness issues: {'; '.join(issues)}",
                metadata=metadata
            )
        else:
            return dg.AssetCheckResult(
                passed=True,
                description="All critical fields meet completeness requirements",
                metadata=metadata
            )
            
    except Exception as e:
        return dg.AssetCheckResult(
            passed=False,
            description=f"Failed to check critical fields: {str(e)}",
            metadata={"error": dg.MetadataValue.text(str(e))}
        )
    finally:
        conn.close()


@dg.asset_check(
    asset=acled_daily_to_postgres,
    name="postgres_coordinate_validity_check",
    description="Verify latitude and longitude values are within valid ranges",
    blocking=False,
)
def postgres_coordinate_validity_check(
    context: dg.AssetCheckExecutionContext,
    postgres: PostgreSQLResource,
) -> dg.AssetCheckResult:
    """Check that coordinates are within valid geographic ranges."""
    
    partition_date_str = context.run.tags.get("dagster/partition")
    try:
        partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()
    except ValueError:
        partition_date = datetime.fromisoformat(partition_date_str).date()
    
    conn = postgres.get_connection()
    try:
        # Check coordinate validity
        coordinates_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN latitude < -90 OR latitude > 90 THEN 1 END) as invalid_latitude,
            COUNT(CASE WHEN longitude < -180 OR longitude > 180 THEN 1 END) as invalid_longitude,
            COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) as records_with_coordinates,
            MIN(latitude) as min_lat,
            MAX(latitude) as max_lat,
            MIN(longitude) as min_lon,
            MAX(longitude) as max_lon
        FROM acled_events_no_delete 
        WHERE event_date = %s
        """
        
        with conn.cursor() as cur:
            cur.execute(coordinates_query, [partition_date])
            result = cur.fetchone()
            
            if not result or result[0] == 0:
                return dg.AssetCheckResult(
                    passed=False,
                    description="No records found for partition date",
                    metadata={"partition_date": partition_date.isoformat()}
                )
            
            total_records = result[0]
            invalid_latitude = result[1]
            invalid_longitude = result[2]
            records_with_coords = result[3]
            min_lat, max_lat = result[4], result[5]
            min_lon, max_lon = result[6], result[7]
        
        issues = []
        if invalid_latitude > 0:
            issues.append(f"{invalid_latitude} records with invalid latitude")
        if invalid_longitude > 0:
            issues.append(f"{invalid_longitude} records with invalid longitude")
        
        metadata = {
            "total_records": total_records,
            "records_with_coordinates": records_with_coords,
            "invalid_latitude_count": invalid_latitude,
            "invalid_longitude_count": invalid_longitude,
            "coordinate_coverage_pct": round((records_with_coords/total_records) * 100, 2) if total_records > 0 else 0,
            "latitude_range": f"{min_lat} to {max_lat}" if min_lat is not None else "No data",
            "longitude_range": f"{min_lon} to {max_lon}" if min_lon is not None else "No data",
            "partition_date": partition_date.isoformat(),
        }
        
        if issues:
            return dg.AssetCheckResult(
                passed=False,
                description=f"Coordinate validity issues: {'; '.join(issues)}",
                metadata=metadata
            )
        else:
            return dg.AssetCheckResult(
                passed=True,
                description="All coordinates are within valid ranges",
                metadata=metadata
            )
            
    except Exception as e:
        return dg.AssetCheckResult(
            passed=False,
            description=f"Failed to check coordinate validity: {str(e)}",
            metadata={"error": dg.MetadataValue.text(str(e))}
        )
    finally:
        conn.close()


@dg.asset_check(
    asset=acled_daily_to_postgres,
    name="postgres_event_type_distribution_check",
    description="Verify event type distribution is reasonable and contains expected categories",
    blocking=False,
)
def postgres_event_type_distribution_check(
    context: dg.AssetCheckExecutionContext,
    postgres: PostgreSQLResource,
) -> dg.AssetCheckResult:
    """Check that event types follow expected ACLED categories and distribution patterns."""
    
    partition_date_str = context.run.tags.get("dagster/partition")
    try:
        partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()
    except ValueError:
        partition_date = datetime.fromisoformat(partition_date_str).date()
    
    # Expected ACLED event types
    expected_event_types = {
        "Violence against civilians",
        "Battles", 
        "Explosions/Remote violence",
        "Riots",
        "Protests",
        "Strategic developments"
    }
    
    conn = postgres.get_connection()
    try:
        # Get event type distribution
        event_types_query = """
        SELECT 
            event_type,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
        FROM acled_events_no_delete 
        WHERE event_date = %s AND event_type IS NOT NULL AND event_type != ''
        GROUP BY event_type
        ORDER BY count DESC
        """
        
        with conn.cursor() as cur:
            cur.execute(event_types_query, [partition_date])
            results = cur.fetchall()
            
            if not results:
                return dg.AssetCheckResult(
                    passed=False,
                    description="No event types found for partition date",
                    metadata={"partition_date": partition_date.isoformat()}
                )
        
        # Analyze results
        found_event_types = {row[0] for row in results}
        total_events = sum(row[1] for row in results)
        
        # Check for unexpected event types
        unexpected_types = found_event_types - expected_event_types
        missing_expected_types = expected_event_types - found_event_types
        
        # Create distribution table for metadata
        distribution_data = [
            {
                "event_type": row[0],
                "count": row[1],
                "percentage": float(row[2])
            }
            for row in results
        ]
        
        issues = []
        if unexpected_types:
            issues.append(f"Unexpected event types: {', '.join(unexpected_types)}")
        
        # Check if any single event type dominates too much (>80%)
        max_percentage = max((row[2] for row in results), default=0)
        if max_percentage > 80:
            dominant_type = next(row[0] for row in results if row[2] == max_percentage)
            issues.append(f"Single event type dominates: {dominant_type} ({max_percentage}%)")
        
        metadata = {
            "total_events": total_events,
            "unique_event_types": len(found_event_types),
            "expected_types_found": len(found_event_types & expected_event_types),
            "unexpected_types_count": len(unexpected_types),
            "missing_expected_types": list(missing_expected_types) if missing_expected_types else "None",
            "partition_date": partition_date.isoformat(),
            "event_type_distribution": dg.TableMetadataValue(
                records=[dg.TableRecord(record) for record in distribution_data],
                schema=dg.TableSchema([
                    dg.TableColumn(name="event_type", type="text"),
                    dg.TableColumn(name="count", type="int"),
                    dg.TableColumn(name="percentage", type="float"),
                ])
            )
        }
        
        if issues:
            return dg.AssetCheckResult(
                passed=False,
                description=f"Event type distribution issues: {'; '.join(issues)}",
                metadata=metadata
            )
        else:
            return dg.AssetCheckResult(
                passed=True,
                description=f"Event type distribution looks normal ({len(found_event_types)} types found)",
                metadata=metadata
            )
            
    except Exception as e:
        return dg.AssetCheckResult(
            passed=False,
            description=f"Failed to check event type distribution: {str(e)}",
            metadata={"error": dg.MetadataValue.text(str(e))}
        )
    finally:
        conn.close()


@dg.asset_check(
    asset=acled_daily_to_postgres,
    name="postgres_fatalities_validity_check",
    description="Verify fatalities data is reasonable and follows expected patterns",
    blocking=False,
)
def postgres_fatalities_validity_check(
    context: dg.AssetCheckExecutionContext,
    postgres: PostgreSQLResource,
) -> dg.AssetCheckResult:
    """Check fatalities data for validity and reasonable distribution."""
    
    partition_date_str = context.run.tags.get("dagster/partition")
    try:
        partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()
    except ValueError:
        partition_date = datetime.fromisoformat(partition_date_str).date()
    
    conn = postgres.get_connection()
    try:
        # Analyze fatalities data
        fatalities_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN fatalities IS NOT NULL THEN 1 END) as records_with_fatalities,
            COUNT(CASE WHEN fatalities < 0 THEN 1 END) as negative_fatalities,
            COUNT(CASE WHEN fatalities > 1000 THEN 1 END) as extreme_fatalities,
            COUNT(CASE WHEN fatalities = 0 THEN 1 END) as zero_fatalities,
            COUNT(CASE WHEN fatalities BETWEEN 1 AND 10 THEN 1 END) as low_fatalities,
            COUNT(CASE WHEN fatalities BETWEEN 11 AND 50 THEN 1 END) as moderate_fatalities,
            COUNT(CASE WHEN fatalities BETWEEN 51 AND 100 THEN 1 END) as high_fatalities,
            COUNT(CASE WHEN fatalities > 100 THEN 1 END) as very_high_fatalities,
            COALESCE(MIN(fatalities), 0) as min_fatalities,
            COALESCE(MAX(fatalities), 0) as max_fatalities,
            COALESCE(ROUND(AVG(fatalities::numeric), 2), 0) as avg_fatalities,
            COALESCE(SUM(fatalities), 0) as total_fatalities
        FROM acled_events_no_delete 
        WHERE event_date = %s
        """
        
        with conn.cursor() as cur:
            cur.execute(fatalities_query, [partition_date])
            result = cur.fetchone()
            
            if not result or result[0] == 0:
                return dg.AssetCheckResult(
                    passed=False,
                    description="No records found for partition date",
                    metadata={"partition_date": partition_date.isoformat()}
                )
            
            (total_records, records_with_fatalities, negative_fatalities, 
             extreme_fatalities, zero_fatalities, low_fatalities, 
             moderate_fatalities, high_fatalities, very_high_fatalities,
             min_fatalities, max_fatalities, avg_fatalities, total_fatalities) = result
        
        issues = []
        warnings = []
        
        # Check for data quality issues
        if negative_fatalities > 0:
            issues.append(f"{negative_fatalities} records with negative fatalities")
        
        # Check for extreme values that might indicate data quality issues
        if extreme_fatalities > 0:
            warnings.append(f"{extreme_fatalities} records with >1000 fatalities (please verify)")
        
        # Calculate data completeness
        fatalities_completeness = round((records_with_fatalities/total_records) * 100, 2) if total_records > 0 else 0
        
        # Create distribution breakdown
        distribution_data = [
            {"range": "0 fatalities", "count": zero_fatalities, "percentage": round((zero_fatalities/total_records) * 100, 1)},
            {"range": "1-10 fatalities", "count": low_fatalities, "percentage": round((low_fatalities/total_records) * 100, 1)},
            {"range": "11-50 fatalities", "count": moderate_fatalities, "percentage": round((moderate_fatalities/total_records) * 100, 1)},
            {"range": "51-100 fatalities", "count": high_fatalities, "percentage": round((high_fatalities/total_records) * 100, 1)},
            {"range": ">100 fatalities", "count": very_high_fatalities, "percentage": round((very_high_fatalities/total_records) * 100, 1)},
        ]
        
        metadata = {
            "total_records": total_records,
            "records_with_fatalities": records_with_fatalities,
            "fatalities_completeness_pct": fatalities_completeness,
            "negative_fatalities_count": negative_fatalities,
            "extreme_fatalities_count": extreme_fatalities,
            "min_fatalities": int(min_fatalities),
            "max_fatalities": int(max_fatalities),
            "avg_fatalities": float(avg_fatalities),
            "total_fatalities": int(total_fatalities),
            "partition_date": partition_date.isoformat(),
            "fatalities_distribution": dg.TableMetadataValue(
                records=[dg.TableRecord(record) for record in distribution_data],
                schema=dg.TableSchema([
                    dg.TableColumn(name="range", type="text"),
                    dg.TableColumn(name="count", type="int"),
                    dg.TableColumn(name="percentage", type="float"),
                ])
            )
        }
        
        # Add warnings to metadata if present
        if warnings:
            metadata["warnings"] = dg.MetadataValue.text("; ".join(warnings))
        
        if issues:
            return dg.AssetCheckResult(
                passed=False,
                description=f"Fatalities data quality issues: {'; '.join(issues)}",
                metadata=metadata
            )
        else:
            description = f"Fatalities data looks valid (avg: {avg_fatalities}, max: {max_fatalities}, total: {total_fatalities})"
            if warnings:
                description += f" - Warnings: {'; '.join(warnings)}"
                
            return dg.AssetCheckResult(
                passed=True,
                description=description,
                metadata=metadata
            )
            
    except Exception as e:
        return dg.AssetCheckResult(
            passed=False,
            description=f"Failed to check fatalities validity: {str(e)}",
            metadata={"error": dg.MetadataValue.text(str(e))}
        )
    finally:
        conn.close()