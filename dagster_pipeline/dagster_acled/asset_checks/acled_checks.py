from datetime import datetime
import io
import os

import dagster as dg
from dagster_acled.assets.acled import acled_request_daily
from dagster_aws.s3 import S3Resource
import polars as pl


@dg.asset_check(
    asset=acled_request_daily, 
    name="acled_daily_data_quality_check",
    description="Verify ACLED daily data completeness and quality"
)
async def acled_daily_data_quality_check(
    context: dg.AssetCheckExecutionContext,
    s3: S3Resource,
) -> dg.AssetCheckResult:
    """
    Check the quality and completeness of ACLED daily data:
    - Verify file exists in S3
    - Check data is not empty
    - Verify event_date matches partition
    - Check for required columns
    - Validate data types and ranges
    """
    # Get the partition day and convert to date object
    day_str = context.run.tags["dagster/partition"]
    try:
        # Parse the string date to a date object
        day = datetime.strptime(day_str, "%Y-%m-%d").date()
    except ValueError:
        # Handle different date formats if needed
        try:
            day = datetime.fromisoformat(day_str).date()
        except ValueError as e:
            context.log.error(f"Could not parse partition date '{day_str}': {e}")
            return dg.AssetCheckResult(
                passed=False,
                description=f"Invalid partition date format: {day_str}",
                metadata={
                    "error": dg.MetadataValue.text(f"Could not parse date: {day_str}")
                }
            )
    
    # Construct S3 path
    bucket = os.environ['S3_BUCKET']
    key = f"acled/daily_partitions/acled_{day}.parquet"
    
    try:
        # Check if file exists and fetch it
        client = s3.get_client()
        response = client.get_object(Bucket=bucket, Key=key)
        
        # Read the response body once and store it
        file_content = response['Body'].read()
        file_size = len(file_content)
        
        # Read the parquet file
        buf = io.BytesIO(file_content)
        df = pl.read_parquet(buf)
        
        # Initialize check results
        checks_passed = []
        checks_failed = []
        
        # Check 1: Data is not empty
        if df.is_empty():
            checks_failed.append("Data is empty")
        else:
            checks_passed.append(f"Data contains {len(df)} records")
        
        # Check 2: Required columns exist
        required_columns = [
            "event_date", "event_type", "country", "admin1", "admin2", 
            "latitude", "longitude", "fatalities"
        ]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            checks_failed.append(f"Missing required columns: {missing_columns}")
        else:
            checks_passed.append("All required columns present")
        
        # Check 3: Event dates match partition
        if not df.is_empty() and "event_date" in df.columns:
            dates = df.select(pl.col("event_date").cast(pl.Date)).to_series().unique()
            invalid_dates = [d for d in dates if d != day]
            if invalid_dates:
                checks_failed.append(f"Invalid event_dates found: {invalid_dates}")
            else:
                checks_passed.append("All event_dates match partition")
        
        # Check 4: Latitude/Longitude ranges
        if "latitude" in df.columns and "longitude" in df.columns:
            # Ensure latitude and longitude are numeric before comparison
            try:
                # Try to cast to float, handle any conversion errors
                df_coords = df.select([
                    pl.col("latitude").cast(pl.Float64, strict=False).alias("lat_numeric"),
                    pl.col("longitude").cast(pl.Float64, strict=False).alias("lon_numeric")
                ])
                
                # Filter out null values (failed conversions) and check ranges
                lat_out_of_range = df_coords.filter(
                    (pl.col("lat_numeric").is_not_null()) &
                    ((pl.col("lat_numeric") < -90) | (pl.col("lat_numeric") > 90))
                ).height
                
                lon_out_of_range = df_coords.filter(
                    (pl.col("lon_numeric").is_not_null()) &
                    ((pl.col("lon_numeric") < -180) | (pl.col("lon_numeric") > 180))
                ).height
                
                # Check for non-numeric coordinates
                lat_non_numeric = df_coords.filter(pl.col("lat_numeric").is_null()).height
                lon_non_numeric = df_coords.filter(pl.col("lon_numeric").is_null()).height
                
                if lat_non_numeric > 0:
                    checks_failed.append(f"{lat_non_numeric} records with non-numeric latitude")
                if lon_non_numeric > 0:
                    checks_failed.append(f"{lon_non_numeric} records with non-numeric longitude")
                if lat_out_of_range > 0:
                    checks_failed.append(f"{lat_out_of_range} records with invalid latitude range")
                if lon_out_of_range > 0:
                    checks_failed.append(f"{lon_out_of_range} records with invalid longitude range")
                
                if all(x == 0 for x in [lat_non_numeric, lon_non_numeric, lat_out_of_range, lon_out_of_range]):
                    checks_passed.append("All coordinates are numeric and within valid ranges")
                    
            except Exception as coord_error:
                checks_failed.append(f"Error validating coordinates: {str(coord_error)}")
        
        # Check 5: Fatalities are non-negative
        if "fatalities" in df.columns:
            try:
                # Ensure fatalities is numeric
                df_fatalities = df.select(pl.col("fatalities").cast(pl.Int64, strict=False).alias("fat_numeric"))
                negative_fatalities = df_fatalities.filter(
                    (pl.col("fat_numeric").is_not_null()) & (pl.col("fat_numeric") < 0)
                ).height
                non_numeric_fatalities = df_fatalities.filter(pl.col("fat_numeric").is_null()).height
                
                if non_numeric_fatalities > 0:
                    checks_failed.append(f"{non_numeric_fatalities} records with non-numeric fatalities")
                if negative_fatalities > 0:
                    checks_failed.append(f"{negative_fatalities} records with negative fatalities")
                if negative_fatalities == 0 and non_numeric_fatalities == 0:
                    checks_passed.append("All fatalities are numeric and non-negative")
            except Exception as fat_error:
                checks_failed.append(f"Error validating fatalities: {str(fat_error)}")
        
        # Check 6: Event types are valid (assuming you have a known set)
        if "event_type" in df.columns:
            valid_event_types = {
                "Violence against civilians", "Battles", "Explosions/Remote violence",
                "Riots", "Protests", "Strategic developments"
            }
            actual_event_types = set(df.select("event_type").to_series().unique())
            invalid_types = actual_event_types - valid_event_types
            if invalid_types:
                checks_failed.append(f"Unknown event types: {invalid_types}")
            else:
                checks_passed.append("All event types are valid")
        
        # Check 7: No duplicate records (based on key fields)
        key_fields = ["event_date", "country", "admin1", "admin2", "latitude", "longitude", "event_type"]
        available_key_fields = [field for field in key_fields if field in df.columns]
        
        if available_key_fields:
            duplicates = df.select(available_key_fields).is_duplicated().sum()
            if duplicates > 0:
                checks_failed.append(f"{duplicates} duplicate records found")
            else:
                checks_passed.append("No duplicate records found")
        
        # Determine overall result
        passed = len(checks_failed) == 0
        
        # Create metadata
        metadata = {
            "file_size_bytes": file_size,
            "record_count": len(df) if not df.is_empty() else 0,
            "checks_passed": dg.MetadataValue.text("\n".join(checks_passed)),
            "partition_date": dg.TimestampMetadataValue(
                datetime.combine(day, datetime.min.time()).timestamp()
            ),
        }
        
        if checks_failed:
            metadata["checks_failed"] = dg.MetadataValue.text("\n".join(checks_failed))
        
        if not df.is_empty() and "event_type" in df.columns:
            # Add event type distribution
            event_type_counts = (
                df.select("event_type")
                .to_series()
                .value_counts()
                .sort("count", descending=True)
            )
            metadata["event_type_distribution"] = dg.TableMetadataValue(
                records=[dg.TableRecord(record) for record in event_type_counts.to_dicts()],
                schema=dg.TableSchema([
                    dg.TableColumn(name="event_type"),
                    dg.TableColumn(name="count", type="int"),
                ])
            )
        
        return dg.AssetCheckResult(
            passed=passed,
            description=f"Completed {len(checks_passed)} checks successfully" + 
                       (f", {len(checks_failed)} checks failed" if checks_failed else ""),
            metadata=metadata
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
    except Exception as e:
        context.log.error(f"Asset check failed with error: {str(e)}")
        return dg.AssetCheckResult(
            passed=False,
            description=f"Check failed due to error: {str(e)}",
            metadata={
                "partition_date": dg.TimestampMetadataValue(
                    datetime.combine(day, datetime.min.time()).timestamp()
                ),
                "error": dg.MetadataValue.text(str(e))
            }
        )