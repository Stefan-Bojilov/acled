from datetime import date, datetime

import aiohttp
import dagster as dg
from dagster_acled.partitions import daily_partition
from dagster_acled.utils import fetch_page
from dagster_acled.acled_request_config import AcledConfig
from dagster_acled.resources.resources import PostgreSQLResource
import polars as pl


@dg.asset(
    name="acled_daily_data", 
    partitions_def=daily_partition,
    description="Fetch ACLED events for the current day.",
    group_name="acled",
    io_manager_key="s3_io_manager", 
)
async def acled_request_daily(
    context: dg.AssetExecutionContext,
    config: AcledConfig,
) -> pl.DataFrame:  
    """
    Fetch ACLED events for this day's partition.
    IO manager handles S3 storage automatically.
    """
    day: date = context.partition_time_window.start.date()
    
    url = f"{config.base_url.rstrip('/')}/{config.endpoint}"
    all_rows = []
    page = 1
    
    async with aiohttp.ClientSession() as session:
        while True:
            params = config.build_params()
            params.update({
                "limit": config.max_pages,
                "page": page,
                "event_date": day.isoformat(),
                "event_date_where": "=",
            })
            
            chunk = await fetch_page(session, url, params)
            if not chunk:
                break
            
            all_rows.extend(chunk)
            
            if len(chunk) < config.max_pages:
                break
                
            page += 1
    
    df = pl.DataFrame(all_rows)
    
    if len(df) == 0:
        context.log.error(f"No data returned for day {day}.")
        raise dg.DagsterInvalidSubsetError()
    
    # verify that event_date matches the partition day
    if not df.is_empty():
        dates = df.select(pl.col("event_date").cast(pl.Date)).to_series().unique().sort()
        if len(dates) > 1 or (len(dates) == 1 and dates[0] != day):
            context.log.warning(
                f"Unexpected event_date values for partition {day}: "
                f"found dates {dates.to_list()}"
            )
    
    context.log.info(f"Fetched {len(df)} records for {day}")
    
    event_type_counts = (df.select("event_type")
                        .to_series()
                        .value_counts()
                        .sort("count", descending=True))
    
    context.add_output_metadata({
        "event_date": dg.TimestampMetadataValue(
            datetime.combine(day, datetime.min.time()).timestamp()
        ),
        "number_of_records": len(df),
        "event_type_distribution": dg.TableMetadataValue(
            records=[dg.TableRecord(record) for record in event_type_counts.to_dicts()],
            schema=dg.TableSchema(columns=[
                dg.TableColumn(name="event_type"),
                dg.TableColumn(name="count", type="int"),
            ])
        )
    })
    
    # Return DataFrame - IO manager handles S3 storage
    return df


@dg.asset(
    name="acled_daily_to_postgres",
    partitions_def=daily_partition,
    description="Load ACLED events from S3 and upsert into PostgreSQL.",
    group_name="acled",
)
async def acled_daily_to_postgres(
    context: dg.AssetExecutionContext,
    acled_daily_data: pl.DataFrame, 
    postgres: PostgreSQLResource,
) -> dg.MaterializeResult:
    """
    Insert ACLED data to Postgres.
    Input DataFrame is automatically loaded from S3 by IO manager.
    """
    partition_date = context.partition_time_window.start.date()
    
    # acled_daily_data is automatically loaded from S3 by the IO manager
    context.log.info(f"Processing {len(acled_daily_data)} records for {partition_date}")
    
    # Cast types (ensure data consistency)
    df = acled_daily_data.with_columns([
        pl.col("event_date").cast(pl.Date),
        pl.col("year").cast(pl.Int32, strict=False),
        pl.col("time_precision").cast(pl.Int32, strict=False),
        pl.col("inter1").cast(pl.Utf8),
        pl.col("inter2").cast(pl.Utf8),
        pl.col("interaction").cast(pl.Utf8),
        pl.col("geo_precision").cast(pl.Int32, strict=False),
        pl.col("fatalities").cast(pl.Int32, strict=False),
        pl.col("latitude").cast(pl.Float64, strict=False),
        pl.col("longitude").cast(pl.Float64, strict=False),
        # Handle timestamp conversion safely
        pl.when(pl.col("timestamp").is_not_null())
        .then(
            pl.col("timestamp")
            .cast(pl.Int64, strict=False)
            .map_batches(lambda arr: arr * 1_000)
            .cast(pl.Datetime(time_unit="ms"))
        )
        .otherwise(None)
        .alias("event_timestamp"),
    ])
    
    if df.is_empty():
        context.log.error(f"No data available for {partition_date}.")
        return dg.MaterializeResult(
            metadata={"error": f"No data for {partition_date}"}
        )
    
    first = df["event_date"].min()
    last = df["event_date"].max()
    missing = []
    if first != partition_date or last != partition_date:
        context.log.error(
            f"Missing or out-of-range data for {partition_date}: fetched {first}→{last}"
        )
        missing = [partition_date]

    conn = postgres.get_connection()
    table = "acled_events_no_delete"
    
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            event_id_cnty TEXT PRIMARY KEY,
            event_date DATE NOT NULL,
            year INTEGER,
            time_precision INTEGER,
            disorder_type TEXT,
            event_type TEXT,
            sub_event_type TEXT,
            actor1 TEXT,
            assoc_actor_1 TEXT,
            inter1 TEXT,
            actor2 TEXT,
            assoc_actor_2 TEXT,
            inter2 TEXT,
            interaction TEXT,
            civilian_targeting TEXT,
            iso TEXT,
            region TEXT,
            country TEXT,
            admin1 TEXT,
            admin2 TEXT,
            admin3 TEXT,
            location TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            geo_precision INTEGER,
            source TEXT,
            source_scale TEXT,
            notes TEXT,
            fatalities INTEGER,
            tags TEXT,
            event_timestamp TIMESTAMP
        );
    """
    
    insert_columns = [
        "event_id_cnty", "event_date", "year", "time_precision", "disorder_type",
        "event_type", "sub_event_type", "actor1", "assoc_actor_1", "inter1",
        "actor2", "assoc_actor_2", "inter2", "interaction", "civilian_targeting",
        "iso", "region", "country", "admin1", "admin2", "admin3", "location",
        "latitude", "longitude", "geo_precision", "source", "source_scale",
        "notes", "fatalities", "tags", "event_timestamp"
    ]
    
    placeholders = ", ".join(["%s"] * len(insert_columns))
    insert_sql = f"""
        INSERT INTO {table} ({', '.join(insert_columns)})
        VALUES ({placeholders})
        ON CONFLICT (event_id_cnty) DO UPDATE SET
        {', '.join([f"{col}=EXCLUDED.{col}" for col in insert_columns if col != "event_id_cnty"])}
    """
    
    inserted = 0
    try:
        with conn.cursor() as cur:
            cur.execute(create_sql)
            conn.commit()
            
            # Select only the columns we need and get rows
            rows = df.select(insert_columns).rows()
            cur.executemany(insert_sql, rows)
            inserted = len(rows)
            conn.commit()
            
            context.log.info(f"Successfully upserted {inserted} records to PostgreSQL")
            
    except Exception as e:
        context.log.error(f"Error inserting into PostgreSQL: {str(e)}")
        conn.rollback()
        raise
    finally:
        conn.close()

    event_counts = (
        df.select("event_type")
        .to_series()
        .value_counts()
        .sort("count", descending=True)
    )

    key_columns = ["country", "admin1", "event_type", "fatalities", "latitude", "longitude", "notes"]
    missing_data = []
    missing_totals = {}

    for col in key_columns:
        if col in df.columns:
            null_count = df.select(pl.col(col).is_null().sum()).item()
            empty_count = df.select((pl.col(col).cast(pl.Utf8) == "").sum()).item()
            total_missing = null_count + empty_count
            
            missing_data.append({
                "column": col,
                "null_count": null_count,
                "empty_count": empty_count,
                "total_missing": total_missing,
                "missing_pct": round((total_missing / len(df)) * 100, 1) if len(df) > 0 else 0
            })
            missing_totals[f"{col}_missing"] = total_missing

    total_missing_values = sum(missing_totals.values())
    total_possible_values = len(df) * len(key_columns)
    data_completeness_pct = round(((total_possible_values - total_missing_values) / total_possible_values) * 100, 1) if total_possible_values > 0 else 0

    schema = dg.TableSchema(columns=[
        dg.TableColumn(name="event_type", type="text"),
        dg.TableColumn(name="count", type="int"),
    ])

    missing_schema = dg.TableSchema(columns=[
        dg.TableColumn(name="column", type="text"),
        dg.TableColumn(name="null_count", type="int"),
        dg.TableColumn(name="empty_count", type="int"),
        dg.TableColumn(name="total_missing", type="int"),
        dg.TableColumn(name="missing_pct", type="float"),
    ])

    metadata = {
        "partition_date": dg.TimestampMetadataValue(
            datetime.combine(partition_date, datetime.min.time()).timestamp()
        ),
        "records_inserted": inserted,
        "event_type_distribution": dg.TableMetadataValue(
            records=[dg.TableRecord(r) for r in event_counts.to_dicts()],
            schema=schema
        ),
        "missing_data_breakdown": dg.TableMetadataValue(
            records=[dg.TableRecord(r) for r in missing_data],
            schema=missing_schema
        ),
        "missing_dates": dg.MetadataValue.json(
            [d.isoformat() for d in missing]
        ) if missing else dg.MetadataValue.text("No missing dates"),
        # Plotted metrics for missing data
        "total_missing_values": total_missing_values,
        "data_completeness_pct": data_completeness_pct,
    }

    metadata.update(missing_totals)

    return dg.MaterializeResult(metadata=metadata)