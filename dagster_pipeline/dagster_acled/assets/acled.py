from datetime import date, datetime, timedelta
import io
import os

import aiohttp
import dagster as dg
from dagster_acled.acled_client import AcledConfig
from dagster_acled.partitions import daily_partition, weekly_partition
from dagster_acled.resources import PostgreSQLResource
from dagster_acled.utils import fetch_page
from dagster_aws.s3 import S3Resource
import polars as pl
import psycopg


@dg.asset(
    name="acled_weekly_request",
    partitions_def=weekly_partition, 
    description="Fetch ACLED events for the current week and store them in S3.", 
    group_name="acled",
)
async def acled_request(
    context: dg.AssetExecutionContext,
    config: AcledConfig,
    s3: S3Resource,
) -> dg.MaterializeResult:
    """
    Fetch ACLED events for this week’s partition,
    verify full coverage of event_date, and log an error if data is missing.
    """
    # derive week bounds from the partition time window
    week_start: date = context.partition_time_window.start.date()
    week_end: date = context.partition_time_window.end.date()

    url = f"{config.base_url.rstrip('/')}/{config.endpoint}"
    all_rows = []
    page = 1

    async with aiohttp.ClientSession() as session:
        # TODO find a better way to parametrize the request
        day = week_start
        while day <= week_end:
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
            day += timedelta(days=1)

    df = pl.DataFrame(all_rows)
    if len(df) == 0:
        context.log.error(f"No data returned for week {week_start}–{week_end}.")
        # TODO make clearer that this is an error from an empty partition
        raise dg.DagsterInvalidSubsetError()
    else: 
        # verify that event_date covers entire week
        if not df.is_empty():
            dates = df.select(pl.col("event_date").cast(pl.Date)).to_series().sort()
            first, last = dates[0], dates[-1]
            if first > week_start or last < week_end:
                context.log.error(
                    f"Missing data for partition [{week_start} to {week_end}]: "
                    f"fetched event_date from {first} to {last}."
                )
        else:
            context.log.error(f"No data returned for week {week_start}–{week_end}.")

        buf = io.BytesIO()
        df.write_parquet(buf)
        buf.seek(0)

        # Upload to S3
        bucket = os.environ['S3_BUCKET']
        key = (
            f"acled/weekly_partitions/"
            f"acled_{week_start}_{week_end}.parquet"
        )
        client = s3.get_client()
        client.put_object(Bucket=bucket, Key=key, Body=buf.read())
        context.log.info(f"Uploaded to s3://{bucket}/{key}")

        # Materialize result with metadata
        first_ts = datetime.combine(first, datetime.min.time()).timestamp()
        last_ts = datetime.combine(last, datetime.min.time()).timestamp()

        event_type_df = (df.select("event_type")
                         .to_series()
                         .value_counts()
                         .sort("count", descending=True))

        schema = dg.TableSchema(
            columns=[
                dg.TableColumn(name="event_type"),
                dg.TableColumn(name="count", type="int"),
            ]
        )

        return dg.MaterializeResult(
            metadata={
                "start_event_date": dg.TimestampMetadataValue(first_ts),
                "end_event_date": dg.TimestampMetadataValue(last_ts),
                "number_of_records": len(df), 
                "event_type_distribution": dg.TableMetadataValue(records=[dg.TableRecord(record) for record in event_type_df.to_dicts()], schema=schema), 
                "preview": dg.MetadataValue.table(records=[dg.TableRecord(record) for record in df.head(5).to_dicts()])
            }
        )
    

@dg.asset(
    name="acled_daily_request",
    partitions_def=daily_partition,
    description="Fetch ACLED events for the current day and store them in S3.",
    group_name="acled",
)
async def acled_request_daily(
    context: dg.AssetExecutionContext,
    config: AcledConfig,
    s3: S3Resource,
) -> dg.MaterializeResult:
    """
    Fetch ACLED events for this day's partition,
    verify coverage of event_date, and log an error if data is missing.
    """
    # derive day from the partition time window
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
        # TODO make clearer that this is an error from an empty partition
        raise dg.DagsterInvalidSubsetError()
    
    # verify that event_date matches the partition day
    if not df.is_empty():
        dates = df.select(pl.col("event_date").cast(pl.Date)).to_series().unique().sort()
        if len(dates) > 1 or (len(dates) == 1 and dates[0] != day):
            context.log.warning(
                f"Unexpected event_date values for partition {day}: "
                f"found dates {dates.to_list()}"
            )
    
    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)
    
    # Upload to S3
    bucket = os.environ['S3_BUCKET']
    key = f"acled/daily_partitions/acled_{day}.parquet"
    
    client = s3.get_client()
    client.put_object(Bucket=bucket, Key=key, Body=buf.read())
    
    context.log.info(f"Uploaded to s3://{bucket}/{key}")
    
    # Materialize result with metadata
    day_ts = datetime.combine(day, datetime.min.time()).timestamp()
    
    event_type_df = (df.select("event_type")
                      .to_series()
                      .value_counts()
                      .sort("count", descending=True))
    
    schema = dg.TableSchema(
        columns=[
            dg.TableColumn(name="event_type"),
            dg.TableColumn(name="count", type="int"),
        ]
    )
    
    return dg.MaterializeResult(
        metadata={
            "event_date": dg.TimestampMetadataValue(day_ts),
            "number_of_records": len(df),
            "event_type_distribution": dg.TableMetadataValue(
                records=[dg.TableRecord(record) for record in event_type_df.to_dicts()], 
                schema=schema
            ),
            "preview": dg.MetadataValue.table(
                records=[dg.TableRecord(record) for record in df.head(5).to_dicts()]
            )
        }
    )


    
@dg.asset(
    name="ACLED_weekly_to_PosgreSQL",
    partitions_def=weekly_partition, 
    description="Fetch ACLED events for the current week and store them in PostgreSQL.", 
    group_name="acled",
)
async def acled_request_to_postgres(
    context: dg.AssetExecutionContext,
    config: AcledConfig,
    postgres: PostgreSQLResource,
) -> dg.MaterializeResult:

    week_start: date = context.partition_time_window.start.date()
    week_end: date = context.partition_time_window.end.date()

    url = f"{config.base_url.rstrip('/')}/{config.endpoint}"
    all_rows = []

    async with aiohttp.ClientSession() as session:
        day = week_start
        while day <= week_end:
            page = 1
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
            day += timedelta(days=1)

    # Create DataFrame with proper schema based on actual data
    df = pl.DataFrame(all_rows).with_columns(
        pl.col("event_date").str.to_date(),
        pl.col("year").cast(pl.Int32),
        pl.col("time_precision").cast(pl.Int32),
        pl.col("inter1").cast(pl.Utf8), 
        pl.col("inter2").cast(pl.Utf8), 
        pl.col("interaction").cast(pl.Utf8), 
        pl.col("geo_precision").cast(pl.Int32),
        pl.col("fatalities").cast(pl.Int32),
        pl.col("latitude").cast(pl.Float64),
        pl.col("longitude").cast(pl.Float64),
        pl.col("timestamp").cast(pl.Int64)
                            .map_batches(lambda arr: arr * 1_000) 
                            .cast(pl.Datetime(time_unit="ms"))
                            .alias("event_timestamp")
                                )
    if df.is_empty():
        context.log.error(f"No data returned for week {week_start}–{week_end}.")
        return dg.MaterializeResult(
            metadata={"error": f"No data for {week_start} to {week_end}"}
        )

    dates = df["event_date"]
    first, last = dates.min(), dates.max()
    if first > week_start or last < week_end:
        context.log.error(
            f"Missing data for partition [{week_start} to {week_end}]: "
            f"Fetched event_date from {first} to {last}."
        )
    # Verify that all dates in the week are present
    all_dates = [week_start + timedelta(days=i) for i in range((week_end - week_start).days + 1)]
    present_dates = set(dates)
    missing_dates = [d for d in all_dates if d not in present_dates]
    if missing_dates:
        context.log.error(f"Missing days in week {week_start}–{week_end}: {missing_dates}")

    conn = postgres.get_connection()
    table_name = "acled_events_no_delete"
    
    try:
        with conn.cursor() as cur:
            try:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        event_id_cnty TEXT PRIMARY KEY,
                        event_date DATE NOT NULL,
                        year INTEGER,
                        time_precision INTEGER,
                        disorder_type TEXT,
                        event_type TEXT,
                        sub_event_type TEXT,
                        actor1 TEXT,
                        assoc_actor_1 TEXT,
                        inter1 TEXT,  -- Changed to TEXT
                        actor2 TEXT,
                        assoc_actor_2 TEXT,
                        inter2 TEXT,  -- Changed to TEXT
                        interaction TEXT,  -- Changed to TEXT
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
                """)
                conn.commit()
                context.log.info(f"Created/verified table {table_name}")
            except psycopg.Error as e:
                conn.rollback()
                context.log.error(f"Table creation error: {e}")
                raise   

            # Prepare batch insertion with corrected schema
            columns = [
                "event_id_cnty", "event_date", "year", "time_precision", "disorder_type",
                "event_type", "sub_event_type", "actor1", "assoc_actor_1", "inter1",
                "actor2", "assoc_actor_2", "inter2", "interaction", "civilian_targeting",
                "iso", "region", "country", "admin1", "admin2", "admin3", "location",
                "latitude", "longitude", "geo_precision", "source", "source_scale",
                "notes", "fatalities", "tags", "event_timestamp"
            ]
            
            # Convert to list of tuples for executemany
            records = df.select(columns).rows()
            
            # Insert records in batches
            insert_sql = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({', '.join(['%s'] * len(columns))})
                ON CONFLICT (event_id_cnty) DO UPDATE
                SET event_date = EXCLUDED.event_date,
                    year = EXCLUDED.year,
                    time_precision = EXCLUDED.time_precision,
                    disorder_type = EXCLUDED.disorder_type,
                    event_type = EXCLUDED.event_type,
                    sub_event_type = EXCLUDED.sub_event_type,
                    actor1 = EXCLUDED.actor1,
                    assoc_actor_1 = EXCLUDED.assoc_actor_1,
                    inter1 = EXCLUDED.inter1,
                    actor2 = EXCLUDED.actor2,
                    assoc_actor_2 = EXCLUDED.assoc_actor_2,
                    inter2 = EXCLUDED.inter2,
                    interaction = EXCLUDED.interaction,
                    civilian_targeting = EXCLUDED.civilian_targeting,
                    iso = EXCLUDED.iso,
                    region = EXCLUDED.region,
                    country = EXCLUDED.country,
                    admin1 = EXCLUDED.admin1,
                    admin2 = EXCLUDED.admin2,
                    admin3 = EXCLUDED.admin3,
                    location = EXCLUDED.location,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    geo_precision = EXCLUDED.geo_precision,
                    source = EXCLUDED.source,
                    source_scale = EXCLUDED.source_scale,
                    notes = EXCLUDED.notes,
                    fatalities = EXCLUDED.fatalities,
                    tags = EXCLUDED.tags,
                    event_timestamp = EXCLUDED.event_timestamp
            """
            
            batch_size = 1000
            inserted_count = 0
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                cur.executemany(insert_sql, batch)
                inserted_count += len(batch)
                conn.commit()
                context.log.info(f"Inserted batch of {len(batch)} records")
            
            context.log.info(f"Total {inserted_count} records inserted for {week_start} to {week_end}")
        
    except Exception as e:
        conn.rollback()
        context.log.error(f"Database operation failed: {str(e)}")
        raise
    finally:
        conn.close()

    # Prepare metadata
    event_type_df = df.select("event_type").to_series().value_counts().sort("count", descending=True)
    schema = dg.TableSchema(
        columns=[
            dg.TableColumn(name="event_type", type="text"),
            dg.TableColumn(name="count", type="int"),
        ]
    )

    #TODO: Insert days with missing data in metadata
    return dg.MaterializeResult(
        metadata={
            "start_event_date": dg.TimestampMetadataValue(datetime.combine(first, datetime.min.time()).timestamp()),
            "end_event_date": dg.TimestampMetadataValue(datetime.combine(last, datetime.max.time()).timestamp()),
            "records_inserted": inserted_count,
            "event_type_distribution": dg.TableMetadataValue(
                records=[dg.TableRecord(r) for r in event_type_df.to_dicts()],
                schema=schema
            ),
            "missing_dates":dg.MetadataValue.json([d.isoformat() for d in missing_dates]) if missing_dates else dg.MetadataValue.text("No missing dates"),
        }
    )


@dg.asset(
    name="ACLED_daily_to_PostgreSQL",
    partitions_def=daily_partition,
    description="Fetch ACLED events for a single day and upsert into PostgreSQL.",
    group_name="acled",
)
async def acled_daily_to_postgres(
    context: dg.AssetExecutionContext,
    config: AcledConfig,
    postgres: PostgreSQLResource,
) -> dg.MaterializeResult:
    # Determine the partition date
    partition_date = context.partition_time_window.start.date()

    # Build URL and fetch all pages for this date
    url = f"{config.base_url.rstrip('/')}/{config.endpoint}"
    all_rows = []
    async with aiohttp.ClientSession() as session:
        page = 1
        while True:
            params = config.build_params()
            params.update({
                "limit": config.max_pages,
                "page": page,
                "event_date": partition_date.isoformat(),
                "event_date_where": "=",
            })
            chunk = await fetch_page(session, url, params)
            if not chunk:
                break
            all_rows.extend(chunk)
            if len(chunk) < config.max_pages:
                break
            page += 1

    # Build DataFrame and cast types
    df = (
        pl.DataFrame(all_rows)
        .with_columns(
            pl.col("event_date").str.to_date(),
            pl.col("year").cast(pl.Int32),
            pl.col("time_precision").cast(pl.Int32),
            pl.col("inter1").cast(pl.Utf8),
            pl.col("inter2").cast(pl.Utf8),
            pl.col("interaction").cast(pl.Utf8),
            pl.col("geo_precision").cast(pl.Int32),
            pl.col("fatalities").cast(pl.Int32),
            pl.col("latitude").cast(pl.Float64),
            pl.col("longitude").cast(pl.Float64),
            pl.col("timestamp")
              .cast(pl.Int64)
              .map_batches(lambda arr: arr * 1_000)
              .cast(pl.Datetime(time_unit="ms"))
              .alias("event_timestamp"),
        )
    )

    # Handle empty or missing data
    if df.is_empty():
        context.log.error(f"No data returned for {partition_date}.")
        return dg.MaterializeResult(metadata={"error": f"No data for {partition_date}"})

    # Compute first/last and detect missing
    first = df["event_date"].min()
    last = df["event_date"].max()
    missing = []
    if first != partition_date or last != partition_date:
        context.log.error(f"Missing or out-of-range data for {partition_date}: fetched {first}→{last}")
        missing = [partition_date]

    # Upsert into PostgreSQL
    conn = postgres.get_connection()
    table = "acled_events_no_delete"
    # Ensure table exists
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
            rows = df.select(insert_columns).rows()
            cur.executemany(insert_sql, rows)
            inserted = len(rows)
            conn.commit()
    finally:
        conn.close()

    # Prepare metadata
    event_counts = df.select("event_type").to_series().value_counts().sort("count", descending=True)
    schema = dg.TableSchema(columns=[
        dg.TableColumn(name="event_type", type="text"),
        dg.TableColumn(name="count", type="int"),
    ])

    return dg.MaterializeResult(
        metadata={
            "partition_date": dg.TimestampMetadataValue(
                datetime.combine(partition_date, datetime.min.time()).timestamp()
            ),
            "records_inserted": inserted,
            "event_type_distribution": dg.TableMetadataValue(
                records=[dg.TableRecord(r) for r in event_counts.to_dicts()],
                schema=schema
            ),
            "missing_dates": dg.MetadataValue.json(
                [d.isoformat() for d in missing]
            ) if missing else dg.MetadataValue.text("No missing dates"),
        }
    )


@dg.asset(
    name="ACLED_daily_to_PostgreSQL_from_s3",
    partitions_def=daily_partition,
    description="Fetch ACLED events for a single day and upsert into PostgreSQL.",
    group_name="acled",
    deps=[acled_request_daily],  # Add this line to create the dependency
)
async def acled_daily_to_postgres_from_s3(
    context: dg.AssetExecutionContext,
    config: AcledConfig,
    postgres: PostgreSQLResource,
    s3: S3Resource,  # Add S3 resource to read the data
) -> dg.MaterializeResult:
    # Determine the partition date
    partition_date = context.partition_time_window.start.date()
    
    # Instead of fetching from API, read from S3 where acled_request_daily stored it
    bucket = os.environ['S3_BUCKET']
    key = f"acled/daily_partitions/acled_{partition_date}.parquet"
    
    try:
        # Read the data from S3
        client = s3.get_client()
        response = client.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read()
        
        # Read the parquet file
        buf = io.BytesIO(file_content)
        df = pl.read_parquet(buf)
        
        context.log.info(f"Read {len(df)} records from s3://{bucket}/{key}")
        
    except client.exceptions.NoSuchKey:
        context.log.error(f"File not found: s3://{bucket}/{key}")
        return dg.MaterializeResult(
            metadata={"error": f"No data file found for {partition_date}"}
        )
    except Exception as e:
        context.log.error(f"Error reading from S3: {str(e)}")
        return dg.MaterializeResult(
            metadata={"error": f"Failed to read data: {str(e)}"}
        )
    
    # Cast types (the data from S3 should already have correct types, but ensure consistency)
    df = df.with_columns([
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
    
    # Handle empty or missing data
    if df.is_empty():
        context.log.error(f"No data available for {partition_date}.")
        return dg.MaterializeResult(
            metadata={"error": f"No data for {partition_date}"}
        )
    
    # Compute first/last and detect missing
    first = df["event_date"].min()
    last = df["event_date"].max()
    missing = []
    if first != partition_date or last != partition_date:
        context.log.error(
            f"Missing or out-of-range data for {partition_date}: fetched {first}→{last}"
        )
        missing = [partition_date]

    # Upsert into PostgreSQL
    conn = postgres.get_connection()
    table = "acled_events_no_delete"
    
    # Ensure table exists
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

    # Prepare metadata
    event_counts = (
        df.select("event_type")
        .to_series()
        .value_counts()
        .sort("count", descending=True)
    )
    
    schema = dg.TableSchema(columns=[
        dg.TableColumn(name="event_type", type="text"),
        dg.TableColumn(name="count", type="int"),
    ])
    
    return dg.MaterializeResult(
        metadata={
            "partition_date": dg.TimestampMetadataValue(
                datetime.combine(partition_date, datetime.min.time()).timestamp()
            ),
            "records_inserted": inserted,
            "source_file": f"s3://{bucket}/{key}",
            "event_type_distribution": dg.TableMetadataValue(
                records=[dg.TableRecord(r) for r in event_counts.to_dicts()],
                schema=schema
            ),
            "missing_dates": dg.MetadataValue.json(
                [d.isoformat() for d in missing]
            ) if missing else dg.MetadataValue.text("No missing dates"),
        }
    )