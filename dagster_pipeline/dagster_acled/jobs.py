import dagster as dg

from dagster_acled.partitions import daily_partition

acled_data = dg.AssetSelection.assets('acled_daily_data', 'acled_daily_to_postgres')
acled_db = dg.AssetSelection.assets('acled_daily_to_postgres')

acled_update_job = dg.define_asset_job(
    name='acled_update_job',
    partitions_def=daily_partition,
    selection=acled_data,
)

acled_db_update_job = dg.define_asset_job(
    name='acled_db_update_job', 
    partitions_def=daily_partition, 
    selection=acled_db
)

acled_report_job = dg.define_asset_job(
    name="acled_report_job",
    selection=dg.AssetSelection.groups("machine_learning", "acled_reports"),
    partitions_def=daily_partition 
)

