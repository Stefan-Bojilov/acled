import dagster as dg

from dagster_acled.partitions import daily_partition

acled_data = dg.AssetSelection.assets('acled_daily_to_s3', 'acled_daily_to_postgres')

acled_update_job = dg.define_asset_job(
    name='acled_update_job',
    partitions_def=daily_partition,
    selection=acled_data,
)
