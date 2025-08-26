import dagster as dg

from dagster_acled.partitions import daily_partition

acled_data = dg.AssetSelection.assets('acled_daily_data', 'acled_daily_to_postgres')

acled_update_job = dg.define_asset_job(
    name='acled_update_job',
    partitions_def=daily_partition,
    selection=acled_data,
)

acled_report_job = dg.define_asset_job(
    name="acled_report_job",
    selection=dg.AssetSelection.groups("acled_reports"),
    partitions_def=daily_partition 
)

