import dagster as dg

from dagster_acled.partitions import daily_partition

acled_azure_data = dg.AssetSelection.assets('acled_azure_daily_data')


acled_update_job = dg.define_asset_job(
    name='acled_update_job',
    partitions_def=daily_partition,
    selection=acled_azure_data,
)



