import dagster as dg

weekly_partition = dg.WeeklyPartitionsDefinition(
    start_date="2025-01-03"
)