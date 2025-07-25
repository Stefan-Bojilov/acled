from dagster import DailyPartitionsDefinition, WeeklyPartitionsDefinition

weekly_partition = WeeklyPartitionsDefinition(
    start_date="2024-12-30",
    day_offset=1
    )

daily_partition = DailyPartitionsDefinition(
    start_date='2024-12-30'
)

