from dagster import DailyPartitionsDefinition, WeeklyPartitionsDefinition
from datetime import date, datetime, timedelta

weekly_partition = WeeklyPartitionsDefinition(
    start_date="2024-12-30",
    day_offset=1
    )

daily_partition = DailyPartitionsDefinition(
    start_date="2024-12-30",
)

