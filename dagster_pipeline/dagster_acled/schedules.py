import dagster as dg 

from dagster_acled.jobs import acled_report_job

daily_schedule = dg.ScheduleDefinition(
    name='daily_report', 
    cron_schedule="0 0 * * *",
    target=acled_report_job
)