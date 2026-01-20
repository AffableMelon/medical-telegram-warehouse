from dagster import ScheduleDefinition
from .jobs import medical_data_pipeline

# Schedule to run every day at midnight
daily_schedule = ScheduleDefinition(
    job=medical_data_pipeline,
    cron_schedule="0 0 * * *",  # Every day at 00:00
)
