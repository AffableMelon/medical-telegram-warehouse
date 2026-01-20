from dagster import Definitions, load_assets_from_modules

from .jobs import medical_data_pipeline
from .schedules import daily_schedule

defs = Definitions(
    jobs=[medical_data_pipeline],
    schedules=[daily_schedule],
)
