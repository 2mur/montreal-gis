from dagster import Definitions, load_assets_from_modules, define_asset_job, ScheduleDefinition
from . import assets

all_assets = load_assets_from_modules([assets])

# Define a job that materializes all assets
pipeline_job = define_asset_job(name="weekly_pipeline_job", selection="*")

# Define the weekly schedule (Monday at midnight)
weekly_schedule = ScheduleDefinition(
    job=pipeline_job,
    cron_schedule="0 0 * * 1",
)

defs = Definitions(
    assets=all_assets,
    schedules=[weekly_schedule],
)