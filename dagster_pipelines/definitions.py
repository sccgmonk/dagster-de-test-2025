from dagster import Definitions, load_assets_from_modules,load_asset_checks_from_modules
from dagster_pipelines import assets
from dagster_pipelines.schedules import kpi_fy_monthly_job_schedule

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    asset_checks=load_asset_checks_from_modules([assets]), 
    schedules=[kpi_fy_monthly_job_schedule],
)