import dagster as dg

# 2.4.1 Set the cron_schedule for the job kpi_fy_monthly_job
kpi_fy_monthly_job_schedule = dg.ScheduleDefinition(
    name="kpi_fy_monthly_job",
    target=dg.define_asset_job(name="kpi_fy_monthly_job", selection=dg.AssetSelection.groups("plan")),
    cron_schedule=["* * 3,21 * *"],
    execution_timezone="Asia/Bangkok",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)