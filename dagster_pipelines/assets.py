import dagster as dg
import duckdb  
from dagster_pipelines.etl.extract import read_excel, read_csv
from dagster_pipelines.etl.transform import pivot_data
from dagster_pipelines.etl.load import load_to_duckdb

@dg.asset(compute_kind="duckdb", group_name="plan1")
def kpi_fy(context: dg.AssetExecutionContext):
    context.log.info("Reading KPI Excel file...")
    df_raw = read_excel(sheet_name="Data to DB")
    context.log.info("Pivoting KPI data...")
    df= pivot_data(df_raw)
    context.log.info("Loading pivoted KPI data into DuckDB table: KPI_FY")
    load_to_duckdb(df, "KPI_FY")
    return df
   

# 2.3.1.2 Load M_Center.csv into M_Center
@dg.asset(compute_kind="duckdb", group_name="plan1")
def m_center(context: dg.AssetExecutionContext):
    context.log.info("Reading M_Center CSV...")
    df = read_csv()
    context.log.info("Loading M_Center data into DuckDB table: M_Center")
    load_to_duckdb(df, "M_Center")
    return df

# 2.3.2 Create asset kpi_fy_final_asset()
@dg.asset(
           compute_kind="duckdb", 
           group_name="plan1",
           deps=[kpi_fy,m_center],
           )



def kpi_fy_final_asset(context: dg.AssetExecutionContext):
    context.log.info("Joining KPI_FY and M_Center tables...")
    con = duckdb.connect("/opt/dagster/app/dagster_pipelines/db/plan.db")
    df_joined = con.execute("""
        SELECT
            k.*,
            m.Center_Name,
            CURRENT_TIMESTAMP AS updated_at
        FROM plan.plan.KPI_FY k
        LEFT JOIN plan.plan.M_Center m
        ON k.Center_ID = m.Center_ID
    """).df()
    con.close()
    # conn = duckdb.connect("dagster_pipelines/db/plan.db", read_only=True)
    # conn.execute("CREATE SCHEMA IF NOT EXISTS plan;")
    context.log.info("Loading final joined data into DuckDB table: KPI_FY_Final")
    load_to_duckdb(df_joined, "KPI_FY_Final")
    return df_joined


    # df = conn.execute("""
    #     SELECT k.*, m.Center_Name, CURRENT_TIMESTAMP AS updated_at
    #     FROM KPI_FY k
    #     LEFT JOIN M_Center m
    #     ON k.Center_ID = m.Center_ID
    # """).fetchdf()

    # load_to_duckdb(df, "KPI_FY_Final")

   