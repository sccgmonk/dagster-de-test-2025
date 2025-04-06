import pandas as pd

def pivot_data(df_KPI_data) -> pd.DataFrame:
    # KPI_PATH=os.path.join(os.getcwd(), "dagster_pipelines/data/KPI_FY.xlsm")
    # df_KPI_data=pd.read_excel(KPI_PATH,sheet_name="Data to DB" ,engine="openpyxl")
    # # plan columns
    
    plan_col=df_KPI_data.melt(
        id_vars=['Fiscal_Year','Center_ID','Kpi Number','Kpi_Name','Unit','Plan_Total', 'Actual_Total'],
        value_vars=['Plan_Q1', 'Plan_Q2', 'Plan_Q3', 'Plan_Q4'],
        var_name='Amount_Name',
        value_name='Amount',

    )
    plan_col['Amount_Type']='Plan'
    #   actual columns
    actual_col=df_KPI_data.melt(
        id_vars=['Fiscal_Year','Center_ID','Kpi Number','Kpi_Name','Unit','Plan_Total', 'Actual_Total'],
        value_vars=['Actual_Q1', 'Actual_Q2', 'Actual_Q3', 'Actual_Q4'],
        var_name='Amount_Name',
        value_name='Amount',

    )
    actual_col['Amount_Type']='Actual'

    pivoted_df = pd.concat([plan_col, actual_col], ignore_index=True)
    pivoted_df=pivoted_df[['Fiscal_Year','Center_ID','Kpi Number','Kpi_Name','Unit','Plan_Total', 'Actual_Total','Amount_Type','Amount_Name','Amount']]

  





    return pivoted_df