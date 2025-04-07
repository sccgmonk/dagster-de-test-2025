import pandas as pd
KPI_FY_PATH="dagster_pipelines/data/KPI_FY.xlsm"
M_CENTER_PATH="dagster_pipelines/data/M_Center.csv"
# 2.1.1 Read KPI evaluation data from the "Data to DB" sheet in the "KPI_FY.xlsm" Excel file
def read_excel(path: str = KPI_FY_PATH, sheet_name: str = "Data to DB") -> pd.DataFrame:
    # clean data
    df = pd.read_excel(path, sheet_name=sheet_name)

    col=["Plan_Total","Plan_Q1","Plan_Q2","Plan_Q3","Plan_Q4",
             "Actual_Total","Actual_Q1","Actual_Q2","Actual_Q3","Actual_Q4"]
    for i in col:
        df[i] = df[i].astype(str).str.replace(r'^[a-zA-Z]+\s*$', '0', regex=True)
        df[i] = df[i].astype(float)
      
    df["Fiscal_Year"].astype(int)
    df["Center_ID"].astype(str)
    df["Kpi Number"].astype(str)
    df["Kpi_Name"].astype(str)
    df["Unit"].astype(str)
    
 

    return df
    
# 2.1.2 Read center master data from the "M_Center.csv" CSV file
def read_csv(path: str = M_CENTER_PATH) -> pd.DataFrame:
    return pd.read_csv(path)
