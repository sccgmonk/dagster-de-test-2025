import pandas as pd
KPI_FY_PATH="dagster_pipelines/data/KPI_FY.xlsm"
M_CENTER_PATH="dagster_pipelines/data/M_Center.csv"
# 2.1.1 Read KPI evaluation data from the "Data to DB" sheet in the "KPI_FY.xlsm" Excel file
def read_excel(path: str = KPI_FY_PATH, sheet_name: str = "Data to DB") -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=sheet_name)
    
# 2.1.2 Read center master data from the "M_Center.csv" CSV file
def read_csv(path: str = M_CENTER_PATH) -> pd.DataFrame:
    return pd.read_csv(path)
