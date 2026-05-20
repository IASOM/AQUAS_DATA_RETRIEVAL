from pathlib import Path 
import pandas as pd

# ========================
# CONFIG
# ========================

DB_SERVER = "synw-aquas.sql.azuresynapse.net"
DB_DATABASE = "aquas"

SCHEMA = "z_inv"
TABLE_NAME = "P1038_prstb015r_filtrat"

DATE_COLUMN = "data_visita"
UP_COLUMN = "up_c"
DIAG_CODE_COLUMN = "problema_salut_c"

BASE_DIR = Path("C:/Users/ghernandezgu/Desktop/PREDAP/diagnosis_pipeline")

STATE_FILE = BASE_DIR / "state" / "state_diagnosis.json"
REDUCED_OUTPUT_FILE = BASE_DIR / "incremental" / "daily_up_icd10_3_counts.parquet"

SELECTED_CODES_FILE = BASE_DIR / "selected_codes" / "selected_codes.csv"

FINAL_OUTPUT_DIR = BASE_DIR / "finals"
INCREMENTAL_OUTPUT_DIR = BASE_DIR / "incremental"

UP_RS_FILE = Path("C:/Users/ghernandezgu/Desktop/PREDAP/UPperRS.xlsx")
UP_RS_SHEET = "UP per RS"

AUTH_MODE = "ActiveDirectoryIntegrated"
MIN_VALID_DATE = "2008-01-01"      #"2008-01-01"
FINAL_START_DATE =  "2008-01-01"   # "2008-01-05"   # només en el document finals
FINAL_END_DATE = pd.Timestamp.today().normalize() - pd.Timedelta(days=1)

