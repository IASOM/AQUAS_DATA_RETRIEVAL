from pathlib import Path 
import pandas as pd

# ========================
# CONFIG
# ========================

DB_SERVER = "synw-aquas.sql.azuresynapse.net"
DB_DATABASE = "aquas"

SCHEMA = "z_inv"
TABLE_NAME = "P1038_visites"
DATE_COLUMN = "DATA_VISITA"

BASE_DIR = Path("C:/Users/ghernandezgu/Desktop/PREDAP/demand_pipeline")

STATE_FILE = BASE_DIR / "state" / "state.json"

OUTPUT_CAT_FILE = BASE_DIR / "incremental" / "demanda_CAT_incremental.csv"
OUTPUT_RS_FILE = BASE_DIR / "incremental" / "demanda_RS_incremental.csv"
OUTPUT_UP_FILE = BASE_DIR / "incremental" / "demanda_UP_incremental.csv"

FINAL_CAT_FILE = BASE_DIR / "finals" / "demanda_CAT.csv"
FINAL_RS_FILE = BASE_DIR / "finals" / "demanda_RS.csv"
FINAL_UP_FILE = BASE_DIR / "finals" / "demanda_UP.csv"

UP_RS_FILE = Path("C:/Users/ghernandezgu/Desktop/AQUAS_INTEGRATION/UPperRS.xlsx")
UP_RS_SHEET = "UP per RS"

AUTH_MODE = "ActiveDirectoryIntegrated"
MIN_VALID_DATE = "2008-01-01"      #"2008-01-01"
FINAL_START_DATE =  "2008-01-01"   # "2008-01-01"   # només en el document finals
FINAL_END_DATE = pd.Timestamp.today().normalize() - pd.Timedelta(days=1)






