from src.diagnosis.config import (
    DB_SERVER,
    DB_DATABASE,
    SCHEMA,
    TABLE_NAME
)
from config.config import *
from src.diagnosis.db import get_connection
import pandas as pd


conn = get_connection(
    db_server = DB_SERVER,
    db_database = DB_DATABASE,
)

query = """
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = ?
    AND TABLE_NAME = ?
ORDER BY ORDINAL_POSITION
"""

df = pd.read_sql_query(query, conn, params = [SCHEMA,TABLE_NAME])
print(df.to_string(index=False))