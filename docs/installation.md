# Installation

## Requirements

- Python 3.9 or newer
- ODBC Driver 18 for SQL Server
- Access to the `aquas` database for real runs

## Setup

```powershell
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
Copy-Item .env.example .env
python setup.py
```

Edit `.env` with the real connection and local paths.

## Main Environment Variables

```env
DB_SERVER=synw-aquas.sql.azuresynapse.net
DB_DATABASE=aquas
AUTH_MODE=ActiveDirectoryIntegrated
BASE_DIR=C:/path/to/AQUAS_DATA_RETRIEVAL
UP_RS_FILE=C:/path/to/AQUAS_DATA_RETRIEVAL/UPperRS.xlsx
SELECTED_RS_FILE=C:/path/to/AQUAS_DATA_RETRIEVAL/selections/selected_rs.csv
SELECTED_UP_FILE=C:/path/to/AQUAS_DATA_RETRIEVAL/selections/selected_up.csv
SELECTED_DIAGNOSIS_CODES_FILE=C:/path/to/AQUAS_DATA_RETRIEVAL/selections/selected_diagnosis_codes.csv
MAX_DIAGNOSIS_FEATURES=200000
LOG_LEVEL=INFO
```

`UP_RS_FILE` must point to the Excel file containing the `UP per RS` sheet. The pipeline uses it to map UP codes to RS values.

If `UPperRS.xlsx` is missing in a local clone, restore it with:

```powershell
git restore UPperRS.xlsx
```
