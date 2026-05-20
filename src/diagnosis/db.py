import pyodbc

def get_connection(
    db_server: str,
    db_database: str,
    auth_mode: str = "ActiveDirectoryIntegrated",
    timeout: int = 60,
) -> pyodbc.Connection:
    connection_string = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server={db_server};"
        "Port=1433;"
        f"Database={db_database};"
        f"Authentication={auth_mode};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )
    return pyodbc.connect(connection_string, timeout = timeout)

    