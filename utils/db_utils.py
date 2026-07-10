import os
import pyodbc
from config.configs import sources

def get_sql_server_connection():
    
    sql_config = sources["database_config"]["sqlserver"]

    username = os.getenv(sql_config["username_env"])
    password = os.getenv(sql_config["password_env"])
    if not username or not password:
        raise ValueError("SQL Server credentials are missing")
    
    conn_str = (
        f"DRIVER={sql_config['driver']};"
        f"SERVER={sql_config['server']},{sql_config['port']};"
        f"DATABASE={sql_config['database']};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )

    return pyodbc.connect(conn_str)
