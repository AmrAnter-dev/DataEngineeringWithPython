import pyodbc
import requests

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=MyDatabase;"
    "UID=sa;"
    "PWD=YourPassword;"
    "TrustServerCertificate=yes;"
)

cursor = conn.cursor()

query = """
SELECT STRING_AGG(schemaName + '.' + tableName, ',')
FROM dbo.dw_table_config
WHERE status = 'ENABLED';
"""

cursor.execute(query)

row = cursor.fetchone()
table_list = row[0] if row else ""

cursor.close()
conn.close()

deb_url = "http://localhost:8083/connectors/sqlserver-connector/config"

headers = {
    "Content-Type": "application/json"
}

body = {
    "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
    "database.hostname": "...",
    "database.port": "1433",
    "database.user": "...",
    "database.password": "...",
    "database.names": "...",
    "topic.prefix": "...",
    "table.include.list": table_list
}

res = requests.put(
    deb_url,
    json=body,
    headers=headers
)

if res.status_code != 200:
    raise Exception(
        f"Request failed ({res.status_code}): {res.text}"
    )

print("Connector updated successfully.")
