from utils.db_utils import get_sql_server_connection
from utils.deb_connect import update_deb_connector


conn=get_sql_server_connection()
try:
    cursor = conn.cursor()

    query = """
    SELECT STRING_AGG(schemaName + '.' + tableName, ',')
    FROM dbo.dw_table_config
    WHERE status = 'ENABLED'
    AND schemaName='person';
    """

    cursor.execute(query)

    row = cursor.fetchone()
    table_list = row[0] if row else ""
    print(table_list)
except Exception as e:
    print(e)
finally:
    cursor.close()
    conn.close()
    

res = update_deb_connector(table_list)

if res.status_code not in [200,201]:
    raise Exception(
        f"Request failed ({res.status_code}): {res.text}"
    )

print("Connector updated successfully.")
