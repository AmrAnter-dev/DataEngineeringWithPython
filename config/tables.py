from utils.db_utils import get_sql_server_connection
import json


conn=get_sql_server_connection()
cur=conn.cursor()
try:
    query="""
    SELECT 
    schemaName,
    tableName,
    enable_CDC,
    status,
    updated_at,
    mergeColumn
    FROM dbo.dw_table_config
    ORDER BY schemaName, tableName
    """
    cur.execute(query)
  
    rows={
            f"dw.AdventureWorks2008R2.{row.schemaName}.{row.tableName}": {
                "enable_CDC": row.enable_CDC,
                "status": row.status,
                "updated_at": str(row.updated_at) if row.updated_at else None,
                "mergeColumn": [
                                    col.strip()
                                    for col in row.mergeColumn.split(",")
                                ]
            }
            for row in cur.fetchall()
        }
    print(rows)
    with open(
        '/home/amr/ven310/db_connect/tables.json',
        'w',
        encoding='utf-8'
        ) as f:
        
        json.dump(rows,
                    f,
                    indent=4,
                    ensure_ascii=False,
                    default=str)
finally:
    cur.close()
    conn.close()
