import os
import requests



def update_deb_connector(table_list):
    username = os.getenv("SRVADMIN")
    password = os.getenv("MSSQL_PWD")
    deb_url = "http://localhost:8083/connectors"
    headers = {
        "Content-Type": "application/json"
    }
    body ={
        
        "name": "sqlserver-connector",
        "config": {
             
            "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
            "tasks.max": "1",
            "database.hostname": "192.168.1.7",
            "database.port": "1433",
            "database.user":username,
            "database.password":password,
            "database.names": "AdventureWorks2008R2",
            "database.encrypt": "true",
            "database.trustServerCertificate": "true",
            "topic.prefix": "dw",
            "table.include.list": table_list,
            "snapshot.mode": "initial",
            "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
            "schema.history.internal.kafka.topic": "schemahistory.datawarehouse",
            "key.converter":"org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable":"false",
            "value.converter":"org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable":"false"
        }
    }
    return requests.post(
        deb_url,
        json=body,
        headers=headers,
        timeout=300
    )
