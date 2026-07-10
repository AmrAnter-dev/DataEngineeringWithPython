
sources = {
    
    

    "database_config": {

        "sqlserver": {

            "server": "192.168.1.7",

            "port": 1433,

            "database": "AdventureWorks2008R2",

            "username_env":"SRVADMIN",

            "password_env":"MSSQL_PWD",

            "driver": "ODBC Driver 18 for SQL Server",

            "authentication": "SqlPassword",

            "encrypt": "yes",

            "trust_server_certificate": "yes",

            "jdbc_driver": 
                
                "com.microsoft.sqlserver.jdbc.SQLServerDriver",

            "odbc_driver":
                
                "ODBC Driver 18 for SQL Server",
                
            "jdbc_url_template": (
                
                "jdbc:sqlserver://{server}:{port};databaseName={database}"
            )

        },
        "postgres": {

        "server": "127.0.0.1",
        
        "port": 5432,
        
        "database": "analytics",
        
        "schema": "gold",
        
        "username_env": "POSTGRES_USER",
        
        "password_env": "POSTGRES_PASSWORD",

        "jdbc_driver": "org.postgresql.Driver",

        "jdbc_url_template": (
            
            "jdbc:postgresql://{server}:{port}/{database}"
        )
    }

    },

    "spark_config": {

        "spark.sql.warehouse.dir": "/home/amr/spark-warehouse",

        "spark.sql.parquet.compression.codec": "uncompressed",

        "spark.sql.shuffle.partitions": "2",
        
        "spark.sql.adaptive.enabled": "true",
        
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        
        "spark.sql.files.maxPartitionBytes": "134217728",
        
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        
        "spark.sql.extensions":"io.delta.sql.DeltaSparkSessionExtension",
        
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        
        "spark.databricks.delta.properties.defaults.engineering.team_name": "eng_Amr_Anter",
        
        "spark.jars.packages":"io.delta:delta-spark_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.10,com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11",
        
        "spark.sql.streaming.kafka.consumer.cache.capacity":"64",
        
        "spark.sql.streaming.kafka.consumer.cache.timeout":"10m",
        
        "spark.sql.streaming.forceDeleteTempCheckpointLocation":"true"                
        

    }
}

