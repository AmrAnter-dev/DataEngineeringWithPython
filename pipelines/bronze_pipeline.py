from utils.spark_utils import SparkManager
from pyspark.sql.functions import col,to_date,current_timestamp,hour,current_date
from pyspark.sql.types import *
from utils.merge_utils import write_each_topic

try:
    spark=SparkManager.create_session('test_streaming')
    
    print('session built')
    
    kafka_opt={
    'kafka.bootstrap.servers':'localhost:9092',
    'subscribePattern':'dw.*',
    'startingOffsets':'earliest',
    'failOnDataLoss':'false',
    'kafka.consumer.config.default.api.timeout.ms':'300000',
    'kafka.consumer.config.request.timeout.ms':'300000',
    'kafka.metadata.max.age.ms':'300000'
    }

    target_path= "/home/amr/ven310/db_connect/bronze_db"   
    checkpoint_location="/home/amr/ven310/db_connect/checkpoints/bronze"

    streamDf = (
        spark.readStream
        .format('kafka')
        .options(**kafka_opt)
        
        .load()
    )
  
    parsedDF=(
    streamDf
        .select(
        col('key'),
        col('value'),
        col('topic'),
        col('partition'),
        col('offset'),
        col('timestamp'),
        col('timestampType')
        
            )
    
        .withColumn(
            'event_date',
            to_date(col('timestamp'))
        )
        .withColumn(
            "ingestion_ts"
            , current_timestamp()
        )
        .withColumn(
            "event_hour", 
            hour("timestamp")
            )
        .withColumn(
            "processing_date",
            current_date()
            )
        .select(
        "key",
        "value",
        "topic",
        "partition",
        "offset",
        "timestamp",
        "timestampType",
        "event_date",
        "ingestion_ts",
        "event_hour",
        "processing_date"
        
        
        
    )
        )
    
    query =(
        parsedDF
        .writeStream 
        .foreachBatch(
            lambda df,bid,tp=target_path:
                write_each_topic(
                    batch_df=df,
                    batch_id=bid,
                    target_path=tp
                )
        ) 
        .option(
            'checkpointLocation',
            f'{checkpoint_location}'
        )
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
except Exception as e:
    print(e)
    
    

