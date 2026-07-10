import json
import os
from pyspark.sql.functions import col, from_json
from utils.spark_utils import SparkManager
from utils.schema_utils import extract_schema
from utils.cdc_processor import CDCProcessor
from utils.merge_utils import map_to_silver


spark=SparkManager.create_session('load_silver')

print('session built')

spark.conf.set('spark.databricks.delta.schema.autoMerge.enable','true')

with open('/home/amr/ven310/db_connect/config/tables.json','r',encoding='utf-8')as f:
    configs=json.load(f)
   
        

bronze_path='/home/amr/ven310/db_connect/bronze_db/'
silver_path='/home/amr/ven310/db_connect/silver_db/'
checkpoint_location="/home/amr/ven310/db_connect/checkpoints/silver"
 
queries=[]   
for table_name,cfg in configs.items():
    
    source_path=f'{bronze_path}/{table_name}'
    
    if not os.path.exists(source_path):
        print(f"Skipping {table_name}: path does not exist")
        continue

    if not os.path.exists(f"{source_path}/_delta_log"):
        print(f"Skipping {table_name}: not a Delta table")
        continue
   
    
    target_path=f'{silver_path}/{table_name}'
    
    merge_column=cfg.get('mergeColumn','')
    if not merge_column:
        raise ValueError(f"{table_name} has no mergeColumn")
    
  

    streaming_df=(
        spark
        .readStream
        .format('delta')
        .load(source_path)
    )
    
    schema=extract_schema(spark,source_path)  
    
    parsed_df =(
                streaming_df
                .withColumn(
                    'parsed_data',
                    from_json(col('value').cast('string'),schema)
                )
                
                .filter(
                    (col("parsed_data").isNotNull())
                           &
                    (col("parsed_data.op").isin("c", "u", "d", "r"))
            )
                )
                        
    silver_df=CDCProcessor.transform(parsed_df)
    
    query = (
        silver_df.writeStream
        .foreachBatch(
            lambda df,
             batch_id,
           tp=target_path,
           mc=merge_column:
                map_to_silver(
                    spark=spark,
                    batch_df=df,
                    target_path=tp,
                    merge_column=mc
                )
        )
        .option(
            "checkpointLocation",
            f"{checkpoint_location}/{table_name}"
        )
        .trigger(availableNow=True)
        .start()
    )

    queries.append(query)
            
for query in queries:
    query.awaitTermination()       
