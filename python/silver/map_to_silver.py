import json
from utils.spark_utils import SparkManager
from pyspark.sql.functions import *
from delta.tables import DeltaTable
spark=SparkManager('pipeline')
with open('/home/amr/bronze/tables.json','r',encoding='utf-8') as f:
    configs=json.load(f)

bronze_path='/home/amr/ven311/bronze'
silver_path='/home/amr/ven311/silver'

def map_to_silver(batch_df, batch_id,table_name,merge_column):
   
    target_table = f'{silver_path}/{table_name}'
    
    if not DeltaTable.isDeltaTable(spark, target_table):
        batch_df\
        .write\
            .format("delta")\
            .mode("overwrite")\
            .save(target_table)
    else:
        dt= DeltaTable.forPath(spark,target_table)
        
    
        dt.alias("tgt").merge(
            batch_df.alias("src"),
            "tgt.{merge_column} = src.{merge_column}" 
        ).whenMatchedUpdateAll(
            condition = "src.op = 'u'"
        ).whenMatchedDelete(
          
            condition = "src.op = 'd'"
        ).whenNotMatchedInsertAll(
            condition = "src.op = 'c' OR src.op = 'r'" 
        ).execute()
for table_name, cfg in configs.items():
    source_table=f'{bronze_path}/{table_name}'
    target_table=f'{table_name}'
    merge_column=cfg.get('mergeColumn','')

    json_schema=extract_schema(source_table)
   
    bronze_df=(
        spark
        .readStream
        .format('delta')
        .load(source_table)
    )
    
    parsed_df = (
        bronze_df
        .withColumn(
            'parsed_data',
            from_json(col('value').cast('string'), json_schema)
        )
    )

    silver_df = (
        parsed_df
        .select(
            'parsed_data.after.*',
            'parsed_data.op'
        )
    )

   
  query = (
  silver_df
  .writeStream
  .foreachBatch(lambda df, bid:
      map_to_silver(df, bid, table_name, merge_column)) 
  .options(**{
      'checkpointLocation': f'/home/amr/checkpoints/silver/{table_name}'
  })
  .start()
  )
  

  query.awaitTermination()
