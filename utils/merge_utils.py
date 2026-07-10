from delta.tables import DeltaTable
from pyspark.sql.functions import col

def write_each_topic(
    batch_df,
    batch_id,
    target_path:str
    )-> None:

        topics = [
            row.topic
            for row in batch_df.select("topic").distinct().collect()
        ]

        for topic in topics:
            topic_df = batch_df.filter(col("topic") == topic)
            (
                topic_df
                .write
                .format("delta")
                .mode("append")
                .save(f'{target_path}/{topic}')
               
        ) 
            
            
            
def map_to_silver(
    spark,
    batch_df,
    target_path: str,
    merge_column: str
) -> None:
    print(type(target_path), target_path)
    if not DeltaTable.isDeltaTable(spark, target_path):

        (
            batch_df
            .write
            .format("delta")
            .mode("overwrite")
            .option('mergeSchema','true')
            .save(target_path)
        )

        return

    dt = DeltaTable.forPath(
        spark,
        target_path
    )

    (
        dt.alias("tgt")
        .merge(
            batch_df.alias("src"),
            f"tgt.{merge_column}=src.{merge_column}"
        )
        .whenMatchedUpdateAll(
            condition="src.op='u'"
        )
        .whenMatchedDelete(
            condition="src.op='d'"
        )
        .whenNotMatchedInsertAll(
            condition="src.op in ('c','r')"
        )
        .execute()
    )
    

            
