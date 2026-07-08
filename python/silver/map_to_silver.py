import json

from pyspark.sql.functions import col, from_json

from utils.spark_utils import SparkManager
from utils.schema_utils import extract_schema
from utils.cdc_processor import CDCProcessor
from utils.merge_utils import merge_batch

spark = SparkManager("silver_pipeline")

with open("/home/amr/bronze/tables.json", "r", encoding="utf-8") as f:
    configs = json.load(f)

bronze_path = "/home/amr/ven311/bronze"
silver_path = "/home/amr/ven311/silver"

queries = []

for table_name, cfg in configs.items():

    source_path = f"{bronze_path}/{table_name}"
    target_path = f"{silver_path}/{table_name}"

    merge_column = cfg.get("mergeColumn")

    if not merge_column:
        raise ValueError(f"{table_name} has no mergeColumn")

    json_schema = extract_schema(source_path)

    bronze_df = (
        spark.readStream
        .format("delta")
        .load(source_path)
    )

    parsed_df = (
        bronze_df
        .withColumn(
            "parsed_data",
            from_json(
                col("value").cast("string"),
                json_schema
            )
        )
    )

    silver_df = CDCProcessor.transform(parsed_df)

    query = (
        silver_df.writeStream
        .foreachBatch(
            lambda df, bid,
           tp=target_path,
           mc=merge_column:
                merge_batch(
                    spark=spark,
                    batch_df=df,
                    target_path=tp,
                    merge_column=mc
                )
        )
        .option(
            "checkpointLocation",
            f"/home/amr/checkpoints/silver/{table_name}"
        )
        .start()
    )

    queries.append(query)

for query in queries:
    query.awaitTermination()
