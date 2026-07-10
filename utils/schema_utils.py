from pyspark.sql.functions import *

def extract_schema(spark,source_path):
  
    
    raw_df=(
        spark
        .read
        .format('delta')
        .load(source_path)
    )
    row=(
        raw_df
        .select(
            col('value').cast('string')
        )
        .first()  
    )
    if row is None:
        raise ValueError(f"No data found in {source_path}")
    sample_json=row[0]
    return (
            spark.range(1)
            .select(schema_of_json(lit(sample_json))
                    )
            .first()[0]
            )
