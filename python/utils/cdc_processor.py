from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    when,
    current_timestamp
)


class CDCProcessor:
    """
    Transform Debezium CDC events into a normalized DataFrame.

    Supported operations:
        c -> Create
        u -> Update
        d -> Delete
        r -> Snapshot Read
    """

    @staticmethod
    def transform(df: DataFrame) -> DataFrame:
        """
        Convert Debezium payload into a normalized dataframe.

        Parameters
        ----------
        df : DataFrame
            Parsed Debezium dataframe.

        Returns
        -------
        DataFrame
        """

        payload = when(
            col("parsed_data.op") == "d",
            col("parsed_data.before")
        ).otherwise(
            col("parsed_data.after")
        )

        return (
            df
            .withColumn("payload", payload)

            .select(
                "payload.*",

                col("parsed_data.op").alias("__operation"),

                col("parsed_data.ts_ms").alias("__event_timestamp"),

                col("parsed_data.source.db").alias("__database"),

                col("parsed_data.source.schema").alias("__schema"),

                col("parsed_data.source.table").alias("__table"),

                current_timestamp().alias("__ingestion_time")
            )
        )
