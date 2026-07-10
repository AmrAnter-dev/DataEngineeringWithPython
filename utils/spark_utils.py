from pyspark.sql import SparkSession
from db_connect.config.configs import sources

config = sources["spark_config"]

class SparkManager:

    @staticmethod
    def create_session(app_name: str):

        builder = (
            SparkSession.builder
            .master("local[*]")
            .appName(app_name)
        )

        # apply configs safely
        for k, v in config.items():
            builder = builder.config(k, v)

        return builder.enableHiveSupport().getOrCreate()
