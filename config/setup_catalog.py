from pyspark.sql import SparkSession

from config.settings import CATALOG, SCHEMA, VOLUME


def setup_unity_catalog(spark: SparkSession) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    print(f"[Setup] Schema {CATALOG}.{SCHEMA} created")

    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
    print(f"[Setup] Volume {CATALOG}.{SCHEMA}.{VOLUME} created")
    print(f"[Setup] Volume path: /Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("nyc-taxi-setup").getOrCreate()
    setup_unity_catalog(spark)
