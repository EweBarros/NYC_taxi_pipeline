from pyspark.sql import SparkSession

from config.settings import BRONZE_SCHEMA, CATALOG, GOLD_SCHEMA, RAW_SCHEMA, SILVER_SCHEMA, VOLUME


def setup_unity_catalog(spark: SparkSession) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}.{VOLUME}")
    print(f"[Setup] {CATALOG}.{RAW_SCHEMA}.{VOLUME} → /Volumes/{CATALOG}/{RAW_SCHEMA}/{VOLUME}/")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.{VOLUME}")
    print(f"[Setup] {CATALOG}.{BRONZE_SCHEMA}.{VOLUME} → /Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME}/")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
    print(f"[Setup] Schema {CATALOG}.{SILVER_SCHEMA} created")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")
    print(f"[Setup] Schema {CATALOG}.{GOLD_SCHEMA} created")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("nyc-taxi-setup").getOrCreate()
    setup_unity_catalog(spark)
