from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def deduplicate_trips(df: DataFrame) -> DataFrame:
    # Chave natural: mesmo equipamento não registra duas corridas com pickup/dropoff idênticos.
    # Tiebreaker _bronze_source_file é determinístico: re-execuções produzem o mesmo resultado.
    window = (
        Window
        .partitionBy(F.coalesce(F.col("VendorID"), F.lit(-1)), "pickup_datetime", "dropoff_datetime")
        .orderBy("_bronze_source_file")
    )
    return (
        df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def filter_valid_records(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(F.col("pickup_datetime").isNotNull())
        .filter(F.col("dropoff_datetime").isNotNull())
        .filter(F.col("total_amount").isNotNull())
    )


def select_canonical_columns(df: DataFrame) -> DataFrame:
    return df.select(
        "VendorID",
        "passenger_count",
        "total_amount",
        "pickup_datetime",
        "dropoff_datetime",
        "taxi_type",
        "_bronze_source_file",
    )
