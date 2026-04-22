from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import CATALOG, PATHS, SILVER_SCHEMA
from src.transformation.transforms import (
    deduplicate_trips,
    filter_valid_records,
    select_canonical_columns,
)

TABLE = f"{CATALOG}.{SILVER_SCHEMA}.green_trips"


def build_silver_green(spark: SparkSession) -> None:
    bronze_path = f"{PATHS['bronze']}/green"

    df = spark.read.format("delta").load(bronze_path)

    silver_df = (
        df.select(
            F.col("VendorID").cast("integer"),
            F.col("passenger_count").cast("integer"),
            F.col("total_amount").cast("double"),
            F.col("lpep_pickup_datetime").alias("pickup_datetime"),
            F.col("lpep_dropoff_datetime").alias("dropoff_datetime"),
            F.lit("green").alias("taxi_type"),
            F.col("_source_file").alias("_bronze_source_file"),
        )
        .transform(filter_valid_records)
        .transform(deduplicate_trips)
        .transform(select_canonical_columns)
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

    (
        silver_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TABLE)
    )

    spark.sql(f"ALTER TABLE {TABLE} CLUSTER BY (pickup_datetime)")

    count = spark.sql(f"SELECT COUNT(*) FROM {TABLE}").collect()[0][0]
    print(f"[Silver Green] {count:,} records written → {TABLE}")

    print("[Silver Green] Rodando OPTIMIZE (liquid clustering) ...")
    spark.sql(f"OPTIMIZE {TABLE}")
    print("[Silver Green] OPTIMIZE concluído.")

    _register_metadata(spark)


def _register_metadata(spark: SparkSession) -> None:
    spark.sql(f"""
        COMMENT ON TABLE {TABLE} IS
        'Camada Silver de corridas de táxi Green de Nova York, Jan-Mai 2023.
         Fonte: Bronze Green (NYC TLC lpep). Schema canônico aplicado: colunas renomeadas,
         tipos normalizados, registros com nulos em campos obrigatórios removidos.
         Liquid clustering por pickup_datetime.'
    """)

    column_comments = {
        "VendorID":               "Código do provedor LPEP que gerou o registro: 1=Creative Mobile Technologies LLC, 2=Curb Mobility LLC, 6=Myle Technologies Inc",
        "passenger_count":        "Número de passageiros informado pelo motorista. Pode ser 0 ou nulo por erro de input",
        "total_amount":           "Valor total cobrado ao passageiro em USD. Inclui tarifas, taxas e gorjetas de cartão de crédito; gorjetas em dinheiro não estão incluídas",
        "pickup_datetime":        "Data e hora de início da corrida (renomeado de lpep_pickup_datetime)",
        "dropoff_datetime":       "Data e hora de fim da corrida (renomeado de lpep_dropoff_datetime)",
        "taxi_type":              "Tipo de táxi: sempre 'green' nesta tabela",
        "_bronze_source_file":    "Caminho do parquet original no Volume Bronze, para rastrear até o arquivo fonte no NYC TLC",
        "_silver_processed_at":   "Timestamp de quando este lote foi processado e escrito na Silver",
    }

    for col, comment in column_comments.items():
        escaped = comment.replace("'", "''")
        spark.sql(f"ALTER TABLE {TABLE} ALTER COLUMN {col} COMMENT '{escaped}'")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("nyc-taxi-silver-green").getOrCreate()
    build_silver_green(spark)
