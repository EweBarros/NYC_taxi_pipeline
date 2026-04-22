from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import CATALOG, GOLD_SCHEMA, PIPELINE_START_DATE, SILVER_SCHEMA


def build_gold_trips(spark: SparkSession) -> None:
    table = f"{CATALOG}.{GOLD_SCHEMA}.trips"

    yellow = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.yellow_trips")
    green  = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.green_trips")

    # Registros com datas absurdas (ex: 2001) são erros de input do motorista.
    # O filtro fica na Gold para que a Silver preserve os dados brutos auditáveis.
    gold = (
        yellow.unionByName(green)
        .filter(F.col("pickup_datetime") >= F.lit(PIPELINE_START_DATE))
        .withColumn("_computed_at", F.current_timestamp())
    )

    (
        gold.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )

    spark.sql(f"ALTER TABLE {table} CLUSTER BY (pickup_datetime)")

    counts = spark.sql(f"""
        SELECT taxi_type, COUNT(*) AS n
        FROM {table}
        GROUP BY taxi_type
    """).collect()
    by_type = {r["taxi_type"]: r["n"] for r in counts}
    total = sum(by_type.values())

    print(f"[Gold] {total:,} records written → {table}")
    print(f"  Yellow: {by_type.get('yellow', 0):,}")
    print(f"  Green:  {by_type.get('green', 0):,}")

    print("[Gold] Rodando OPTIMIZE (liquid clustering) ...")
    spark.sql(f"OPTIMIZE {table}")
    print("[Gold] OPTIMIZE concluído.")

    _register_metadata(spark, table)


def _register_metadata(spark: SparkSession, table: str) -> None:
    spark.sql(f"""
        COMMENT ON TABLE {table} IS
        'Tabela Gold unificada de corridas de táxi de Nova York (Yellow + Green), Jan-Mai 2023.
         Fonte: NYC TLC Trip Record Data. Liquid clustering por pickup_datetime.
         Registros com datas corrompidas (< {PIPELINE_START_DATE}) excluídos.
         FHV e FHVHV não incluídos por incompatibilidade de schema (sem passenger_count/total_amount).'
    """)

    column_comments = {
        "VendorID":              "Código do provedor TPEP/LPEP que gerou o registro: 1=Creative Mobile Technologies LLC, 2=Curb Mobility LLC, 6=Myle Technologies Inc, 7=Helix",
        "passenger_count":       "Número de passageiros informado pelo motorista. Pode ser 0 ou nulo por erro de input",
        "total_amount":          "Valor total cobrado ao passageiro em USD. Inclui tarifas, taxas e gorjetas de cartão de crédito; gorjetas em dinheiro não estão incluídas",
        "pickup_datetime":       "Data e hora de início da corrida (renomeado de tpep_/lpep_pickup_datetime)",
        "dropoff_datetime":      "Data e hora de fim da corrida (renomeado de tpep_/lpep_dropoff_datetime)",
        "taxi_type":             "Tipo de táxi: 'yellow' (Manhattan/aeroportos) ou 'green' (outer boroughs)",
        "_bronze_source_file":   "Caminho do parquet original no Volume Bronze, para rastrear até o arquivo fonte no NYC TLC",
        "_silver_processed_at":  "Timestamp de quando o registro foi processado e escrito na Silver",
        "_computed_at":          "Timestamp de quando esta tabela Gold foi computada (última execução do pipeline)",
    }

    for col, comment in column_comments.items():
        escaped = comment.replace("'", "''")
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {col} COMMENT '{escaped}'")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("nyc-taxi-gold").getOrCreate()
    build_gold_trips(spark)
