from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from config.settings import MONTHS, PATHS, TAXI_TYPES, YEARS

_INT_TYPES = (T.IntegerType, T.LongType, T.ShortType, T.ByteType)


def _normalize_types(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, _INT_TYPES):
            df = df.withColumn(field.name, F.col(field.name).cast("double"))
    return df


def ingest_bronze(spark: SparkSession, taxi_type: str, year: int, month: int) -> None:
    raw_path    = f"{PATHS['raw']}/{taxi_type}/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    bronze_path = f"{PATHS['bronze']}/{taxi_type}"

    df = (
        spark.read.parquet(raw_path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .transform(_normalize_types)
        .withColumn("_ingested_at",   F.current_timestamp())
        .withColumn("_source_system", F.lit("nyc_tlc"))
        .withColumn("year",           F.lit(year))
        .withColumn("month",          F.lit(month))
    )

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"year = {year} AND month = {month}")
        .option("mergeSchema", "true")
        .partitionBy("year", "month")
        .save(bronze_path)
    )

    count = (
        spark.read.format("delta").load(bronze_path)
        .filter((F.col("year") == year) & (F.col("month") == month))
        .count()
    )
    print(f"[Bronze] {taxi_type} {year}-{month:02d}: {count:,} records")


def ingest_all_bronze(spark: SparkSession) -> None:
    for taxi_type in TAXI_TYPES:
        for year in YEARS:
            for month in MONTHS:
                ingest_bronze(spark, taxi_type, year, month)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("nyc-taxi-bronze").getOrCreate()
    ingest_all_bronze(spark)
