from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_layer(df: DataFrame, layer_name: str) -> None:
    total = df.count()
    print(f"\n=== Quality Report: {layer_name} ({total:,} records) ===")

    checks = {
        "null_pickup_datetime":  df.filter(F.col("pickup_datetime").isNull()).count(),
        "null_dropoff_datetime": df.filter(F.col("dropoff_datetime").isNull()).count(),
        "null_total_amount":     df.filter(F.col("total_amount").isNull()).count(),
        "negative_total_amount": df.filter(F.col("total_amount") < 0).count(),
        "null_passenger_count":  df.filter(F.col("passenger_count").isNull()).count(),
        "zero_passenger_count":  df.filter(F.col("passenger_count") == 0).count(),
    }

    for check, count in checks.items():
        pct = (count / total * 100) if total > 0 else 0
        if count == 0:
            status = "PASS"
        elif pct < 5:
            status = "WARN"
        else:
            status = "FAIL"
        print(f"  [{status}] {check}: {count:,} ({pct:.2f}%)")
