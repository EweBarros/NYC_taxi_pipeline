BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

CATALOG        = "nyc_taxi"
RAW_SCHEMA     = "raw"
BRONZE_SCHEMA  = "bronze"
SILVER_SCHEMA  = "silver"
GOLD_SCHEMA    = "gold"
VOLUME         = "files"

YEARS               = [2023]
PIPELINE_START_DATE = "2023-01-01"
MONTHS     = list(range(1, 6))
TAXI_TYPES = ["yellow", "green", "fhv", "fhvhv"]

PATHS = {
    "raw":    f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{VOLUME}",
    "bronze": f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME}",
}
