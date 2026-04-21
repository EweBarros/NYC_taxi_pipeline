BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

CATALOG     = "nyc_taxi"
SCHEMA      = "main"
VOLUME      = "data"
VOLUME_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

YEARS              = [2023]
PIPELINE_START_DATE = "2023-01-01"
MONTHS     = list(range(1, 6))
TAXI_TYPES = ["yellow", "green", "fhv", "fhvhv"]

PATHS = {
    "raw":    f"{VOLUME_BASE}/raw",
    "bronze": f"{VOLUME_BASE}/bronze",
    "silver": f"{VOLUME_BASE}/silver",
    "gold":   f"{VOLUME_BASE}/gold",
}
