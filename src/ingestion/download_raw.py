import os
import urllib.request

from config.settings import BASE_URL, MONTHS, PATHS, TAXI_TYPES, YEARS


def download_parquet(taxi_type: str, year: int, month: int) -> str:
    filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    url      = f"{BASE_URL}/{filename}"
    dest_dir = f"{PATHS['raw']}/{taxi_type}"
    dest_path = f"{dest_dir}/{filename}"

    os.makedirs(dest_dir, exist_ok=True)
    print(f"Downloading {url}")
    urllib.request.urlretrieve(url, dest_path)
    size_mb = os.path.getsize(dest_path) / 1e6
    print(f"  OK: {size_mb:.1f} MB → {dest_path}")
    return dest_path


def download_all() -> None:
    for taxi_type in TAXI_TYPES:
        for year in YEARS:
            for month in MONTHS:
                try:
                    download_parquet(taxi_type, year, month)
                except Exception as e:
                    print(f"  WARN: {taxi_type} {year}-{month:02d} skipped: {e}")


if __name__ == "__main__":
    download_all()
