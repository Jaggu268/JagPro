"""
extract.py
----------
Downloads NYC Yellow Taxi Trip parquet files from the TLC public dataset.
Data source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
"""

import os
import logging
import requests
from pathlib import Path
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# NYC TLC base URL for yellow taxi parquet files
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
RAW_DATA_DIR = Path(os.getenv("RAW_DATA_DIR", "/opt/airflow/data/raw"))


def build_url(year: int, month: int) -> str:
    """Build the download URL for a given year/month."""
    return f"{BASE_URL}/yellow_tripdata_{year}-{month:02d}.parquet"


def download_taxi_data(year: int, month: int, output_dir: Optional[Path] = None) -> Path:
    """
    Download NYC Yellow Taxi trip data for a specific year/month.

    Args:
        year:       Year of the dataset (e.g. 2024)
        month:      Month of the dataset (1-12)
        output_dir: Directory to save the file (defaults to RAW_DATA_DIR)

    Returns:
        Path to the downloaded file.

    Raises:
        requests.HTTPError: If the download fails.
    """
    output_dir = output_dir or RAW_DATA_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    url = build_url(year, month)
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    output_path = output_dir / filename

    if output_path.exists():
        logger.info("File already exists, skipping download: %s", output_path)
        return output_path

    logger.info("Downloading: %s", url)
    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()

    total_bytes = 0
    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            total_bytes += len(chunk)

    logger.info("Downloaded %.2f MB -> %s", total_bytes / 1_048_576, output_path)
    return output_path


def get_available_months(year: int, months: list[int] | None = None) -> list[tuple[int, int]]:
    """
    Return (year, month) tuples for the given year/months to process.
    Defaults to all 12 months if none specified.
    """
    months = months or list(range(1, 13))
    return [(year, m) for m in months]


if __name__ == "__main__":
    # Quick smoke test
    path = download_taxi_data(2024, 1)
    print(f"Downloaded to: {path}")
