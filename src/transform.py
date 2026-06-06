"""
transform.py
------------
Cleans and enriches raw NYC Yellow Taxi trip data using pandas.

Transformations applied:
  - Drop rows with null pickup/dropoff timestamps or locations
  - Filter out negative/zero fare amounts and trip distances
  - Calculate trip duration in minutes
  - Derive time-based features (hour, day of week, is_weekend)
  - Classify trips into distance buckets
  - Cap outliers in fare_amount and trip_distance
"""

import logging
import pandas as pd
import numpy as np
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Business rule thresholds
MAX_FARE = 500.0          # $ — cap extreme outliers
MAX_DISTANCE = 100.0      # miles
MAX_DURATION_MINS = 180   # 3 hours


def load_parquet(file_path: Path) -> pd.DataFrame:
    """Load a parquet file into a DataFrame."""
    logger.info("Loading parquet: %s", file_path)
    df = pd.read_parquet(file_path)
    logger.info("Loaded %d rows, %d columns", len(df), df.shape[1])
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Remove invalid and out-of-range records."""
    initial_count = len(df)

    # Standardise column names to snake_case
    df.columns = [c.lower().strip() for c in df.columns]

    # Drop rows with missing critical fields
    critical_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime",
                     "pulocationid", "dolocationid", "fare_amount", "trip_distance"]
    df = df.dropna(subset=critical_cols)

    # Remove bad fares and distances
    df = df[(df["fare_amount"] > 0) & (df["fare_amount"] <= MAX_FARE)]
    df = df[(df["trip_distance"] > 0) & (df["trip_distance"] <= MAX_DISTANCE)]

    # Ensure timestamps are datetime
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    # Remove trips where dropoff is before pickup
    df = df[df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"]]

    dropped = initial_count - len(df)
    logger.info("Cleaned: dropped %d rows (%.1f%%), %d rows remain",
                dropped, 100 * dropped / max(initial_count, 1), len(df))
    return df.reset_index(drop=True)


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns for analytics."""

    # Trip duration
    df["trip_duration_mins"] = (
        (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
        .dt.total_seconds() / 60
    ).round(2)

    # Cap extreme durations
    df = df[df["trip_duration_mins"] <= MAX_DURATION_MINS]

    # Time features
    df["pickup_hour"]       = df["tpep_pickup_datetime"].dt.hour
    df["pickup_day_of_week"] = df["tpep_pickup_datetime"].dt.dayofweek   # 0=Mon
    df["pickup_month"]      = df["tpep_pickup_datetime"].dt.month
    df["pickup_year"]       = df["tpep_pickup_datetime"].dt.year
    df["is_weekend"]        = df["pickup_day_of_week"].isin([5, 6]).astype(int)

    # Speed (mph) — useful for detecting data anomalies
    df["speed_mph"] = (df["trip_distance"] / (df["trip_duration_mins"] / 60)).round(2)

    # Distance bucket
    df["distance_bucket"] = pd.cut(
        df["trip_distance"],
        bins=[0, 1, 3, 7, 15, 100],
        labels=["<1mi", "1-3mi", "3-7mi", "7-15mi", "15+mi"],
    ).astype(str)

    # Revenue per mile
    df["revenue_per_mile"] = (df["fare_amount"] / df["trip_distance"]).round(2)

    # Total revenue (fare + tips + tolls + surcharges)
    tip_col    = "tip_amount"    if "tip_amount"    in df.columns else None
    tolls_col  = "tolls_amount"  if "tolls_amount"  in df.columns else None
    extra_col  = "extra"         if "extra"         in df.columns else None

    df["total_revenue"] = df["fare_amount"].copy()
    if tip_col:   df["total_revenue"] += df[tip_col].fillna(0)
    if tolls_col: df["total_revenue"] += df[tolls_col].fillna(0)
    if extra_col: df["total_revenue"] += df[extra_col].fillna(0)

    logger.info("Enrichment complete: %d rows, %d columns", len(df), df.shape[1])
    return df


def select_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only the columns we'll load into the warehouse."""
    keep = [
        "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "pulocationid", "dolocationid",
        "passenger_count", "trip_distance",
        "fare_amount", "total_revenue", "revenue_per_mile",
        "trip_duration_mins", "speed_mph",
        "pickup_hour", "pickup_day_of_week", "pickup_month", "pickup_year",
        "is_weekend", "distance_bucket",
        "payment_type",
    ]
    available = [c for c in keep if c in df.columns]
    return df[available]


def transform(file_path: Path) -> pd.DataFrame:
    """Full transform pipeline: load → clean → enrich → select columns."""
    df = load_parquet(file_path)
    df = clean(df)
    df = enrich(df)
    df = select_output_columns(df)
    return df


if __name__ == "__main__":
    import sys
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data/raw/yellow_tripdata_2024-01.parquet")
    result = transform(path)
    print(result.head())
    print(result.dtypes)
