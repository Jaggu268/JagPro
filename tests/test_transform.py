"""
test_transform.py
-----------------
Unit tests for src/transform.py

Run: pytest tests/ -v --cov=src
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parents[1]))

from src.transform import clean, enrich, select_output_columns, transform


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_df(**overrides) -> pd.DataFrame:
    """Build a minimal valid trip DataFrame for testing."""
    base = {
        "tpep_pickup_datetime":  [datetime(2024, 1, 15, 9, 0)],
        "tpep_dropoff_datetime": [datetime(2024, 1, 15, 9, 20)],
        "pulocationid":   [161],
        "dolocationid":   [236],
        "passenger_count": [2],
        "trip_distance":  [3.5],
        "fare_amount":    [15.0],
        "tip_amount":     [3.0],
        "tolls_amount":   [0.0],
        "extra":          [0.5],
        "payment_type":   [1],
    }
    base.update(overrides)
    return pd.DataFrame(base)


# ---------------------------------------------------------------------------
# clean() tests
# ---------------------------------------------------------------------------

class TestClean:

    def test_valid_row_passes(self):
        df = make_df()
        result = clean(df)
        assert len(result) == 1

    def test_drops_null_fare(self):
        df = make_df(fare_amount=[None])
        result = clean(df)
        assert len(result) == 0

    def test_drops_zero_fare(self):
        df = make_df(fare_amount=[0.0])
        result = clean(df)
        assert len(result) == 0

    def test_drops_negative_fare(self):
        df = make_df(fare_amount=[-5.0])
        result = clean(df)
        assert len(result) == 0

    def test_drops_extreme_fare(self):
        df = make_df(fare_amount=[501.0])
        result = clean(df)
        assert len(result) == 0

    def test_drops_zero_distance(self):
        df = make_df(trip_distance=[0.0])
        result = clean(df)
        assert len(result) == 0

    def test_drops_extreme_distance(self):
        df = make_df(trip_distance=[101.0])
        result = clean(df)
        assert len(result) == 0

    def test_drops_dropoff_before_pickup(self):
        df = make_df(
            tpep_pickup_datetime=[datetime(2024, 1, 15, 9, 0)],
            tpep_dropoff_datetime=[datetime(2024, 1, 15, 8, 0)],
        )
        result = clean(df)
        assert len(result) == 0

    def test_mixed_valid_invalid(self):
        rows = pd.concat([make_df(), make_df(fare_amount=[-1.0])], ignore_index=True)
        result = clean(rows)
        assert len(result) == 1

    def test_columns_lowercase(self):
        df = make_df()
        df.columns = [c.upper() for c in df.columns]
        result = clean(df)
        assert all(c == c.lower() for c in result.columns)


# ---------------------------------------------------------------------------
# enrich() tests
# ---------------------------------------------------------------------------

class TestEnrich:

    @pytest.fixture
    def cleaned_df(self):
        return clean(make_df())

    def test_trip_duration_computed(self, cleaned_df):
        result = enrich(cleaned_df)
        assert "trip_duration_mins" in result.columns
        assert result["trip_duration_mins"].iloc[0] == pytest.approx(20.0, abs=0.1)

    def test_pickup_hour_correct(self, cleaned_df):
        result = enrich(cleaned_df)
        assert result["pickup_hour"].iloc[0] == 9

    def test_pickup_day_of_week(self, cleaned_df):
        # 2024-01-15 is a Monday → day_of_week = 0
        result = enrich(cleaned_df)
        assert result["pickup_day_of_week"].iloc[0] == 0

    def test_is_weekend_monday(self, cleaned_df):
        result = enrich(cleaned_df)
        assert result["is_weekend"].iloc[0] == 0

    def test_is_weekend_saturday(self):
        df = clean(make_df(
            tpep_pickup_datetime=[datetime(2024, 1, 13, 10, 0)],   # Saturday
            tpep_dropoff_datetime=[datetime(2024, 1, 13, 10, 20)],
        ))
        result = enrich(df)
        assert result["is_weekend"].iloc[0] == 1

    def test_distance_bucket_short_trip(self, cleaned_df):
        result = enrich(cleaned_df)
        # 3.5 miles → "3-7mi"
        assert result["distance_bucket"].iloc[0] == "3-7mi"

    def test_total_revenue_includes_tip(self, cleaned_df):
        result = enrich(cleaned_df)
        # fare=15 + tip=3 + extra=0.5 = 18.5
        assert result["total_revenue"].iloc[0] == pytest.approx(18.5, abs=0.01)

    def test_speed_mph_positive(self, cleaned_df):
        result = enrich(cleaned_df)
        assert result["speed_mph"].iloc[0] > 0

    def test_drops_extreme_duration(self):
        df = clean(make_df(
            tpep_dropoff_datetime=[datetime(2024, 1, 15, 9, 0) + timedelta(hours=4)],
        ))
        result = enrich(df)
        assert len(result) == 0   # >180 min → dropped


# ---------------------------------------------------------------------------
# select_output_columns() tests
# ---------------------------------------------------------------------------

class TestSelectOutputColumns:

    def test_returns_subset_of_columns(self):
        df = clean(make_df())
        df = enrich(df)
        result = select_output_columns(df)
        # Should only have known output columns, not raw intermediates
        assert "tpep_pickup_datetime" in result.columns
        assert "trip_duration_mins"   in result.columns

    def test_no_unknown_columns(self):
        df = clean(make_df())
        df = enrich(df)
        result = select_output_columns(df)
        allowed = {
            "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "pulocationid", "dolocationid", "passenger_count",
            "trip_distance", "fare_amount", "total_revenue", "revenue_per_mile",
            "trip_duration_mins", "speed_mph", "pickup_hour",
            "pickup_day_of_week", "pickup_month", "pickup_year",
            "is_weekend", "distance_bucket", "payment_type",
        }
        assert set(result.columns).issubset(allowed)
