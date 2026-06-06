-- =============================================================================
-- create_tables.sql
-- NYC Yellow Taxi Data Warehouse — DDL
-- =============================================================================

-- Schema
CREATE SCHEMA IF NOT EXISTS taxi;

-- =============================================================================
-- Dimension: Date
-- =============================================================================
CREATE TABLE IF NOT EXISTS taxi.dim_date (
    date_key        INTEGER PRIMARY KEY,   -- YYYYMMDD
    full_date       DATE    NOT NULL,
    year            SMALLINT NOT NULL,
    month           SMALLINT NOT NULL,
    day             SMALLINT NOT NULL,
    day_of_week     SMALLINT NOT NULL,     -- 0=Mon, 6=Sun
    day_name        VARCHAR(9) NOT NULL,
    month_name      VARCHAR(9) NOT NULL,
    quarter         SMALLINT NOT NULL,
    is_weekend      BOOLEAN  NOT NULL
);

-- Pre-populate dim_date for 2019–2026
INSERT INTO taxi.dim_date
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER           AS date_key,
    d::DATE                                    AS full_date,
    EXTRACT(YEAR    FROM d)::SMALLINT          AS year,
    EXTRACT(MONTH   FROM d)::SMALLINT          AS month,
    EXTRACT(DAY     FROM d)::SMALLINT          AS day,
    EXTRACT(ISODOW  FROM d)::SMALLINT - 1      AS day_of_week,
    TO_CHAR(d, 'Day')                          AS day_name,
    TO_CHAR(d, 'Month')                        AS month_name,
    EXTRACT(QUARTER FROM d)::SMALLINT          AS quarter,
    EXTRACT(ISODOW  FROM d) IN (6, 7)          AS is_weekend
FROM generate_series('2019-01-01'::DATE, '2026-12-31'::DATE, '1 day'::INTERVAL) d
ON CONFLICT (date_key) DO NOTHING;

-- =============================================================================
-- Dimension: Location (NYC TLC taxi zones)
-- =============================================================================
CREATE TABLE IF NOT EXISTS taxi.dim_location (
    location_id     SMALLINT PRIMARY KEY,
    borough         VARCHAR(50),
    zone            VARCHAR(100),
    service_zone    VARCHAR(50)
);

-- =============================================================================
-- Fact: Trips
-- =============================================================================
CREATE TABLE IF NOT EXISTS taxi.fact_trips (
    trip_id             BIGSERIAL PRIMARY KEY,

    -- Timestamps
    tpep_pickup_datetime    TIMESTAMP NOT NULL,
    tpep_dropoff_datetime   TIMESTAMP NOT NULL,

    -- Location FKs
    pulocationid            SMALLINT REFERENCES taxi.dim_location(location_id),
    dolocationid            SMALLINT REFERENCES taxi.dim_location(location_id),

    -- Trip metrics
    passenger_count         SMALLINT,
    trip_distance           NUMERIC(8, 2),
    trip_duration_mins      NUMERIC(8, 2),
    speed_mph               NUMERIC(8, 2),
    distance_bucket         VARCHAR(10),

    -- Financials
    fare_amount             NUMERIC(10, 2),
    total_revenue           NUMERIC(10, 2),
    revenue_per_mile        NUMERIC(10, 2),

    -- Derived time features
    pickup_hour             SMALLINT,
    pickup_day_of_week      SMALLINT,
    pickup_month            SMALLINT,
    pickup_year             SMALLINT,
    is_weekend              SMALLINT,

    -- Payment
    payment_type            SMALLINT,

    -- Audit
    loaded_at               TIMESTAMP DEFAULT NOW()
);

-- Indexes for common analytical query patterns
CREATE INDEX IF NOT EXISTS idx_fact_trips_pickup_dt  ON taxi.fact_trips (tpep_pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_fact_trips_puloc      ON taxi.fact_trips (pulocationid);
CREATE INDEX IF NOT EXISTS idx_fact_trips_doloc      ON taxi.fact_trips (dolocationid);
CREATE INDEX IF NOT EXISTS idx_fact_trips_year_month ON taxi.fact_trips (pickup_year, pickup_month);
