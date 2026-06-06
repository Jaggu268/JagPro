-- =============================================================================
-- create_analytics_views.sql
-- Analytics layer — pre-aggregated views for dashboards and ad-hoc queries
-- =============================================================================

-- 1. Hourly demand pattern
CREATE OR REPLACE VIEW taxi.vw_hourly_demand AS
SELECT
    pickup_year,
    pickup_month,
    pickup_hour,
    COUNT(*)                        AS trip_count,
    ROUND(AVG(trip_distance), 2)    AS avg_distance_miles,
    ROUND(AVG(trip_duration_mins), 2) AS avg_duration_mins,
    ROUND(AVG(fare_amount), 2)      AS avg_fare,
    ROUND(SUM(total_revenue), 2)    AS total_revenue
FROM taxi.fact_trips
GROUP BY pickup_year, pickup_month, pickup_hour;

-- 2. Daily revenue summary
CREATE OR REPLACE VIEW taxi.vw_daily_revenue AS
SELECT
    DATE_TRUNC('day', tpep_pickup_datetime)::DATE  AS trip_date,
    COUNT(*)                                        AS trip_count,
    ROUND(SUM(total_revenue), 2)                    AS total_revenue,
    ROUND(AVG(total_revenue), 2)                    AS avg_revenue_per_trip,
    ROUND(AVG(trip_distance), 2)                    AS avg_distance_miles
FROM taxi.fact_trips
GROUP BY trip_date
ORDER BY trip_date;

-- 3. Top pickup zones
CREATE OR REPLACE VIEW taxi.vw_top_pickup_zones AS
SELECT
    ft.pulocationid,
    dl.zone,
    dl.borough,
    COUNT(*)                        AS trip_count,
    ROUND(SUM(ft.total_revenue), 2) AS total_revenue,
    ROUND(AVG(ft.fare_amount), 2)   AS avg_fare
FROM taxi.fact_trips ft
LEFT JOIN taxi.dim_location dl ON ft.pulocationid = dl.location_id
GROUP BY ft.pulocationid, dl.zone, dl.borough
ORDER BY trip_count DESC;

-- 4. Weekend vs weekday comparison
CREATE OR REPLACE VIEW taxi.vw_weekend_vs_weekday AS
SELECT
    CASE WHEN is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(*)                            AS trip_count,
    ROUND(AVG(trip_distance), 2)        AS avg_distance_miles,
    ROUND(AVG(trip_duration_mins), 2)   AS avg_duration_mins,
    ROUND(AVG(fare_amount), 2)          AS avg_fare,
    ROUND(AVG(total_revenue), 2)        AS avg_total_revenue
FROM taxi.fact_trips
GROUP BY is_weekend;

-- 5. Distance bucket distribution
CREATE OR REPLACE VIEW taxi.vw_distance_distribution AS
SELECT
    distance_bucket,
    COUNT(*)                            AS trip_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total,
    ROUND(AVG(fare_amount), 2)          AS avg_fare,
    ROUND(AVG(trip_duration_mins), 2)   AS avg_duration_mins
FROM taxi.fact_trips
GROUP BY distance_bucket
ORDER BY
    CASE distance_bucket
        WHEN '<1mi'   THEN 1
        WHEN '1-3mi'  THEN 2
        WHEN '3-7mi'  THEN 3
        WHEN '7-15mi' THEN 4
        WHEN '15+mi'  THEN 5
    END;

-- 6. Monthly KPI summary (executive-level)
CREATE OR REPLACE VIEW taxi.vw_monthly_kpis AS
SELECT
    pickup_year                         AS year,
    pickup_month                        AS month,
    COUNT(*)                            AS total_trips,
    ROUND(SUM(total_revenue), 2)        AS total_revenue,
    ROUND(AVG(fare_amount), 2)          AS avg_fare,
    ROUND(AVG(trip_distance), 2)        AS avg_distance_miles,
    ROUND(AVG(trip_duration_mins), 2)   AS avg_duration_mins,
    ROUND(AVG(speed_mph), 2)            AS avg_speed_mph,
    COUNT(DISTINCT pulocationid)        AS unique_pickup_zones
FROM taxi.fact_trips
GROUP BY pickup_year, pickup_month
ORDER BY pickup_year, pickup_month;
