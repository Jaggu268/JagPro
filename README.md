# 🚕 NYC Yellow Taxi — End-to-End ETL Pipeline

> Built by **Jagadeesh Ch** · Master's Student in Data Science @ University of Central Missouri  
> 📧 jagadeeshchodaboina@gmail.com · [GitHub](https://github.com/Jaggu268)  
> 🎯 Seeking full-time roles in **Data Engineering** and **Data Science**

---

A production-style data engineering pipeline that ingests, transforms, and warehouses **NYC Yellow Taxi trip data** (40M+ rows/year) using Apache Airflow, PostgreSQL, and Docker.

This project demonstrates hands-on experience building real-world data pipelines — from raw public data ingestion to a queryable analytics warehouse — using the same tools and patterns used at companies like Uber, Lyft, and Airbnb.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Apache Airflow DAG                      │
│  (monthly schedule · backfill · retry logic · XCom state)  │
└────────────┬────────────────┬──────────────────┬────────────┘
             │                │                  │
             ▼                ▼                  ▼
      ┌─────────────┐  ┌─────────────┐  ┌──────────────────┐
      │   EXTRACT   │  │  TRANSFORM  │  │      LOAD        │
      │             │  │             │  │                  │
      │ NYC TLC API │  │ pandas      │  │ PostgreSQL       │
      │ (parquet)   │→ │ clean +     │→ │ COPY bulk insert │
      │             │  │ enrich      │  │ + views refresh  │
      └─────────────┘  └─────────────┘  └──────────────────┘
             │                                   │
             ▼                                   ▼
      /data/raw/                          taxi schema
      yellow_tripdata_                    ├── fact_trips
      YYYY-MM.parquet                     ├── dim_date
                                          ├── dim_location
                                          └── analytics views
```

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9 |
| Transform | Python 3.11 · pandas · NumPy |
| Storage | PostgreSQL 15 |
| Containerisation | Docker · Docker Compose |
| Data Format | Parquet (Apache Arrow) |
| Testing | pytest · pytest-cov |
| Linting | Ruff |

---

## Project Structure

```
nyc-taxi-pipeline/
├── dags/
│   └── nyc_taxi_etl_dag.py     # Airflow DAG (monthly schedule)
├── src/
│   ├── extract.py              # Download TLC parquet files
│   ├── transform.py            # Clean, validate, enrich data
│   └── load.py                 # Bulk-load into PostgreSQL
├── sql/
│   ├── create_tables.sql       # Fact + dimension DDL
│   └── create_analytics_views.sql  # Pre-aggregated views
├── tests/
│   └── test_transform.py       # 18 unit tests for transform logic
├── scripts/
│   └── init_db.sql             # One-time DB/user setup
├── docker-compose.yml          # Full stack: PG + Airflow
├── requirements.txt
└── .env.example
```

---

## Quickstart

### Prerequisites
- Docker ≥ 24 and Docker Compose v2
- 4 GB RAM available for Docker

### 1. Clone and configure

```bash
git clone https://github.com/Jaggu268/nyc-taxi-pipeline.git
cd nyc-taxi-pipeline
cp .env.example .env
```

### 2. Start the stack

```bash
docker compose up --build -d
```

This starts:
- **PostgreSQL** on `localhost:5432`
- **Airflow webserver** on `http://localhost:8080` (admin / admin)
- **Airflow scheduler**

First run takes ~3 minutes while Airflow installs dependencies.

### 3. Trigger the pipeline

Open `http://localhost:8080`, enable the `nyc_taxi_etl_pipeline` DAG, and trigger a run — or let the scheduler pick it up automatically.

### 4. Query the warehouse

```bash
docker exec -it nyc-taxi-pipeline-postgres-1 \
  psql -U taxi_user -d taxi_db
```

```sql
-- Monthly KPIs
SELECT * FROM taxi.vw_monthly_kpis ORDER BY year, month;

-- Top 10 busiest pickup zones
SELECT zone, borough, trip_count
FROM taxi.vw_top_pickup_zones LIMIT 10;

-- Peak hour demand
SELECT pickup_hour, SUM(trip_count) AS trips
FROM taxi.vw_hourly_demand
GROUP BY pickup_hour ORDER BY trips DESC;
```

### 5. Run tests

```bash
pip install -r requirements.txt
pytest tests/ -v --cov=src --cov-report=term-missing
```

---

## Pipeline Details

### Extract
- Downloads monthly Parquet files (~500 MB each) from the [NYC TLC public dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- Idempotent — skips files already on disk
- Streams download in chunks to avoid memory spikes

### Transform
- **Cleaning:** drops null critical fields, zero/negative fares, impossible distances, trips where dropoff precedes pickup
- **Outlier capping:** fares > $500, distances > 100 miles, durations > 3 hours
- **Feature engineering:**
  - `trip_duration_mins` — actual trip time
  - `speed_mph` — average speed (useful for anomaly detection)
  - `distance_bucket` — categorical (`<1mi`, `1-3mi`, `3-7mi`, `7-15mi`, `15+mi`)
  - `is_weekend`, `pickup_hour`, `pickup_day_of_week`
  - `total_revenue` — fare + tip + tolls + surcharges
  - `revenue_per_mile`

### Load
- Uses PostgreSQL `COPY` (bulk fast path) for large DataFrames
- Falls back to chunked `INSERT` for smaller batches
- Refreshes 6 analytics views after each load

### Analytics Views

| View | Description |
|---|---|
| `vw_monthly_kpis` | Executive summary — trips, revenue, avg fare per month |
| `vw_daily_revenue` | Day-by-day revenue trend |
| `vw_hourly_demand` | Trip volume and revenue by hour of day |
| `vw_top_pickup_zones` | Ranked pickup locations by volume |
| `vw_weekend_vs_weekday` | Behavioural comparison |
| `vw_distance_distribution` | Fare and duration by trip length bucket |

---

## Key Design Decisions

**Why Airflow?** Industry-standard for pipeline orchestration; `catchup=True` enables automatic backfilling of historical data when deploying for the first time.

**Why PostgreSQL COPY?** 10–100× faster than row-by-row INSERT for bulk loads; critical when processing 3M+ rows per monthly file.

**Why Parquet?** Columnar format reduces I/O by 5–10× for analytical queries compared to CSV; native support in pandas and Spark.

**Why Docker Compose?** Any reviewer can run the full stack with a single command — no environment setup friction.

---

## Future Enhancements

- [ ] Swap pandas transforms for **PySpark** to handle multi-year backfills in parallel
- [ ] Add **dbt** models for a proper transformation layer with lineage tracking
- [ ] Deploy to **AWS** (S3 + Glue + Redshift) or **GCP** (GCS + Dataflow + BigQuery)
- [ ] Add **Great Expectations** data quality checks as an Airflow task
- [ ] Stream real-time trip updates with **Apache Kafka**
- [ ] Build a **Grafana / Metabase** dashboard on top of the analytics views

---

## Data Source

NYC Taxi and Limousine Commission (TLC) Trip Record Data — publicly available at:  
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

---

## About Me

I'm **Jagadeesh Ch**, a Master's student in Data Science at the **University of Central Missouri**, passionate about building scalable data systems and turning raw data into actionable insights.

This project is part of my portfolio showcasing hands-on data engineering skills. I'm actively seeking full-time opportunities in **Data Engineering** and **Data Science** where I can contribute to real-world data infrastructure and analytics.

**Skills:** Python · SQL · Apache Airflow · Apache Spark · PostgreSQL · Docker · pandas · NumPy · Machine Learning · ETL Pipeline Design

**Connect with me:**  
📧 jagadeeshchodaboina@gmail.com  
🐙 [github.com/Jaggu268](https://github.com/Jaggu268)

---

*If you're a recruiter or hiring manager — feel free to reach out! I'm open to Data Engineer, Data Scientist, and Analytics Engineer roles.*
