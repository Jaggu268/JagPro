"""
load.py
-------
Loads transformed NYC Taxi data into a PostgreSQL data warehouse.

Uses SQLAlchemy + psycopg2 for bulk upserts via COPY (fast path)
and falls back to chunked INSERT for smaller batches.
"""

import os
import io
import logging
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Connection helpers
# -------------------------------------------------------------------

def get_engine(connection_string: str | None = None) -> Engine:
    """
    Build a SQLAlchemy engine.
    Falls back to the POSTGRES_CONN_STR environment variable.
    """
    conn_str = connection_string or os.environ["POSTGRES_CONN_STR"]
    engine = create_engine(conn_str, pool_pre_ping=True)
    logger.info("Database engine created.")
    return engine


# -------------------------------------------------------------------
# Schema initialisation
# -------------------------------------------------------------------

def init_schema(engine: Engine, sql_dir: Path | None = None) -> None:
    """
    Run create_tables.sql to ensure the target schema exists.
    Safe to call on every run (uses CREATE TABLE IF NOT EXISTS).
    """
    sql_dir = sql_dir or Path(__file__).parents[1] / "sql"
    ddl_path = sql_dir / "create_tables.sql"

    logger.info("Initialising schema from: %s", ddl_path)
    with engine.begin() as conn:
        conn.execute(text(ddl_path.read_text()))
    logger.info("Schema ready.")


# -------------------------------------------------------------------
# Bulk load (fast path via PostgreSQL COPY)
# -------------------------------------------------------------------

def _df_to_csv_buffer(df: pd.DataFrame) -> io.StringIO:
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    return buf


def bulk_load(df: pd.DataFrame, engine: Engine,
              table: str = "fact_trips", schema: str = "taxi") -> int:
    """
    Load a DataFrame into PostgreSQL using the fast COPY protocol.

    Returns the number of rows inserted.
    """
    if df.empty:
        logger.warning("Empty DataFrame — nothing to load.")
        return 0

    columns = ", ".join(df.columns.tolist())
    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        buf = _df_to_csv_buffer(df)
        copy_sql = (
            f"COPY {schema}.{table} ({columns}) "
            f"FROM STDIN WITH CSV NULL ''"
        )
        cursor.copy_expert(copy_sql, buf)
        raw_conn.commit()
        row_count = len(df)
        logger.info("Bulk loaded %d rows into %s.%s", row_count, schema, table)
        return row_count
    finally:
        raw_conn.close()


# -------------------------------------------------------------------
# Chunked INSERT (fallback / staging loads)
# -------------------------------------------------------------------

def chunked_load(df: pd.DataFrame, engine: Engine,
                 table: str = "fact_trips", schema: str = "taxi",
                 chunk_size: int = 50_000) -> int:
    """
    Load DataFrame in chunks using pandas to_sql.
    Slower than bulk_load but works without superuser COPY permission.
    """
    if df.empty:
        logger.warning("Empty DataFrame — nothing to load.")
        return 0

    total = 0
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i: i + chunk_size]
        chunk.to_sql(
            name=table,
            schema=schema,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )
        total += len(chunk)
        logger.info("Inserted chunk %d/%d (%d rows)",
                    i // chunk_size + 1, -(-len(df) // chunk_size), len(chunk))

    logger.info("Chunked load complete: %d rows into %s.%s", total, schema, table)
    return total


# -------------------------------------------------------------------
# Update analytics views
# -------------------------------------------------------------------

def refresh_views(engine: Engine, sql_dir: Path | None = None) -> None:
    """Recreate/refresh analytics views after each load."""
    sql_dir = sql_dir or Path(__file__).parents[1] / "sql"
    view_path = sql_dir / "create_analytics_views.sql"

    logger.info("Refreshing analytics views from: %s", view_path)
    with engine.begin() as conn:
        conn.execute(text(view_path.read_text()))
    logger.info("Analytics views refreshed.")


# -------------------------------------------------------------------
# High-level entry point
# -------------------------------------------------------------------

def load(df: pd.DataFrame,
         connection_string: str | None = None,
         use_bulk: bool = True) -> int:
    """
    Full load pipeline:
      1. Connect
      2. Ensure schema exists
      3. Bulk-load (or chunked fallback)
      4. Refresh analytics views

    Returns rows inserted.
    """
    engine = get_engine(connection_string)
    init_schema(engine)

    if use_bulk:
        rows = bulk_load(df, engine)
    else:
        rows = chunked_load(df, engine)

    refresh_views(engine)
    return rows
