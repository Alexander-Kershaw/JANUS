from sqlalchemy import create_engine, text

"""
---------------------------------------------------------------------------------------------------
Serves to create Silver schema and tables for cleaned data

Silver layer objectives:
- Typed columns, consistent schema (cleaned events)
- Room for dedupplication and late-arrival handling
- Data lineage back to bronze layer 
- Quarantine for invalid/malformed records within reason
- Index spine for faster transformations

- Quaratine table stores: raw quarantied record, the reason for its rejection,
    its lineage back to bronze
---------------------------------------------------------------------------------------------------
"""

WAREHOUSE_URL = "postgresql+psycopg2://janus:janus_password@localhost:5433/airflow"

DDL = """
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.silver_events (
    event_id        TEXT NOT NULL,
    event_ts        TIMESTAMPTZ NOT NULL,
    received_ts     TIMESTAMPTZ NOT NULL,
    user_id         TEXT,
    device_id       TEXT,
    session_id      TEXT,
    event_type      TEXT NOT NULL,
    props           JSONB NOT NULL,

    bronze_row_hash TEXT NOT NULL,
    source_file     TEXT NOT NULL,
    ingestion_ts    TIMESTAMPTZ NOT NULL,

    is_late         BOOLEAN NOT NULL DEFAULT FALSE,
    lateness_sec    INTEGER
);

CREATE TABLE IF NOT EXISTS silver.silver_events_quarantine (
    bronze_row_hash TEXT NOT NULL,
    source_file     TEXT NOT NULL,
    ingestion_ts    TIMESTAMPTZ NOT NULL,

    reason_code     TEXT NOT NULL,
    reason_detail   TEXT,

    raw_record      JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS silver_events_event_ts_idx
  ON silver.silver_events (event_ts);

CREATE INDEX IF NOT EXISTS silver_events_user_id_idx
  ON silver.silver_events (user_id);

CREATE INDEX IF NOT EXISTS silver_quarantine_reason_idx
  ON silver.silver_events_quarantine (reason_code);
"""

def main() -> None:
    engine = create_engine(WAREHOUSE_URL, future=True)
    with engine.begin() as conn:
        for stmt in DDL.split(";"):
            if stmt.strip():
                conn.execute(text(stmt))
    print("OK: silver schema and tables are ready")

if __name__ == "__main__":
    main()