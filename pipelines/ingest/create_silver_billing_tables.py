from sqlalchemy import create_engine, text

"""
---------------------------------------------------------------------------------------------------
Silver tables for billing timeline

Silver billing ensures:
- Typed and standardized events (start/upgrade/cancel)
- Foundation for subscription timeline and revenue facts 
---------------------------------------------------------------------------------------------------
"""

WAREHOUSE_URL = "postgresql+psycopg2://janus:janus_password@localhost:5433/airflow"

DDL = """
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.silver_billing (
    billing_date    DATE NOT NULL,
    user_id         TEXT NOT NULL,
    event           TEXT NOT NULL,
    plan_id         TEXT NOT NULL,

    bronze_row_hash TEXT NOT NULL,
    source_file     TEXT NOT NULL,
    ingestion_ts    TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.silver_billing_quarantine (
    bronze_row_hash TEXT NOT NULL,
    source_file     TEXT NOT NULL,
    ingestion_ts    TIMESTAMPTZ NOT NULL,

    reason_code     TEXT NOT NULL,
    raw_record      JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS silver_billing_date_idx
  ON silver.silver_billing (billing_date);

CREATE INDEX IF NOT EXISTS silver_billing_user_idx
  ON silver.silver_billing (user_id);
"""

def main() -> None:
    engine = create_engine(WAREHOUSE_URL, future=True)
    with engine.begin() as conn:
        for stmt in DDL.split(";"):
            if stmt.strip():
                conn.execute(text(stmt))
    print("OK: silver billing tables are ready")

if __name__ == "__main__":
    main()