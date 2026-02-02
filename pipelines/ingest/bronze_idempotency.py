from sqlalchemy import create_engine, text

WAREHOUSE_URL = "postgresql+psycopg2://janus:janus_password@localhost:5433/airflow"

SQL = """
-- Events
ALTER TABLE bronze.bronze_events
  ADD CONSTRAINT bronze_events_row_hash_uk UNIQUE (row_hash);

CREATE INDEX IF NOT EXISTS bronze_events_ingestion_ts_idx
  ON bronze.bronze_events (ingestion_ts);

CREATE INDEX IF NOT EXISTS bronze_events_event_ts_idx
  ON bronze.bronze_events (event_ts);

-- Billing
ALTER TABLE bronze.bronze_billing
  ADD CONSTRAINT bronze_billing_row_hash_uk UNIQUE (row_hash);

CREATE INDEX IF NOT EXISTS bronze_billing_ingestion_ts_idx
  ON bronze.bronze_billing (ingestion_ts);

CREATE INDEX IF NOT EXISTS bronze_billing_billing_date_idx
  ON bronze.bronze_billing (billing_date);
"""

def main() -> None:
    engine = create_engine(WAREHOUSE_URL, future=True)
    with engine.begin() as conn:
        for stmt in SQL.split(";"):
            s = stmt.strip()
            if not s:
                continue
            try:
                conn.execute(text(s))
            except Exception as e:
                msg = str(e).lower()
                if "already exists" in msg or "duplicate" in msg:
                    print(f"SKIP: {s.splitlines()[0]} (already exists)")
                else:
                    raise
    print("OK: bronze idempotency constraints/indexes prepared")

if __name__ == "__main__":
    main()