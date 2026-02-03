from sqlalchemy import create_engine, text

"""
------------------------------------------------------------------------------------------------
Transform bronze.bronze_billing -> silver.silver_billing

Serves to:
- Quarantine invalid rows
- Promote valid billing events
------------------------------------------------------------------------------------------------
"""

WAREHOUSE_URL = "postgresql+psycopg2://janus:janus_password@localhost:5433/airflow"

SQL = """
TRUNCATE TABLE silver.silver_billing;
TRUNCATE TABLE silver.silver_billing_quarantine;

INSERT INTO silver.silver_billing_quarantine (
    bronze_row_hash, source_file, ingestion_ts,
    reason_code, raw_record
)
SELECT
    b.row_hash,
    b.source_file,
    b.ingestion_ts,
    CASE
      WHEN b.billing_date IS NULL THEN 'missing_billing_date'
      WHEN b.user_id IS NULL THEN 'missing_user_id'
      WHEN b.event IS NULL THEN 'missing_event'
      WHEN b.event NOT IN ('start', 'upgrade', 'cancel') THEN 'invalid_event'
      WHEN b.plan_id IS NULL THEN 'missing_plan_id'
      ELSE 'unknown_invalid'
    END AS reason_code,
    jsonb_build_object(
      'billing_date', b.billing_date,
      'user_id', b.user_id,
      'event', b.event,
      'plan_id', b.plan_id,
      'source_file', b.source_file,
      'ingestion_ts', b.ingestion_ts,
      'row_hash', b.row_hash
    ) AS raw_record
FROM bronze.bronze_billing b
WHERE b.billing_date IS NULL
   OR b.user_id IS NULL
   OR b.event IS NULL
   OR b.event NOT IN ('start', 'upgrade', 'cancel')
   OR b.plan_id IS NULL;

INSERT INTO silver.silver_billing (
    billing_date, user_id, event, plan_id,
    bronze_row_hash, source_file, ingestion_ts
)
SELECT
    b.billing_date,
    b.user_id,
    b.event,
    b.plan_id,
    b.row_hash AS bronze_row_hash,
    b.source_file,
    b.ingestion_ts
FROM bronze.bronze_billing b
WHERE b.billing_date IS NOT NULL
  AND b.user_id IS NOT NULL
  AND b.event IN ('start', 'upgrade', 'cancel')
  AND b.plan_id IS NOT NULL;
"""

def main() -> None:
    engine = create_engine(WAREHOUSE_URL, future=True)
    with engine.begin() as conn:
        for stmt in SQL.split(";"):
            if stmt.strip():
                conn.execute(text(stmt))
    print("OK: bronze -> silver billing transformation complete")

if __name__ == "__main__":
    main()