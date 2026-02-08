from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from janus.db import make_engine


# Fix: CREATE INDEX IF NOT EXISTS ... is idempotent, but alter table statements are not
# Since CREATE INDEX IS NOT EXISTS is not supported for UNIQUE constraints, DO $$ ... $$ blocking
# Checks the constraint 
STATEMENTS: list[str] = [
    """
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'bronze'
          AND t.relname = 'bronze_events'
          AND c.conname = 'bronze_events_row_hash_uk'
      ) THEN
        ALTER TABLE bronze.bronze_events
          ADD CONSTRAINT bronze_events_row_hash_uk UNIQUE (row_hash);
      END IF;
    END $$;
    """,
    "CREATE INDEX IF NOT EXISTS bronze_events_ingestion_ts_idx ON bronze.bronze_events (ingestion_ts);",
    "CREATE INDEX IF NOT EXISTS bronze_events_event_ts_idx ON bronze.bronze_events (event_ts);",

    """
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'bronze'
          AND t.relname = 'bronze_billing'
          AND c.conname = 'bronze_billing_row_hash_uk'
      ) THEN
        ALTER TABLE bronze.bronze_billing
          ADD CONSTRAINT bronze_billing_row_hash_uk UNIQUE (row_hash);
      END IF;
    END $$;
    """,
    "CREATE INDEX IF NOT EXISTS bronze_billing_ingestion_ts_idx ON bronze.bronze_billing (ingestion_ts);",
    "CREATE INDEX IF NOT EXISTS bronze_billing_billing_date_idx ON bronze.bronze_billing (billing_date);",
]

VERIFY_SQL = """
select
  n.nspname as schema,
  t.relname as table,
  c.conname as constraint_name,
  c.contype as constraint_type
from pg_constraint c
join pg_class t on t.oid = c.conrelid
join pg_namespace n on n.oid = t.relnamespace
where n.nspname = 'bronze'
  and c.conname in (
    'bronze_events_row_hash_uk',
    'bronze_billing_row_hash_uk'
  )
order by t.relname;

select
  schemaname,
  tablename,
  indexname
from pg_indexes
where schemaname = 'bronze'
  and indexname in (
    'bronze_events_ingestion_ts_idx',
    'bronze_events_event_ts_idx',
    'bronze_billing_ingestion_ts_idx',
    'bronze_billing_billing_date_idx'
  )
order by tablename, indexname;
"""

def main() -> None:
    engine = make_engine()

    for i, stmt in enumerate(STATEMENTS, start=1):
        preview = stmt.strip().splitlines()[0][:120]
        try:
            with engine.begin() as conn:
                conn.execute(text(stmt))
            print(f"OK [{i}/{len(STATEMENTS)}]: {preview}")
        except SQLAlchemyError as e:
            print(f"\nFAILED [{i}/{len(STATEMENTS)}]: {preview}")
            print("STATEMENT:\n", stmt.strip())
            print("\nERROR:\n", str(e))
            raise

    print("\nOK: bronze idempotency constraints/indexes prepared")

    # Verification that constants and indexes patch is effective
    print("\nVERIFY: constraints and indexes")

    with engine.begin() as conn:
        result_sets = conn.execute(text(VERIFY_SQL)).fetchall()

    if not result_sets:
        raise RuntimeError("Verification failed: no constraints or indexes found")

    print("OK: bronze constraints and indexes verified")


if __name__ == "__main__":
    main()