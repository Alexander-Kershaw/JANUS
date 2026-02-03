from sqlalchemy import create_engine, text

"""
---------------------------------------------------------------------------------------------------
Transformation of bronze.bronze_events -> silver.silver_events

Silver transformation steps:
- Quarantine rows missing critical fields
- Deduplicate by event_id using latest received_ts (then ingestion_ts)
- Compute late-arrival indicators

---------------------------------------------------------------------------------------------------

Rules implemented:
- bronze events graduate to silver onlf if its fields are not null
- Otherwise, events are routed to quarantine store with raw record and metadata
- Deduplication keeps only 1 row per event_id, the row that is preserved
    is the duplicate with the latest recieved_ts (late-arrival is prioritised),
    if arrival time is tied, ingestion_ts is used instead in same manner
- Late arrival metrics:
    - lateness_sec = max(0, received_ts - event_ts)
    - is_late = lateness_sex > 0
---------------------------------------------------------------------------------------------------
"""

WAREHOUSE_URL = "postgresql+psycopg2://janus:janus_password@localhost:5433/airflow"

SQL = """
TRUNCATE TABLE silver.silver_events;
TRUNCATE TABLE silver.silver_events_quarantine;

INSERT INTO silver.silver_events_quarantine (
    bronze_row_hash, source_file, ingestion_ts,
    reason_code, reason_detail,
    raw_record
)
SELECT
    b.row_hash,
    b.source_file,
    b.ingestion_ts,
    CASE
      WHEN b.event_id IS NULL THEN 'missing_event_id'
      WHEN b.event_ts IS NULL THEN 'missing_event_ts'
      WHEN b.received_ts IS NULL THEN 'missing_received_ts'
      WHEN b.event_type IS NULL THEN 'missing_event_type'
      ELSE 'unknown_invalid'
    END AS reason_code,
    NULL::text AS reason_detail,
    jsonb_build_object(
      'event_id', b.event_id,
      'event_ts', b.event_ts,
      'received_ts', b.received_ts,
      'user_id', b.user_id,
      'device_id', b.device_id,
      'session_id', b.session_id,
      'event_type', b.event_type,
      'props', b.props,
      'source_file', b.source_file,
      'ingestion_ts', b.ingestion_ts,
      'row_hash', b.row_hash
    ) AS raw_record
FROM bronze.bronze_events b
WHERE b.event_id IS NULL
   OR b.event_ts IS NULL
   OR b.received_ts IS NULL
   OR b.event_type IS NULL;

INSERT INTO silver.silver_events (
    event_id, event_ts, received_ts, user_id, device_id, session_id, event_type, props,
    bronze_row_hash, source_file, ingestion_ts,
    is_late, lateness_sec
)
WITH valid AS (
    SELECT
        b.*,
        EXTRACT(EPOCH FROM (b.received_ts - b.event_ts))::int AS lateness_sec_raw
    FROM bronze.bronze_events b
    WHERE b.event_id IS NOT NULL
      AND b.event_ts IS NOT NULL
      AND b.received_ts IS NOT NULL
      AND b.event_type IS NOT NULL
),
ranked AS (
    SELECT
        v.*,
        ROW_NUMBER() OVER (
            PARTITION BY v.event_id
            ORDER BY v.received_ts DESC, v.ingestion_ts DESC
        ) AS rn
    FROM valid v
)
SELECT
    r.event_id,
    r.event_ts,
    r.received_ts,
    r.user_id,
    r.device_id,
    r.session_id,
    r.event_type,
    COALESCE(r.props, '{}'::jsonb) AS props,
    r.row_hash AS bronze_row_hash,
    r.source_file,
    r.ingestion_ts,
    (GREATEST(r.lateness_sec_raw, 0) > 0) AS is_late,
    GREATEST(r.lateness_sec_raw, 0) AS lateness_sec
FROM ranked r
WHERE r.rn = 1;
"""

def main() -> None:
    engine = create_engine(WAREHOUSE_URL, future=True)
    with engine.begin() as conn:
        for stmt in SQL.split(";"):
            if stmt.strip():
                conn.execute(text(stmt))
    print("OK: bronze -> silver events transformation complete")

if __name__ == "__main__":
    main()