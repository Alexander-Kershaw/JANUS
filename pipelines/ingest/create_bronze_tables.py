from sqlalchemy import create_engine, text


WAREHOUSE_URL = "postgresql+psycopg2://janus:janus_password@localhost:5433/airflow"


DDL = """
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.bronze_events (
    event_id        TEXT,
    event_ts        TIMESTAMPTZ,
    received_ts     TIMESTAMPTZ,
    user_id         TEXT,
    device_id       TEXT,
    session_id      TEXT,
    event_type      TEXT,
    props           JSONB,

    source_file     TEXT NOT NULL,
    ingestion_ts    TIMESTAMPTZ NOT NULL,
    row_hash        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.bronze_billing (
    billing_date    DATE,
    user_id         TEXT,
    event           TEXT,
    plan_id         TEXT,

    source_file     TEXT NOT NULL,
    ingestion_ts    TIMESTAMPTZ NOT NULL,
    row_hash        TEXT NOT NULL
);
"""


def main() -> None:
    engine = create_engine(WAREHOUSE_URL, future=True)

    with engine.begin() as conn:
        for stmt in DDL.split(";"):
            if stmt.strip():
                conn.execute(text(stmt))

    print("OK: prepared bronze schema and tables")


if __name__ == "__main__":
    main()