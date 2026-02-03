from __future__ import annotations

import argparse
import csv
import hashlib
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List

from sqlalchemy import create_engine, text

WAREHOUSE_URL = "postgresql+psycopg2://janus:janus_password@localhost:5433/airflow"

INSERT_SQL = """
INSERT INTO bronze.bronze_billing (
    billing_date, user_id, event, plan_id,
    source_file, ingestion_ts, row_hash
)
VALUES (
    :billing_date, :user_id, :event, :plan_id,
    :source_file, :ingestion_ts, :row_hash
)
ON CONFLICT (row_hash) DO NOTHING;
"""

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def compute_row_hash(source_file: str, row: Dict[str, str]) -> str:
    payload = source_file + "|" + "|".join(
        f"{k}={row.get(k, '').strip()}" for k in ("billing_date", "user_id", "event", "plan_id")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def parse_date(s: str) -> date:
    return date.fromisoformat(s.strip())

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--billing-dir", default="data/raw/billing")
    ap.add_argument("--glob", default="*.csv")
    ap.add_argument("--limit-files", type=int, default=0)
    ap.add_argument("--batch-size", type=int, default=1000)
    args = ap.parse_args()

    billing_dir = Path(args.billing_dir)
    if not billing_dir.exists():
        raise SystemExit(f"ERROR: billing dir not found: {billing_dir}")

    files = sorted(billing_dir.glob(args.glob))
    if args.limit_files and args.limit_files > 0:
        files = files[: args.limit_files]

    if not files:
        raise SystemExit(f"ERROR: no files matched {billing_dir}/{args.glob}")

    engine = create_engine(WAREHOUSE_URL, future=True)
    ingestion_ts = utc_now()

    total_read = 0
    total_insert_attempts = 0

    with engine.begin() as conn:
        for fp in files:
            source_file = str(fp)
            file_read = 0
            batch: List[Dict[str, object]] = []

            with fp.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    file_read += 1
                    total_read += 1

                    params = {
                        "billing_date": parse_date(row["billing_date"]),
                        "user_id": row.get("user_id"),
                        "event": row.get("event"),
                        "plan_id": row.get("plan_id"),
                        "source_file": source_file,
                        "ingestion_ts": ingestion_ts,
                        "row_hash": compute_row_hash(source_file, row),
                    }
                    batch.append(params)

                    if len(batch) >= args.batch_size:
                        conn.execute(text(INSERT_SQL), batch)
                        total_insert_attempts += len(batch)
                        batch.clear()

            if batch:
                conn.execute(text(INSERT_SQL), batch)
                total_insert_attempts += len(batch)

            print(f"[billing->bronze] file={fp.name} read={file_read:,}")

    print(f"COMPLETE: records_read={total_read:,} insert_attempts={total_insert_attempts:,}")

if __name__ == "__main__":
    main()