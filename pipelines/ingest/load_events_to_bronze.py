from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from sqlalchemy import create_engine, text
from psycopg2.extras import Json

"""
------------------------------------------------------------------------------------------------
Serves to load raw JSONL telemetry events into bronze.bronze_events (idempotent).

For each JSONL file parses JSON computes row hash as SHA256 (canonical representation of
the raw record and the source file name), Including source file makes reruns of the same
file idempotent (duplicated events across two files handled in silver layer), Inserts into
Postgres

Overall strategy:
- Compute row_hash = sha256(source_file + canonical_json_line)
- Insert with ON CONFLICT (row_hash) DO NOTHING
------------------------------------------------------------------------------------------------
"""

WAREHOUSE_URL = "postgresql+psycopg2://janus:janus_password@localhost:5433/airflow"


INSERT_SQL = """
INSERT INTO bronze.bronze_events (
    event_id, event_ts, received_ts, user_id, device_id, session_id, event_type, props,
    source_file, ingestion_ts, row_hash
)
VALUES (
    :event_id, :event_ts, :received_ts, :user_id, :device_id, :session_id, :event_type, :props,
    :source_file, :ingestion_ts, :row_hash
)
ON CONFLICT (row_hash) DO NOTHING;
"""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def canonical_dumps(obj: Dict[str, Any]) -> str:
    # Ensures stable JSON string for hashing
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compute_row_hash(source_file: str, record: Dict[str, Any]) -> str:
    payload = source_file + "|" + canonical_dumps(record)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def parse_iso_z(ts: str | None) -> datetime | None:
    if ts is None:
        return None
    if ts.endswith("Z"):
        ts = ts.replace("Z", "+00:00")
    return datetime.fromisoformat(ts)


def iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def to_params(source_file: str, ingestion_ts: datetime, rec: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "event_id": rec.get("event_id"),
        "event_ts": parse_iso_z(rec.get("event_ts")),
        "received_ts": parse_iso_z(rec.get("received_ts")),
        "user_id": rec.get("user_id"),
        "device_id": rec.get("device_id"),
        "session_id": rec.get("session_id"),
        "event_type": rec.get("event_type"),
        "props": Json(rec.get("props") or {}),
        "source_file": source_file,
        "ingestion_ts": ingestion_ts,
        "row_hash": compute_row_hash(source_file, rec),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events-dir", default="data/raw/events", help="Directory containing daily JSONL partitions")
    ap.add_argument("--glob", default="*.jsonl")
    ap.add_argument("--limit-files", type=int, default=0, help="If >0, only ingest N files (for quick tests)")
    ap.add_argument("--batch-size", type=int, default=500)
    args = ap.parse_args()

    events_dir = Path(args.events_dir)
    if not events_dir.exists():
        raise SystemExit(f"ERROR: events dir not found: {events_dir}")

    files = sorted(events_dir.glob(args.glob))
    if args.limit_files and args.limit_files > 0:
        files = files[: args.limit_files]

    if not files:
        raise SystemExit(f"ERROR: no files matched {events_dir}/{args.glob}")

    engine = create_engine(WAREHOUSE_URL, future=True)
    ingestion_ts = utc_now()

    total_read = 0
    total_insert_attempts = 0

    with engine.begin() as conn:
        for fp in files:
            source_file = str(fp)
            batch: List[Dict[str, Any]] = []
            file_read = 0

            for rec in iter_jsonl(fp):
                file_read += 1
                total_read += 1
                batch.append(to_params(source_file, ingestion_ts, rec))

                if len(batch) >= args.batch_size:
                    conn.execute(text(INSERT_SQL), batch)
                    total_insert_attempts += len(batch)
                    batch.clear()

            if batch:
                conn.execute(text(INSERT_SQL), batch)
                total_insert_attempts += len(batch)

            print(f"[events->bronze] file={fp.name} read={file_read:,}")

    print(f"COMPLETE: records_read={total_read:,} insert_attempts={total_insert_attempts:,}")


if __name__ == "__main__":
    main()