from __future__ import annotations

import argparse
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, List

# Events that will occur in user data
EVENT_TYPES = [
    "page_view",
    "signup",
    "login",
    "feature_use",
    "upgrade",
    "cancel",
    "support_ticket",
    "purchase",
]


@dataclass
class Config:
    start_date: str
    days: int
    users: int
    events_per_day: int
    seed: int
    drift_day: int
    missing_user_rate: float
    duplicate_rate: float
    late_arrival_rate: float


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

# Events and billing directories 
def ensure_dirs(root: Path) -> Dict[str, Path]:
    events_dir = root / "data" / "raw" / "events"
    billing_dir = root / "data" / "raw" / "billing"
    events_dir.mkdir(parents=True, exist_ok=True)
    billing_dir.mkdir(parents=True, exist_ok=True)
    return {"events": events_dir, "billing": billing_dir}


def gen_event(rng: random.Random, user_ids: List[str], day_start: datetime, cfg: Config, k: int) -> Dict[str, Any]:
    # event time during the day
    event_ts = day_start + timedelta(seconds=rng.randint(0, 86399))

    # late arrival -> received later than event_ts
    received_ts = event_ts
    if rng.random() < cfg.late_arrival_rate:
        received_ts = event_ts + timedelta(minutes=rng.randint(10, 6 * 60))

    event_type = rng.choice(EVENT_TYPES)

    # user_id sometimes missing for anomalies
    user_id = rng.choice(user_ids)
    if rng.random() < cfg.missing_user_rate:
        user_id = None

    event = {
        "event_id": f"evt_{day_start.strftime('%Y%m%d')}_{k}_{rng.randint(1000, 9999)}",
        "event_ts": iso(event_ts),
        "received_ts": iso(received_ts),
        "user_id": user_id,
        "device_id": f"dev_{rng.randint(1, cfg.users * 3)}",
        "session_id": f"sess_{rng.randint(1, cfg.users * 10)}",
        "event_type": event_type,
        "props": {
            "channel": rng.choice(["organic", "paid", "referral", "partner"]),
            "country": rng.choice(["GB", "IE", "DE", "FR", "NL"]),
        },
    }

    return event


def write_jsonl(path: Path, events: List[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", default="2026-01-01")
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--users", type=int, default=200)
    ap.add_argument("--events-per-day", type=int, default=2000)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--drift-day", type=int, default=2, help="Day index (0-based) after which ui_variant appears in props")
    ap.add_argument("--missing-user-rate", type=float, default=0.02)
    ap.add_argument("--duplicate-rate", type=float, default=0.01)
    ap.add_argument("--late-arrival-rate", type=float, default=0.05)
    args = ap.parse_args()

    cfg = Config(
        start_date=args.start_date,
        days=args.days,
        users=args.users,
        events_per_day=args.events_per_day,
        seed=args.seed,
        drift_day=args.drift_day,
        missing_user_rate=args.missing_user_rate,
        duplicate_rate=args.duplicate_rate,
        late_arrival_rate=args.late_arrival_rate,
    )

    repo_root = Path(__file__).resolve().parents[2]
    dirs = ensure_dirs(repo_root)

    rng = random.Random(cfg.seed)
    user_ids = [f"usr_{i:05d}" for i in range(1, cfg.users + 1)]

    start = datetime.fromisoformat(cfg.start_date).replace(tzinfo=timezone.utc)

    for d in range(cfg.days):
        day = start + timedelta(days=d)
        day_str = day.date().isoformat()

        events: List[Dict[str, Any]] = []
        for k in range(cfg.events_per_day):
            e = gen_event(rng, user_ids, day, cfg, k)

            # schema drift -> add new prop after drift_day
            if d >= cfg.drift_day:
                e["props"]["ui_variant"] = rng.choice(["A", "B", "C"])

            events.append(e)

            # duplicates -> repeat the same event object 
            if rng.random() < cfg.duplicate_rate:
                events.append(e)

        out_path = dirs["events"] / f"{day_str}.jsonl"
        write_jsonl(out_path, events)

        print(f"[events] wrote {len(events):,} rows -> {out_path}")

    print("COMPLETE")


if __name__ == "__main__":
    main()