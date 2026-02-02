from __future__ import annotations

import argparse
import csv
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict

# Plans and pricing
PLANS = [
    {"plan_id": "free", "price_gbp": 0},
    {"plan_id": "basic", "price_gbp": 19},
    {"plan_id": "pro", "price_gbp": 49},
    {"plan_id": "team", "price_gbp": 149},
]


@dataclass
class Config:
    start_date: str
    days: int
    users: int
    seed: int
    new_sub_rate: float
    cancel_rate: float
    upgrade_rate: float


def ensure_billing_dir(repo_root: Path) -> Path:
    p = repo_root / "data" / "raw" / "billing"
    p.mkdir(parents=True, exist_ok=True)
    return p


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", default="2026-01-01")
    ap.add_argument("--days", type=int, default=2)
    ap.add_argument("--users", type=int, default=50)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--new-sub-rate", type=float, default=0.04, help="fraction of users starting a paid sub per day")
    ap.add_argument("--cancel-rate", type=float, default=0.01, help="fraction of active subs cancelling per day")
    ap.add_argument("--upgrade-rate", type=float, default=0.01, help="fraction of active subs upgrading per day")
    args = ap.parse_args()

    cfg = Config(
        start_date=args.start_date,
        days=args.days,
        users=args.users,
        seed=args.seed,
        new_sub_rate=args.new_sub_rate,
        cancel_rate=args.cancel_rate,
        upgrade_rate=args.upgrade_rate,
    )

    repo_root = Path(__file__).resolve().parents[2]
    out_dir = ensure_billing_dir(repo_root)

    rng = random.Random(cfg.seed)
    user_ids = [f"usr_{i:05d}" for i in range(1, cfg.users + 1)]
    start = datetime.fromisoformat(cfg.start_date).replace(tzinfo=timezone.utc).date()

    # Current subscribers and plans they have
    active_plan: Dict[str, str] = {}

    for d in range(cfg.days):
        day: date = start + timedelta(days=d)
        day_str = day.isoformat()

        rows: List[Dict[str, str]] = []

        # New subscriptions
        for u in user_ids:
            if u not in active_plan and rng.random() < cfg.new_sub_rate:
                plan = rng.choice([p["plan_id"] for p in PLANS if p["plan_id"] != "free"])
                active_plan[u] = plan
                rows.append(
                    {
                        "billing_date": day_str,
                        "user_id": u,
                        "event": "start",
                        "plan_id": plan,
                    }
                )

        # Upgrades and cancels among active subscriptions
        for u, plan in list(active_plan.items()):
            if rng.random() < cfg.upgrade_rate and plan in ("basic", "pro"):
                new_plan = "pro" if plan == "basic" else "team"
                active_plan[u] = new_plan
                rows.append(
                    {
                        "billing_date": day_str,
                        "user_id": u,
                        "event": "upgrade",
                        "plan_id": new_plan,
                    }
                )

            if rng.random() < cfg.cancel_rate:
                rows.append(
                    {
                        "billing_date": day_str,
                        "user_id": u,
                        "event": "cancel",
                        "plan_id": active_plan[u],
                    }
                )
                del active_plan[u]

        out_path = out_dir / f"{day_str}.csv"
        with out_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["billing_date", "user_id", "event", "plan_id"])
            w.writeheader()
            w.writerows(rows)

        print(f"[billing] wrote {len(rows):,} rows -> {out_path}")

    print("COMPLETE")


if __name__ == "__main__":
    main()