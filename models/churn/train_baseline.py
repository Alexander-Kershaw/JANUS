from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import text
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, average_precision_score
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

from janus.db import make_engine


REPORTS_DIR = Path("reports/model_cards")
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# Predictive features from gold churn labels table
FEATURE_COLS = [
    "events_7d",
    "sessions_7d",
    "feature_use_7d",
    "support_tickets_14d",
    "late_rate_7d",
]


TARGET = "churn_7d"


def load_data() -> pd.DataFrame:
    sql = """
    select
        date_day,
        user_id,
        plan_id,
        events_7d,
        sessions_7d,
        feature_use_7d,
        support_tickets_14d,
        late_rate_7d,
        churn_7d
    from dbt.gold_user_features_daily
    order by date_day, user_id
    """
    eng = make_engine()
    return pd.read_sql(text(sql), eng)



def main() -> None:
    df = load_data()

    # time split: last day is test, earlier days are train 
    df["date_day"] = pd.to_datetime(df["date_day"]).dt.date
    df = df.sort_values(["date_day", "user_id"]).reset_index(drop=True)

    unique_days = sorted(df["date_day"].unique())
    if len(unique_days) < 2:
        raise ValueError(f"Need at least 2 days to split, got {len(unique_days)}")

    test_day = unique_days[-1]
    train_mask = df["date_day"] < test_day
    test_mask = df["date_day"] == test_day

    X = df[FEATURE_COLS].copy()
    y = df["churn_7d"].astype(int).copy()

    X_train, y_train = X.loc[train_mask], y.loc[train_mask]
    X_test, y_test = X.loc[test_mask], y.loc[test_mask]

    print("SPLIT_CHECK:")
    print("  train_days:", unique_days[:-1])
    print("  test_day:", test_day)
    print("  X_train:", X_train.shape, "y_train:", y_train.shape, "pos_train:", int(y_train.sum()))
    print("  X_test :", X_test.shape,  "y_test :", y_test.shape,  "pos_test :", int(y_test.sum()))

    if X_train.shape[0] == 0 or X_test.shape[0] == 0:
        raise ValueError("Empty train/test split. Check date_day values and split logic.")


    pipeline = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "model",
                LogisticRegression(
                    max_iter=1000,
                    class_weight="balanced",
                    solver="lbfgs",
                ),
            ),
        ]
    )

    pipeline.fit(X_train, y_train)

    probs = pipeline.predict_proba(X_test)[:, 1]

    # Useful metrics
    metrics = {
        "rows_train": int(len(X_train)),
        "rows_test": int(len(X_test)),
        "churn_rate_train": float(y_train.mean()),
        "churn_rate_test": float(y_test.mean()),
        "roc_auc": float(roc_auc_score(y_test, probs)),
        "pr_auc": float(average_precision_score(y_test, probs)),
    }

    with open(REPORTS_DIR / "churn_baseline_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    # Coefficients
    coefs = (
        pd.Series(
            pipeline.named_steps["model"].coef_[0],
            index=FEATURE_COLS,
            name="coef",
        )
        .sort_values(ascending=False)
        .reset_index()
        .rename(columns={"index": "feature"})
    )

    coefs.to_csv(REPORTS_DIR / "churn_baseline_coefficients.csv", index=False)

    print("Baseline churn model trained")
    print(json.dumps(metrics, indent=2))
    print("\nTop coefficients:")
    print(coefs)


if __name__ == "__main__":
    main()