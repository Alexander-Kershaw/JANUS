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
FEATURES = [
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


def temporal_split(df: pd.DataFrame, test_days: int = 7):

    # Trains on all but last N days, test on last N days, creating test train split.

    cutoff = df["date_day"].max() - pd.Timedelta(days=test_days)
    train = df[df["date_day"] <= cutoff]
    test = df[df["date_day"] > cutoff]
    return train, test


def main() -> None:
    df = load_data()

    df = df.copy()
    df["plan_id"] = df["plan_id"].fillna("unknown")
    df = pd.get_dummies(df, columns=["plan_id"], drop_first=True)
    train, test = temporal_split(df, test_days=7)

    feature_cols = FEATURES + [c for c in train.columns if c.startswith("plan_id_")]
    X_train = train[feature_cols]
    y_train = train[TARGET]
    X_test = test[feature_cols]
    y_test = test[TARGET]

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
        "rows_train": int(len(train)),
        "rows_test": int(len(test)),
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
            index=feature_cols,
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