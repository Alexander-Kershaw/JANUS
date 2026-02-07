"""
---------------------------------------------------------------------------------------------------
*** UPDATED CHURN MODEL

---------------------------------------------------------------------------------------------------
JANUS - Churn baseline training (temporal CV)
---------------------------------------------------------------------------------------------------

- Loads predictive features from dbt.gold_user_features_daily
- Applies label censoring to avoid leakage issues from previous iterations
- Runs weekly temporal CV (7 day test windows by default instead of previous 5)
- Trains a final baseline model on all censored data
- Saves:
    models/churn/artifacts/baseline_model.joblib
    reports/model_cards/churn_baseline_metrics.json

Justification:

- Earlier iterations saw 92 percent churn on the last day
- Churn label = churn on the next 7 days, therefore the last 6 days of data presents issues
    since there is not future data so can't reliably present any churn events
- The dataset tail can end up having unusually massive positive, negative, or missing labels due
    to the churn label temporal window
- So removing the final 7 days from training/evaluation should help to achieve a healthy 
    churn prediction

---------------------------------------------------------------------------------------------------
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import text
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from janus.db import make_engine

MODEL_VERSION = "baseline_v1"

REPORTS_DIR = Path("reports/model_cards")
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# Features pulled from dbt.gold_user_features_daily
NUMERIC_FEATURES = [
    "events_7d",
    "sessions_7d",
    "feature_use_7d",
    "support_tickets_14d",
    "late_rate_7d",
]
CATEGORICAL_FEATURES = [
    "plan_id",
]

TARGET = "churn_7d"


@dataclass(frozen=True)
class TemporalCVConfig:
    # too close to the dataset end -> drops the last `label_horizon_days` days (leakage fix)
    label_horizon_days: int = 7

    # evaluate one day at a time 
    test_window_days: int = 1

    # Minimum number of training days before evaluation starts
    min_train_days: int = 21

    # Skip folds where y_test has 0 positives
    skip_if_no_test_positives: bool = True

    # Randomness for the final fit
    seed: int = 7

# Load gold mart features
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
    df = pd.read_sql(text(sql), eng)

    # Canonical types
    df["date_day"] = pd.to_datetime(df["date_day"]).dt.date
    df["churn_7d"] = df["churn_7d"].astype(int)

    # Some rows can have null plan_id, given explicit "unknown" label
    # Note: adaptations to gold mart have a SQL case denoting that null values be given the 
    # free plan (assumption is registering for the service requires a free or paid subscription)
    if "plan_id" in df.columns:
        df["plan_id"] = df["plan_id"].fillna("unknown").astype(str)

    return df


def build_pipeline() -> Pipeline:
    numeric_transform = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value=0.0)),
            ("scaler", StandardScaler()),
        ]
    )

    categorical_transform = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value="unknown")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]
    )

    pre = ColumnTransformer(
        transformers=[
            ("num", numeric_transform, NUMERIC_FEATURES),
            ("cat", categorical_transform, CATEGORICAL_FEATURES),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )

    model = LogisticRegression(
        max_iter=2000,
        class_weight="balanced",
        solver="lbfgs",
    )

    return Pipeline(steps=[("pre", pre), ("model", model)])


def temporal_day_folds(df: pd.DataFrame, cfg: TemporalCVConfig) -> Tuple[List[date], date]:
    days = sorted(df["date_day"].unique())
    if len(days) < 2:
        raise ValueError(f"Need at least 2 days of data, got {len(days)}")

    # Censor tail 
    effective_max = days[-1]  # max day present
    cutoff = effective_max.fromordinal(effective_max.toordinal() - cfg.label_horizon_days)

    eligible_days = [d for d in days if d <= cutoff]
    if len(eligible_days) < max(2, cfg.min_train_days + cfg.test_window_days):
        raise ValueError(
            "Not enough eligible days after censoring. "
            f"days_total={len(days)} eligible_days={len(eligible_days)} "
            f"cutoff={cutoff} label_horizon_days={cfg.label_horizon_days}"
        )

    return eligible_days, cutoff


def evaluate_fold(
    pipeline: Pipeline,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
) -> Dict[str, float]:
    pipeline.fit(X_train, y_train)
    probs = pipeline.predict_proba(X_test)[:, 1]

    out: Dict[str, float] = {
        "rows_train": int(len(X_train)),
        "rows_test": int(len(X_test)),
        "positives_train": int(y_train.sum()),
        "positives_test": int(y_test.sum()),
        "churn_rate_train": float(y_train.mean()) if len(y_train) else float("nan"),
        "churn_rate_test": float(y_test.mean()) if len(y_test) else float("nan"),
        "pr_auc": float("nan"),
        "roc_auc": float("nan"),
    }

    # ROC-AUC requires both classes in y_test
    if y_test.nunique() == 2:
        out["roc_auc"] = float(roc_auc_score(y_test, probs))

    # PR-AUC is defined even with all-zeros test
    out["pr_auc"] = float(average_precision_score(y_test, probs))
    return out


def main() -> None:
    cfg = TemporalCVConfig()

    df = load_data()
    df = df.sort_values(["date_day", "user_id"]).reset_index(drop=True)

    # Useful checks for functionality
    nulls = df[["date_day", "user_id", "plan_id", *NUMERIC_FEATURES, TARGET]].isna().sum().to_dict()
    print("DATA_SHAPE:", df.shape)
    print("DATE_RANGE:", df["date_day"].min(), "->", df["date_day"].max())
    print("CHURN_COUNTS:", df[TARGET].value_counts().to_dict())
    print("NULLS:", nulls)

    eligible_days, cutoff = temporal_day_folds(df, cfg)

    # Walk-forward CV window -> train on first N days, test on next day, repeat moving CV window forward
    fold_rows: List[Dict[str, object]] = []
    pipeline = build_pipeline()

    # evaluation window is one day ahead folds
    # training_days = eligible_days[:i], test_day = eligible_days[i]
    start_i = cfg.min_train_days
    for i in range(start_i, len(eligible_days)):
        test_day = eligible_days[i]
        train_days = eligible_days[:i]

        train_mask = df["date_day"].isin(train_days)
        test_mask = df["date_day"] == test_day

        X = df[[*NUMERIC_FEATURES, *CATEGORICAL_FEATURES]].copy()
        y = df[TARGET].copy()

        X_train, y_train = X.loc[train_mask], y.loc[train_mask]
        X_test, y_test = X.loc[test_mask], y.loc[test_mask]

        positives_test = int(y_test.sum())
        if cfg.skip_if_no_test_positives and positives_test == 0:
            # Still record the fold but skip metrics
            fold_rows.append(
                {
                    "test_day": test_day.isoformat(),
                    "rows_test": int(len(X_test)),
                    "positives_test": positives_test,
                    "skipped": True,
                    "pr_auc": float("nan"),
                    "roc_auc": float("nan"),
                }
            )
            continue

        metrics = evaluate_fold(pipeline, X_train, y_train, X_test, y_test)
        fold_rows.append(
            {
                "test_day": test_day.isoformat(),
                "rows_test": int(metrics["rows_test"]),
                "positives_test": int(metrics["positives_test"]),
                "skipped": False,
                "pr_auc": float(metrics["pr_auc"]),
                "roc_auc": float(metrics["roc_auc"]),
            }
        )

    folds_df = pd.DataFrame(fold_rows)

    # Summaries
    def finite_series(s: pd.Series) -> pd.Series:
        return s.replace([np.inf, -np.inf], np.nan).dropna()

    used = folds_df.loc[~folds_df["skipped"]].copy()
    pr_used = finite_series(used["pr_auc"]) if not used.empty else pd.Series(dtype=float)
    roc_used = finite_series(used["roc_auc"]) if not used.empty else pd.Series(dtype=float)

    summary = {
        "label_horizon_days": cfg.label_horizon_days,
        "min_train_days": cfg.min_train_days,
        "cutoff_day_inclusive": cutoff.isoformat(),
        "days_total": int(df["date_day"].nunique()),
        "days_eligible_after_censor": int(len(eligible_days)),
        "rows_total": int(len(df)),
        "positives_total": int(df[TARGET].sum()),
        "folds_total": int(len(folds_df)),
        "folds_used": int(len(used)),
        "folds_skipped_no_test_positives": int((folds_df["skipped"] == True).sum()),
        "pr_auc_mean": float(pr_used.mean()) if len(pr_used) else float("nan"),
        "pr_auc_std": float(pr_used.std(ddof=1)) if len(pr_used) > 1 else float("nan"),
        "roc_auc_mean": float(roc_used.mean()) if len(roc_used) else float("nan"),
        "roc_auc_std": float(roc_used.std(ddof=1)) if len(roc_used) > 1 else float("nan"),
    }

    # CV results
    (REPORTS_DIR / "churn_temporal_cv_summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    folds_df.to_csv(REPORTS_DIR / "churn_temporal_cv_folds.csv", index=False)

    print("\nTEMPORAL CV SUMMARY")
    print(pd.DataFrame({"pr_auc": pr_used, "roc_auc": roc_used}).describe().loc[["mean", "std", "count"]])

    print("\nLAST 10 FOLDS")
    tail = folds_df.tail(10).copy()
    for col in ["pr_auc", "roc_auc"]:
        if col in tail.columns:
            tail[col] = tail[col].map(lambda x: f"{x:.6f}" if pd.notna(x) else "NaN")
    print(tail.to_string(index=False))

    # Final fit train on all eligible days (except the last eligible day),
    # then export coefficients
    final_days = eligible_days  # all eligible after censoring dataset tail
    final_mask = df["date_day"].isin(final_days)

    X_all = df.loc[final_mask, [*NUMERIC_FEATURES, *CATEGORICAL_FEATURES]].copy()
    y_all = df.loc[final_mask, TARGET].copy()

    final_model = build_pipeline()
    final_model.fit(X_all, y_all)

    # Export coefficients 
    pre: ColumnTransformer = final_model.named_steps["pre"] 
    feature_names = pre.get_feature_names_out()

    lr: LogisticRegression = final_model.named_steps["model"]
    coefs = pd.DataFrame(
        {"feature": feature_names, "coef": lr.coef_[0]},
    ).sort_values("coef", ascending=False)

    coefs.to_csv(REPORTS_DIR / "churn_baseline_coefficients.csv", index=False)

    # Final model metadata
    (REPORTS_DIR / "churn_baseline_final_fit.json").write_text(
        json.dumps(
            {
                "rows_fit": int(len(X_all)),
                "positives_fit": int(y_all.sum()),
                "churn_rate_fit": float(y_all.mean()),
                "notes": "Final fit trained on all eligible days after censoring. "
                "Use temporal CV files for performance.",
            },
            indent=2,
        )
        + "\n"
    )

    print("\nBaseline churn model trained (final fit on eligible days)")
    print("\nTop coefficients:")
    print(coefs.head(25).to_string(index=False))


    # Model monitoring (persist fold metrics)
    folds_path = REPORTS_DIR / "churn_baseline_v1_temporal_cv_folds.csv"
    folds_df.to_csv(folds_path, index=False)

    summary = {
        "n_folds_total": int(len(folds_df)),
        "n_folds_skipped": int(folds_df["skipped"].sum()) if "skipped" in folds_df.columns else 0,
        "pr_auc_mean": float(folds_df.loc[~folds_df["skipped"], "pr_auc"].mean()),
        "pr_auc_std": float(folds_df.loc[~folds_df["skipped"], "pr_auc"].std(ddof=0)),
        "roc_auc_mean": float(folds_df.loc[~folds_df["skipped"], "roc_auc"].mean()),
        "roc_auc_std": float(folds_df.loc[~folds_df["skipped"], "roc_auc"].std(ddof=0)),
    }

    with open(REPORTS_DIR / "churn_baseline_v1_temporal_cv_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\nWrote CV folds -> {folds_path}")
    print("Wrote CV summary -> reports/model_cards/churn_baseline_v1_temporal_cv_summary.json")



if __name__ == "__main__":
    main()
