# JANUS Churn Model Report: Baseline v1

**Model name:** churn_baseline_v1  
**Goal:** Predict whether a user will churn within the next 7 days (`churn_7d`) using behavioral and operational signals.  
**Scope:** Offline baseline. Not deployed. Designed to be a trustworthy benchmark for future models.

---

## Executive summary

Trained a baseline churn classifier using **90 days of synthetic product telemetry** and subscription/billing activity. The syntehtic product telemetry data was generated in such a way that it is realistic with injected data discrepencies, schema violations, and so forth.
Evaluation uses **temporal cross-validation** with a daily window that walks forward in time to avoid leakage.

**Result:** The model shows **modest discriminative power** (ROC-AUC > 0.5 on average), but **precision is low** because churn is rare. This was anticipated. Despite this, baseline v1 is still valuable because:

- It is evaluated correctly (time-aware, no random split).
- It produces stable, explainable coefficients.
- It provides a measurable benchmark to beat (future models must exceed it using the same CV protocol).

---

## Data and pipeline context

### Data sources
- **Events (telemetry):** `silver.silver_events` derived from raw JSONL (90 daily files).
- **Billing/subscriptions:** `silver.silver_billing` derived from raw CSV (90 daily files).
- **dbt marts used for modeling:**
  - `dbt.gold_user_features_daily` (feature matrix and label)
  - `dbt.gold_churn_labels_daily` (label definition and censoring logic)

### Why 90 days matters 

Initial development of the churn model encountered issues due to shallow temporal depth, with only ~14 days, the churn label becomes fragile:
- The final days are **right-censored** (The next 7 days are not known), which can create an abnormal explosion in churn signals.
- Some test days contain **zero positives**, breaking metrics like ROC-AUC and PR-AUC.

With ~90 days, churn appears across time and the model can be evaluated without structural idiosyncracies causing problems.

---

## Label definition

**Target:** `churn_7d`  
Interpretation: user is active on day D and becomes inactive (churn event) within the next 7 days.

### Censoring
Days too close to the dataset end are excluded from training/evaluation because churn outcomes are unknown beyond the window. This is important since the lack of data after the dataset end gives the illusion that nearly all users have churned from the model's perspective, causing an explosion in churn rate. 
This is enforced in the dbt gold layer so the modeling code does not “cheat”.

---

## Features (Baseline v1)

Features are intentionally simple. They are computed daily per user from the gold model.

- `events_7d` — number of events in trailing 7 days  
- `sessions_7d` — number of sessions in trailing 7 days  
- `feature_use_7d` — count of “feature_use” events in trailing 7 days  
- `support_tickets_14d` — support tickets in trailing 14 days  
- `late_rate_7d` — fraction of events flagged “late” in trailing 7 days  
- `plan_id` — categorical plan tier (including free tier)

**Why these features:**  
They represent engagement, friction, and operational reliability. They are also easy to explain to a non-ML audience.

---

## Model choice

**Algorithm:** Logistic Regression  
**Preprocessing:** StandardScaler for numeric features  
**Class imbalance handling:** `class_weight="balanced"`

### Why logistic regression?
Because it’s a baseline you can actually interrogate. Before introducing different model algorithms and more sophisticated ML methodologies, I elected starting at a relatively simple baseline. Since the logistic regression model (baseline v1) is stable and the time awareness logic is locked in, a more complex approach potentially yielding greater predictive capabilities may be implemented in future.

---

## Evaluation methodology

### Temporal cross-validation
We evaluate using **walk-forward folds by day**:
- Train on all eligible days up to day T-1
- Test on day T
- Skip folds where the test day has 0 positives (metrics are undefined)

This matches how churn prediction would be used in practice: predicting future churn from past behavior.

### Metrics reported
- **ROC-AUC**: ranking quality (can look decent even when churn is rare)
- **PR-AUC**: more realistic under imbalance (will be low when churn prevalence is low)

---

## Results

### Dataset snapshot (Baseline v1 run)
- Rows: ~40k user-days
- Churn positives: ~250 (rare outcome)

### Temporal CV summary
- Mean ROC-AUC and PR-AUC are reported across folds
- Expectation: PR-AUC is low due to rarity; this is expected

**Interpretation:**
- ROC-AUC consistently above 0.5 suggests the model ranks churners somewhat higher than non-churners.
- PR-AUC remains small because even a good ranker struggles when positives are scarce.

---

## Model interpretability (coefficients)

Baseline v1 coefficients typically align with product logic:

- **Higher late_rate_7d:**  higher churn risk (operational pain and dissatisfaction increases churn)
- **Higher engagement (events/sessions):** often lower churn risk (depends on scaling and interactions)
- **Plan tier** strongly affects baseline churn propensity  
  - Free plan can behave very differently, and can dominate coefficient space

**Important note:** Coefficients reflect correlation, not causation. They show what the model is using to make predictions and what has the most prevelant signals, they are not to be taken at face value as the causative factors influencing churn.

---

## Model monitoring

Generated monitoring plots to validate model stability and data behavior:

Recommended plots (produced in notebook):
- Actual churn rate over time
- Predicted score distribution over time
- Fold-level PR-AUC / ROC-AUC trend
- Late-rate trend and relationship to churn
- Churn by plan tier

This is not production monitoring, but it proves the pipeline can produce monitoring-grade artifacts.

---

## Limitations

- This is synthetic data: patterns are realistic, but not a real business, actual data may be more variable and subject to more complex interactions.
- Churn is rare: PR-AUC will look bad even if the model is functioning very well. 
- Features are basic: no embeddings, no sequences, no retention cohort features.
- No calibration step yet (probabilities may not match true churn rates).
- No threshold policy defined (we do not yet choose who to intervene on).

Baseline v1 is not the final model. Future models will address these limitations.

---
