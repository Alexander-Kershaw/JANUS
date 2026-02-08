PIPELINE_HEALTH = """
with
counts as (
  select 'bronze.bronze_events' as table_name, count(*)::int as rows from bronze.bronze_events
  union all
  select 'silver.silver_events', count(*)::int from silver.silver_events
  union all
  select 'silver.silver_events_quarantine', count(*)::int from silver.silver_events_quarantine
  union all
  select 'bronze.bronze_billing', count(*)::int from bronze.bronze_billing
  union all
  select 'silver.silver_billing', count(*)::int from silver.silver_billing
  union all
  select 'silver.silver_billing_quarantine', count(*)::int from silver.silver_billing_quarantine
),
freshness as (
  select
    max(ingestion_ts) as latest_ingestion_ts
  from bronze.bronze_events
),
late as (
  select
    (avg(is_late::int)::float * 100.0) as late_rate_pct,
    sum(is_late::int)::int as late_events,
    count(*)::int as events
  from silver.silver_events
),
drift as (
  select
    min(event_ts::date) filter (where props ? 'ui_variant') as first_day_with_ui_variant,
    count(*) filter (where props ? 'ui_variant')::int as rows_with_ui_variant
  from silver.silver_events
)
select
  (select latest_ingestion_ts from freshness) as latest_ingestion_ts,
  (select late_rate_pct from late) as late_rate_pct,
  (select late_events from late) as late_events,
  (select events from late) as total_events,
  (select first_day_with_ui_variant from drift) as first_day_with_ui_variant,
  (select rows_with_ui_variant from drift) as rows_with_ui_variant;
"""

TABLE_COUNTS = """
select table_name, rows
from (
  select 'bronze.bronze_events' as table_name, count(*)::int as rows from bronze.bronze_events
  union all
  select 'silver.silver_events', count(*)::int from silver.silver_events
  union all
  select 'silver.silver_events_quarantine', count(*)::int from silver.silver_events_quarantine
  union all
  select 'bronze.bronze_billing', count(*)::int from bronze.bronze_billing
  union all
  select 'silver.silver_billing', count(*)::int from silver.silver_billing
  union all
  select 'silver.silver_billing_quarantine', count(*)::int from silver.silver_billing_quarantine
) t
order by table_name;
"""

EVENTS_DAILY = """
select
  date_day,
  events,
  dau,
  late_events
from dbt.fct_events_daily
order by date_day;
"""

BILLING_DAILY = """
select
  date_day,
  plan_id,
  starts,
  new_paid_users
from dbt.fct_billing_daily
order by date_day, plan_id;
"""

ACTIVE_SUBS_DAILY = """
select
  date_day,
  is_active
from dbt.fct_subscriptions_daily
order by date_day;
"""

CHURN_RATE_DAILY = """
select
  date_day,
  count(*)::int as rows,
  sum(churn_7d)::int as churners,
  round(100.0 * avg(churn_7d)::numeric, 2) as churn_rate_pct
from dbt.gold_user_features_daily
group by 1
order by 1;
"""
