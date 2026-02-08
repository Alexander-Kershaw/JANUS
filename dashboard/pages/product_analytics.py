from __future__ import annotations

import streamlit as st
import pandas as pd

from dashboard.lib.db import make_engine, read_sql_df
from dashboard.lib.queries import EVENTS_DAILY, BILLING_DAILY, ACTIVE_SUBS_DAILY
from dashboard.lib.charts import line_chart, pivot_line_chart


st.title("Product Analytics")

eng = make_engine()

events = read_sql_df(eng, EVENTS_DAILY)
billing = read_sql_df(eng, BILLING_DAILY)
active = read_sql_df(eng, ACTIVE_SUBS_DAILY)

# Normalize date types 
for df in (events, billing, active):
    df["date_day"] = pd.to_datetime(df["date_day"])

# Controls
st.sidebar.header("Filters")
min_day = min(events["date_day"].min(), billing["date_day"].min(), active["date_day"].min())
max_day = max(events["date_day"].max(), billing["date_day"].max(), active["date_day"].max())

date_range = st.sidebar.date_input(
    "Date range",
    value=(min_day.date(), max_day.date()),
    min_value=min_day.date(),
    max_value=max_day.date(),
)

if isinstance(date_range, tuple) and len(date_range) == 2:
    start, end = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
else:
    start, end = min_day, max_day

events = events[(events["date_day"] >= start) & (events["date_day"] <= end)].copy()
billing = billing[(billing["date_day"] >= start) & (billing["date_day"] <= end)].copy()
active = active[(active["date_day"] >= start) & (active["date_day"] <= end)].copy()

# Derived metrics 
events["late_rate_pct"] = (events["late_events"] / events["events"]).fillna(0.0) * 100.0

# KPI row
k1, k2, k3, k4 = st.columns(4)
k1.metric("Days", int(events["date_day"].nunique()))
k2.metric("Total events", int(events["events"].sum()))
k3.metric("Avg DAU", int(round(events["dau"].mean(), 0)))
k4.metric("Avg late rate (%)", float(round(events["late_rate_pct"].mean(), 2)))

st.divider()

# Events charts 
st.subheader("Engagement and system behaviour")

fig = line_chart(
    events,
    x="date_day",
    y_cols=["events", "dau", "late_events"],
    title="Events, DAU, Late Events (daily)",
    ylabel="count",
)
st.pyplot(fig, clear_figure=True)

fig = line_chart(
    events,
    x="date_day",
    y_cols=["late_rate_pct"],
    title="Late rate (%)",
    ylabel="percent",
)
st.pyplot(fig, clear_figure=True)

with st.expander("Show events daily table"):
    st.dataframe(events, use_container_width=True)

st.divider()

# Billing charts
st.subheader("Billing funnel")

fig = pivot_line_chart(
    billing,
    x="date_day",
    category="plan_id",
    value="starts",
    title="Starts by plan (daily)",
    ylabel="starts",
)
st.pyplot(fig, clear_figure=True)

fig = pivot_line_chart(
    billing,
    x="date_day",
    category="plan_id",
    value="new_paid_users",
    title="New paid users by plan (daily)",
    ylabel="users",
)
st.pyplot(fig, clear_figure=True)

with st.expander("Show billing daily table"):
    st.dataframe(billing, use_container_width=True)

st.divider()
