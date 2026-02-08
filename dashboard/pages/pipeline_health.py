import streamlit as st

from dashboard.lib.db import make_engine, read_sql_df
from dashboard.lib.queries import PIPELINE_HEALTH, TABLE_COUNTS

st.title("Pipeline Health")

eng = make_engine()

health = read_sql_df(eng, PIPELINE_HEALTH)
counts = read_sql_df(eng, TABLE_COUNTS)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Latest ingestion", str(health.loc[0, "latest_ingestion_ts"]))
c2.metric("Late rate (%)", f"{health.loc[0, 'late_rate_pct']:.2f}")
c3.metric("Late events", int(health.loc[0, "late_events"]))
c4.metric("Rows with ui_variant", int(health.loc[0, "rows_with_ui_variant"]))

st.subheader("Row counts")
st.dataframe(counts, use_container_width=True)

st.subheader("Drift signal")
st.write(
    {
        "first_day_with_ui_variant": str(health.loc[0, "first_day_with_ui_variant"]),
        "rows_with_ui_variant": int(health.loc[0, "rows_with_ui_variant"]),
    }
)
