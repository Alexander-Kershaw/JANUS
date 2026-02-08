import streamlit as st

st.set_page_config(page_title="JANUS Dashboards", layout="wide")

st.title("JANUS")
st.caption("Data engineering, analytics, and churn modelling")

st.markdown(
    """
Use the pages in the left sidebar:
- **Pipeline Health**: row counts, lateness, drift, freshness
- **Product Analytics**: events + billing + active subs
- **Churn Model Monitor**: temporal CV + monitoring plots
- **Data Explorer**: inspect key tables quickly
"""
)
