# dashboard/main.py

import streamlit as st
import altair as alt
from load_data import (
    ib_busiest_cities_24h,
    ib_preview_table
)

# ------------------------------------------------------
# Dashboard Title
# ------------------------------------------------------
st.set_page_config(page_title="Flight Dashboard", layout="wide")
st.title("🌍 Top 10 Busiest Origin Countries (Past 24 Hours)")

# ------------------------------------------------------
# Load data (from Iceberg query)
# ------------------------------------------------------
with st.spinner("Loading latest flight data..."):
    df = ib_busiest_cities_24h()

if df.empty:
    st.warning("No data available for the past 24 hours.")
else:
    # --------------------------------------------------
    # Display table and chart
    # --------------------------------------------------
    st.subheader("Flight Counts by Country")
    st.dataframe(df, use_container_width=True)

    # Altair chart
    bar_chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("flight_count:Q", title="Number of Flights"),
            y=alt.Y("city:N", sort="-x", title="City"),
            color=alt.Color("city:N", legend=None),
            tooltip=["city", "flight_count"]
        )
        .properties(width=700, height=400)
    )

    st.altair_chart(bar_chart, use_container_width=True)


df_2 = ib_preview_table()
st.dataframe(df_2, use_container_width=True)