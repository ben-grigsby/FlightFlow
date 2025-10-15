# dashboard/main.py

import streamlit as st
import altair as alt
import pandas as pd


from datetime import datetime

from configs.configs import (
    timezones
)

from load_data import (
    get_gold_table,
    get_top_origin_countries
)

from functions.timezone_clocks import (
    top_clock
)

top_clock()

st.title("Top 10 Countries with Highest Flight")
df = get_top_origin_countries()


bar_chart = alt.Chart(df).mark_bar().encode(
    x=alt.X('flight_count:Q', title='Flight Count'),
    y=alt.Y('origin_country:N', sort='-x', title='Origin Country'),
    color=alt.Color('origin_country:N', legend=None),
    tooltip=['origin_country', 'flight_count']
).properties(
    width=600,
    height=400,
    title='Top 10 Origin Countries by Flight Count'
)

st.altair_chart(bar_chart)
