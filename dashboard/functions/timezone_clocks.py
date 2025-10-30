# dashboard/functions/timezone_clocks.py

import streamlit as st
import pytz
import time

from datetime import datetime
from configs.configs import (
    timezones
)


def get_current_time(timezone_str):
    tz = pytz.timezone(timezone_str)
    current_time = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    return current_time


def top_clock(loc1="New York", loc2="London", loc3="Tokyo"):
    st.markdown("<h2 style='text-align: center;'>Local Times</h2>", unsafe_allow_html=True)

    # Use 5 columns: left spacer, clock1, middle spacer, clock2, right clock
    col1, spacer1, col2, spacer2, col3 = st.columns([1, 0.2, 1, 0.2, 1])

    clock1 = col1.empty()
    clock2 = col2.empty()
    clock3 = col3.empty()

    while True:
        time1 = get_current_time(timezones[loc1])
        time2 = get_current_time(timezones[loc2])
        time3 = get_current_time(timezones[loc3])

        clock1.markdown(f"""
            <div style='padding: 10px; border-radius: 10px; background-color: #f0f2f6;'>
                <h4 style='text-align: center;'>{loc1}</h4>
                <div style='font-size: 36px; color: blue; text-align: center;'>{time1}</div>
            </div>
        """, unsafe_allow_html=True)

        clock2.markdown(f"""
            <div style='padding: 10px; border-radius: 10px; background-color: #f0f2f6;'>
                <h4 style='text-align: center;'>{loc2}</h4>
                <div style='font-size: 36px; color: blue; text-align: center;'>{time2}</div>
            </div>
        """, unsafe_allow_html=True)
        clock3.markdown(f"""
            <div style='padding: 10px; border-radius: 10px; background-color: #f0f2f6;'>
                <h4 style='text-align: center;'>{loc3}</h4>
                <div style='font-size: 36px; color: blue; text-align: center;'>{time3}</div>
            </div>
        """, unsafe_allow_html=True)

        time.sleep(1)