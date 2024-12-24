import streamlit as st
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import pandas as pd

import plotly.graph_objects as pltg
import matplotlib.pyplot as plt

from NasdaqDF import NasdaqDF
from DataFrameOperations import DataFrameOperations
from app.globals import get_stocks_df, get_company_info, get_logger
from utils.const import EnumPeriod
from utils.utils import period_to_yf_time_frame

st.header("🧠 Insights de trading")


companies = get_company_info()
tickers = companies.select("Ticker").distinct().rdd.flatMap(lambda x: x).collect()
company_names = {
    row["Ticker"]: row["Company"] for row in companies.distinct().collect()
}

# barre principale de séléction des tickers et
with st.container(border=True):
    bar_l, bar_r = st.columns(2, gap="medium")

    with bar_l:
        time_window = st.pills(
            "Type de trading",
            options=[
                EnumPeriod.DAY,
                EnumPeriod.WEEK,
                EnumPeriod.MONTH,
                EnumPeriod.QUARTER,
                EnumPeriod.YEAR,
            ],
            format_func=period_to_yf_time_frame,
            selection_mode="single",
            default=EnumPeriod.WEEK,
        )

    with bar_r:
        selected_tickers = st.multiselect(
            label="Tickers",
            options=tickers,
            default=(
                st.session_state["selected_tickers"]
                if "selected_tickers" in st.session_state
                else []
            ),
            format_func=lambda x: f"{x} - {company_names[x]}",
            key="selected_tickers",
        )
