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

st.title("Dashboard des tickers")

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
            "Période d'analyse",
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

# pas la peine de tout process si aucun ticker n'est séléctionné
if len(selected_tickers) == 0:
    st.write("Séléctionnez au moins un ticker dans la liste")
    st.stop()

# stocks
ticker_values = get_stocks_df(period_to_yf_time_frame(time_window), selected_tickers)


roi_time_window = (
    EnumPeriod.DAY
    if time_window == EnumPeriod.WEEK or time_window == EnumPeriod.DAY
    else EnumPeriod.WEEK
)

# classe pour les op sur le dataframe
operations = DataFrameOperations(get_logger(), ticker_values)
return_rates = operations.avg_daily_return_by_period(roi_time_window)

for t_idx, ticker in enumerate(selected_tickers):
    with st.expander(f"{ticker} - {company_names[ticker]}", expanded=t_idx == 0):
        st.subheader(f"{ticker} - {company_names[ticker]}")
        stock_values = ticker_values.filter(ticker_values["Ticker"] == ticker)

        # pour plotly
        stock_pd = stock_values.select(
            "Date", "Open", "High", "Low", "Close"
        ).toPandas()
        roi_pd = return_rates.filter(return_rates["Ticker"] == ticker).toPandas()

        st.plotly_chart(
            pltg.Figure(
                pltg.Candlestick(
                    x=stock_pd["Date"],
                    open=stock_pd["Open"],
                    high=stock_pd["High"],
                    close=stock_pd["Close"],
                    low=stock_pd["Low"],
                ),
                layout={"autosize": True, "xaxis": {"dtick": "W1"}},
            ),
            key=ticker,
        )

        stats = (
            stock_values.agg(
                F.min("Open").alias("min_open"),
                F.max("Open").alias("max_open"),
                F.mean("Open").alias("mean_open"),
                F.min("Close").alias("min_close"),
                F.max("Close").alias("max_close"),
                F.mean("Close").alias("mean_close"),
            )
            .collect()[0]
            .asDict()
        )

        ticker_a, ticker_b = st.columns(2, gap="medium")

        with ticker_a:
            with st.container(border=True):
                st.table(
                    pd.DataFrame(
                        {
                            "Min": [stats["min_open"], stats["min_close"]],
                            "Max": [stats["max_open"], stats["max_close"]],
                            "Mean": [stats["mean_open"], stats["mean_close"]],
                        },
                        index=["Open", "Close"],
                    )
                )

        with ticker_b:
            with st.container(border=True):
                st.subheader("Rendement {} (%)".format("quotidien" if roi_time_window == EnumPeriod.DAY else "hebdomadaire"))
                st.plotly_chart(
                    pltg.Figure(
                        pltg.Scatter(
                            y=roi_pd["avg_daily_return"],
                            x=roi_pd[
                                (
                                    "day_period"
                                    if roi_time_window == EnumPeriod.DAY
                                    else "week_period"
                                )
                            ],
                        )
                    )
                )
