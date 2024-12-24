import streamlit as st
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import pandas as pd

from utils.const import ColumnNames
from utils.utils import period_to_yf_time_frame
import plotly.graph_objects as pltg
from app.globals import get_company_info, get_logger, get_stocks_df
from utils.const import EnumPeriod
from utils.utils import period_to_yf_time_frame
from DataFrameOperations import DataFrameOperations

st.header("🔍💲 Recherche de stocks profitables")

st.markdown("_TODO: check pk c'est pas dans l'ordre alors que la fx odnne les résultats triés_")

company_info = get_company_info()
sectors = company_info.select(
    "Sector").distinct().rdd.flatMap(lambda x: x).collect()

# barre principale de séléction des secteurs
with st.container(border=True):
    bar_g, bar_l, bar_r = st.columns(3, gap="medium")

    with bar_l:
        time_window = st.pills(
            "Période d'analyse",
            options=[
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
        selected_sectors = st.multiselect(
            label="Secteur",
            options=sectors,
        )

    with bar_g:
        n_coms = st.number_input("Nombre de résultats", min_value=1, value=4)

# on early return si aucun secteur d'intérêt n'est séléctionné
if len(selected_sectors) == 0:
    st.text("Séléctionnez un ou plusieurs secteurs d'intérêt.")
    st.stop()

company_tickers = (
    company_info.filter(F.col("Sector").isin(selected_sectors))
    .select("Ticker")
    .rdd.flatMap(lambda x: x)
    .collect()
)

stocks_df = get_stocks_df(
    period_to_yf_time_frame(time_window), company_tickers)

df_ops = DataFrameOperations(get_logger(), stocks_df)

return_df = df_ops.calculate_daily_return()
highest_yield = df_ops.stocks_with_highest_daily_return(
    return_df, n_coms).join(company_info.select(["Ticker", "Company"]), on="Ticker")

st.dataframe(highest_yield.toPandas())

# les tickers suivis sur le dashboard
tracked_tickers = (
    st.session_state["selected_tickers"]
    if "selected_tickers" in st.session_state
    else []
)

col_a, col_b = st.columns(2, gap="medium")
for idx, row in enumerate(highest_yield.distinct().collect()):
    col = col_a if idx % 2 == 0 else col_b
    ticker = row["Ticker"]

    with col:
        with st.container(border=True):
            st.subheader(
                row["Company"]
                + " - "
                + ticker
                + (" 📜" if ticker in tracked_tickers else "")
            )

            roi = round(row["a_daily_return"], 2)
            st.text(f"Rendement quotidien moyen: {roi}%")
            if st.button(
                "Ajouter aux tickers suivis",
                key="btn_" + row["Ticker"] + "_" +
                    row["Company"] + "_" + str(idx),
                icon="📜"
            ):
                tracked_tickers.append(ticker)
                st.session_state["selected_tickers"] = tracked_tickers
                st.toast(
                    f"🎈 Ticker `{ticker}` ajouté à la liste des tickers suivis du dashboard."
                )
                st.rerun()
