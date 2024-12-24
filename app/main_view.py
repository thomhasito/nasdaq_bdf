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
tickers = companies.select("Ticker").distinct(
).rdd.flatMap(lambda x: x).collect()

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
    with st.container(border=True):
        st.write("Séléctionnez au moins un ticker dans la liste")
        with st.container(border=True):
            st.page_link(page="app/roi_finder.py",
                         label="Rechercher des actions profitables", icon="🔍")
    st.stop()

waiting_spinner = st.spinner(
    "Calcul en cours (peut prendre quelques minutes) ...")
waiting_spinner.__enter__()

# stocks
ticker_values = get_stocks_df(
    period_to_yf_time_frame(time_window), selected_tickers)

# période pour le calcul des retours sur période
roi_time_window = (
    EnumPeriod.DAY
    if time_window in [EnumPeriod.WEEK, EnumPeriod.DAY]
    else EnumPeriod.WEEK
)

# classe pour les op sur le dataframe
operations = DataFrameOperations(get_logger(), ticker_values)

# retours sur la période selectionnée + volumes aggrégés
return_rates = operations.avg_daily_return_by_period(roi_time_window)
summed_volumes = operations.avg_volumes_by_period(roi_time_window)

ad_line = operations.calc_ad_line()

# moyenne mobile sur 3j
moving_avg = operations.calculate_moving_average("daily_return", 3)

for t_idx, ticker in enumerate(selected_tickers):

    # les DF pandas sont pour plotly
    stock_values = ticker_values.filter(ticker_values["Ticker"] == ticker)
    stock_pd = stock_values.select(
        "Date", "Open", "High", "Low", "Close", "Volume"
    ).toPandas()

    mov_pd = moving_avg.filter(moving_avg["Ticker"] == ticker).toPandas()

    svolumes_pd = summed_volumes.filter(
        summed_volumes["Ticker"] == ticker).toPandas()

    roi_pd = return_rates.filter(
        return_rates["Ticker"] == ticker).toPandas()

    ad_line_pd = ad_line.filter(ad_line["Ticker"] == ticker).toPandas()

    emote_state = "📈" if roi_pd["avg_daily_return"].iloc[-1] > 0. else "📉"

    with st.expander("{} - {} {}".format(ticker, company_names[ticker], emote_state), expanded=t_idx == 0):
        st.subheader("{} - {} {}".format(ticker,
                                         company_names[ticker], emote_state))

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
                F.min("High").alias("min_high"),
                F.max("High").alias("max_high"),
                F.mean("High").alias("mean_high"),
                F.min("Close").alias("min_close"),
                F.max("Close").alias("max_close"),
                F.mean("Close").alias("mean_close"),
                F.min("Low").alias("min_low"),
                F.max("Low").alias("max_low"),
                F.mean("Low").alias("mean_low"),
            )
            .collect()[0]
            .asDict()
        )

        with st.container(border=True):

            tab_rmm, tab_graphes, tab_tableau, tabr_entries = st.tabs(
                ["📊 Rendement quo. mobile (3j)",
                 "📊 Rendement / Volume hebdo.", "🔢 Stats", "〰 A / D Line"]
            )

            with tab_graphes:
                ticker_a, ticker_b = st.columns(2, gap="medium")

                if roi_time_window == EnumPeriod.DAY:
                    st.markdown(
                        " ❌ __Rendement & Volumes hebdo non disponibles sur une analyse journalière / hebdomadaire__")
                else:
                    with ticker_a:
                        with st.container(border=False):
                            st.subheader(
                                "Volume vendu {}".format(
                                    "quotidien"
                                    if roi_time_window == EnumPeriod.DAY
                                    else "hebdomadaire"
                                ), help="Volume d'actions vendues quotidiennement / hebdomadairement au cours de la période d'analyse")
                            st.plotly_chart(
                                pltg.Figure(
                                    pltg.Scatter(
                                        y=svolumes_pd["summed_volume"],
                                        x=svolumes_pd[(
                                            "day_period"
                                            if roi_time_window == EnumPeriod.DAY
                                            else "week_period"
                                        )],
                                    ),
                                    layout={"autosize": True,
                                            "xaxis": {"dtick": "W1"}},
                                )
                            )

                    with ticker_b:
                        with st.container(border=False):
                            st.subheader(
                                "Rendement moyen {} (%)".format(
                                    "quotidien"
                                    if roi_time_window == EnumPeriod.DAY
                                    else "hebdomadaire"
                                ),
                                help="Rendement moyen de l'action sur la période d'analyse",
                            )
                            fig = pltg.Figure(layout={"autosize": True,
                                                      "xaxis": {"dtick": "W1"}},)
                            fig.add_trace(pltg.Scatter(
                                y=roi_pd["avg_daily_return"],
                                x=roi_pd[
                                    (
                                        "day_period"
                                        if roi_time_window == EnumPeriod.DAY
                                        else "week_period"
                                    )
                                ],
                            ))

                            st.plotly_chart(
                                fig,
                                key=f"return_{ticker}"
                            )

            with tab_tableau:
                ticker_a, ticker_b = st.columns(2, gap="medium")
                with ticker_a:
                    st.subheader("Statistiques",
                                 help="Statistiques sur la période d'analyse")
                    st.table(
                        pd.DataFrame(
                            {
                                "Min": [
                                    stats["min_open"],
                                    stats["min_close"],
                                    stats["min_high"],
                                    stats["min_low"],
                                ],
                                "Max": [
                                    stats["max_open"],
                                    stats["max_close"],
                                    stats["max_high"],
                                    stats["max_low"],
                                ],
                                "Mean": [
                                    stats["mean_open"],
                                    stats["mean_close"],
                                    stats["mean_high"],
                                    stats["mean_low"],
                                ],
                            },
                            index=["Open", "Close", "High", "Low"],
                        )
                    )

                with ticker_b:
                    st.subheader(
                        "Volatilité hebdomadaire (écart-type)",
                        help="Indique la volatilité hebdomadaire du rendement quotidien de l'action. Une volatilité trop grande porte un risque a l'investissement",
                    )
                    if roi_time_window == EnumPeriod.DAY:
                        st.markdown(
                            "❌ Volatilité non disponible sur une analyse journalière / hebdomadaire")
                    else:
                        st.plotly_chart(
                            pltg.Figure(
                                pltg.Scatter(
                                    y=roi_pd["return_dev"],
                                    x=roi_pd[
                                        (
                                            "day_period"
                                            if roi_time_window == EnumPeriod.DAY
                                            else "week_period"
                                        )
                                    ],
                                ),
                            ),
                            key=f"vola_{ticker}"
                        )

            with tab_rmm:
                ticker_a, ticker_b = st.columns(2, gap="small")

                with ticker_a:
                    st.subheader("Volume vendu quotidien",
                                 help="Volume d'actions vendues quotidiennement")
                    st.plotly_chart(
                        pltg.Figure(
                            pltg.Scatter(
                                y=mov_pd["Volume"],
                                x=mov_pd["Date"],
                            ),
                            layout={"autosize": True,
                                    "xaxis": {"dtick": "W1"}}
                        ),
                        key=f"{ticker}_vd",
                    )

                with ticker_b:
                    st.subheader("Rendement quotidien (moyenne mobile sur 3j)",
                                 help="Rendement quotidien sur moyenne mobile de 3j")
                    st.plotly_chart(
                        pltg.Figure(
                            pltg.Scatter(
                                y=mov_pd["daily_return_moving_avg_3_days"] if roi_time_window != EnumPeriod.DAY else roi_pd["avg_daily_return"],
                                x=mov_pd["Date"] if roi_time_window != EnumPeriod.DAY else roi_pd["day_period"],
                            ),
                            layout={"autosize": True,
                                    "xaxis": {"dtick": "W1"}}
                        ),
                        key=f"{ticker}_rmm"
                    )

                with tabr_entries:
                    st.subheader(
                        "A/D Line", help="Mesure la pression d'achat et de vente en combinant les variations de rendement et le volume des transactions")
                    with st.popover("Mémo A/D line (Cliquez pour ouvrir)"):
                        st.table({
                            "Indicateur": [
                                "Augmentation de l'AD Line",
                                "Diminution de l'AD Line",
                                "Divergence haussière",
                                "Divergence baissière",
                                "Rendement et AD Line en accord (augmentation)",
                                "Rendement et AD Line en accord (diminution)"
                            ],
                            "Interprétation": [
                                "Accumulation (plus d'achats)",
                                "Distribution (plus de ventes)",
                                "Rendement en baisse, mais AD Line en hausse",
                                "Rendement en hausse, mais AD Line en baisse",
                                "Rendement et AD Line augmentent simultanément",
                                "Rendement et AD Line diminuent simultanément"
                            ],
                            "Signification": [
                                "La pression d'achat est plus forte que la pression de vente, ce qui peut annoncer une tendance haussière.",
                                "La pression de vente est plus forte que la pression d'achat, ce qui peut annoncer une tendance baissière.",
                                "Les acheteurs accumulent des positions malgré la baisse des rendement, signalant un potentiel retournement à la hausse.",
                                "L'absence de pression d'achat malgré la hausse des rendement, ce qui pourrait signaler un affaiblissement de la tendance haussière et une correction. Il est intéressant d'attendre ce signal pour vendre pour faire un profit maximum.",
                                "Conformité entre le rendement et l'indicateur, confirmant la validité de la tendance haussière. Il est intéressant d'attendre un signal de correction / survente pour vendre afin de faire un profit maximum car la tendance haussière peut continuer",
                                "Conformité entre le rendement et l'indicateur, confirmant la validité de la tendance baissière. Il est intéressant d'acheter dans ces périodes de tendance baissière."
                            ]
                        })

                    st.plotly_chart(
                        pltg.Figure(
                            pltg.Scatter(
                                y=ad_line_pd["AD_line"],
                                x=ad_line_pd["Date"]
                            )
                        ),
                        key=f"ad_line_{ticker}"
                    )

                    # st.page_link("Accéder au insights pour cette action", icon="🧠", page="app/insights.py")
