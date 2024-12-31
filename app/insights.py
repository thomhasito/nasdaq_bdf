import streamlit as st
import pyspark.sql.functions as F
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

from DataFrameOperations import DataFrameOperations
from app.globals import get_stocks_df, get_logger, get_companies_info, get_companies_df, select_options, display_no_ticker_message
from utils.const import EnumPeriod, ColumnNames, mapping_return_column_date
from utils.utils import convert_to_date, period_to_yf_time_frame


def fig_rendement_mobile(moving_average_pd: pd.DataFrame, num_days: int = 3) -> go.Figure:
    """
    Create a plotly figure for the moving average of daily return.

    Args:
        moving_average_pd (pd.DataFrame): The moving average dataframe.
        num_days (int), optionnal

    Returns:
        go.Figure: The plotly figure.
    """
    fig = go.Figure()

    # Add daily return
    fig.add_trace(go.Scatter(
        x=moving_average_pd[ColumnNames.DATE.value],
        y=moving_average_pd[ColumnNames.DAILY_RETURN.value],
        mode='lines',
        name=ColumnNames.DAILY_RETURN.value,
        line=dict(color='orange')
    ))

    # Add Moving Average (3D)
    col_name = f"{ColumnNames.DAILY_RETURN.value}_MVG_AVG_{num_days}D"
    fig.add_trace(go.Scatter(
        x=moving_average_pd[ColumnNames.DATE.value],
        y=moving_average_pd[col_name],
        mode='lines',
        name='3-Day Moving Avg',
        line=dict(color='blue')
    ))

    # Add difference
    col_name = f"{ColumnNames.DAILY_RETURN.value}_DIFF_{num_days}D"
    fig.add_trace(go.Bar(
        x=moving_average_pd[ColumnNames.DATE.value],
        y=moving_average_pd[col_name],
        name='Difference',
        marker=dict(color='green'),
        opacity=0.5
    ))

    # Layout
    fig.update_layout(
        title="Daily Return vs Moving Average",
        xaxis_title="Date",
        yaxis_title="Return rate (%)",
        template="plotly_dark",
        height=600
    )

    return fig


def fig_volume_by_day(stock_pd: pd.DataFrame, ticker: str) -> go.Figure:
    """
    Create a plotly figure for the volume by day.

    Args:
        stock_pd (pd.DataFrame): The stock dataframe.
        ticker (str): The stock ticker for the chart title.

    Returns:
        go.Figure: The plotly figure.
    """
    # Initialize the figure
    fig = go.Figure()

    # Add bar chart for daily volume
    fig.add_trace(go.Bar(
        x=stock_pd[ColumnNames.DATE.value],
        y=stock_pd[ColumnNames.VOLUME.value],
        name="Daily Volume",
        marker=dict(color="blue", opacity=0.6)
    ))

    # Update layout for better visibility and interactivity
    fig.update_layout(
        title=f"Volume Trends: {ticker}",
        xaxis=dict(
            type="date",
            title="Date",
            showgrid=True,
            showline=True,
            tickformatstops=[
                {"dtickrange": [None, "M1"],
                    "value": "%d %b %Y"},  # Daily format
                # Monthly format
                {"dtickrange": ["M1", None], "value": "%b %Y"}
            ],
            # Enable range slider for date navigation
            rangeslider=dict(visible=True)
        ),
        yaxis=dict(
            title="Volume",
            autorange=True,
            showgrid=True
        ),
        template="plotly_dark",
        autosize=True
    )

    return fig


def fig_volume_by_period(stats_volumes_pd: pd.DataFrame, time_window: EnumPeriod) -> go.Figure:
    """
    Create a plotly figure for the volume by period.

    Args:
        stats_volumes_pd (pd.DataFrame): The volume dataframe.
        time_window (EnumPeriod): The time window.

    Returns:
        go.Figure: The plotly figure.
    """
    fig = go.Figure()

    date_col = f"{time_window.value}_period" if time_window != EnumPeriod.DAY else ColumnNames.DATE.value

    fig.add_trace(go.Bar(
        x=stats_volumes_pd[date_col],
        y=stats_volumes_pd['total_volume'],
        name="Volume",
        marker=dict(color='blue'),
        opacity=0.6
    ))

    # Superposer moyenne et volatilité
    fig.add_trace(go.Scatter(
        x=stats_volumes_pd[date_col],
        y=stats_volumes_pd['avg_volume'],
        mode='lines',
        name="Average Volume",
        line=dict(color='orange', dash='dash')
    ))

    fig.add_trace(go.Scatter(
        x=stats_volumes_pd[date_col],
        y=stats_volumes_pd['volume_volatility'],
        mode='lines',
        name="Volatility",
        line=dict(color='red', dash='dot')
    ))

    # Mise en forme du graphique
    fig.update_layout(
        title="Volume Analysis Over Time",
        xaxis_title="Date",
        yaxis_title="Volume",
        legend_title="Metrics",
        template="plotly_dark"
    )

    return fig


def fig_volume_ratio(stats_volumes_pd: pd.DataFrame, time_window: EnumPeriod) -> go.Figure:
    """
    Create a plotly figure for the volume ratio.

    Args:
        stats_volumes_pd (pd.DataFrame): The volume dataframe.
        time_window (EnumPeriod): The time window.

    Returns:
        go.Figure: The plotly figure.
    """
    fig = go.Figure()

    date_col = f"{time_window.value}_period" if time_window != EnumPeriod.DAY else ColumnNames.DATE.value

    fig.add_trace(go.Scatter(
        x=stats_volumes_pd[date_col],
        y=stats_volumes_pd['volume_ratio'],
        mode='lines+markers',
        name="Volume Ratio",
        line=dict(color='purple', dash='dot'),
        marker=dict(size=6, color='purple')
    ))

    # add threshold line at 1
    fig.add_hline(
        y=1,
        line_dash="dash",
        line_color="red",
        annotation_text="Volume Ratio Threshold (1)",
        annotation_position="top right"
    )

    fig.update_layout(
        title="Volume Ratio Over Time",
        xaxis_title="Date",
        yaxis_title="Volume Ratio",
        legend_title="Metrics",
        template="plotly_white",
        height=600
    )

    return fig


def fig_sharpe(return_rate_pd: pd.DataFrame, time_window: EnumPeriod, num_avg_days: int = 3, annual_risk_free_rate: float = 0.03) -> go.Figure:
    """
    Create a plotly figure for the Sharpe Ratio.

    Args:
        return_rate_pd (pd.DataFrame): The return rate dataframe
        time_window (EnumPeriod): The time window.
        num_avg_days (int, optional): The number of days for the moving average. Defaults to 3.
        annual_risk_free_rate (float, optional): The annual risk-free rate. Defaults to 0.03.

    Returns:
        go.Figure: The plotly figure.
    """
    fig = go.Figure()

    col_return_name = mapping_return_column_date.get(time_window)

    # Calcul du taux sans risque ajusté
    if time_window == EnumPeriod.DAY:
        annual_risk_free_rate /= 252  # 252 trading days in a year
    elif time_window == EnumPeriod.WEEK:
        annual_risk_free_rate /= 52  # 52 weeks in a year
    elif time_window == EnumPeriod.MONTH:
        annual_risk_free_rate /= 12  # 12 months in a year
    elif time_window == EnumPeriod.QUARTER:
        annual_risk_free_rate /= 4  # 4 quarters in a year
    elif time_window == EnumPeriod.YEAR:
        pass  # already annual rate

    return_rate_pd[ColumnNames.SHARPE_RATIO.value] = (return_rate_pd[col_return_name].rolling(
        num_avg_days).mean() - annual_risk_free_rate) / return_rate_pd[col_return_name].rolling(num_avg_days).std()
    date_col = f"{time_window.value}_period" if time_window != EnumPeriod.DAY else ColumnNames.DATE.value
    return_rate_pd = return_rate_pd[return_rate_pd[ColumnNames.SHARPE_RATIO.value].notnull(
    )]

    # Add Sharpe Ratio
    fig.add_trace(go.Scatter(
        x=return_rate_pd[date_col],
        y=return_rate_pd[ColumnNames.SHARPE_RATIO.value],
        mode='lines',
        name='Sharpe Ratio',
        line=dict(color='green')
    ))

    # Add threshold line at 1 (risk-free rate)
    fig.add_hline(
        y=1,
        line_dash="dash",
        line_color="red",
        annotation_text="Sharpe Ratio Threshold (1)",
        annotation_position="top right"
    )

    # Add threshold line at 2 (good performance)
    fig.add_hline(
        y=2,
        line_dash="dash",
        line_color="green",
        annotation_text="Good Performance (2)",
        annotation_position="bottom right"
    )

    # Add threshold line at 3 (excellent performance)
    fig.add_hline(
        y=3,
        line_dash="dash",
        line_color="blue",
        annotation_text="Excellent Performance (3)",
        annotation_position="bottom right"
    )

    # Update layout for better visualization
    fig.update_layout(
        title="Sharpe Ratio Over Time",
        xaxis_title="Date",
        yaxis_title="Sharpe Ratio",
        template="plotly_dark",
        height=600
    )

    return fig


def fig_corr(ticker_correlation_df: pd.DataFrame) -> px.scatter:
    """
    Create a scatter plot for correlation values.

    Args:
        ticker_correlation_df (pd.DataFrame): The dataframe with correlation data.

    Returns:
        px.scatter: The Plotly scatter plot.
    """
    fig = px.scatter(
        ticker_correlation_df,
        x="Rank",
        y=ColumnNames.CORRELATION.value,
        text="correlation ticker",
        title="Correlation with Other Variables",
        labels={"Rank": "Ranking", "Correlation": "Correlation Value"},
        template="plotly_dark",
        color="Correlation",
        color_continuous_scale=px.colors.sequential.Viridis
    )

    fig.update_traces(marker=dict(size=10), textposition="top center")
    fig.update_layout(autosize=True)
    return fig


def fig_ad_line(ad_line_pd: pd.DataFrame) -> go.Figure:
    """
    Create a plotly figure for the A/D Line.

    Args:
        ad_line_pd (pd.DataFrame): The A/D Line dataframe.

    Returns:
        go.Figure: The plotly figure.
    """
    fig = go.Figure()

    # Plot A/D Line on the primary y-axis
    fig.add_trace(go.Scatter(
        x=ad_line_pd[ColumnNames.DATE.value],
        y=ad_line_pd[ColumnNames.AD_LINE.value],
        mode='lines',
        name='A/D Line',
        line=dict(color='blue'),
        yaxis='y1'
    ))

    # Plot Close Price on the secondary y-axis
    fig.add_trace(go.Scatter(
        x=ad_line_pd[ColumnNames.DATE.value],
        y=ad_line_pd[ColumnNames.CLOSE.value],
        mode='lines',
        name='Close Price',
        line=dict(color='green'),
        yaxis='y2'
    ))

    # Update layout to include the second y-axis
    fig.update_layout(
        title="A/D Line and Close Prices",
        xaxis_title="Date",
        yaxis_title="A/D Line Value",
        yaxis=dict(
            title="A/D Line",
            showgrid=False,
            zeroline=False,
            showticklabels=False
        ),
        yaxis2=dict(
            title="Close Price",
            overlaying='y',
            side='right',
            showgrid=False,
            zeroline=False,
            showticklabels=False
        ),
        template="plotly_dark",
        height=600,
        legend=dict(orientation="h", yanchor="bottom",
                    y=1.02, xanchor="right", x=1)
    )

    return fig


def fig_rsi(rsi_pd: pd.DataFrame, column_rsi_name: str) -> go.Figure:
    """
    Create a plotly figure for the RSI.

    Args:
        rsi_pd (pd.DataFrame): The RSI dataframe.
        column_rsi_name (str): The column name for RSI.

    Returns:
        go.Figure: The plotly figure.
    """
    fig = go.Figure()

    # Add RSI line
    fig.add_trace(go.Scatter(
        x=rsi_pd[ColumnNames.DATE.value],
        y=rsi_pd[column_rsi_name],
        mode='lines',
        name='RSI',
        line=dict(color='blue')
    ))

    HIGH_RSI = 70
    # Add threshold line at 70 (Overbought)
    fig.add_hline(
        y=HIGH_RSI,
        line_dash="dash",
        line_color="red",
        annotation_text="Overbought (70)",
        annotation_position="top right"
    )

    LOW_RSI = 30
    # Add threshold line at 30 (Oversold)
    fig.add_hline(
        y=LOW_RSI,
        line_dash="dash",
        line_color="green",
        annotation_text="Oversold (30)",
        annotation_position="bottom right"
    )

    # ligne neutre
    fig.add_hline(
        y=50, 
        line_dash="dash", 
        line_color="white",
        annotation_text="Neutral",
    )

    # Update layout for better visualization
    fig.update_layout(
        title="RSI (Relative Strength Index)",
        xaxis_title="Date",
        yaxis_title="RSI Value",
        template="plotly_white",
        height=600,
    )

    return fig


############################################################## Insights ##########################################################
st.title("🧠 Trading Insights")

# get tickers and company names
companies_df = get_companies_df()
dict_companies = get_companies_info(companies_df)
if dict_companies is None:
    st.error("Failed to load company data. Please try again later.")
    st.stop()

# ticker selection
selected_tickers, time_window = select_options(dict_companies, "insights")

# Handle no ticker selected
if len(selected_tickers) == 0:
    display_no_ticker_message()

waiting_spinner = st.spinner(
    "Data is being loaded, please wait... This may take a while if you have selected multiple tickers or one/several sector(s).")
waiting_spinner.__enter__()

# stocks
ticker_values = get_stocks_df(
    period_to_yf_time_frame(time_window), selected_tickers)

# période pour le calcul des retours sur période
roi_time_window = None
if time_window in [EnumPeriod.WEEK, EnumPeriod.DAY]:
    roi_time_window = EnumPeriod.DAY
elif time_window in [EnumPeriod.MONTH, EnumPeriod.QUARTER]:
    roi_time_window = EnumPeriod.WEEK
elif time_window == EnumPeriod.YEAR:
    roi_time_window = EnumPeriod.MONTH

DEFAULT_MVG_AVG_DAYS = 3
DEFAULT_ADX_RS_DAYS = 14

if roi_time_window not in [EnumPeriod.DAY, EnumPeriod.WEEK, EnumPeriod.MONTH]:
    st.error("Invalid time window for return calculation.")
    st.stop()

# class for data operations
operations = DataFrameOperations(get_logger(), ticker_values, selected_tickers)
return_rates = operations.calculate_period_return(roi_time_window)
stats_volumes = operations.analyze_volume_by_period(roi_time_window)
moving_avg = operations.calculate_moving_average(DEFAULT_MVG_AVG_DAYS)
correlation = operations.get_correlation_df()
ad_line = operations.calc_ad_line()
rsi = operations.calc_rsi(DEFAULT_ADX_RS_DAYS)
waiting_spinner.__exit__(None, None, None)

for t_idx, ticker in enumerate(selected_tickers):
    # stock data
    stock_pd = ticker_values.filter(
        F.col(ColumnNames.TICKER.value) == ticker).toPandas()
    date_series = stock_pd[ColumnNames.DATE.value]
    stock_pd[ColumnNames.DATE.value] = convert_to_date(date_series)

    # return rate dataframe
    return_rate_pd = return_rates.filter(
        F.col(ColumnNames.TICKER.value) == ticker).toPandas()

    # volume dataframe
    stats_volumes_pd = stats_volumes.filter(
        F.col(ColumnNames.TICKER.value) == ticker).toPandas()

    # moving average dataframe
    moving_avg_pd = moving_avg.filter(
        F.col(ColumnNames.TICKER.value) == ticker).toPandas()

    # correlation dataframe
    corr_col_a = "ticker_a"
    corr_col_b = "ticker_b"
    correlation_pd = correlation.filter(
        (F.col(corr_col_a) == ticker) | (F.col(corr_col_b) == ticker)).toPandas()

    # ad_line dataframe
    ad_line_pd = ad_line.filter(
        F.col(ColumnNames.TICKER.value) == ticker).toPandas()

    # rsi dataframe (14 days)
    rsi_pd = rsi.filter(F.col(ColumnNames.TICKER.value) == ticker).toPandas()

    emote_state = "📈" if return_rate_pd[mapping_return_column_date.get(
        roi_time_window)].iloc[-1] > 0 else "📉"
    with st.expander("{} - {} {}".format(ticker, dict_companies[ticker], emote_state), expanded=t_idx == 0):
        st.subheader("{} - {} {}".format(ticker,
                     dict_companies[ticker], emote_state))

        with st.container(border=True):
            # tabs for different insights
            tab_rendement, tab_volume, tab_corr, tab_ad, tab_rsi = st.tabs(
                ["📊 Rendement Mobile & Volume", "📊 Volume",
                    "📊 Corrélation", "〰 A / D Line", "📈RSI"]
            )

            with tab_rendement:
                # Section 1: Daily Return and Moving Average
                st.subheader("📊 Daily Return and Moving Average")
                st.write("""
                The daily return reflects the average day-to-day performance of the stock, 
                while the moving average smoothens out fluctuations over time to reveal trends.
                """)

                st.plotly_chart(
                    fig_rendement_mobile(moving_avg_pd, DEFAULT_MVG_AVG_DAYS),
                    key=f"{ticker}_rm",
                    use_container_width=True
                )

                st.markdown("""
                    ### Insights:
                    - When the daily return is consistently above the moving average, it indicates **positive momentum**, which might suggest a **buying opportunity**.
                    - Conversely, if the daily return remains below the moving average, it shows **negative momentum**, possibly signaling a **sell or avoid signal**.
                    - Sharp deviations from the moving average can indicate overbought or oversold conditions.

                    **Tip**: Use this in conjunction with other indicators to confirm trends.
                """)

                # Section 2: Moving Average Variance Analysis
                st.subheader("📈 Moving Average Variance Analysis")
                st.write(
                    "Variance measures the difference between the daily return and the moving average over time.")

                diff_col = f"{ColumnNames.DAILY_RETURN.value}_DIFF_{DEFAULT_MVG_AVG_DAYS}D"
                df_stats = moving_avg_pd[diff_col]
                stats_summary = df_stats.describe().round(
                    4)[["min", "max", "mean"]]
                min_stat, max_stat, mean_stat = stats_summary.loc[
                    "min"], stats_summary.loc["max"], stats_summary.loc["mean"]
                min_date, max_date = moving_avg_pd.loc[moving_avg_pd[diff_col].idxmin(
                )][ColumnNames.DATE.value], moving_avg_pd.loc[moving_avg_pd[diff_col].idxmax()][ColumnNames.DATE.value]
                st.markdown(f"""
                - **Min Variance**: {min_stat} (on {min_date})
                - **Max Variance**: {max_stat} (on {max_date})
                - **Mean Variance**: {round(mean_stat, 4)}
                """)

                # Section 3: Sharpe Ratio
                st.subheader("📊 Sharpe Ratio Analysis")
                st.write("""
                The Sharpe Ratio measures risk-adjusted return. A higher Sharpe Ratio indicates better return per unit of risk.
                """)

                # Create Sharpe Ratio plot
                fig_ratio_sharpe = fig_sharpe(return_rate_pd, roi_time_window)
                st.plotly_chart(
                    fig_ratio_sharpe, key=f"{ticker}_sharpe", use_container_width=True)

                # Insights and Trading Tips
                st.markdown("""
                ### Insights:
                - **High Sharpe Ratio (>1)**: Indicates favorable risk-adjusted returns. Consider maintaining or increasing positions.
                - **Low Sharpe Ratio (<1)**: Suggests returns do not justify the risk. Reevaluate investments.
                - **Negative Sharpe Ratio**: Indicates losses or risk outweighing returns. Strongly consider avoiding or exiting positions.

                **Tip**: A Sharpe Ratio is more meaningful when compared across assets or periods.
                """)

            with tab_volume:
                # Section 1: Daily Volume
                st.subheader("📊 Daily Volume Analysis")
                st.write("""
                The daily volume represents the number of shares traded in a single day. 
                Higher volume often indicates stronger market interest, while lower volume suggests weaker activity.
                """)

                # Create volume plot
                fig_volume = fig_volume_by_day(stock_pd, ticker)
                st.plotly_chart(
                    fig_volume, key=f"{ticker}_daily_volume", use_container_width=True)

                # Stats and Insights
                df_stats = stock_pd[ColumnNames.VOLUME.value]
                stats_summary = df_stats.describe().round(
                    4)[["min", "max", "mean", "std"]]
                min_stat, max_stat, mean_stat, std_stat = stats_summary.loc["min"], stats_summary.loc[
                    "max"], stats_summary.loc["mean"], stats_summary.loc["std"]
                min_date, max_date = moving_avg_pd.loc[moving_avg_pd[diff_col].idxmin(
                )][ColumnNames.DATE.value], moving_avg_pd.loc[moving_avg_pd[diff_col].idxmax()][ColumnNames.DATE.value]
                st.markdown(f"""
                - **Min Volume**: {min_stat} (on {min_date})
                - **Max Volume**: {max_stat} (on {max_date})
                - **Mean Volume**: {mean_stat}
                - **Standard Deviation**: {std_stat}

                ### What This Means:
                - High volume can indicate **strong market interest** and potential price movement. It can also suggest **liquidity** and **ease of trading**.
                - Low volume suggests **weaker activity** and possible consolidation or lack of interest.
                - **Volume spikes** often coincide with significant price movements or news events.
                - High volatility can lead to **price gaps** and **slippage** in trading
                """)

                # Section 2: Volume by Period
                st.subheader("📈 Volume by Period")
                st.write("""
                Aggregated volume over weekly or monthly periods provides a broader perspective on market activity.
                """)

                if roi_time_window == EnumPeriod.DAY:
                    st.markdown(
                        "❌ **Volume by Period not available for a daily or a week analysis.**")
                else:
                    # Create volume by period plot
                    fig_volume_period = fig_volume_by_period(
                        stats_volumes_pd, roi_time_window)
                    st.plotly_chart(
                        fig_volume_period, key=f"{ticker}_volume_period", use_container_width=True)

                    st.markdown("""
                    ### Insights:
                    - **Increasing Average Volume**: Indicates growing market interest, often preceding strong price movements.
                    - **High Volatility in Volume**: Suggests uncertainty or speculative activity.
                    - **Decreasing Volume**: May indicate a lack of interest or consolidation.
                    """)

                # Section 3: Volume Ratio Analysis
                st.subheader("📊 Volume Ratio Analysis")
                st.write("""
                The volume ratio compares current volume to a historical benchmark. A ratio above 1 suggests higher-than-usual activity, 
                while a ratio below 1 indicates lower activity.
                """)

                # Create volume ratio plot
                fig_ratio_volume = fig_volume_ratio(
                    stats_volumes_pd, roi_time_window)
                st.plotly_chart(
                    fig_ratio_volume, key=f"{ticker}_volume_ratio", use_container_width=True)

                st.markdown("""
                ### Insights:
                - **Volume Ratio > 1**: Signals higher-than-usual market interest. This can indicate potential breakouts or strong trends.
                - **Volume Ratio < 1**: Suggests subdued market interest, often during consolidations or less active trading periods.
                """)

            # Correlation
            with tab_corr:
                st.write(
                    f"Analyze the correlation between {ticker} and other variables.")

                if len(selected_tickers) == 1:
                    st.info("Add more tickers to compare and compute correlations.")
                elif len(selected_tickers) > 1 and len(correlation_pd) > 0:
                    # Create correlation dataframe
                    correlation_pd.loc[:, ColumnNames.TICKER.value] = correlation_pd.apply(
                        lambda x: x[corr_col_a] if x[corr_col_a] == ticker else x[corr_col_b], axis=1)

                    col_corr_ticker = "correlation ticker"
                    correlation_pd.loc[:, col_corr_ticker] = correlation_pd.apply(
                        lambda x: x[corr_col_b] if x[corr_col_b] != ticker else x[corr_col_a], axis=1)

                    ticker_correlation_df = correlation_pd[
                        [ColumnNames.TICKER.value, col_corr_ticker,
                            ColumnNames.CORRELATION.value]
                    ]
                    ticker_correlation_df = ticker_correlation_df.sort_values(
                        ColumnNames.CORRELATION.value, ascending=False).reset_index(drop=True)
                    ticker_correlation_df["Rank"] = ticker_correlation_df.index + 1

                    # Display correlation data
                    st.subheader("Correlation Table")
                    st.dataframe(
                        ticker_correlation_df[[
                            "Rank", "correlation ticker", ColumnNames.CORRELATION.value]],
                        hide_index=True,
                    )

                    # Generate correlation plot
                    st.subheader("Correlation Scatter Plot")
                    fig_corr_plot = fig_corr(ticker_correlation_df)
                    st.plotly_chart(fig_corr_plot, use_container_width=True)

                    # Additional Insights
                    st.markdown("""
                        ### What is Correlation and Why Does it Matter in Trading?
                        Correlation measures the relationship between two assets. In trading, understanding correlation helps you make informed decisions about diversification and risk management.

                        - **Positive Correlation (+1)**: Assets move in the same direction. If one asset increases in value, the other is likely to do the same.
                        - **Negative Correlation (-1)**: Assets move in opposite directions. If one asset increases in value, the other is likely to decrease.
                        - **Low or No Correlation (0)**: There is no clear relationship between the movements of the assets.

                        **Trading Strategy Insights**:
                        - **Positive correlation**: You may want to pair assets with positive correlation if you're aiming to maximize exposure to a trend. However, this increases risk if the trend reverses.
                        - **Negative correlation**: You can use negatively correlated assets to hedge your position or diversify your portfolio. For example, if one asset falls, the other may rise, reducing your overall risk.
                        - **Low or no correlation**: When assets are not correlated, it can help in portfolio diversification since they don't influence each other much.

                        **Tip**: Use correlation in combination with other indicators (like volume, RSI, or moving averages) to confirm trends and make more balanced decisions.
                    """)

                else:
                    st.info("Click the button above to calculate correlations.")

            # AD line
            with tab_ad:
                st.subheader(
                    "📈 A/D Line", help="Measures buying and selling pressure by combining price changes and transaction volume.")

                # Explanation and table
                st.markdown("""
                ### What is the A/D Line?
                The Accumulation/Distribution (A/D) Line is a momentum indicator that assesses buying and selling pressure.
                
                #### Key Insights:
                - **Rising A/D Line**: Indicates accumulation (more buying pressure). 
                - **Falling A/D Line**: Indicates distribution (more selling pressure).
                - **Bullish Divergence**: A/D Line increases while stock price decreases. Potential reversal to the upside.
                - **Bearish Divergence**: A/D Line decreases while stock price increases. Potential reversal to the downside.
                
                **Tip**: Combine the A/D Line with price trends for better confirmation of market movements.
                """)

                st.markdown("""
                ### Trading Tips:
                - When the A/D Line and stock price **rise together**, it confirms a strong uptrend. **Consider holding or adding positions.**
                - When the A/D Line and stock price **fall together**, it confirms a strong downtrend. **Consider reducing or exiting positions.**
                - **Divergences**: 
                    - Bullish divergence suggests a potential reversal to the upside. **Watch for entry signals.**
                    - Bearish divergence suggests a potential reversal to the downside. **Prepare for possible exits.**
                """)

                # Create A/D Line plot
                fig = fig_ad_line(ad_line_pd)
                # Display chart in Streamlit
                st.plotly_chart(
                    fig, key=f"{ticker}_ad", use_container_width=True)

            # RSI
            with tab_rsi:
                st.subheader(
                    "📈 RSI", help="The RSI measures the intensity of overbought or oversold conditions.")

                # Explanations for users
                st.write("""
                ### What is RSI?
                RSI (Relative Strength Index) is a momentum indicator that measures the speed and change of price movements.
                RSI values range from 0 to 100.
                Formula: RSI = 100 - (100 / (1 + RS)), where RS = Average Gain / Average Loss.
                
                #### Key Levels:
                - **Overbought (Above 70)**: The stock is potentially overvalued, and prices may reverse or correct downwards. 
                - **Advice**: Consider selling or reducing positions if the RSI stays above 70 for a prolonged period.
                - **Oversold (Below 30)**: The stock is potentially undervalued, and prices may reverse upwards. 
                - **Advice**: Consider buying or increasing positions cautiously.
                - **Between 30 and 70**: This range indicates normal market behavior. 
                - **Advice**: No immediate action needed; monitor other indicators for confirmation.
                """)

                if time_window in [EnumPeriod.WEEK, EnumPeriod.DAY]:
                    st.markdown(
                        "❌ RSI is not displayed for daily or weekly data. RSI needs at least 14 days of data to calculate. Please select a longer time frame for RSI analysis.")
                else:
                    # Create RSI plot
                    fig = fig_rsi(
                        rsi_pd, column_rsi_name=f"RSI_{DEFAULT_ADX_RS_DAYS}D")

                    # Display chart in Streamlit
                    st.plotly_chart(
                        fig, key=f"{ticker}_rsi", use_container_width=True)
