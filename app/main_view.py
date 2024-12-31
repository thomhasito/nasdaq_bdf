import streamlit as st
import pandas as pd
import plotly.graph_objects as pltg

from app.globals import get_stocks_df, get_companies_info, get_companies_df, select_options, display_no_ticker_message
from utils.utils import period_to_yf_time_frame, convert_to_date
from utils.const import ColumnNames


def create_candlestick_chart(stock_pd: pd.DataFrame, ticker: str, frame_height: int = 600) -> pltg.Figure:
    """Creates a candlestick chart using Plotly.

    Args:
        stock_pd (pd.DataFrame): Stock data.
        ticker (str): Ticker symbol.
        frame_height (int, optional): Height of the chart. Defaults to 600.

    Returns:
        pltg.Figure: Plotly figure object.
    """
    fig = pltg.Figure(
        data=[pltg.Candlestick(
            x=stock_pd[ColumnNames.DATE.value],
            open=stock_pd[ColumnNames.OPEN.value],
            high=stock_pd[ColumnNames.HIGH.value],
            close=stock_pd[ColumnNames.CLOSE.value],
            low=stock_pd[ColumnNames.LOW.value],
            increasing_line_color="green",
            decreasing_line_color="red",
            showlegend=False,
        )],
        layout={
            "autosize": True,
            "xaxis": {
                "type": "date",
                "title": "Date",
                "showgrid": True,
                "tickformatstops": [
                    # Show Day-Month-Year for zoomed-in view
                    {"dtickrange": [None, "M1"], "value": "%d %b %Y"},
                    # Show Month-Year for mid-range view
                    {"dtickrange": ["M1", None], "value": "%b %Y"},
                ],
                "showline": True,
                # Enable range slider for zooming in/out
                "rangeslider": {"visible": True},
            },
            "yaxis": {
                "title": "Stock Price (USD)",
                "autorange": True,  # Auto-adjust y-axis range
                "showgrid": True,  # Enable grid for better visualization
                "tickprefix": "$",
            },
            "title": f"Stock Price Trends: {ticker}",
            "template": "plotly_dark",
        },
    )
    fig.update_layout(height=frame_height)
    return fig


def display_tabs(stock_pd: pd.DataFrame) -> None:
    """Displays three tabs: Stats, Info, and History.

    Args:
        stock_pd (pd.DataFrame): Stock data.
    """
    tab_stats, tab_info, tab_history = st.tabs(
        ["📊 Stats", "ℹ️ Info", "📜 History"])

    # Tab: Stats
    with tab_stats:
        st.write("### Descriptive Statistics")
        numeric_cols = stock_pd.select_dtypes(include=["number"])
        stats = numeric_cols.describe().T.round(2)
        stats = stats.map(lambda x: f"{x:.2f}")
        stats.rename(columns={"50%": "Median"}, inplace=True)

        st.markdown(
            "<div style='text-align: center; font-weight: bold; color: #4CAF50;'>"
            "Summary Statistics Table</div>",
            unsafe_allow_html=True,
        )
        st.table(stats[["mean", "std", "min", "25%", "Median", "75%", "max"]])

    # Tab: Info
    with tab_info:
        st.write("### Additional Information")
        count = len(stock_pd)

        date_range_days = (stock_pd[ColumnNames.DATE.value].max(
        ) - stock_pd[ColumnNames.DATE.value].min()).days + 1
        all_days_between = pd.date_range(start=stock_pd[ColumnNames.DATE.value].min(
        ), end=stock_pd[ColumnNames.DATE.value].max())
        missing_days = 100 * (1 - count / date_range_days)

        not_valid_days = 0  # holidays and weekends
        CHRISTMAS = "12-25"
        NEW_YEAR = "01-01"
        holiday_days = all_days_between.strftime(
            "%m-%d").isin([CHRISTMAS, NEW_YEAR])  # Christmas and New Year
        not_valid_days += holiday_days.sum()

        weekend_days = all_days_between.weekday.isin(
            [5, 6])  # Saturday and Sunday
        not_valid_days += weekend_days.sum()

        holidays_on_weekends = (holiday_days & weekend_days).sum()
        # Remove holidays on weekends (counted twice)
        not_valid_days -= holidays_on_weekends
        true_missing_days = 100 * \
            (1 - count / (date_range_days - not_valid_days))

        stock_pd["date_diff"] = stock_pd[ColumnNames.DATE.value].diff().dt.days
        mean_period = stock_pd["date_diff"].mean()
        stock_pd.drop(columns=["date_diff"], inplace=True)

        st.markdown(
            f"<div style='font-weight: bold; color: #2196F3;'>Dataset Insights</div>",
            unsafe_allow_html=True,
        )
        number_of_observations = f"- Number of observations: {count}"
        percentage_missing_days = (
            f"- Percentage of missing days: "
            f"With holidays and weekends: {missing_days:.2f}%, Without (true %): {true_missing_days:.2f}%"
        )
        average_period = f"- Average period between points: {mean_period:.2f} days"
        st.markdown(number_of_observations)
        st.markdown(percentage_missing_days)
        st.markdown(average_period)

    # Tab: History
    with tab_history:
        st.write("### Historical Data")
        count = len(stock_pd)
        default_choices = [5, 10, 20, 40]
        options = [i if i <= count else count for i in default_choices if i <
                   count or i == default_choices[-1]]
        # options will be [5, 10, 20, 40] if count is greater than 40, else it will reach the maximum value of count
        # Example: if count is 15, options will be [5, 10, 15]

        rows_to_display = st.radio(
            "Select the number of rows to display:",
            options=options,
            index=0,
            horizontal=True,
            key=f"rows_to_display_{ticker}",
        )

        history_df = stock_pd.tail(rows_to_display).sort_values(
            by=ColumnNames.DATE.value, ascending=False)
        # Format the date and numeric columns
        history_df[ColumnNames.DATE.value] = history_df[ColumnNames.DATE.value].dt.strftime(
            "%Y-%m-%d")
        for cols in numeric_cols.columns:
            history_df[cols] = history_df[cols].map(lambda x: f"{x:.2f}")

        st.dataframe(history_df, hide_index=True, key=f"history_df_{ticker}")


def display_stock_data(fig: pltg.Figure, stock_pd: pd.DataFrame, ticker: str, company_name: str) -> None:
    """Displays stock data for a given ticker.

    Args:
        fig (pltg.Figure): Plotly figure object.
        stock_pd (pd.DataFrame): Stock data.
        ticker (str): Ticker symbol.
        company_name (str): Company name.

    Returns:
        None
    """
    emote_state = "📈" if stock_pd[ColumnNames.OPEN.value].iloc[0] < stock_pd[ColumnNames.CLOSE.value].iloc[-1] else "📉"
    with st.expander(f"{ticker} - {company_name} {emote_state}", expanded=True):
        st.subheader(f"{ticker} - {company_name} {emote_state}")
        st.plotly_chart(fig, use_container_width=True,
                        key=f"plot_candlestick_{ticker}")

        # Display tabs
        display_tabs(stock_pd)

        # Add link for more insights
        with st.container():
            st.markdown("#### More Insights about this ticker")
            if st.button("🧠 Click for more insights", key=f"insights_button_{ticker}"):
                st.write("Redirecting to the insights page...")
                st.page_link("app/insights.py")


############################################################## Main View ##########################################################
st.title("Ticker dashboard")

# get tickers and company names
companies_df = get_companies_df()
dict_companies = get_companies_info(companies_df)
if dict_companies is None:
    st.error("Failed to load company data. Please try again later.")
    st.stop()

# ticker selection
selected_tickers, time_window = select_options(dict_companies, "main_view")

# Handle no ticker selected
if len(selected_tickers) == 0:
    display_no_ticker_message()

waiting_spinner = st.spinner(
    "Data is being loaded, please wait... This may take a while if you have selected multiple tickers or one/several sector(s).")
waiting_spinner.__enter__()

# stocks
ticker_values = get_stocks_df(
    period_to_yf_time_frame(time_window), selected_tickers)

for t_idx, ticker in enumerate(selected_tickers):
    # get stock data for the selected ticker and convert to pandas
    # display the candlestick chart
    stock_values = ticker_values.filter(
        ticker_values[ColumnNames.TICKER.value] == ticker)
    stock_pd = stock_values.select(
        ColumnNames.DATE.value,
        ColumnNames.OPEN.value,
        ColumnNames.HIGH.value,
        ColumnNames.LOW.value,
        ColumnNames.CLOSE.value,
    ).toPandas()
    date_series = stock_pd[ColumnNames.DATE.value]
    stock_pd[ColumnNames.DATE.value] = convert_to_date(date_series)

    # Create and display the candlestick chart
    fig = create_candlestick_chart(stock_pd, ticker)
    company_name = dict_companies[ticker]
    display_stock_data(fig, stock_pd, ticker=ticker, company_name=company_name)

waiting_spinner.__exit__(None, None, None)
