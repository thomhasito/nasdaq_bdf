import streamlit as st
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import pandas as pd

from utils.const import ColumnNames, VERY_STABLE, RELATIVELY_STABLE, RELATIVELY_UNSTABLE, VERY_UNSTABLE, BACKGROUND_COLOR, TEXT_COLOR, VERY_POSITIVE_COLOR, POSITIVE_COLOR, NEGATIVE_COLOR, NEUTRAL_COLOR
from utils.utils import period_to_yf_time_frame, convert_to_date
from app.globals import get_logger, get_stocks_df, get_companies_info, get_companies_df
from utils.const import EnumPeriod
from DataFrameOperations import DataFrameOperations

def display_sector_selection(companies_df: DataFrame) -> list:
    """
    Displays sector selection and returns selected sectors.

    Args:
        companies_df (DataFrame): The companies dataframe.

    Returns:
        list: List of selected sectors.
    """
    sectors = [row[ColumnNames.SECTOR.value] for row in companies_df.select(ColumnNames.SECTOR.value).distinct().collect()]
    # Add All option
    sectors.insert(0, "All")
    selected_sectors = st.multiselect(ColumnNames.SECTOR.value, options=sectors, key="sectors_roi_finder")

    if len(selected_sectors) == 0:
        st.text("Please select one or more sectors.")
        st.stop()

    return selected_sectors

def label_and_sort_returns(df_ops: DataFrameOperations, return_df: DataFrame, n_coms: int) -> DataFrame:
    """
    Calculates and sorts the highest daily returns.

    Args:
        df_ops (DataFrameOperations): DataFrame operations object.
        return_df (DataFrame): DataFrame containing daily return data.
        n_coms (int): Number of top companies to select.

    Returns:
        DataFrame: Sorted DataFrame with highest daily returns.
    """
    column_to_sort = "index"
    highest_yield = (
        df_ops.highest_avg_return_period(return_df, EnumPeriod.DAY, n_coms)
        .withColumn(column_to_sort, F.monotonically_increasing_id())
    )
    return (
        highest_yield
        .select(ColumnNames.TICKER.value, ColumnNames.COMPANY_NAME.value, ColumnNames.AVG_DAILY_RETURN.value, ColumnNames.SECTOR.value)
        .orderBy(column_to_sort)
        .drop(column_to_sort)
    )

def prepare_display_data(highest_yield_sorted: DataFrame, selected_sectors: list) -> pd.DataFrame:
    """
    Prepares data for display,convert into Pandas DataFrame and removing the sector column if only one sector is selected.

    Args:
        highest_yield_sorted (DataFrame): Sorted DataFrame with highest daily returns.
        selected_sectors (list): List of selected sectors.

    Returns:
        pd.DataFrame: Pandas DataFrame ready for display.
    """
    highest_yield_pd = highest_yield_sorted.toPandas()

    if len(selected_sectors) == 1:
        highest_yield_pd = highest_yield_pd.drop(columns=[ColumnNames.SECTOR.value])

    return highest_yield_pd

def display_main_card(highest_yield_pd: pd.DataFrame) -> None:
    """
    Displays the main card with the application header.

    Args:
        highest_yield_pd (pd.DataFrame): Pandas DataFrame containing stock data.

    Returns:
        None
    """
    with st.container(border=True):
        col1, col2 = st.columns(2)
        with col1:
            # create column rank => index + 1
            highest_yield_pd["Rank"] = highest_yield_pd.index + 1
            # not display the index column (not a column from the DataFrame)
            st.dataframe(highest_yield_pd, hide_index=True)
        with col2:
            with st.expander("❓ About Returns"):
                st.markdown(
                    """
                    **Daily return** is the profit or loss made by a stock in a single day.
                    It is calculated as the percentage difference between the closing price of a stock today and yesterday.
                    - 📈 Positive: Daily return > 0
                    - 📉 Negative: Daily return < 0
                    """
                )

            with st.expander(f"❓ About Stability"):
                st.markdown(
                    """
                    **Stability** is a measure of how consistent the daily returns are for a given stock.
                    We use the standard deviation which formula is the square root of the variance.
                    It represents how much the daily returns deviate from the average daily return.
                    - 🔒 Very stable: Standard deviation <= 1
                    - 🟢 Stable: 1 < Standard deviation <= 2
                    - 🟡 Unstable: 2 < Standard deviation <= 3
                    - 🔴 Very unstable: Standard deviation > 3
                    """
                )

def display_ticker_cards(highest_yield_pd: pd.DataFrame, return_df: DataFrame, tracked_tickers: list) -> None:
    """
    Displays cards for each ticker in a two-column layout.

    Args:
        highest_yield_pd (pd.DataFrame): Pandas DataFrame containing stock data.
        return_df (DataFrame): DataFrame containing daily return data.
        tracked_tickers (list): List of currently tracked tickers.

    Returns:
        None
    """
    col_a, col_b = st.columns(2, gap="medium")
    for idx, row in highest_yield_pd.iterrows():
        col = col_a if idx % 2 == 0 else col_b
        ticker = row[ColumnNames.TICKER.value]
        company_name = row[ColumnNames.COMPANY_NAME.value]

        # Filter returns for the current ticker
        ticker_returns = return_df.filter(F.col(ColumnNames.TICKER.value) == ticker)

        # Calculate statistics
        MIN_DAILY_RETURN = "min_daily_return"
        MAX_DAILY_RETURN = "max_daily_return"
        STD_DEV_DAILY_RETURN = "std_dev_daily_return"
        stats = ticker_returns.agg(
            F.min(ColumnNames.DAILY_RETURN.value).alias(MIN_DAILY_RETURN),
            F.max(ColumnNames.DAILY_RETURN.value).alias(MAX_DAILY_RETURN),
            F.stddev(ColumnNames.DAILY_RETURN.value).alias(STD_DEV_DAILY_RETURN)
        )
        assert isinstance(stats, DataFrame)
        min_return, max_return, std_dev = stats.collect()[0]

        min_df = ticker_returns.filter(F.col(ColumnNames.DAILY_RETURN.value) == min_return).first()
        max_df = ticker_returns.filter(F.col(ColumnNames.DAILY_RETURN.value) == max_return).first()
        min_date = min_df[ColumnNames.DATE.value] if min_df else None
        max_date = max_df[ColumnNames.DATE.value] if max_df else None

        stability = None
        stability = VERY_STABLE if std_dev <= 1 else \
                    RELATIVELY_STABLE if 1 < std_dev <= 2 else \
                    RELATIVELY_UNSTABLE if 2 < std_dev <= 3 else \
                    VERY_UNSTABLE

        st.markdown(
            f"""
            <style>
                body {{
                    background-color: {BACKGROUND_COLOR};
                    color: {TEXT_COLOR};
                }}
                .metric {{
                    background: #1a1a1a;
                    border-radius: 10px;
                    padding: 10px;
                    text-align: center;
                    margin-bottom: 10px;
                }}
                .icon {{
                    font-size: 24px;
                    margin-right: 5px;
                }}
                .very_positive {{ color: {VERY_POSITIVE_COLOR}; font-weight: bold; }}
                .positive {{ color: {POSITIVE_COLOR}; font-weight: bold; }}
                .negative {{ color: {NEGATIVE_COLOR}; font-weight: bold; }}
                .neutral {{ color: {NEUTRAL_COLOR}; font-weight: bold; }}
            </style>
            """,
            unsafe_allow_html=True,
        )

        stability_mapping = {
            VERY_STABLE: {"icon": "🔒", "color": POSITIVE_COLOR},
            RELATIVELY_STABLE: {"icon": "🟢", "color": POSITIVE_COLOR},
            RELATIVELY_UNSTABLE: {"icon": "🟡", "color": NEUTRAL_COLOR},
            VERY_UNSTABLE: {"icon": "🔴", "color": NEGATIVE_COLOR},
        }

        with col:
            with st.container(border=True):
                st.subheader(
                    f"{company_name} - {ticker}"
                    + (" 📜" if ticker in tracked_tickers else "")
                )

                roi = round(row[ColumnNames.AVG_DAILY_RETURN.value], 2)
                st.markdown(
                    f"""
                    <div class="metric">
                        <span class="icon">💰</span>
                        <strong>Avg. Daily Return:</strong>
                        <p class="{'very_positive' if roi > 0 else 'negative' if roi < 0 else 'neutral'}">{roi}%</p>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
                
                col1, col2, col3 = st.columns(3)

                with col1:
                    st.markdown(
                        f"""
                        <div class="metric">
                            <span class="icon">📈</span>
                            <strong>Max Return</strong>
                            <p class="positive">{round(max_return, 2)}%</p>
                            <small>({max_date})</small>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

                with col2:
                    st.markdown(
                        f"""
                        <div class="metric">
                            <span class="icon">📉</span>
                            <strong>Min Return</strong>
                            <p class="negative">{round(min_return, 2)}%</p>
                            <small>({min_date})</small>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

                with col3:
                    stability_icon = stability_mapping[stability]["icon"]
                    stability_color = stability_mapping[stability]["color"]
                    stability_score = f"Std. Dev: {round(std_dev, 2)}"
                    st.markdown(
                        f"""
                        <div class="metric">
                            <span class="icon">⚖️</span>
                            <strong>Stability</strong>
                            <span class="icon">{stability_icon}</span>
                            <p style="color: {stability_color};">{stability}</p>
                            <small>{stability_score}</small>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

                # Draw a chart line for the daily returns
                st.write("### Daily Returns Data")
                plot_df = ticker_returns.toPandas()
                st.line_chart(
                    data=plot_df.set_index(ColumnNames.DATE.value)[ColumnNames.DAILY_RETURN.value],
                    use_container_width=True,
                    height=200,
                    x_label="Date",
                    y_label="Daily Return (%)",
                )

                if st.button(
                    "Add to tracked tickers",
                    key=f"btn_{ticker}_{company_name}_{idx}",
                    icon="📜"
                ):
                    tracked_tickers.append(ticker)
                    st.session_state["selected_tickers"] = tracked_tickers
                    st.toast(f"🎈 Ticker `{ticker}` added to tracked tickers.")
                    st.rerun()

# Main application header
st.header("🔍💲 Profitable stock finder")

# Fetch company info
df_ops = None
company_df = get_companies_df()

# Sector and ticker filtering
selected_sectors = display_sector_selection(company_df)
company_tickers = get_companies_info(company_df, by_sector=True, selected_sectors=selected_sectors)

# Handle no company data
if not company_tickers:
    st.error("Failed to load company data. Please try again later.")
    st.stop()

# Time window selection
time_window = st.selectbox(
    "Analysis Period",
    options=[EnumPeriod.WEEK, EnumPeriod.MONTH, EnumPeriod.QUARTER, EnumPeriod.YEAR],
    format_func=period_to_yf_time_frame,
    index=0
)

# Number of results selection
MIN_VALUE = 1
MAX_VALUE = len(company_tickers)
DEFAULT_VALUE = 4
if MAX_VALUE < DEFAULT_VALUE:
    DEFAULT_VALUE = MAX_VALUE
n_coms = st.number_input("Number of results", min_value=MIN_VALUE, max_value=MAX_VALUE, value=DEFAULT_VALUE)

# Fetch stock data
stocks_df = get_stocks_df(period_to_yf_time_frame(time_window), company_tickers)

# Initialize DataFrameOperations for stock operations (daily return calculation)
df_ops = DataFrameOperations(get_logger(), stocks_df)

# Calculate daily returns and sort by highest yield
return_df = df_ops.calculate_daily_return()
highest_yield_sorted = label_and_sort_returns(df_ops, return_df, n_coms)
highest_yield_pd = prepare_display_data(highest_yield_sorted, selected_sectors)

# Display main card
display_main_card(highest_yield_pd)

# Tracked tickers state
tracked_tickers = (
    st.session_state["selected_tickers"]
    if "selected_tickers" in st.session_state
    else []
)

# Display ticker cards
display_ticker_cards(highest_yield_pd, return_df, tracked_tickers)