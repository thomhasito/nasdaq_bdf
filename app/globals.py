import streamlit as st
import logging as log

from requests import Session as RequestSession
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter

from pyspark.sql import SparkSession, DataFrame, functions as F
from NasdaqDF import NasdaqDF

from utils.const import APP_NAME, APP_VERSION, YF_CACHE, COMPANIES_CSV, ColumnNames, EnumPeriod
from utils.utils import period_to_yf_time_frame


@st.cache_resource(show_spinner=False)
def get_pyspark_session() -> SparkSession:
    """
    Récupère la session singleton de pyspark
    """
    builder = SparkSession.builder
    assert isinstance(builder, SparkSession.Builder)
    spark = builder.appName(APP_NAME).master("local[8]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


@st.cache_resource(show_spinner=False)
def get_logger() -> log.Logger:
    """
    Récupère le logger singleton de l'application
    """
    logger = log.getLogger(f"{APP_NAME}")
    logger.setLevel(log.INFO)
    handler = log.StreamHandler()
    handler.setFormatter(
        log.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(handler)
    return logger


class CachedLimiterSession(CacheMixin, LimiterMixin, RequestSession):
    pass

def get_request_session() -> RequestSession:
    """
    Récupère la session personnalisée pour les requêtes HTTP
    """
    session = CachedLimiterSession(
        limiter=Limiter(RequestRate(10, Duration.SECOND * 5)),
        bucket_class=MemoryQueueBucket,
        cache=SQLiteCache(YF_CACHE),
    )
    session.headers["User-agent"] = (
        f"{APP_NAME}/{APP_VERSION} (Windows NT 10.0; Win64; x64)"
    )
    return session


@st.cache_resource(show_spinner=False)
def get_stocks_df(time_window: str, tickers=[]) -> DataFrame:
    """
    Retourne le dataframe de stock
    """
    nasdaq = NasdaqDF(
        get_pyspark_session(),
        get_logger(),
        get_request_session(),
        COMPANIES_CSV,
        time_window,
    )

    with st.spinner("Chargement des données d'entreprise"):
        companies = nasdaq.load_companies_df()
        companies.cache()  # garde une copie du DF en cache dans la mémoire, pour accélérer les choses

    with st.spinner("Chargement des données de stock (peut prendre du temps)"):
        stocks = nasdaq.load_stocks_df(tickers=tickers)

    dfs = nasdaq.merge_dataframes(stocks, companies)

    dfs.cache()  # garde une copie du DF en cache dans la mémoire, pour accélérer les choses

    return dfs


@st.cache_resource(show_spinner=False)
def get_companies_df() -> DataFrame:
    """
    Return the companies dataframe
    """
    nasdaq = NasdaqDF(
        get_pyspark_session(), get_logger(), get_request_session(), COMPANIES_CSV, None
    )
    companies = nasdaq.load_companies_df()
    companies.cache() # keep a copy of the DF in memory cache, to speed things up
    return companies

@st.cache_resource(show_spinner=False)
def get_companies_info(_companies_df: DataFrame, by_sector: bool = False, selected_sectors: list = None) -> dict:
    """
    Fetches the list of tickers and company names.

    Args:
        _companies_df (DataFrame): The companies dataframe.
        by_sector (bool): If True, filters by sector.
        selected_sectors (list): List of selected sectors.

    Returns:
        dict: keys are tickers and values are company names.
    """
    try:
        companies_df = _companies_df

        if by_sector and selected_sectors:
            if "All" not in selected_sectors:
                companies_df = companies_df.filter(F.col(ColumnNames.SECTOR.value).isin(selected_sectors))

        dict_company = {
            row[ColumnNames.TICKER.value]: row[ColumnNames.COMPANY_NAME.value]
            for row in companies_df.select(
                ColumnNames.TICKER.value, ColumnNames.COMPANY_NAME.value
            ).collect()
        }

        return dict_company

    except Exception as e:
        st.error(f"Error fetching company data: {e}")
        return {}
    

def select_options(dict_companies: dict, page: str) -> tuple:
    """Displays time window selection and ticker selection.

    Args:
        dict_companies (dict): Dictionary containing tickers as key and company names as values.
        page (str): The page to link to.

    Returns:
        tuple: Tuple containing selected tickers and time window.
    """
    time_options = [EnumPeriod.DAY, EnumPeriod.WEEK, EnumPeriod.MONTH, EnumPeriod.QUARTER, EnumPeriod.YEAR]
    if page == "insights":
        time_options.remove(EnumPeriod.DAY)

    with st.container(border=True):
        bar_l, bar_r = st.columns(2, gap="medium")
        with bar_l:
            time_window = st.pills(
                "Analysis Period",
                options=time_options,
                format_func=period_to_yf_time_frame,
                selection_mode="single",
                default=EnumPeriod.WEEK,
                key=f"time_window_{page}",
            )
        with bar_r:
            selected_tickers = st.multiselect(
                label="Tickers",
                options=dict_companies.keys(),
                default=st.session_state.get("selected_tickers", []),
                format_func=lambda x: f"{x} - {dict_companies[x]}",
                key=f"selected_tickers_{page}",
            )
    return selected_tickers, time_window

def display_no_ticker_message() -> None:
    """Displays a message when no ticker is selected."""
    with st.container(border=True):
        st.write("Please select at least one ticker from the list.")
        with st.container(border=True):
            st.page_link(page="app/roi_finder.py", label="Find Profitable Stocks", icon="🔍")
    st.stop()