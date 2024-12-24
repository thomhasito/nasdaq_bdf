import streamlit as st
import logging as log

from requests import Session as RequestSession
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter

from pyspark.sql import SparkSession, DataFrame
from NasdaqDF import NasdaqDF

from utils.const import APP_NAME, APP_VERSION, YF_CACHE, COMPANIES_CSV


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

    with st.spinner("Chargement des données de stock (peut prendre du temps)"):
        stocks = nasdaq.load_stocks_df(tickers=tickers)

    dfs = nasdaq.merge_dataframes(stocks, companies)

    dfs.cache()  # garde une copie du DF en cache dans la mémoire, pour accélérer les choses

    return dfs


@st.cache_resource(show_spinner=False)
def get_company_info():
    nasdaq = NasdaqDF(
        get_pyspark_session(), get_logger(), get_request_session(), COMPANIES_CSV, "1d"
    )
    companies = nasdaq.load_companies_df()
    companies.cache()
    return companies
