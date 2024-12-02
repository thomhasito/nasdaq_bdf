import requests_cache
import logging
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter
from pyspark.sql import SparkSession
from utils.const import APP_NAME, APP_VERSION, YF_CACHE

class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass

class SessionApp:
    _instance = None
    _app_name = APP_NAME
    _app_version = APP_VERSION

    def __init__(self):
        if SessionApp._instance is not None:
            raise Exception("App is a singleton! Use App.get_instance() instead.")
        
        self.logger = self._setup_logger()
        self.logger.info(f"Starting {SessionApp._app_name} version {SessionApp._app_version}")

        builder = SparkSession.builder
        assert isinstance(builder, SparkSession.Builder)
        self.spark = builder.master("local[*]").config("spark.driver.host", "localhost").appName(SessionApp._app_name).getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        self.session = self._setup_session()
        SessionApp._instance = self

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = SessionApp()
        return cls._instance

    def _setup_logger(self):
        logger = logging.getLogger(f"{SessionApp._app_name} Logger")
        logger.setLevel(logging.DEBUG)  # Set the overall level to DEBUG

        if not logger.handlers:
            # StreamHandler for console (INFO+)
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(console_formatter)
            logger.addHandler(ch)

            # FileHandler for file (DEBUG+)
            fh = logging.FileHandler('debug.log')  # Save logs to debug.log
            fh.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            fh.setFormatter(file_formatter)
            logger.addHandler(fh)

        return logger

    def _setup_session(self) -> Session:
        
        limiter = Limiter(RequestRate(10, Duration.SECOND*5))
        bucket_class = MemoryQueueBucket
        backend = SQLiteCache(YF_CACHE)
        session = CachedLimiterSession(
                    limiter=limiter,
                    bucket_class=bucket_class,
                    cache=backend)
        session.headers['User-agent'] = f"{SessionApp._app_name}/{SessionApp._app_version} (Windows NT 10.0; Win64; x64)"

        return session

    def get_logger(self) -> logging.Logger:
        return self.logger

    def get_session(self) -> Session:
        return self.session

    def get_spark_session(self) -> SparkSession:
        return self.spark