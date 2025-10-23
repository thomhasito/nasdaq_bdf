import logging
from typing import Iterable, Optional, Union

import yfinance as yf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from requests import Session as RequestSesssion

from logging import Logger

from utils.const import ColumnNames
from pathlib import Path

class NasdaqDF:    
    def __init__(
        self,
        spark: SparkSession,
        logger: Logger,
        req_session: RequestSesssion,
        csv_path: Path,
        analysis_period: str,
    ):
        self.spark = spark
        self.logger = logger
        self.session = req_session

        self.list_nasdaq_path = str(csv_path)
        self.stock_schema = self._define_stock_schema()
        self.nasdaq_schema = self._define_nasdaq_schema()
        self.analysis_period = analysis_period
        self.tickers_df: Optional[DataFrame] = None

    def _define_stock_schema(self):
        """Define the schema for the Spark DataFrame."""
        return StructType([
            StructField(ColumnNames.DATE.value, DateType(), True),
            StructField(ColumnNames.TICKER.value, StringType(), True),
            StructField(ColumnNames.OPEN.value, FloatType(), True),
            StructField(ColumnNames.HIGH.value, FloatType(), True),
            StructField(ColumnNames.LOW.value, FloatType(), True),
            StructField(ColumnNames.CLOSE.value, FloatType(), True),
            # StructField(ColumnNames.ADJ_CLOSE.value, FloatType(), True),
            StructField(ColumnNames.VOLUME.value, IntegerType(), True)
        ])

    def _define_nasdaq_schema(self):
        """Define the schema for Nasdaq company information."""
        return StructType([
            StructField(ColumnNames.TICKER.value, StringType(), True),
            StructField(ColumnNames.COMPANY_NAME.value, StringType(), True),
            StructField(ColumnNames.INDUSTRY.value, StringType(), True),
            StructField(ColumnNames.SECTOR.value, StringType(), True)
        ])

    def load_companies_df(self):
        """Load the Nasdaq company data with error handling."""
        self.logger.info("Beginning download of companies Dataframe")
        nasdaq_df = None
        try:
            nasdaq_df = (
                self.spark.read.option("delimiter", ";")
                .csv(self.list_nasdaq_path, header=True, schema=self.nasdaq_schema)
                .select(
                    ColumnNames.COMPANY_NAME.value,
                    ColumnNames.TICKER.value,
                    ColumnNames.INDUSTRY.value,
                    ColumnNames.SECTOR.value,
                )
                .dropna(subset=[ColumnNames.TICKER.value])
            )

            self.tickers_df = (
                nasdaq_df.select(ColumnNames.TICKER.value)
                .dropna()
                .distinct()
                .cache()
            )
            ticker_count = self.tickers_df.count()

        except Exception as e:
            self.logger.error(f"Failed to load companies or tickers in DataFrame: {e}")
            return None

        if nasdaq_df is None:
            self.logger.warning("The nasdaq_df is None. No data loaded.")
            return None

        if self.tickers_df is None:
            ticker_count = 0
        total_rows = nasdaq_df.count()

        if total_rows == 0:
            self.logger.warning("The nasdaq_df is empty after loading. No companies found.")
        elif ticker_count == 0:
            self.logger.warning("No distinct tickers found in nasdaq_df.")

        self.logger.info(
            f"Companies DataFrame loaded successfully for {ticker_count} distinct tickers"
        )
        return nasdaq_df

    def _normalise_tickers_input(
        self, tickers: Optional[Union[DataFrame, str, Iterable[str]]]
    ) -> Optional[DataFrame]:
        if tickers is None:
            if self.tickers_df is None:
                self.logger.warning(
                    "Companies DataFrame not loaded yet. Call load_companies_df first."
                )
                return None
            return self.tickers_df

        if isinstance(tickers, DataFrame):
            return tickers.select(ColumnNames.TICKER.value).dropna().distinct()

        if isinstance(tickers, str):
            tickers = [tickers]

        if isinstance(tickers, Iterable):
            ticker_rows = [(ticker,) for ticker in tickers if ticker]
            if not ticker_rows:
                return None
            schema = StructType(
                [StructField(ColumnNames.TICKER.value, StringType(), True)]
            )
            return self.spark.createDataFrame(ticker_rows, schema=schema)

        self.logger.error(
            "Unsupported tickers argument type %s. Expected DataFrame or iterable.",
            type(tickers),
        )
        return None

    def load_stocks_df(
        self,
        tickers: Optional[Union[DataFrame, str, Iterable[str]]] = None,
        repartition_hint: Optional[int] = None,
    ):
        """
        Download data for the provided tickers using Spark mapInPandas to avoid
        materialising results on the driver.
        """

        tickers_df = self._normalise_tickers_input(tickers)
        if tickers_df is None:
            self.logger.warning("No tickers provided or available for download.")
            return None

        if tickers_df.rdd.isEmpty():
            self.logger.warning("Ticker DataFrame is empty. Nothing to download.")
            return None

        partitions = repartition_hint if repartition_hint and repartition_hint > 0 else None
        if partitions:
            tickers_df = tickers_df.repartition(
                partitions, F.col(ColumnNames.TICKER.value)
            )
        else:
            tickers_df = tickers_df.repartition(F.col(ColumnNames.TICKER.value))

        logger_name = getattr(self.logger, "name", __name__)
        analysis_period = self.analysis_period
        request_headers = dict(self.session.headers) if self.session else {}

        def download_partition(iterator):
            from requests import Session as RequestsSession

            import pandas as pd

            local_logger = logging.getLogger(logger_name)
            if not local_logger.handlers:
                logging.basicConfig(level=logging.INFO)

            session = RequestsSession()
            session.headers.update(request_headers)

            for pdf in iterator:
                tickers_batch = (
                    pdf[ColumnNames.TICKER.value]
                    .dropna()
                    .astype(str)
                    .str.strip()
                    .unique()
                )

                batch_frames = []
                for ticker in tickers_batch:
                    try:
                        data = yf.download(
                            ticker,
                            period=analysis_period,
                            rounding=True,
                            session=session,
                            progress=False,
                        )
                    except Exception as exc:  # pragma: no cover - network errors
                        local_logger.warning(
                            "Download failed for %s with error %s", ticker, exc
                        )
                        continue

                    if data.empty:
                        local_logger.info("No data returned for ticker %s", ticker)
                        continue

                    data.columns = data.columns.get_level_values(0)
                    data.columns.name = None
                    data.index = data.index.tz_localize("UTC")
                    data[ColumnNames.DATE.value] = data.index.date
                    data[ColumnNames.TICKER.value] = ticker

                    batch_frames.append(data[ColumnNames.get_ordered_items()])

                if batch_frames:
                    yield pd.concat(batch_frames, ignore_index=True)

        try:
            stock_df = tickers_df.mapInPandas(
                download_partition, schema=self.stock_schema
            )
        except Exception as e:
            self.logger.error(f"Failed to load stock DataFrame: {e}")
            return None

        if stock_df.rdd.isEmpty():
            self.logger.warning("Stock DataFrame is empty after downloads.")
            return None

        stock_df = stock_df.withColumn(
            ColumnNames.DATE.value, F.to_date(F.col(ColumnNames.DATE.value))
        )

        self.logger.info("Stock DataFrame loaded successfully.")
        return stock_df

    def merge_dataframes(self, stock_df: DataFrame, companies_df: DataFrame) -> DataFrame:
        """Merge stock_df and companies_df with error handling."""
        self.logger.info("Merging stock and companies Dataframe")
        if stock_df is None or companies_df is None:
            self.logger.warning("DataFrames stock and/or companies not loaded.")
            return None

        try:
            merged_df = stock_df.join(
                companies_df, on=ColumnNames.TICKER.value, how="inner"
            )
        except Exception as e:
            self.logger.error(f"Failed to merge DataFrames: {e}")
            return None

        self.logger.info("Merged stock and companies DataFrame successfully.")
        return merged_df
