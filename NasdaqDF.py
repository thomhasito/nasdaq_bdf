import yfinance as yf
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, IntegerType
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from logging import Logger
from requests import Session as RequestSesssion

from utils.const import ColumnNames
from pathlib import Path

class NasdaqDF:    
    def __init__(self, spark: SparkSession, logger: Logger, req_session: RequestSesssion, csv_path: Path, analysis_period: str):
        self.spark = spark
        self.logger = logger
        self.session = req_session

        self.list_nasdaq_path = str(csv_path)
        self.stock_schema = self._define_stock_schema()
        self.nasdaq_schema = self._define_nasdaq_schema()
        self.analysis_period = analysis_period

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
            nasdaq_df = self.spark.read.option("delimiter", ";").csv(self.list_nasdaq_path, header=True, schema=self.nasdaq_schema)
            tickers_df = nasdaq_df.select(ColumnNames.TICKER.value).distinct()
            self.tickers = [row[ColumnNames.TICKER.value] for row in tickers_df.collect()]
            nasdaq_df = nasdaq_df.select(ColumnNames.COMPANY_NAME.value, ColumnNames.TICKER.value,
                                    ColumnNames.INDUSTRY.value, ColumnNames.SECTOR.value)
        except Exception as e:
            self.logger.error(f"Failed to load companies or tickers in DataFrame: {e}")
            return None

        finally:
            if nasdaq_df is None:
                self.logger.warning("The nasdaq_df is None. No data loaded.")
            else:
                ticker_count = nasdaq_df.select(ColumnNames.TICKER.value).distinct().count()
                total_rows = nasdaq_df.count()
    
                if total_rows == 0:
                    self.logger.warning("The nasdaq_df is empty after loading. No companies found.")
                elif ticker_count == 0:
                    self.logger.warning("No distinct tickers found in nasdaq_df.")
                elif total_rows < len(self.tickers):
                    self.logger.warning(f"Loaded DataFrame has fewer rows ({total_rows}) than expected tickers ({len(self.tickers)}).")
                self.logger.info(f"Companies DataFrame loaded successfully for {ticker_count} distinct tickers")
            return nasdaq_df

    def _download_single_ticker(self, ticker: str) -> DataFrame:
        self.logger.info(f"Downloading ticker {ticker} ...")
        stock_data = None
        try:
            stock_data = yf.download(ticker, period=self.analysis_period, rounding=True, session=self.session, progress=False)
            
            if stock_data.empty:
                self.logger.warning(f"No data returned for ticker: {ticker}")
                return None
            
            stock_data.columns = stock_data.columns.get_level_values(0)
            stock_data.columns.name = None
            date = stock_data.index
            stock_data.index = stock_data.index.tz_localize('UTC')
            stock_data[ColumnNames.DATE.value] = stock_data.index.date
            stock_data[ColumnNames.TICKER.value] = ticker

            list_features = ColumnNames.get_ordered_items()
            stock_data = stock_data[list_features].reset_index(drop=True)

        except Exception as e:
            self.logger.error(f"Download failed for {ticker}: {e}")
            return None

        finally:
            nan_count = stock_data.isnull().sum().sum()
            if nan_count > 0:
                self.logger.warning(f"Found {nan_count} NaN values in the data for ticker: {ticker} at date: {stock_data['Date']}")
            return stock_data  

    def load_stocks_df(self, tickers = []):
        """
        Download data for all tickers with error handling and timeout. 
        Can also additionnaly only download specified tickers to speed up downloading.
        """
        ttickers = tickers if len(tickers) > 0 else self.tickers

        self.logger.info(f"Beginning download of {len(ttickers)} tickers")
        stock_df = None
        combined_rdd = self.spark.sparkContext.emptyRDD()
        try:
            for ticker in ttickers:
                data = self._download_single_ticker(ticker)
                if data is not None:
                    rows = [tuple(row) for row in data.to_numpy()]
                    rdd = self.spark.sparkContext.parallelize(rows)
                    combined_rdd = combined_rdd.union(rdd)
            stock_df = self.spark.createDataFrame(combined_rdd, schema=self.stock_schema)
            stock_df = stock_df.repartition(len(self.tickers), F.col(ColumnNames.TICKER.value))

        except Exception as e:
            self.logger.error(f"Failed to load stock DataFrame: {e}")
            return None

        finally:
            if stock_df is None:
                self.logger.warning("The stock_df is None. No data loaded.")
            else:
                #if stock_df.count() < len(self.tickers):
                #    self.logger.warning(f"Downloaded data contains fewer rows ({stock_df.count()}) than tickers ({len(self.tickers)})")
                self.logger.info("Stock DataFrame loaded successfully.")
            return stock_df

    def merge_dataframes(self, stock_df: DataFrame, companies_df: DataFrame) -> DataFrame:
        """Merge stock_df and companies_df with error handling."""
        self.logger.info("Merging stock and companies Dataframe")
        merged_df = None
        try:
            if stock_df is None or companies_df is None:
                self.logger.warning("DataFrames stock and/or companies not loaded.")
            merged_df = stock_df.join(companies_df, on=ColumnNames.TICKER.value, how='inner')
            
        except Exception as e:
            self.logger.error(f"Failed to merge DataFrames: {e}")
            return None

        finally:
            if merged_df is not None:
                self.logger.info("Merged stock and companies DataFrame successfully.")  
            return merged_df