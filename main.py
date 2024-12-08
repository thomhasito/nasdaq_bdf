from Session import Session
from NasdaqDF import NasdaqDF
from NasdaqAnalysis import NasdaqAnalysis
from utils.const import COMPANIES_CSV
import pyspark.sql.functions as F

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

session = Session.get_instance()

analysis_period = "6mo"  # 1 year
nasdaq_data = NasdaqDF(
    spark=session.get_spark_session(),
    logger=session.get_logger(),
    req_session=session.get_request_session(),
    csv_path=COMPANIES_CSV,
    analysis_period=analysis_period,
)

companies_df = nasdaq_data.load_companies_df()
stock_df = nasdaq_data.load_stocks_df()

tickers_df = nasdaq_data.merge_dataframes(stock_df, companies_df)
analysis = NasdaqAnalysis(logger=session.get_logger(), stock_df=tickers_df)

print(analysis.calculate_correlation())

