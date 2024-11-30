from Session import Session
from NasdaqDF import NasdaqDF
from utils.const import COMPANIES_CSV

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

session = Session.get_instance()
analysis_period = "1d"  # 1 year
nasdaq_data = NasdaqDF(csv_path=COMPANIES_CSV, analysis_period=analysis_period)
companies_df = nasdaq_data.load_companies_df()
stock_df = nasdaq_data.load_stock_df()
merged_df = nasdaq_data.merge_dataframes(stock_df, companies_df)