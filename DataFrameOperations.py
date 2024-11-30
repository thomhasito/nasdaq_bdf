from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import round
from pyspark.sql.window import Window

import logging

from utils.const import ColumnNames, EnumPeriod
from utils.utils import format_period_column, add_period
from Session import Session

class DataFrameOperations:
    def __init__(self, logger: logging.Logger, stock_df: DataFrame):
        self.logger = logger
        self.stock_df = stock_df

    def df_periodic_change(self, period: EnumPeriod, column: ColumnNames) -> DataFrame:
        self.logger.info("Calculating periodic change for column: %s with period: %s", column.value, period.value)

        if period == EnumPeriod.DAY:
            window_spec = Window.partitionBy(ColumnNames.TICKER.value).orderBy(ColumnNames.DATE.value)
            daily_change_df = self.stock_df.withColumn(f"daily_change_{column.value}", 
                round(F.col(column.value) - F.lag(column.value, 1).over(window_spec), 2))
            return daily_change_df.select(ColumnNames.TICKER.value, ColumnNames.DATE.value, f"daily_change_{column.value}")

        period_col = format_period_column(period, ColumnNames.DATE.value)

        return (
            self.stock_df
            .groupBy(ColumnNames.TICKER.value, period_col.alias(f"{period.value}_period"))
            .agg(
                F.first(column.value).alias(f"first_{column.value}"),
                F.last(column.value).alias(f"last_{column.value}")
            )
            .withColumn(f"{period.value}_change_{column.value}", 
                        round(F.col(f"last_{column.value}") - F.col(f"first_{column.value}"), 2))
            .select(ColumnNames.TICKER.value, f"{period.value}_period", f"{period.value}_change_{column.value}")
        )

    def avg_open_close_by_period(self, period: EnumPeriod) -> DataFrame:
        self.logger.info("Calculating average Open and Close by period: %s", period.value)

        period_col = format_period_column(period, ColumnNames.DATE.value)

        return (
            self.stock_df
            .groupBy(ColumnNames.TICKER.value, period_col.alias(f"{period.value}_period"))
            .agg(
                round(F.avg(ColumnNames.OPEN.value), 2).alias("avg_open"), 
                round(F.avg(ColumnNames.CLOSE.value), 2).alias("avg_close")
            )
        )

    def calculate_return_rate(self, period: EnumPeriod, column: str) -> DataFrame:
        self.logger.info("Calculating return rate for column: %s with period: %s", column, period.value)
    
        if period == EnumPeriod.DAY:
            window_spec = Window.partitionBy(ColumnNames.TICKER.value).orderBy(ColumnNames.Date.value)
            daily_return_df = self.stock_df.withColumn(
                f"{period.value}_return_rate",
                F.round(((F.col(column.value) - F.lag(column.value, 1).over(window_spec)) / F.lag(column.value, 1).over(window_spec)) * 100, 2)
            )
            return daily_return_df.select(ColumnNames.TICKER.value, ColumnNames.DATE.value, f"{period.value}_return_rate")
        
        period_col = format_period_column(period, ColumnNames.DATE.value)
        
        return_rate_df = (
            self.stock_df
            .groupBy(ColumnNames.TICKER.value, period_col.alias(f"{period.value}_period"))
            .agg(
                F.first(column.value).alias(f"first_{column.value}"),
                F.last(column.value).alias(f"last_{column.value}")
            )
            .withColumn(
                f"{period.value}_return_rate",
                F.round(((F.col(f"last_{column.value}") - F.col(f"first_{column.value}")) / F.col(f"first_{column.value}")) * 100, 2)
            )
            .select(ColumnNames.TICKER.value, f"{period.value}_period", f"{period.value}_return_rate")
        )
        
        return return_rate_df

    def calculate_daily_return(self) -> DataFrame:
        self.logger.info("Calculating daily return for each stock.")
        
        new_stock_df = self.stock_df.withColumn("daily_return", 
            round(100 * ((F.col(ColumnNames.CLOSE.value) - F.col(ColumnNames.OPEN.value)) / F.col(ColumnNames.OPEN.value)), 4)
        )

        self.stock_df = new_stock_df
        
        return new_stock_df

    def avg_daily_return_by_period(self, period: EnumPeriod) -> DataFrame:
        self.logger.info("Calculating average daily return for each stock within the period.")
        
        if "daily_return" not in self.stock_df.columns:
            self.logger.warning("daily_return column not found. Calculating daily returns first.")
            self.calculate_daily_return()

        period_col = format_period_column(period, ColumnNames.DATE.value)

        return (self.stock_df
                .groupBy(ColumnNames.TICKER.value, period_col.alias(f"{period.value}_period"))
                .agg(round(F.avg("daily_return"), 4).alias("avg_daily_return"))
               )

    def stocks_with_highest_daily_return(self, daily_return_df: DataFrame, top_n: int = 5) -> DataFrame:
        self.logger.info("Finding stocks with the highest daily return.")

        if "daily_return" not in self.stock_df.columns:
            self.logger.warning("daily_return column not found. Calculating daily returns first.")
            self.calculate_daily_return()

        return daily_return_df.orderBy(F.desc("daily_return")).limit(top_n)

    def calculate_moving_average(self, column: str, num_days: int) -> DataFrame:
        self.logger.info(f"Calculating moving average on {num_days} days period.")

        moving_avg_window = Window.partitionBy(ColumnNames.TICKER.value).orderBy(ColumnNames.DATE.value).rowsBetween(-num_days + 1, 0)
        count_col = F.count(F.col(column)).over(moving_avg_window)
    
        return (self.stock_df
                .withColumn(f"{column}_moving_avg_{num_days}_days",
                F.when(count_col == num_days, F.avg(F.col(column)).over(moving_avg_window)).otherwise(None))
               )

    def calculate_correlation_pairs(self) -> DataFrame:
        self.logger.info("Calculating correlations between all possible ticker pairs.")

        if "daily_return" not in self.stock_df.columns:
            self.logger.warning("daily_return column not found. Calculating daily returns first.")
            self.calculate_daily_return()

        daily_return_df_a = self.stock_df.alias("a")
        daily_return_df_b = self.stock_df.alias("b")

        joined_df = (
            daily_return_df_a.join(daily_return_df_b, (F.col("a.Date") == F.col("b.Date")) & 
                                                     (F.col("a.Ticker") < F.col("b.Ticker")))
        )

        correlation_df = (
            joined_df.groupBy("a.Ticker", "b.Ticker")
                     .agg(F.corr("a.daily_return", "b.daily_return").alias("correlation"))
                     .filter(F.col("correlation").isNotNull())
                     .orderBy(F.desc("correlation"))
        )

        return correlation_df

    def calculate_periodic_return(self, period: str, price_column: str = "Close") -> DataFrame:
        period_col = format_period_column(period, ColumnNames.DATE.value)
        return_df = (self.stock_df
                     .groupBy(ColumnNames.TICKER.value, period_col.alias(f"{period.value}_period"))
                     .agg(
                         F.first(price_column).alias("first_price"),
                         F.last(price_column).alias("last_price")
                     )
                     .withColumn(f"{period}_return_rate", 
                                 (F.col("last_price") - F.col("first_price")) / F.col("first_price") * 100)
                     .select("Ticker", period_col, f"{period}_return_rate")
                    )
    
        return return_df

    def calculate_best_return_rate(self, start_date: str, period: EnumPeriod, column: ColumnNames):
        self.logger.info("Calculating best return rate from %s for period: %s", start_date, period.value)
    
        end_date = add_period(start_date, period, 1)
    
        filtered_df = self.stock_df.filter((F.col(ColumnNames.DATE.value) >= start_date) & (F.col(ColumnNames.DATE.value) <= end_date))
    
        return_rate_df = filtered_df.groupBy(ColumnNames.TICKER.value).agg(
            F.first(column.value).alias("first_price"),
            F.last(column.value).alias("last_price")
        ).withColumn(
            "return_rate",
            F.round(((F.col("last_price") - F.col("first_price")) / F.col("first_price")) * 100, 2)
        )
    
        return return_rate_df.orderBy(F.col("return_rate").desc())
    
# TODO: Download stock_df
stock_df = DataFrame # Replace DataFrame with the actual DataFrame
df_operations = DataFrameOperations(Session.get_instance().get_logger(), stock_df)
period = EnumPeriod.MONTH
column = ColumnNames.CLOSE
df_periodic_change = df_operations.df_periodic_change(period, column)
df_avg_close = df_operations.avg_open_close_by_period(period)
df_month_return = df_operations.calculate_return_rate(period, column)
df_daily_return = df_operations.calculate_daily_return()
df_highest_daily_return = df_operations.stocks_with_highest_daily_return(df_daily_return)
df_avg_daily_return = df_operations.avg_daily_return_by_period(period)
nb_days = 5
df_mvg_avg = df_operations.calculate_moving_average(ColumnNames.OPEN.value, nb_days)
df_correlations = df_operations.calculate_correlation_pairs()
start_date = "2024-01-01"
df_best_return_rate = df_operations.calculate_best_return_rate(start_date, EnumPeriod.MONTH, ColumnNames.CLOSE)