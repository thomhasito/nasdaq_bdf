from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import round
from pyspark.sql.window import Window

import logging
import pandas as pd
import streamlit as st
from utils.const import ColumnNames, EnumPeriod, mapping_return_column_date
from utils.utils import format_period_column

class DataFrameOperations:
    def __init__(self, logger: logging.Logger, stock_df: DataFrame, selected_tickers: list = []):
        self.logger = logger
        self.stock_df = stock_df
        self.selected_tickers = selected_tickers

    ################################################ Return Rate ####################################################
    def calculate_daily_return(
        self
    ) -> DataFrame:
        """
        Calculate the daily return for each stock.
        
        Returns
        -------
        DataFrame
            The DataFrame with the daily return for each stock.
        """
        self.logger.info("Calculating daily return for each stock.")

        selected_df = self.stock_df.filter(F.col(ColumnNames.TICKER.value).isin(self.selected_tickers)) if self.selected_tickers else self.stock_df

        new_stock_df = selected_df.withColumn(
            ColumnNames.DAILY_RETURN.value,
            round(
                100
                * (
                    (F.col(ColumnNames.CLOSE.value) - F.col(ColumnNames.OPEN.value))
                    / F.col(ColumnNames.OPEN.value)
                ),
                4,
            ),
        )

        return new_stock_df

    def calculate_period_return(
        self,
        period: EnumPeriod
    ) -> DataFrame:
        """
        Calculate the return rate for a given period.
        
        Parameters
        ----------
        period : EnumPeriod
            The period to calculate the return rate for.
        
        Returns
        -------
        DataFrame
            The DataFrame with the return rate for each stock.
        """
        selected_df = self.stock_df.filter(F.col(ColumnNames.TICKER.value).isin(self.selected_tickers)) if self.selected_tickers else self.stock_df

        column_rate_name = mapping_return_column_date.get(period)

        if period == EnumPeriod.DAY:
            return self.calculate_daily_return()
        else:
            self.logger.info("Calculating return rate for period: %s", period.value)
            period_col = format_period_column(period, ColumnNames.DATE.value).alias(f"{period.value}_period")
            return_rate_df = (
                selected_df.groupBy(
                    ColumnNames.TICKER.value, period_col
                )
                .agg(
                    F.first(ColumnNames.OPEN.value).alias(f"first_{ColumnNames.OPEN.value}"),
                    F.last(ColumnNames.CLOSE.value).alias(f"last_{ColumnNames.CLOSE.value}"),
                )
                .withColumn(
                    column_rate_name,
                    round(
                        100
                        * (
                            (F.col(f"last_{ColumnNames.CLOSE.value}") - F.col(f"first_{ColumnNames.OPEN.value}"))
                            / F.col(f"first_{ColumnNames.OPEN.value}")
                        ),
                        4,
                    ),
                )
            )

        return return_rate_df
    

    def highest_avg_return_period(
        self,
        return_df: DataFrame,
        period: EnumPeriod,
        top_n: int = 5,
    ) -> DataFrame:
        """
        Find the stocks with the highest average return for a given period.
        
        Parameters
        ----------
        return_df : DataFrame
            The DataFrame with the return for each stock.
        period : EnumPeriod
            The period to calculate the return rate for.
        top_n : int, optional
            The number of stocks to return. The default is 5.

        Returns
        -------
        DataFrame
            The DataFrame with the top n stocks with the highest average return for the given period.
        """
        self.logger.info("Finding stocks with the highest average return for period: %s", period.value)

        selected_df = return_df.filter(F.col(ColumnNames.TICKER.value).isin(self.selected_tickers)) if self.selected_tickers else return_df

        column_rate_name = mapping_return_column_date.get(period)

        avg_rate_name = f"AVG {column_rate_name}"

        if column_rate_name not in selected_df.columns:
            self.logger.warning(
                "%s column not found. Calculating returns first.", column_rate_name
            )
            selected_df = self.calculate_period_return(period, self.selected_tickers)

        return selected_df.groupBy([ColumnNames.TICKER.value, ColumnNames.COMPANY_NAME.value, ColumnNames.SECTOR.value]) \
                                        .agg(F.mean(F.col(column_rate_name)).alias(avg_rate_name)) \
                                        .orderBy(F.desc(avg_rate_name)) \
                                        .limit(top_n)

    ################################################ Volume ####################################################
    def analyze_volume_by_period(
        self,
        period: EnumPeriod,
    ) -> DataFrame:
        """
        Analyze trading volumes over a specified period and provide insights on trends, 
        volatility, and significant periods.

        Parameters
        ----------
        period : EnumPeriod
            The period to analyze the volumes by (e.g., WEEK).

        Returns
        -------
        DataFrame
            A DataFrame containing aggregated volume insights such as average, volatility, and trends.
        """
        self.logger.info("Calculating total volumes exchanged by period.")

        selected_df = self.stock_df.filter(F.col(ColumnNames.TICKER.value).isin(self.selected_tickers)) if self.selected_tickers else self.stock_df

        period_col = format_period_column(period, ColumnNames.DATE.value).alias(f"{period.value}_period") if period != EnumPeriod.DAY else ColumnNames.DATE.value
        volume_window = Window.partitionBy(ColumnNames.TICKER.value, period_col)

        volume_df = selected_df \
        .withColumn(
            "min_volume_date",
            F.first(ColumnNames.DATE.value).over(volume_window.orderBy(F.col(ColumnNames.VOLUME.value).asc()))
        ).withColumn(
            "max_volume_date",
            F.first(ColumnNames.DATE.value).over(volume_window.orderBy(F.col(ColumnNames.VOLUME.value).desc()))
        ).groupBy(
            ColumnNames.TICKER.value, period_col
        ).agg(
            F.sum(ColumnNames.VOLUME.value).alias("total_volume"),
            F.min(ColumnNames.VOLUME.value).alias("min_volume"),
            F.max(ColumnNames.VOLUME.value).alias("max_volume"),
            F.first("min_volume_date").alias("min_volume_date"),
            F.first("max_volume_date").alias("max_volume_date"),
            F.round(F.avg(ColumnNames.VOLUME.value), 4).alias("avg_volume"),
            F.round(F.stddev(ColumnNames.VOLUME.value), 4).alias("volume_volatility")
        )
        
        global_avg_volume_ticker = volume_df.groupBy(ColumnNames.TICKER.value).agg(
            F.avg("total_volume").alias("avg_total_volume_per_ticker")
        )

        volume_df = volume_df.join(
            global_avg_volume_ticker,
            on=ColumnNames.TICKER.value,
            how="left"
        )

        volume_df = volume_df.withColumn(
            "volume_ratio",
            F.round(F.col("total_volume") / F.col("avg_total_volume_per_ticker"), 4)
        )
        return volume_df

    ################################################ Moving Average ####################################################
    def calculate_moving_average(
        self,
        num_days: int=3
    ) -> DataFrame:
        """Calculate the moving average for a given column and number of days.

        Parameters
        ----------
        column : str
            The column to calculate the moving average for.
        num_days : int , optional
            The number of days to calculate the moving average for. The default is 3.

        Returns
        -------
        DataFrame
            The DataFrame with the moving average for the given column and number of days.
        """
        self.logger.info(
            f"Calculating moving average on {num_days} days period.")

        # Define the window for the moving average
        moving_avg_window = (
            Window.partitionBy(ColumnNames.TICKER.value)
            .orderBy(ColumnNames.DATE.value)
            .rowsBetween(-num_days + 1, 0)
        )

        # Calculate the daily return
        selected_df = self.calculate_daily_return()

        # Add the moving average for the daily return
        mvg_avg_col = f"{ColumnNames.DAILY_RETURN.value}_MVG_AVG_{num_days}D"
        mvg_avg_df = selected_df.withColumn(
            mvg_avg_col,
            F.avg(F.col(ColumnNames.DAILY_RETURN.value)).over(moving_avg_window)
        )

        # Calculate the difference between the daily return and its moving average
        diff_col = f"{ColumnNames.DAILY_RETURN.value}_DIFF_{num_days}D"
        result_df = mvg_avg_df.withColumn(
            diff_col,
            F.col(ColumnNames.DAILY_RETURN.value) - F.col(mvg_avg_col)
        )

        return result_df
    
    ################################################ Correlation ####################################################
    def _calculate_correlation_pairs(
        self,
        top_n: int = 5
    ) -> DataFrame:
        """Calculate the correlation between all possible ticker pairs.

        Parameters
        -------
        top_n: int, optionnal
            return only top correlations. Default value si 5.
            
        Returns
        -------
        DataFrame
            The DataFrame with the correlation between all possible ticker pairs.
            """
        self.logger.info(
            "Calculating correlations between all possible ticker pairs. This may take a while.")
        
        selected_df = self.stock_df

        if ColumnNames.DAILY_RETURN.value not in selected_df.columns:
            self.logger.warning(
                "daily_return column not found. Calculating daily returns first."
            )
            selected_df = self.calculate_daily_return()

        selected_df_a = selected_df.alias("a")
        selected_df_b = selected_df.alias("b")

        joined_df = selected_df_a.join(
            selected_df_b,
            (F.col(f"a.{ColumnNames.DATE.value}") == F.col(f"b.{ColumnNames.DATE.value}"))
            & (F.col(f"a.{ColumnNames.TICKER.value}") < F.col(f"b.{ColumnNames.TICKER.value}"))
            & (F.col(f"a.{ColumnNames.TICKER.value}").isin(self.selected_tickers) |
                F.col(f"b.{ColumnNames.TICKER.value}").isin(self.selected_tickers))
        )

        correlation_df = (
            joined_df.groupBy(
                F.col("a." + ColumnNames.TICKER.value).alias("ticker_a"),
                F.col("b." + ColumnNames.TICKER.value).alias("ticker_b"),
            )
            .agg(
                F.corr(
                    F.col("a." + ColumnNames.DAILY_RETURN.value),
                    F.col("b." + ColumnNames.DAILY_RETURN.value),
                ).alias(ColumnNames.CORRELATION.value)
            )
            .select(
                "ticker_a",
                "ticker_b",
                ColumnNames.CORRELATION.value
            )
            .orderBy(F.desc(ColumnNames.CORRELATION.value))
            .limit(top_n)
        )

        return correlation_df
    
    @st.cache_resource(show_spinner=False)
    def get_correlation_df(
        _self,
        top_n: int = 5
    ) -> DataFrame:
        """
        Get the DataFrame with the correlation between all possible ticker pairs.

        Parameters
        -------
        top_n: int, optionnal
            return only top correlations. Default value si 5.

        Returns
        -------
        DataFrame
            The DataFrame with the correlation between all possible ticker pairs.
        """
        df = _self._calculate_correlation_pairs(top_n)
        return df
    
    ################################################ AD Line ####################################################
    def calc_ad_line(
        self,
        with_oscillator: bool = False
    ) -> DataFrame:
        """
        Calculate the Accumulation/Distribution line.

        Parameters
        ----------
        with_oscillator : bool, optional
            If True, calculate the Accumulation/Distribution Oscillator. The default is False.

        Returns
        -------
        DataFrame
            The DataFrame with the Accumulation/Distribution line and oscillator if requested.
        """
        self.logger.info("Calculating Accumulation/Distribution line.")

        selected_df = self.stock_df.filter(F.col(ColumnNames.TICKER.value).isin(self.selected_tickers)) if self.selected_tickers else self.stock_df

        # MFM stands for Money Flow Multiplier
        MFM_1 = (F.col(ColumnNames.CLOSE.value) - F.col(ColumnNames.LOW.value))
        MFM_2 = (F.col(ColumnNames.HIGH.value) - F.col(ColumnNames.CLOSE.value))
        MFM_3 = F.col(ColumnNames.HIGH.value) - F.col(ColumnNames.LOW.value)
        MFM = F.when(MFM_3 != 0, (MFM_1 - MFM_2) / MFM_3).otherwise(0)
        mfm = selected_df.withColumn("MFM", MFM)

        # MFV stands for Money Flow Volume
        MFV = F.col("MFM") * F.col(ColumnNames.VOLUME.value)
        mfv = mfm.withColumn("MFV", MFV)

        # AD line stands for Accumulation/Distribution line
        ad_line = F.sum("MFV").over(Window.orderBy(ColumnNames.DATE.value))
        ad_df = mfv.withColumn(ColumnNames.AD_LINE.value, ad_line)

        if with_oscillator:
            ad_df = ad_df.withColumn(
                ColumnNames.AD_OSCILLATOR.value,
                F.col(ColumnNames.AD_LINE.value) - F.avg(ColumnNames.AD_LINE.value).over(Window.orderBy(ColumnNames.DATE.value)),
            )

        ad_df = ad_df.repartition(ColumnNames.TICKER.value).orderBy(ColumnNames.DATE.value)
        return ad_df

    ################################################ RSI ####################################################
    def calc_rsi(
        self,
        num_days: int = 14
    ) -> DataFrame:
        """Indicator of the relative strength of a stock.

        Parameters
        ----------
        num_days : int, optional
            The number of days to calculate the RSI for. The default is 14.
        
        Returns
        -------
        DataFrame
            The DataFrame with the Relative Strength Index (RSI).
        """
        self.logger.info("Calculating RSI over a given period.")

        selected_df = self.stock_df.filter(F.col(ColumnNames.TICKER.value).isin(self.selected_tickers)) if self.selected_tickers else self.stock_df

        window_spec = Window.partitionBy(ColumnNames.TICKER.value).orderBy(ColumnNames.DATE.value)

        CHANGE = F.col(ColumnNames.CLOSE.value) - F.lag(ColumnNames.CLOSE.value, 1).over(window_spec)
        GAIN = F.when(CHANGE > 0, CHANGE).otherwise(0)
        LOSS = F.when(CHANGE < 0, F.abs(CHANGE)).otherwise(0)
        change_df = selected_df.withColumn("gain", GAIN).withColumn("loss", LOSS)

        avg_window = window_spec.rowsBetween(-num_days + 1, 0)

        rsi_df = change_df.withColumn(
                            "avg_gain", F.avg("gain").over(avg_window)
                        ).withColumn(
                            "avg_loss", F.avg("loss").over(avg_window)
                        ).withColumn(
                            f"RSI_{num_days}D", 100 - (100 / (1 + (F.col("avg_gain") / F.col("avg_loss"))))
                        )

        return rsi_df.select(ColumnNames.TICKER.value, ColumnNames.DATE.value, f"RSI_{num_days}D")