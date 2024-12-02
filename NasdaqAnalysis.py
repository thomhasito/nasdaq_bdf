from pyspark.sql.types import FloatType, IntegerType
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from utils.const import ColumnNames
from Session import SessionApp as Session

class NasdaqAnalysis:
    
    def __init__(self, stock_df: DataFrame):
        self.stock_df = stock_df
        self.logger = Session.get_instance().get_logger()

    def display_stock_bounds(self, ticker: str, num_rows: int = 40) -> None:
        """
        Display the first and last `num_rows` rows for the specified ticker in the DataFrame.
        
        Args:
            ticker (str): The stock ticker symbol to filter data.
            num_rows (int): Number of rows to display at the start and end of the data. Default is 40.
        
        Returns:
            None
        """
        try:
            self.logger.info(f"Displaying first and last {num_rows} rows for ticker: {ticker}")
            df_filtered = self.stock_df.filter(F.col(ColumnNames.TICKER.value) == ticker)
            
            print(f"First {num_rows} rows for ticker: {ticker}")
            df_filtered.orderBy(F.asc(ColumnNames.DATE.value)).show(num_rows)
            
            print(f"Last {num_rows} rows for ticker: {ticker}")
            df_filtered.orderBy(F.desc(ColumnNames.DATE.value)).show(num_rows)
        
        except Exception as e:
            self.logger.error(f"Error displaying bounds for ticker {ticker}: {e}")

    def count_observations(self):
        """
        Count the total number of observations (rows) in the DataFrame.
        
        Returns:
            int: Total number of observations.
        """
        try:
            total_count = self.stock_df.count()
            self.logger.info(f"Total observations: {total_count}")
            return total_count
        except Exception as e:
            self.logger.error(f"Error counting observations: {e}")
            return None

    def deduce_data_period(self):
        """
        Calculate both the average and most common data period in days based on date differences.
        
        Returns:
            dict: A dictionary with "average_period" as a float and "most_common_period" as an integer.
        """
        df = self.stock_df
        try:
            logger = Session.get_instance().get_logger()
            logger.info("Inferring data period based on date differences.")
    
            window_spec = Window.partitionBy(ColumnNames.TICKER.value).orderBy(ColumnNames.DATE.value)
    
            df_with_diff = df.withColumn("date_diff", F.datediff(F.col(ColumnNames.DATE.value), F.lag(ColumnNames.DATE.value, 1).over(window_spec)))
    
            avg_diff = df_with_diff.agg(F.mean("date_diff")).first()[0]
    
            most_common_diff = (
                df_with_diff.groupBy("date_diff")
                .count()
                .orderBy(F.desc("count"))
                .first()
            )
            most_common_period = most_common_diff["date_diff"]
    
            logger.info(f"Inferred average data period: {avg_diff} days")
            logger.info(f"Inferred most common data period: {most_common_period} days")
            
            return {"average_period": round(avg_diff, 2), "most_common_period": most_common_period}
            
        except Exception as e:
            logger.error(f"Error inferring data period: {e}")
            return None

    def descriptive_statistics(self) -> DataFrame:
        """
        Display basic descriptive statistics (min, max, mean, standard deviation) for numeric columns.

        Returns:
            DataFrame: A DataFrame containing descriptive statistics for numeric columns.
        """
        try:
            self.logger.info("Calculating descriptive statistics for numeric columns.")
            return self.stock_df.describe()
        except Exception as e:
            self.logger.error(f"Error calculating descriptive statistics: {e}")

    def count_missing_values(self):
        """
        Count the number of missing values for each column in the DataFrame.

        Returns:
            DataFrame: A DataFrame containing counts of missing values per column.
        """
        try:
            self.logger.info("Counting missing values for each column.")
            missing_df = self.stock_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in self.stock_df.columns])
            return missing_df
        except Exception as e:
            self.logger.error(f"Error counting missing values: {e}")
            return None

    def calculate_correlation(self):
        """
        Calculate correlations between numeric columns in the DataFrame.

        Returns:
            dict: A dictionary with tuple keys (column1, column2) and correlation values.
        """
        try:
            self.logger.info("Calculating correlations between numeric columns.")
            numeric_columns = [field.name for field in self.stock_df.schema.fields if isinstance(field.dataType, (FloatType, IntegerType))]
            correlations = {}
            for i in range(len(numeric_columns)):
                for j in range(i + 1, len(numeric_columns)):
                    col1, col2 = numeric_columns[i], numeric_columns[j]
                    corr_value = self.stock_df.corr(col1, col2)
                    correlations[(col1, col2)] = corr_value
            return correlations
        except Exception as e:
            self.logger.error(f"Error calculating correlations: {e}")
            return {}
        
# TODO: Download stock_df
stock_df = DataFrame # Replace DataFrame with the actual DataFrame
analysis = NasdaqAnalysis(stock_df)
ticker = 'AAPL'
analysis.display_stock_bounds(ticker)
nb_count = analysis.count_observations()
dict_period = analysis.deduce_data_period()
df_stats = analysis.descriptive_statistics()
missing_df = analysis.count_missing_values()
correlations = analysis.calculate_correlation()