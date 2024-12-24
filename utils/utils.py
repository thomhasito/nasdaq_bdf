from utils.const import EnumPeriod
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import calendar


def format_period_column(period: EnumPeriod, date_column: str) -> str:
    """
    Formats a date column to a specific period.

    Parameters
    ----------
    period : EnumPeriod
        The period to format the column to.
        date_column : str
        The name of the date column.

    Returns
    -------
    str
    The formatted column.
    """
    if period == EnumPeriod.WEEK:
        year_col = F.year(date_column).cast("string")
        week_col = F.format_string("%02d", F.weekofyear(date_column))
        return F.concat(year_col, F.lit("-W"), week_col)
    else:
        truncated_col = F.date_trunc(period.value, date_column)
        return F.date_format(truncated_col, period.get_format())


def add_period(start_date: str, period: EnumPeriod, amount: int) -> str:
    """
    Adds a period to a date.

    Parameters
    ----------
    start_date : str
        The start date.
    period : EnumPeriod
        The period to add.
    amount : int
        The amount of periods to add.

    Returns
    -------
    str
    The new date.
    """
    date_obj = datetime.strptime(start_date, "%Y-%m-%d")

    if period == EnumPeriod.DAY:
        new_date = date_obj + timedelta(days=amount)
    elif period == EnumPeriod.WEEK:
        new_date = date_obj + timedelta(weeks=amount)
    elif period == EnumPeriod.MONTH:
        new_month = (date_obj.month + amount - 1) % 12 + 1
        new_year = date_obj.year + (date_obj.month + amount - 1) // 12
        last_day_of_new_month = calendar.monthrange(new_year, new_month)[1]
        new_day = min(date_obj.day, last_day_of_new_month)
        new_date = date_obj.replace(
            year=new_year, month=new_month, day=new_day)
    elif period == EnumPeriod.QUARTER:
        new_month = (date_obj.month + amount * 3 - 1) % 12 + 1
        new_year = date_obj.year + (date_obj.month + amount * 3 - 1) // 12
        last_day_of_new_month = calendar.monthrange(new_year, new_month)[1]
        new_day = min(date_obj.day, last_day_of_new_month)
        new_date = date_obj.replace(
            year=new_year, month=new_month, day=new_day)
    elif period == EnumPeriod.YEAR:
        new_date = date_obj.replace(year=date_obj.year + amount)
    else:
        raise ValueError(f"Unknown period: {period}")

    return new_date.strftime("%Y-%m-%d")


def period_to_yf_time_frame(date: EnumPeriod) -> str:
    analyse_date_mapping = {
        EnumPeriod.DAY: "1d",
        EnumPeriod.WEEK: "5d",
        EnumPeriod.MONTH: "1mo",
        EnumPeriod.QUARTER: "3mo",
        EnumPeriod.YEAR: "1y",
    }

    return analyse_date_mapping[date] if date in analyse_date_mapping else "5d"
