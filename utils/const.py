from enum import Enum
from pathlib import Path


class ColumnNames(Enum):
    DATE = "Date"
    TICKER = "Ticker"
    OPEN = "Open"
    HIGH = "High"
    LOW = "Low"
    CLOSE = "Close"
    VOLUME = "Volume"
    COMPANY_NAME = "Company"
    INDUSTRY = "Industry"
    SECTOR = "Sector"
    DAILY_RETURN = "Daily Return"
    AVG_DAILY_RETURN = "Avg Daily Return"
    WEEKLY_RETURN = "Weekly Return"
    AVG_WEEKLY_RETURN = "Avg Weekly Return"
    MONTHLY_RETURN = "Monthly Return"
    AVG_MONTHLY_RETURN = "Avg Monthly Return"
    QUARTERLY_RETURN = "Quarterly Return"
    AVG_QUARTERLY_RETURN = "Avg Quarterly Return"
    YEARLY_RETURN = "Yearly Return"
    AVG_YEARLY_RETURN = "Avg Yearly Return"
    AD_LINE = "AD line"
    AD_OSCILLATOR = "AD Oscillator"
    CORRELATION = "Correlation"
    SHARPE_RATIO = "Sharpe Ratio"   

    @staticmethod
    def get_ordered_items(all: bool = False) -> list:
        ordered_items = [
            ColumnNames.DATE.value, ColumnNames.TICKER.value, ColumnNames.OPEN.value,
            ColumnNames.HIGH.value, ColumnNames.LOW.value, ColumnNames.CLOSE.value, ColumnNames.VOLUME.value
        ]
        if all:
            ordered_items.extend([
                ColumnNames.COMPANY_NAME.value, ColumnNames.INDUSTRY.value, ColumnNames.SECTOR.value
            ])
        return ordered_items


class EnumPeriod(Enum):
    YEAR = "year"
    QUARTER = "quarter"
    MONTH = "month"
    WEEK = "week"
    DAY = "day"

    def get_format(self):
        formats = {
            "year": "yyyy",
            "quarter": "yyyy-'Q'Q",
            "month": "yyyy-MM",
            "week": "yyyy-'W'ww",
            "day": "yyyy-MM-dd"
        }
        return formats[self.value]
    
    def get_weights(self):
        weights = {
            "day": 1,
            "week": 2,
            "month": 3,
            "quarter": 4,
            "year": 5
        }
        return weights[self.value]


DIR_DATA = "data"
DIR_APP = "app"
DIR_CACHE = "cache"


def get_path(folder: str, file_name: str) -> Path:
    """
    Returns a path to a file in a folder.

    Parameters
    ----------
    folder : str
        The folder name.
    file_name : str
        The file name.

    Returns
    -------
    Path
    The path to the file.
    """
    # get project root
    project_root = Path(__file__).parent.parent

    # return path to file
    file_dir = project_root / folder
    file_dir.mkdir(parents=True, exist_ok=True)
    return file_dir / file_name


COMPANIES_CSV = get_path(DIR_DATA, "companies.csv")
APP_FILE = get_path(DIR_APP, "app.py")
YF_CACHE = get_path(DIR_CACHE, "yf.cache")

APP_NAME = "MyStockApp"
APP_VERSION = "1.0"

# stability roi_finder.py
VERY_STABLE = "Very Stable"
RELATIVELY_STABLE = "Stable"
RELATIVELY_UNSTABLE = "Unstable"
VERY_UNSTABLE = "Very Unstable"

# Colors roi_finder.py
BACKGROUND_COLOR = "#0e0e0e"
TEXT_COLOR = "#ffffff"
VERY_POSITIVE_COLOR = "#4CAF50"
POSITIVE_COLOR = "#8BC34A"
NEUTRAL_COLOR = "#FF9800"
NEGATIVE_COLOR = "#F44336"

mapping_return_column_date = {
    EnumPeriod.DAY: ColumnNames.DAILY_RETURN.value,
    EnumPeriod.WEEK: ColumnNames.WEEKLY_RETURN.value,
    EnumPeriod.MONTH: ColumnNames.MONTHLY_RETURN.value,
    EnumPeriod.QUARTER: ColumnNames.QUARTERLY_RETURN.value,
    EnumPeriod.YEAR: ColumnNames.YEARLY_RETURN.value,
}