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


DIR_DATA = "data"
DIR_SESSION = "session"
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
SESSION_FILE = get_path(DIR_SESSION, "session.pkl")
YF_CACHE = get_path(DIR_CACHE, "yf.cache")

APP_NAME = "MyStockApp"
APP_VERSION = "1.0"
