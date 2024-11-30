from enum import Enum
from utils.utils import get_path

class ColumnNames(Enum):
    DATE = "Date"
    TICKER = "Ticker"
    OPEN = "Open"
    HIGH = "High"
    LOW = "Low"
    CLOSE = "Close"
    ADJ_CLOSE = "Adj Close"
    VOLUME = "Volume"
    COMPANY_NAME = "Company"
    INDUSTRY = "Industry"
    SECTOR = "Sector"

    def get_ordered_items(self, all: bool = False) -> list:
        ordered_items = [self.DATE, self.TICKER, self.OPEN, self.HIGH, self.LOW, self.CLOSE, self.ADJ_CLOSE, self.VOLUME]
        if all:
            ordered_items.extend([self.COMPANY_NAME, self.INDUSTRY, self.SECTOR])
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

COMPANIES_CSV = get_path(DIR_DATA, "companies.csv")
APP_FILE = get_path(DIR_APP, "app.py")
SESSION_FILE = get_path(DIR_SESSION, "session.pkl")
YF_CACHE = get_path(DIR_CACHE, "yf.cache")

APP_NAME = "MyStockApp"
APP_VERSION = "1.0"