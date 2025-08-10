"""
Market Data ETL - A Python package for extracting and storing market data from Yahoo Finance.

This package provides tools to:
- Fetch historical price data (OHLC + Volume) using yfinance
- Fetch standardized financial statements for comprehensive analysis
- Store data in SQLite database with SQLAlchemy ORM
- Provide CLI interface for data operations
- Handle rate limiting with exponential backoff retry logic
- Detect and fill missing data gaps intelligently
- Support global companies with proper currency handling

Author: Generated with Claude Code
Version: 2.0.0
License: MIT
"""

__version__ = "2.0.0"
__author__ = "Generated with Claude Code"
__license__ = "MIT"

# Core imports for easy access
from .data.fetchers import PriceFetcher, FundamentalsFetcher
from .data.financial_fetcher import FinancialStatementFetcher
from .data.financial_standardizer import FinancialStandardizer
from .database.manager import DatabaseManager
# FinancialDatabaseManager merged into DatabaseManager
from .utils.exceptions import MarketDataETLError, YahooFinanceError

__all__ = [
    "PriceFetcher",
    "FundamentalsFetcher", 
    "FinancialStatementFetcher",
    "FinancialStandardizer",
    "DatabaseManager",
    "MarketDataETLError",
    "YahooFinanceError"
]