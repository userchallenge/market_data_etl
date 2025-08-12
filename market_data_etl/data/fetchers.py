"""
Data fetching classes with retry logic for Yahoo Finance APIs.

This module provides classes to fetch price and fundamental data
from Yahoo Finance with robust error handling and exponential backoff.
"""

import time
from typing import Optional, Dict, Any
from datetime import date, datetime, timedelta
import yfinance as yf
import pandas as pd
import requests
import json

from ..config import config
from ..utils.logging import get_logger
from ..utils.exceptions import YahooFinanceError


class DataFetcher:
    """
    Base class for data fetching with retry logic.
    
    Implements exponential backoff strategy using configuration:
    - Configurable retry count and backoff timing
    - Only retries on transient errors (HTTP 429, 500-599)
    """
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.retry_config = config.retry
    
    def _should_retry(self, error: Exception) -> bool:
        """
        Determine if an error should trigger a retry.
        
        Args:
            error: Exception that occurred during request
            
        Returns:
            True if the error is transient and should be retried
        """
        if isinstance(error, requests.HTTPError):
            status_code = error.response.status_code if hasattr(error, 'response') and error.response else 0
            return status_code == 429 or (500 <= status_code <= 599)
        
        # Network errors, connection timeouts
        if isinstance(error, (requests.ConnectionError, requests.Timeout)):
            return True
            
        return False
    
    def _retry_with_backoff(self, func, *args, **kwargs):
        """
        Execute function with exponential backoff retry logic.
        
        Args:
            func: Function to execute
            *args: Arguments for the function
            **kwargs: Keyword arguments for the function
            
        Returns:
            Result of the function call
            
        Raises:
            YahooFinanceError: If all retries are exhausted
        """
        last_error = None
        backoff_delay = self.retry_config.initial_backoff
        
        for attempt in range(self.retry_config.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as error:
                last_error = error
                
                if attempt == self.retry_config.max_retries:
                    break
                
                if not self._should_retry(error):
                    break
                
                self.logger.warning(
                    f"Attempt {attempt + 1} failed: {error}. "
                    f"Retrying in {backoff_delay}s..."
                )
                time.sleep(backoff_delay)
                backoff_delay *= self.retry_config.backoff_multiplier
        
        # All retries exhausted
        error_msg = f"Failed after {self.retry_config.max_retries} retries"
        if hasattr(last_error, 'response') and last_error.response:
            error_msg += f". HTTP {last_error.response.status_code}: {last_error.response.reason}"
        error_msg += f". Last error: {last_error}"
        
        raise YahooFinanceError(error_msg)


class PriceFetcher(DataFetcher):
    """
    Fetches historical price data using yfinance.
    
    Provides methods to fetch OHLC data with volume for specified date ranges
    and handles missing data detection.
    """
    
    def fetch_price_data(
        self, 
        ticker: str, 
        start_date: date, 
        end_date: Optional[date] = None
    ) -> pd.DataFrame:
        """
        Fetch price data for a ticker within a date range.
        
        Args:
            ticker: Stock ticker symbol
            start_date: Start date for data fetch
            end_date: End date for data fetch (defaults to today)
            
        Returns:
            DataFrame with OHLC price data
            
        Raises:
            YahooFinanceError: If data fetch fails after retries
        """
        if end_date is None:
            end_date = date.today()
        
        def _fetch():
            self.logger.info(f"Fetching price data for {ticker} from {start_date} to {end_date}")
            
            try:
                yf_ticker = yf.Ticker(ticker)
                
                # Try using period first if requesting recent data (more reliable)
                days_requested = (end_date - start_date).days
                if days_requested <= 30 and end_date == date.today():
                    period_map = {
                        1: "1d", 2: "2d", 5: "5d", 7: "1wk", 14: "2wk", 30: "1mo"
                    }
                    period = None
                    for max_days, period_str in period_map.items():
                        if days_requested <= max_days:
                            period = period_str
                            break
                    
                    if period:
                        self.logger.info(f"Using period={period} for recent data")
                        data = yf_ticker.history(period=period, auto_adjust=False, prepost=False)
                    else:
                        # Add one day to end_date for exclusive range
                        end_date_exclusive = end_date + timedelta(days=1)
                        data = yf_ticker.history(
                            start=start_date.strftime('%Y-%m-%d'),
                            end=end_date_exclusive.strftime('%Y-%m-%d'),
                            auto_adjust=False,
                            prepost=False
                        )
                else:
                    # Use date range for historical data
                    # Add one day to end_date for exclusive range
                    end_date_exclusive = end_date + timedelta(days=1)
                    data = yf_ticker.history(
                        start=start_date.strftime('%Y-%m-%d'),
                        end=end_date_exclusive.strftime('%Y-%m-%d'),
                        auto_adjust=False,
                        prepost=False
                    )
                
                if data.empty:
                    raise YahooFinanceError(f"No price data found for ticker {ticker}")
                
                # Reset index to get date as column
                data = data.reset_index()
                
                # Standardize column names
                data.columns = [col.replace(' ', '_').lower() for col in data.columns]
                
                # Handle date column - newer yfinance versions may have different formats
                if 'date' in data.columns:
                    # Convert timezone-aware timestamps to date objects
                    data['date'] = [d.date() for d in data['date']]
                    # Filter to requested date range
                    data = data[(data['date'] >= start_date) & (data['date'] <= end_date)]
                
                return data
                
            except Exception as e:
                if "No data found" in str(e) or "delisted" in str(e):
                    raise YahooFinanceError(f"Ticker {ticker} not found or has no data")
                raise e
        
        return self._retry_with_backoff(_fetch)


class FundamentalsFetcher(DataFetcher):
    """
    Fetches fundamental data using yfinance.
    
    Provides methods to fetch comprehensive fundamental data including
    income statements, balance sheets, cash flows, and other financial metrics
    optimized for fundamental analysis.
    """
    
    # Available fundamental data types in yfinance
    FUNDAMENTAL_DATA_TYPES = {
        'income_stmt': 'Annual income statement',
        'quarterly_income_stmt': 'Quarterly income statement', 
        'balance_sheet': 'Annual balance sheet',
        'quarterly_balance_sheet': 'Quarterly balance sheet',
        'cashflow': 'Annual cash flow statement',
        'quarterly_cashflow': 'Quarterly cash flow statement',
        'earnings': 'Earnings data',
        'calendar': 'Financial calendar',
        'info': 'Company information and key statistics',
        'recommendations': 'Analyst recommendations',
        'institutional_holders': 'Institutional ownership',
        'mutualfund_holders': 'Mutual fund ownership',
        'insider_purchases': 'Insider purchase transactions',
        'insider_transactions': 'All insider transactions',
        'sec_filings': 'SEC filings'
    }
    
    def fetch_fundamentals(self, ticker: str) -> Dict[str, Any]:
        """
        Fetch comprehensive fundamental data for a ticker using yfinance.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Dictionary with fundamental data organized by type
            
        Raises:
            YahooFinanceError: If data fetch fails after retries
        """
        def _fetch():
            self.logger.info(f"Fetching fundamental data for {ticker}")
            
            try:
                yf_ticker = yf.Ticker(ticker)
                
                results = {}
                successful_fetches = []
                failed_fetches = []
                
                # Fetch each type of fundamental data
                for data_type, description in self.FUNDAMENTAL_DATA_TYPES.items():
                    try:
                        self.logger.debug(f"Fetching {description} for {ticker}")
                        
                        if data_type == 'info':
                            data = yf_ticker.info
                        elif data_type == 'calendar':
                            data = yf_ticker.calendar
                        elif data_type == 'earnings':
                            data = yf_ticker.earnings
                        elif data_type == 'recommendations':
                            data = yf_ticker.recommendations
                        elif data_type == 'institutional_holders':
                            data = yf_ticker.institutional_holders
                        elif data_type == 'mutualfund_holders':
                            data = yf_ticker.mutualfund_holders
                        elif data_type == 'insider_purchases':
                            data = yf_ticker.insider_purchases
                        elif data_type == 'insider_transactions':
                            data = yf_ticker.insider_transactions
                        elif data_type == 'sec_filings':
                            data = yf_ticker.sec_filings
                        else:
                            # Use getattr for financial statements
                            data = getattr(yf_ticker, data_type)
                        
                        # Check if data is valid and not empty
                        if self._is_valid_data(data):
                            # Convert DataFrame to dict if needed for JSON storage
                            if isinstance(data, pd.DataFrame):
                                if not data.empty:
                                    results[data_type] = self._prepare_for_json_storage(data.to_dict('index'))
                                    successful_fetches.append(data_type)
                                else:
                                    self.logger.debug(f"Empty DataFrame for {data_type}")
                            elif isinstance(data, dict) and data:
                                results[data_type] = self._prepare_for_json_storage(data)
                                successful_fetches.append(data_type)
                            elif data is not None:
                                results[data_type] = str(data)  # Convert other types to string
                                successful_fetches.append(data_type)
                        else:
                            self.logger.debug(f"No valid data for {data_type}")
                    
                    except Exception as e:
                        failed_fetches.append(f"{data_type}: {str(e)}")
                        self.logger.debug(f"Failed to fetch {data_type}: {e}")
                
                if not results:
                    raise YahooFinanceError(
                        f"No fundamental data found for ticker {ticker}. "
                        f"All data types failed: {'; '.join(failed_fetches)}"
                    )
                
                self.logger.info(
                    f"Successfully fetched {len(successful_fetches)} fundamental data types for {ticker}: "
                    f"{', '.join(successful_fetches)}"
                )
                
                if failed_fetches:
                    self.logger.debug(f"Failed to fetch {len(failed_fetches)} data types: {'; '.join(failed_fetches)}")
                
                return results
                
            except Exception as e:
                if "No data found" in str(e) or "not found" in str(e).lower():
                    raise YahooFinanceError(f"Ticker {ticker} not found or has no fundamental data")
                raise e
        
        return self._retry_with_backoff(_fetch)
    
    def _is_valid_data(self, data: Any) -> bool:
        """
        Check if the fetched data is valid and not empty.
        
        Args:
            data: Data from yfinance
            
        Returns:
            True if data is valid and contains useful information
        """
        if data is None:
            return False
        
        if isinstance(data, pd.DataFrame):
            return not data.empty
        
        if isinstance(data, dict):
            return len(data) > 0 and not all(v is None for v in data.values())
        
        if isinstance(data, (list, tuple)):
            return len(data) > 0
        
        if isinstance(data, str):
            return len(data.strip()) > 0
        
        # For other types, consider them valid if not None
        return True
    
    def _prepare_for_json_storage(self, data: Any) -> Any:
        """
        Prepare data for JSON storage by converting non-serializable types.
        
        Args:
            data: Data to prepare for JSON storage
            
        Returns:
            JSON-serializable version of the data
        """
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                # Convert pandas Timestamp keys to strings
                if hasattr(key, 'strftime'):
                    key = key.strftime('%Y-%m-%d')
                elif hasattr(key, 'date'):
                    key = key.date().strftime('%Y-%m-%d')
                
                # Recursively prepare nested data
                result[str(key)] = self._prepare_for_json_storage(value)
            return result
        
        elif isinstance(data, (list, tuple)):
            return [self._prepare_for_json_storage(item) for item in data]
        
        elif hasattr(data, 'strftime'):  # datetime/Timestamp objects
            return data.strftime('%Y-%m-%d')
        
        elif hasattr(data, 'date'):  # date objects
            return data.date().strftime('%Y-%m-%d')
        
        elif pd.isna(data):  # Handle pandas NaN/NaT
            return None
        
        elif isinstance(data, (int, float, str, bool, type(None))):
            return data
        
        else:
            # Convert other types to string
            return str(data)


class EconomicDataFetcher(DataFetcher):
    """
    Economic data fetcher following established patterns.
    
    Provides methods to fetch economic data from multiple sources
    with consistent error handling and retry logic.
    """
    
    def __init__(self):
        super().__init__()
        self.logger = get_logger(__name__)
    
    def fetch_eurostat_json(self, data_code: str, from_date: str) -> Dict[str, Any]:
        """
        Fetch data from Eurostat API for European economic statistics.
        
        Args:
            data_code: Eurostat dataset code (e.g., "prc_hicp_midx")
            from_date: Start date in 'YYYY-MM-DD' format
            
        Returns:
            Dictionary with raw JSON response
            
        Raises:
            YahooFinanceError: If request fails after retries
        """
        def _fetch():
            self.logger.info(f"Fetching Eurostat data for {data_code} from {from_date}")
            
            from_year_month = self._to_year_month(from_date)
            
            url = (
                f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{data_code}"
                f"?geo=EU27_2020"
                f"&sinceTimePeriod={from_year_month}"
                f"&format=JSON"
            )
            
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                json_data = response.json()
                
                return {
                    'source': 'eurostat',
                    'data_code': data_code,
                    'from_date': from_date,
                    'from_year_month': from_year_month,
                    'url': url,
                    'extraction_timestamp': datetime.utcnow().isoformat(),
                    'raw_data': json_data
                }
                
            except requests.exceptions.RequestException as e:
                raise YahooFinanceError(f"Eurostat API request failed: {e}")
            except Exception as e:
                if "JSONDecodeError" in str(type(e)):
                    raise YahooFinanceError(f"Eurostat JSON decode error: {e}")
                raise e
        
        return self._retry_with_backoff(_fetch)
    
    def fetch_ecb_json(self, dataflow_ref: str, series_key: str, from_date: str, to_date: str) -> Dict[str, Any]:
        """
        Fetch data from ECB Data Portal API.
        
        Args:
            dataflow_ref: ECB dataflow reference (e.g., "FM")
            series_key: ECB series key (e.g., "B.U2.EUR.4F.KR.MRR_FR.LEV")
            from_date: Start date in 'YYYY-MM-DD' format
            to_date: End date in 'YYYY-MM-DD' format
            
        Returns:
            Dictionary with raw JSON response
            
        Raises:
            YahooFinanceError: If request fails after retries
        """
        def _fetch():
            self.logger.info(
                f"Fetching ECB data for dataflow {dataflow_ref}, series {series_key}, "
                f"from {from_date} to {to_date}"
            )
            
            from_year_month = self._to_year_month(from_date)
            to_year_month = self._to_year_month(to_date)
            
            url = (
                f"https://data-api.ecb.europa.eu/service/data/{dataflow_ref}/{series_key}"
                f"?format=jsondata&startPeriod={from_year_month}"
                f"&endPeriod={to_year_month}"
            )
            
            headers = {"Accept": "application/json"}
            
            try:
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                
                json_data = response.json()
                
                return {
                    'source': 'ecb',
                    'dataflow_ref': dataflow_ref,
                    'series_key': series_key,
                    'from_date': from_date,
                    'to_date': to_date,
                    'from_year_month': from_year_month,
                    'to_year_month': to_year_month,
                    'url': url,
                    'extraction_timestamp': datetime.utcnow().isoformat(),
                    'raw_data': json_data
                }
                
            except requests.exceptions.RequestException as e:
                raise YahooFinanceError(f"ECB API request failed: {e}")
            except Exception as e:
                if "JSONDecodeError" in str(type(e)):
                    raise YahooFinanceError(f"ECB JSON decode error: {e}")
                raise e
        
        return self._retry_with_backoff(_fetch)
    
    def fetch_fred_json(self, series_id: str, api_key: str, from_date: str, to_date: str) -> Dict[str, Any]:
        """
        Fetch data from FRED API (US Federal Reserve Economic Data).
        
        Args:
            series_id: FRED series ID (e.g., "UNRATE", "CPIAUCSL", "DFF")
            api_key: FRED API key
            from_date: Start date in 'YYYY-MM-DD' format
            to_date: End date in 'YYYY-MM-DD' format
            
        Returns:
            Dictionary with raw JSON response
            
        Raises:
            YahooFinanceError: If request fails after retries
        """
        def _fetch():
            self.logger.info(f"Fetching FRED data for series ID {series_id}, from {from_date} to {to_date}")
            
            url = (
                f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}"
                f"&api_key={api_key}"
                f"&file_type=json&frequency=m"
                f"&observation_start={from_date}&observation_end={to_date}"
            )
            
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                json_data = response.json()
                
                return {
                    'source': 'fred',
                    'series_id': series_id,
                    'from_date': from_date,
                    'to_date': to_date,
                    'url': url,
                    'extraction_timestamp': datetime.utcnow().isoformat(),
                    'raw_data': json_data
                }
                
            except requests.exceptions.RequestException as e:
                raise YahooFinanceError(f"FRED API request failed: {e}")
            except Exception as e:
                if "JSONDecodeError" in str(type(e)):
                    raise YahooFinanceError(f"FRED JSON decode error: {e}")
                raise e
        
        return self._retry_with_backoff(_fetch)
    
    def _to_year_month(self, date_str: str) -> str:
        """
        Convert date string to YYYY-MM format.
        
        Args:
            date_str: Date string in YYYY-MM-DD format
            
        Returns:
            Date in YYYY-MM format
        """
        if not date_str:
            # Return today's date
            today = date.today()
            return today.strftime("%Y-%m")
        
        try:
            parsed_date = pd.to_datetime(date_str)
            return parsed_date.strftime("%Y-%m")
        except Exception:
            # Fallback to current date
            today = date.today()
            return today.strftime("%Y-%m")