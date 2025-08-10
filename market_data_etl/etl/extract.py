"""
Extract phase - Pure data extraction from external sources.

This module is responsible ONLY for extracting raw data from external sources
like Yahoo Finance. No transformation or loading logic should be here.
"""

from typing import Dict, Any, Optional
from datetime import date, datetime
import yfinance as yf
import pandas as pd

from ..utils.logging import get_logger
from ..utils.exceptions import YahooFinanceError
from ..data.fetchers import DataFetcher


class FinancialDataExtractor(DataFetcher):
    """
    Pure extractor for financial data from Yahoo Finance.
    
    Responsibility: EXTRACT ONLY
    - Fetch raw data from yfinance API
    - Handle retry logic and errors
    - Return raw data as-is (no transformation)
    """
    
    # Define what data to extract
    FINANCIAL_DATA_SOURCES = {
        'income_stmt': 'financials',
        'quarterly_income_stmt': 'quarterly_financials',
        'balance_sheet': 'balance_sheet', 
        'quarterly_balance_sheet': 'quarterly_balance_sheet',
        'cash_flow': 'cashflow',
        'quarterly_cash_flow': 'quarterly_cashflow',
        'company_info': 'info'
    }
    
    def __init__(self):
        super().__init__()
    
    def extract_financial_data(self, ticker: str) -> Dict[str, Any]:
        """
        Extract raw financial data for a ticker.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Dictionary with raw data from yfinance (no transformation)
            
        Raises:
            YahooFinanceError: If extraction fails after retries
        """
        def _extract():
            self.logger.info(f"Extracting raw financial data for {ticker}")
            
            try:
                yf_ticker = yf.Ticker(ticker)
                
                raw_data = {
                    'ticker': ticker,
                    'extraction_timestamp': datetime.utcnow().isoformat(),
                    'data_sources': {}
                }
                
                extraction_summary = []
                failed_extractions = []
                
                # Extract each data source
                for data_type, yf_attribute in self.FINANCIAL_DATA_SOURCES.items():
                    try:
                        self.logger.debug(f"Extracting {data_type} from {yf_attribute}")
                        
                        # Get raw data from yfinance
                        raw_source_data = getattr(yf_ticker, yf_attribute)
                        
                        # Store raw data without any transformation
                        if self._is_valid_extraction(raw_source_data):
                            raw_data['data_sources'][data_type] = {
                                'source_attribute': yf_attribute,
                                'data_type': type(raw_source_data).__name__,
                                'raw_data': raw_source_data,
                                'extracted_at': datetime.utcnow().isoformat()
                            }
                            extraction_summary.append(data_type)
                        else:
                            self.logger.debug(f"No valid data for {data_type}")
                            failed_extractions.append(f"{data_type}: no data")
                    
                    except Exception as e:
                        failed_extractions.append(f"{data_type}: {str(e)}")
                        self.logger.debug(f"Extraction failed for {data_type}: {e}")
                
                if not raw_data['data_sources']:
                    raise YahooFinanceError(
                        f"No data could be extracted for {ticker}. "
                        f"All sources failed: {'; '.join(failed_extractions)}"
                    )
                
                self.logger.info(
                    f"Successfully extracted {len(extraction_summary)} data sources for {ticker}: "
                    f"{', '.join(extraction_summary)}"
                )
                
                return raw_data
                
            except Exception as e:
                if "No data found" in str(e) or "not found" in str(e).lower():
                    raise YahooFinanceError(f"Ticker {ticker} not found")
                raise e
        
        return self._retry_with_backoff(_extract)
    
    def _is_valid_extraction(self, data: Any) -> bool:
        """
        Check if extracted data is valid (not empty).
        
        Args:
            data: Raw data from yfinance
            
        Returns:
            True if data contains useful information
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


class PriceDataExtractor(DataFetcher):
    """
    Pure extractor for price data from Yahoo Finance.
    
    Responsibility: EXTRACT ONLY
    - Fetch raw price data from yfinance API
    - Handle retry logic and errors  
    - Return raw data as-is (no transformation)
    """
    
    def __init__(self):
        super().__init__()
    
    def extract_price_data(
        self,
        ticker: str,
        start_date: date,
        end_date: Optional[date] = None
    ) -> Dict[str, Any]:
        """
        Extract raw price data for a ticker and date range.
        
        Args:
            ticker: Stock ticker symbol
            start_date: Start date for data extraction
            end_date: End date for data extraction (defaults to today)
            
        Returns:
            Dictionary with raw price data (no transformation)
            
        Raises:
            YahooFinanceError: If extraction fails after retries
        """
        if end_date is None:
            end_date = date.today()
        
        def _extract():
            self.logger.info(f"Extracting raw price data for {ticker} from {start_date} to {end_date}")
            
            try:
                yf_ticker = yf.Ticker(ticker)
                
                # Extract raw price data
                raw_price_data = yf_ticker.history(
                    start=start_date.strftime('%Y-%m-%d'),
                    end=end_date.strftime('%Y-%m-%d'),
                    auto_adjust=False,
                    prepost=False
                )
                
                if raw_price_data.empty:
                    raise YahooFinanceError(f"No price data found for ticker {ticker}")
                
                return {
                    'ticker': ticker,
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat(),
                    'extraction_timestamp': datetime.utcnow().isoformat(),
                    'raw_data': raw_price_data,
                    'data_type': type(raw_price_data).__name__,
                    'shape': raw_price_data.shape
                }
                
            except Exception as e:
                if "No data found" in str(e) or "delisted" in str(e):
                    raise YahooFinanceError(f"Ticker {ticker} not found or has no price data")
                raise e
        
        return self._retry_with_backoff(_extract)