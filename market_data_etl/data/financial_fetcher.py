"""
Financial statement data fetcher focused on core financial analysis.

This module provides a specialized fetcher for extracting only the core
financial statement data (income statement, balance sheet, cash flow)
with standardization and currency handling for global companies.
"""

from typing import Optional, Dict, Any, Tuple, List
from datetime import date, datetime
import yfinance as yf
import pandas as pd

from ..config import config
from ..utils.logging import get_logger
from ..utils.exceptions import YahooFinanceError
from .financial_standardizer import FinancialStandardizer
from .fetchers import DataFetcher


class FinancialStatementFetcher(DataFetcher):
    """
    Focused fetcher for core financial statement data with standardization.
    
    Fetches only income statement, balance sheet, and cash flow data
    with proper standardization and currency handling for rigorous 
    financial analysis.
    """
    
    # Core financial statement types for financial analysis
    CORE_STATEMENTS = {
        'income_stmt': {
            'yf_attr': 'financials',
            'quarterly_attr': 'quarterly_financials',
            'description': 'Annual income statement'
        },
        'balance_sheet': {
            'yf_attr': 'balance_sheet',
            'quarterly_attr': 'quarterly_balance_sheet', 
            'description': 'Annual balance sheet'
        },
        'cash_flow': {
            'yf_attr': 'cashflow',
            'quarterly_attr': 'quarterly_cashflow',
            'description': 'Annual cash flow statement'
        }
    }
    
    def __init__(self):
        super().__init__()
        self.standardizer = FinancialStandardizer()
    
    def fetch_financial_statements(
        self, 
        ticker: str,
        include_quarterly: bool = True
    ) -> Dict[str, Any]:
        """
        Fetch core financial statements for comprehensive analysis.
        
        Args:
            ticker: Stock ticker symbol
            include_quarterly: Whether to include quarterly data
            
        Returns:
            Dictionary containing standardized financial statement data
            
        Raises:
            YahooFinanceError: If data fetch fails after retries
        """
        def _fetch():
            self.logger.info(f"Fetching financial statements for {ticker}")
            
            try:
                yf_ticker = yf.Ticker(ticker)
                
                # Get company info for currency detection
                info = yf_ticker.info
                currency = self._extract_currency(info)
                
                results = {
                    'ticker': ticker,
                    'currency': currency,
                    'company_info': self._extract_company_info(info),
                    'statements': {},
                    'fetch_timestamp': datetime.utcnow().isoformat()
                }
                
                successful_fetches = []
                failed_fetches = []
                
                # Fetch each core financial statement
                for statement_type, config_info in self.CORE_STATEMENTS.items():
                    try:
                        # Fetch annual data
                        annual_data = self._fetch_statement_data(
                            yf_ticker, 
                            config_info['yf_attr'],
                            statement_type,
                            'annual'
                        )
                        
                        quarterly_data = {}
                        if include_quarterly:
                            # Fetch quarterly data
                            quarterly_data = self._fetch_statement_data(
                                yf_ticker,
                                config_info['quarterly_attr'], 
                                statement_type,
                                'quarterly'
                            )
                        
                        # Standardize the data
                        standardized_data = self._standardize_statement(
                            statement_type,
                            annual_data,
                            quarterly_data,
                            currency
                        )
                        
                        if standardized_data:
                            results['statements'][statement_type] = standardized_data
                            successful_fetches.append(statement_type)
                            
                            periods_count = len(standardized_data.get('annual', {})) + len(standardized_data.get('quarterly', {}))
                            self.logger.debug(f"Successfully standardized {statement_type}: {periods_count} periods")
                        else:
                            failed_fetches.append(f"{statement_type}: no data after standardization")
                    
                    except Exception as e:
                        failed_fetches.append(f"{statement_type}: {str(e)}")
                        self.logger.warning(f"Failed to fetch {statement_type}: {e}")
                
                if not results['statements']:
                    raise YahooFinanceError(
                        f"No financial statement data found for {ticker}. "
                        f"All statements failed: {'; '.join(failed_fetches)}"
                    )
                
                # Calculate derived metrics if we have sufficient data
                if len(results['statements']) >= 2:
                    try:
                        derived_metrics = self._calculate_derived_metrics(results['statements'])
                        if derived_metrics:
                            results['derived_metrics'] = derived_metrics
                    except Exception as e:
                        self.logger.warning(f"Failed to calculate derived metrics: {e}")
                
                self.logger.info(
                    f"Successfully fetched {len(successful_fetches)} financial statements for {ticker}: "
                    f"{', '.join(successful_fetches)}"
                )
                
                if failed_fetches:
                    self.logger.debug(f"Failed to fetch {len(failed_fetches)} statements: {'; '.join(failed_fetches)}")
                
                return results
                
            except Exception as e:
                if "No data found" in str(e) or "not found" in str(e).lower():
                    raise YahooFinanceError(f"Ticker {ticker} not found or has no financial data")
                raise e
        
        return self._retry_with_backoff(_fetch)
    
    def _fetch_statement_data(
        self, 
        yf_ticker: yf.Ticker,
        attr_name: str,
        statement_type: str,
        period_type: str
    ) -> Dict[str, Any]:
        """
        Fetch raw statement data from yfinance.
        
        Args:
            yf_ticker: yfinance Ticker object
            attr_name: Attribute name to fetch from ticker
            statement_type: Type of statement (income_stmt, balance_sheet, cash_flow)
            period_type: 'annual' or 'quarterly'
            
        Returns:
            Raw statement data dictionary
        """
        try:
            data = getattr(yf_ticker, attr_name)
            
            if data is None or (isinstance(data, pd.DataFrame) and data.empty):
                self.logger.debug(f"No {period_type} {statement_type} data available")
                return {}
            
            if isinstance(data, pd.DataFrame):
                # Convert DataFrame to dictionary format suitable for standardization
                # yfinance returns DataFrame with dates as columns and metrics as index
                # We need to transpose and convert to nested dict: {date: {metric: value}}
                if not data.empty:
                    # Transpose so dates become index and metrics become columns
                    transposed = data.transpose()
                    # Convert to nested dictionary format expected by standardizer
                    result = {}
                    for date_col in transposed.index:
                        # Convert timestamp to string format
                        date_str = date_col.strftime('%Y-%m-%d') if hasattr(date_col, 'strftime') else str(date_col)
                        # Get all metrics for this date
                        metrics = transposed.loc[date_col].to_dict()
                        # Only include non-null values
                        cleaned_metrics = {k: v for k, v in metrics.items() if pd.notna(v)}
                        if cleaned_metrics:
                            result[date_str] = cleaned_metrics
                    return result
                return {}
            
            return data if isinstance(data, dict) else {}
            
        except Exception as e:
            self.logger.debug(f"Error fetching {period_type} {statement_type}: {e}")
            return {}
    
    def _standardize_statement(
        self,
        statement_type: str,
        annual_data: Dict[str, Any],
        quarterly_data: Dict[str, Any], 
        currency: str
    ) -> Dict[str, Any]:
        """
        Standardize financial statement data using the standardizer.
        
        Args:
            statement_type: Type of statement
            annual_data: Raw annual data
            quarterly_data: Raw quarterly data
            currency: Currency code
            
        Returns:
            Standardized statement data
        """
        standardized = {}
        
        # Standardize annual data
        if annual_data:
            if statement_type == 'income_stmt':
                annual_std = self.standardizer.standardize_income_statement(annual_data, currency)
            elif statement_type == 'balance_sheet':
                annual_std = self.standardizer.standardize_balance_sheet(annual_data, currency)
            elif statement_type == 'cash_flow':
                annual_std = self.standardizer.standardize_cash_flow(annual_data, currency)
            else:
                annual_std = {}
            
            if annual_std:
                standardized['annual'] = annual_std
        
        # Standardize quarterly data
        if quarterly_data:
            if statement_type == 'income_stmt':
                quarterly_std = self.standardizer.standardize_income_statement(quarterly_data, currency)
            elif statement_type == 'balance_sheet':
                quarterly_std = self.standardizer.standardize_balance_sheet(quarterly_data, currency)
            elif statement_type == 'cash_flow':
                quarterly_std = self.standardizer.standardize_cash_flow(quarterly_data, currency)
            else:
                quarterly_std = {}
            
            if quarterly_std:
                standardized['quarterly'] = quarterly_std
        
        return standardized
    
    def _extract_currency(self, info: Dict[str, Any]) -> str:
        """
        Extract currency from company info.
        
        Args:
            info: Company info dictionary from yfinance
            
        Returns:
            Currency code (e.g., 'USD', 'EUR', 'SEK')
        """
        # Try various currency fields
        currency_fields = ['currency', 'financialCurrency', 'quoteCurrency']
        
        for field in currency_fields:
            if field in info and info[field]:
                currency = str(info[field]).upper()
                # Validate currency code (should be 3 characters)
                if len(currency) == 3 and currency.isalpha():
                    return currency
        
        # Fallback based on exchange
        exchange = info.get('exchange', '').upper()
        if 'NYSE' in exchange or 'NASDAQ' in exchange:
            return 'USD'
        elif 'LSE' in exchange or 'LON' in exchange:
            return 'GBP'
        elif 'FRA' in exchange or 'XETRA' in exchange:
            return 'EUR'
        elif 'STO' in exchange:
            return 'SEK'
        elif 'TSE' in exchange or 'TYO' in exchange:
            return 'JPY'
        
        # Default to USD if cannot determine
        self.logger.warning(f"Could not determine currency from info, defaulting to USD")
        return 'USD'
    
    def _extract_company_info(self, info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract relevant company information for analysis.
        
        Args:
            info: Company info dictionary from yfinance
            
        Returns:
            Cleaned company information dictionary
        """
        return {
            'company_name': info.get('longName', ''),
            'sector': info.get('sector', ''),
            'industry': info.get('industry', ''),
            'country': info.get('country', ''),
            'market_cap': info.get('marketCap'),
            'employees': info.get('fullTimeEmployees'),
            'founded_year': None,  # Not typically available in yfinance
            'exchange': info.get('exchange', ''),
            'quote_type': info.get('quoteType', ''),
            'business_summary': info.get('longBusinessSummary', '')[:500]  # Limit length
        }
    
    def _calculate_derived_metrics(self, statements: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate derived financial metrics from the statements.
        
        Args:
            statements: Standardized financial statements
            
        Returns:
            Derived metrics by period
        """
        income_data = statements.get('income_stmt', {})
        balance_data = statements.get('balance_sheet', {})
        cashflow_data = statements.get('cash_flow', {})
        
        derived = {}
        
        # Calculate for annual data
        annual_income = income_data.get('annual', {})
        annual_balance = balance_data.get('annual', {})
        annual_cashflow = cashflow_data.get('annual', {})
        
        if annual_income or annual_balance or annual_cashflow:
            annual_derived = self.standardizer.calculate_derived_metrics(
                annual_income, annual_balance, annual_cashflow
            )
            if annual_derived:
                derived['annual'] = annual_derived
        
        # Calculate for quarterly data
        quarterly_income = income_data.get('quarterly', {})
        quarterly_balance = balance_data.get('quarterly', {})
        quarterly_cashflow = cashflow_data.get('quarterly', {})
        
        if quarterly_income or quarterly_balance or quarterly_cashflow:
            quarterly_derived = self.standardizer.calculate_derived_metrics(
                quarterly_income, quarterly_balance, quarterly_cashflow
            )
            if quarterly_derived:
                derived['quarterly'] = quarterly_derived
        
        return derived
