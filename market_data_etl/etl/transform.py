"""
Transform phase - Pure data transformation and standardization.

This module is responsible ONLY for transforming raw data into clean,
standardized formats. No extraction or loading logic should be here.
"""

from typing import Dict, Any, Optional
from datetime import date, datetime
import pandas as pd

from ..utils.logging import get_logger
from ..data.financial_standardizer import FinancialStandardizer


class FinancialDataTransformer:
    """
    Pure transformer for financial data.
    
    Responsibility: TRANSFORM ONLY
    - Convert raw extracted data to standardized format
    - Calculate derived metrics and ratios
    - Clean and validate data
    - NO extraction or loading logic
    """
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.standardizer = FinancialStandardizer()
    
    def transform_financial_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw financial data into standardized format.
        
        Args:
            raw_data: Raw data from FinancialDataExtractor
            
        Returns:
            Dictionary with transformed, standardized financial data
        """
        self.logger.info(f"Transforming financial data for {raw_data.get('ticker', 'unknown')}")
        
        # Extract company info and currency first
        company_info = self._transform_company_info(raw_data)
        currency = self._extract_currency(raw_data)
        
        transformed_data = {
            'ticker': raw_data.get('ticker'),
            'currency': currency,
            'company_info': company_info,
            'transformation_timestamp': datetime.utcnow().isoformat(),
            'statements': {}
        }
        
        data_sources = raw_data.get('data_sources', {})
        
        # Transform financial statements
        statements_transformed = self._transform_financial_statements(data_sources, currency)
        if statements_transformed:
            transformed_data['statements'] = statements_transformed
        
        # Calculate derived metrics if we have sufficient data
        if len(transformed_data['statements']) >= 2:
            derived_metrics = self._calculate_derived_metrics(transformed_data['statements'])
            if derived_metrics:
                transformed_data['derived_metrics'] = derived_metrics
        
        self.logger.info(
            f"Transformation complete for {raw_data.get('ticker')}: "
            f"{len(transformed_data['statements'])} statements, "
            f"{'with' if 'derived_metrics' in transformed_data else 'without'} derived metrics"
        )
        
        return transformed_data
    
    def transform_price_data(self, raw_price_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw price data into standardized format.
        
        Args:
            raw_price_data: Raw price data from PriceDataExtractor
            
        Returns:
            Dictionary with transformed price data
        """
        ticker = raw_price_data.get('ticker')
        self.logger.info(f"Transforming price data for {ticker}")
        
        raw_df = raw_price_data.get('raw_data')
        
        if raw_df is None or raw_df.empty:
            return {
                'ticker': ticker,
                'transformed_data': pd.DataFrame(),
                'transformation_timestamp': datetime.utcnow().isoformat(),
                'record_count': 0
            }
        
        # Transform the DataFrame
        transformed_df = self._clean_price_dataframe(raw_df)
        
        return {
            'ticker': ticker,
            'start_date': raw_price_data.get('start_date'),
            'end_date': raw_price_data.get('end_date'), 
            'transformation_timestamp': datetime.utcnow().isoformat(),
            'transformed_data': transformed_df,
            'record_count': len(transformed_df)
        }
    
    def _transform_company_info(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw company info into standardized format."""
        company_info_source = raw_data.get('data_sources', {}).get('company_info', {})
        raw_info = company_info_source.get('raw_data', {})
        
        if not raw_info:
            return {}
        
        return {
            'company_name': raw_info.get('longName', ''),
            'sector': raw_info.get('sector', ''),
            'industry': raw_info.get('industry', ''),
            'country': raw_info.get('country', ''),
            'market_cap': raw_info.get('marketCap'),
            'employees': raw_info.get('fullTimeEmployees'),
            'founded_year': None,  # Not typically available in yfinance
            'exchange': raw_info.get('exchange', ''),
            'quote_type': raw_info.get('quoteType', ''),
            'business_summary': raw_info.get('longBusinessSummary', '')[:500] if raw_info.get('longBusinessSummary') else ''
        }
    
    def _extract_currency(self, raw_data: Dict[str, Any]) -> str:
        """Extract currency from raw company info."""
        company_info_source = raw_data.get('data_sources', {}).get('company_info', {})
        info = company_info_source.get('raw_data', {})
        
        if not info:
            return 'USD'
        
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
        
        # Default to USD
        return 'USD'
    
    def _transform_financial_statements(
        self, 
        data_sources: Dict[str, Any], 
        currency: str
    ) -> Dict[str, Any]:
        """Transform raw financial statement data using standardizer."""
        statements = {}
        
        # Map of statement types to their annual and quarterly sources
        statement_mapping = {
            'income_stmt': {
                'annual': 'income_stmt',
                'quarterly': 'quarterly_income_stmt'
            },
            'balance_sheet': {
                'annual': 'balance_sheet',
                'quarterly': 'quarterly_balance_sheet'
            },
            'cash_flow': {
                'annual': 'cash_flow', 
                'quarterly': 'quarterly_cash_flow'
            }
        }
        
        for statement_type, source_mapping in statement_mapping.items():
            try:
                # Get annual data
                annual_data = self._convert_dataframe_to_dict(
                    data_sources.get(source_mapping['annual'], {}).get('raw_data')
                )
                
                # Get quarterly data
                quarterly_data = self._convert_dataframe_to_dict(
                    data_sources.get(source_mapping['quarterly'], {}).get('raw_data')
                )
                
                # Standardize using the financial standardizer
                standardized = self._standardize_statement_data(
                    statement_type, annual_data, quarterly_data, currency
                )
                
                if standardized:
                    statements[statement_type] = standardized
                    
            except Exception as e:
                self.logger.warning(f"Failed to transform {statement_type}: {e}")
        
        return statements
    
    def _convert_dataframe_to_dict(self, data: Any) -> Dict[str, Any]:
        """Convert yfinance DataFrame to dictionary format for standardizer."""
        if data is None or not isinstance(data, pd.DataFrame) or data.empty:
            return {}
        
        # yfinance returns DataFrame with dates as columns and metrics as index
        # We need to transpose and convert to nested dict: {date: {metric: value}}
        try:
            # Transpose so dates become index and metrics become columns
            transposed = data.transpose()
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
            
        except Exception as e:
            self.logger.debug(f"Failed to convert DataFrame to dict: {e}")
            return {}
    
    def _standardize_statement_data(
        self,
        statement_type: str,
        annual_data: Dict[str, Any],
        quarterly_data: Dict[str, Any],
        currency: str
    ) -> Dict[str, Any]:
        """Standardize financial statement data using the standardizer."""
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
    
    def _calculate_derived_metrics(self, statements: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate derived financial metrics from standardized statements."""
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
    
    def _clean_price_dataframe(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize price DataFrame."""
        # Reset index to get date as column
        df = raw_df.reset_index()
        
        # Standardize column names
        df.columns = [col.replace(' ', '_').lower() for col in df.columns]
        
        # Handle date column - newer yfinance versions may have different formats
        if 'date' in df.columns:
            # Convert timezone-aware timestamps to date objects
            df['date'] = [d.date() for d in df['date']]
        
        return df