"""
Transform phase - Pure data transformation and standardization.

This module is responsible ONLY for transforming raw data into clean,
standardized formats. No extraction or loading logic should be here.
"""

from typing import Dict, Any, Optional, List
from datetime import date, datetime, timezone
import pandas as pd

from ..utils.logging import get_logger
from ..data.financial_standardizer import FinancialStandardizer
from ..config import config


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
            'transformation_timestamp': datetime.now(timezone.utc).isoformat(),
            'statements': {},
            'events': []
        }
        
        data_sources = raw_data.get('data_sources', {})
        
        # Transform financial statements
        statements_transformed = self._transform_financial_statements(data_sources, currency)
        if statements_transformed:
            transformed_data['statements'] = statements_transformed
        
        # Transform events/calendar data
        events_transformed = self._transform_events_calendar(data_sources, raw_data.get('ticker'))
        if events_transformed:
            transformed_data['events'] = events_transformed
        
        # Calculate derived metrics if we have sufficient data
        if len(transformed_data['statements']) >= 2:
            derived_metrics = self._calculate_derived_metrics(transformed_data['statements'])
            if derived_metrics:
                transformed_data['derived_metrics'] = derived_metrics
        
        self.logger.info(
            f"Transformation complete for {raw_data.get('ticker')}: "
            f"{len(transformed_data['statements'])} statements, "
            f"{len(transformed_data['events'])} events, "
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
                'transformation_timestamp': datetime.now(timezone.utc).isoformat(),
                'record_count': 0,
                'instrument_type': raw_price_data.get('instrument_type'),
                'instrument_info': raw_price_data.get('instrument_info')
            }
        
        # Transform the DataFrame
        transformed_df = self._clean_price_dataframe(raw_df)
        
        return {
            'ticker': ticker,
            'start_date': raw_price_data.get('start_date'),
            'end_date': raw_price_data.get('end_date'), 
            'transformation_timestamp': datetime.now(timezone.utc).isoformat(),
            'transformed_data': transformed_df,
            'record_count': len(transformed_df),
            'instrument_type': raw_price_data.get('instrument_type'),
            'instrument_info': raw_price_data.get('instrument_info')
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
        
        # Map of statement types to their annual, quarterly, and TTM sources
        statement_mapping = {
            'income_stmt': {
                'annual': 'income_stmt',
                'quarterly': 'quarterly_income_stmt',
                'ttm': 'ttm_income_stmt'
            },
            'balance_sheet': {
                'annual': 'balance_sheet',
                'quarterly': 'quarterly_balance_sheet'
                # Note: No TTM balance sheet - balance sheets are point-in-time
            },
            'cash_flow': {
                'annual': 'cash_flow', 
                'quarterly': 'quarterly_cash_flow',
                'ttm': 'ttm_cash_flow'
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
                
                # Get TTM data (if available for this statement type)
                ttm_data = {}
                if 'ttm' in source_mapping:
                    ttm_data = self._convert_dataframe_to_dict(
                        data_sources.get(source_mapping['ttm'], {}).get('raw_data')
                    )
                
                # Standardize using the financial standardizer (extended for TTM)
                standardized = self._standardize_statement_data(
                    statement_type, annual_data, quarterly_data, currency, ttm_data
                )
                
                if standardized:
                    statements[statement_type] = standardized
                    
            except Exception as e:
                self.logger.warning(f"Failed to transform {statement_type}: {e}")
        
        return statements
    
    def _convert_dataframe_to_dict(self, data: Any) -> Dict[str, Any]:
        """Convert yfinance DataFrame or TTM dict to dictionary format for standardizer."""
        if data is None:
            return {}
        
        # Handle TTM data which comes as a nested dict
        if isinstance(data, dict):
            # TTM data format: {metric_name: {date: value}}
            # Need to convert to: {date: {metric_name: value}}
            if not data:
                return {}
            
            result = {}
            # Get all unique dates from all metrics
            all_dates = set()
            for metric_data in data.values():
                if isinstance(metric_data, dict):
                    all_dates.update(metric_data.keys())
            
            # Reorganize by date
            for date_key in all_dates:
                date_str = date_key.strftime('%Y-%m-%d') if hasattr(date_key, 'strftime') else str(date_key)
                result[date_str] = {}
                
                for metric_name, metric_data in data.items():
                    if isinstance(metric_data, dict) and date_key in metric_data:
                        result[date_str][metric_name] = metric_data[date_key]
            
            return result
        
        # Handle DataFrame data (annual/quarterly)
        if not isinstance(data, pd.DataFrame) or data.empty:
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
        currency: str,
        ttm_data: Optional[Dict[str, Any]] = None
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
        
        # Standardize TTM data
        if ttm_data:
            if statement_type == 'income_stmt':
                ttm_std = self.standardizer.standardize_income_statement(ttm_data, currency)
            elif statement_type == 'cash_flow':
                ttm_std = self.standardizer.standardize_cash_flow(ttm_data, currency)
            else:
                # No TTM for balance sheet (point-in-time data)
                ttm_std = {}
            
            if ttm_std:
                standardized['ttm'] = ttm_std
        
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
    
    def _transform_events_calendar(self, data_sources: Dict[str, Any], ticker: str) -> List[Dict[str, Any]]:
        """
        Transform raw calendar/events data into standardized event records.
        
        Args:
            data_sources: Raw data sources from extraction
            ticker: Stock ticker symbol
            
        Returns:
            List of standardized event dictionaries
        """
        events = []
        
        if 'calendar_events' not in data_sources:
            return events
        
        calendar_raw = data_sources['calendar_events'].get('raw_data', {})
        
        if not calendar_raw:
            self.logger.debug(f"No calendar events data for {ticker}")
            return events
        
        self.logger.debug(f"Processing calendar events for {ticker}")
        
        # Process calendar data (upcoming earnings)
        if 'calendar' in calendar_raw:
            calendar_data = calendar_raw['calendar']
            events.extend(self._process_calendar_data(calendar_data, ticker))
        
        # Process earnings dates data (historical and upcoming)
        if 'earnings_dates' in calendar_raw:
            earnings_dates = calendar_raw['earnings_dates']
            events.extend(self._process_earnings_dates(earnings_dates, ticker))
        
        # Process historical earnings data
        if 'earnings' in calendar_raw:
            earnings_data = calendar_raw['earnings']
            events.extend(self._process_historical_earnings(earnings_data, ticker))
        
        self.logger.debug(f"Transformed {len(events)} events for {ticker}")
        return events
    
    def _process_calendar_data(self, calendar_data: Dict[str, Any], ticker: str) -> List[Dict[str, Any]]:
        """Process calendar data from yfinance into standardized events."""
        events = []
        
        try:
            # calendar_data is typically a dict with date keys
            for date_key, event_info in calendar_data.items():
                if isinstance(event_info, dict):
                    event = {
                        'ticker_symbol': ticker,
                        'event_type': 'earnings',
                        'event_date': self._parse_event_date(date_key),
                        'description': event_info.get('description', 'Earnings announcement'),
                        'estimated_eps': self._safe_float(event_info.get('estimated_eps')),
                        'event_time': event_info.get('time', 'Before Market Open'),
                        'source': 'calendar'
                    }
                    if event['event_date']:
                        events.append(event)
        
        except Exception as e:
            self.logger.debug(f"Error processing calendar data: {e}")
        
        return events
    
    def _process_earnings_dates(self, earnings_dates: Dict[str, Any], ticker: str) -> List[Dict[str, Any]]:
        """Process earnings dates data into standardized events."""
        events = []
        
        try:
            # earnings_dates is typically a dict with date keys and earnings info
            for date_key, earnings_info in earnings_dates.items():
                if isinstance(earnings_info, dict):
                    event = {
                        'ticker_symbol': ticker,
                        'event_type': 'earnings',
                        'event_date': self._parse_event_date(date_key),
                        'description': 'Earnings release',
                        'estimated_eps': self._safe_float(earnings_info.get('Estimate')),
                        'reported_eps': self._safe_float(earnings_info.get('Reported')),
                        'eps_surprise': self._calculate_eps_surprise(
                            earnings_info.get('Reported'), 
                            earnings_info.get('Estimate')
                        ),
                        'event_time': earnings_info.get('Time', 'Before Market Open'),
                        'source': 'earnings_dates'
                    }
                    if event['event_date']:
                        events.append(event)
        
        except Exception as e:
            self.logger.debug(f"Error processing earnings dates: {e}")
        
        return events
    
    def _process_historical_earnings(self, earnings_data: Dict[str, Any], ticker: str) -> List[Dict[str, Any]]:
        """Process historical earnings data into events."""
        events = []
        
        try:
            # earnings_data is typically historical data with quarters/years
            for period_key, period_data in earnings_data.items():
                if isinstance(period_data, dict):
                    event = {
                        'ticker_symbol': ticker,
                        'event_type': 'earnings_historical',
                        'event_date': self._parse_earnings_period(period_key),
                        'description': f'Historical earnings - {period_key}',
                        'reported_eps': self._safe_float(period_data.get('EPS')),
                        'source': 'historical_earnings'
                    }
                    if event['event_date']:
                        events.append(event)
        
        except Exception as e:
            self.logger.debug(f"Error processing historical earnings: {e}")
        
        return events
    
    def _parse_event_date(self, date_str: str) -> Optional[date]:
        """Parse event date from various string formats."""
        if not date_str:
            return None
        
        try:
            # Handle pandas timestamp string format
            if 'Timestamp' in str(date_str):
                # Extract date from Timestamp string
                import re
                match = re.search(r"'(\d{4}-\d{2}-\d{2})", str(date_str))
                if match:
                    date_str = match.group(1)
            
            # Try standard date parsing
            if isinstance(date_str, str):
                # Common date formats
                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y.%m.%d']:
                    try:
                        return datetime.strptime(date_str, fmt).date()
                    except ValueError:
                        continue
        
        except Exception as e:
            self.logger.debug(f"Could not parse event date '{date_str}': {e}")
        
        return None
    
    def _parse_earnings_period(self, period_str: str) -> Optional[date]:
        """Parse earnings period (quarter/year) into approximate date."""
        if not period_str:
            return None
        
        try:
            # Handle quarterly data (e.g., "Q1 2024", "2024-Q1")
            if 'Q' in str(period_str):
                import re
                match = re.search(r'Q(\d)\s*(\d{4})|(\d{4})\s*Q(\d)', str(period_str))
                if match:
                    if match.group(1) and match.group(2):  # Q1 2024 format
                        quarter, year = int(match.group(1)), int(match.group(2))
                    else:  # 2024 Q1 format
                        year, quarter = int(match.group(3)), int(match.group(4))
                    
                    # Map quarter to approximate end date
                    quarter_end_months = {1: 3, 2: 6, 3: 9, 4: 12}
                    month = quarter_end_months.get(quarter, 12)
                    return date(year, month, 1)  # Use first day of end month
            
            # Handle yearly data (e.g., "2024")
            if period_str.isdigit() and len(period_str) == 4:
                return date(int(period_str), 12, 31)  # End of year
        
        except Exception as e:
            self.logger.debug(f"Could not parse earnings period '{period_str}': {e}")
        
        return None
    
    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float."""
        if value is None or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _calculate_eps_surprise(self, reported: Any, estimated: Any) -> Optional[float]:
        """Calculate EPS surprise (reported - estimated)."""
        reported_float = self._safe_float(reported)
        estimated_float = self._safe_float(estimated)
        
        if reported_float is not None and estimated_float is not None:
            return reported_float - estimated_float
        
        return None


class EconomicDataTransformer:
    """
    Pure transformer for economic data.
    
    Responsibility: TRANSFORM ONLY
    - Convert raw extracted data to standardized format
    - Parse dates and values from different API formats
    - Clean and validate data
    - NO extraction or loading logic
    """
    
    def __init__(self):
        self.logger = get_logger(__name__)
    
    def transform_eurostat_data(self, raw_data: Dict[str, Any], intended_indicator_name: str = None) -> Dict[str, Any]:
        """
        Transform raw Eurostat data into standardized format.
        
        Args:
            raw_data: Raw data from EconomicDataExtractor
            intended_indicator_name: Intended indicator name to use for mapping (e.g., 'inflation_monthly_sweden')
            
        Returns:
            Dictionary with transformed, standardized economic data
        """
        data_code = raw_data.get('data_code')
        self.logger.info(f"Transforming Eurostat data for {data_code}")
        
        try:
            json_data = raw_data.get('raw_data', {})
            
            # Extract time series data from Eurostat JSON structure
            data_points = self._parse_eurostat_json(json_data)
            
            name = self._get_eurostat_indicator_name(data_code)
            standardized_name = self._get_standardized_name('eurostat', data_code, name)
            
            # Get standardized mapping for this indicator
            # If intended_indicator_name is provided, use direct lookup, otherwise use source/identifier lookup
            if intended_indicator_name:
                mapping = self._get_indicator_mapping_by_name(intended_indicator_name)
            else:
                mapping = self._get_indicator_mapping('eurostat', data_code)
            
            transformed_data = {
                'name': mapping['name'],
                'source': mapping['source'], 
                'source_identifier': mapping['source_identifier'],
                'description': mapping['description'],
                'unit': self._extract_eurostat_unit(json_data),
                'frequency': self._extract_eurostat_frequency(json_data),
                'transformation_timestamp': datetime.now(timezone.utc).isoformat(),
                'data_points': data_points
            }
            
            # Add config information for country code determination
            if 'geo_filter' in mapping:
                transformed_data['geo_filter'] = mapping['geo_filter']
            if 'country_code' in mapping:
                transformed_data['country_code'] = mapping['country_code']
            
            self.logger.info(f"Transformed Eurostat data for {data_code}: {len(data_points)} data points")
            return transformed_data
            
        except Exception as e:
            self.logger.error(f"Failed to transform Eurostat data for {data_code}: {e}")
            raise e
    
    def transform_ecb_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw ECB data into standardized format.
        
        Args:
            raw_data: Raw data from EconomicDataExtractor
            
        Returns:
            Dictionary with transformed, standardized economic data
        """
        dataflow_ref = raw_data.get('dataflow_ref')
        series_key = raw_data.get('series_key')
        indicator_id = f"{dataflow_ref}.{series_key}"
        
        self.logger.info(f"Transforming ECB data for {indicator_id}")
        
        try:
            json_data = raw_data.get('raw_data', {})
            
            # Extract time series data from ECB JSON structure
            data_points = self._parse_ecb_json(json_data)
            
            name = self._get_ecb_indicator_name(dataflow_ref, series_key)
            standardized_name = self._get_standardized_name('ecb', indicator_id, name)
            
            # Get standardized mapping for this indicator
            mapping = self._get_indicator_mapping('ecb', indicator_id)
            
            transformed_data = {
                'name': mapping['name'],
                'source': mapping['source'],
                'source_identifier': mapping['source_identifier'], 
                'description': mapping['description'],
                'unit': self._extract_ecb_unit(json_data),
                'frequency': 'monthly',  # Most ECB data is monthly
                'transformation_timestamp': datetime.now(timezone.utc).isoformat(),
                'data_points': data_points
            }
            
            # Add config information for country code determination
            if 'geo_filter' in mapping:
                transformed_data['geo_filter'] = mapping['geo_filter']
            if 'country_code' in mapping:
                transformed_data['country_code'] = mapping['country_code']
            
            self.logger.info(f"Transformed ECB data for {indicator_id}: {len(data_points)} data points")
            return transformed_data
            
        except Exception as e:
            self.logger.error(f"Failed to transform ECB data for {indicator_id}: {e}")
            raise e
    
    def transform_fred_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw FRED data into standardized format.
        
        Args:
            raw_data: Raw data from EconomicDataExtractor
            
        Returns:
            Dictionary with transformed, standardized economic data
        """
        series_id = raw_data.get('series_id')
        requested_name = raw_data.get('requested_name')  # From CLI command
        self.logger.info(f"Transforming FRED data for {series_id}")
        
        try:
            json_data = raw_data.get('raw_data', {})
            
            # Extract time series data from FRED JSON structure
            data_points = self._parse_fred_json(json_data)
            
            # Special handling for CPI data - calculate both index and inflation rate
            if series_id == 'CPIAUCSL':
                # Calculate year-over-year inflation rate
                rate_data_points = self._calculate_inflation_rate(data_points)
                
                # Store both the index and the calculated inflation rate
                transformed_data = [
                    {
                        'name': 'inflation_index_us',
                        'source': 'fred',
                        'source_identifier': series_id,
                        'description': 'US Consumer Price Index (CPI)', 
                        'unit': 'index',
                        'frequency': 'monthly',
                        'transformation_timestamp': datetime.now(timezone.utc).isoformat(),
                        'data_points': data_points,
                        'country_code': 'US'  # FRED data is US-specific
                    },
                    {
                        'name': 'inflation_us',
                        'source': 'fred',
                        'source_identifier': series_id,
                        'description': 'US CPI (Year-over-Year Inflation Rate)', 
                        'unit': 'percent',
                        'frequency': 'monthly',
                        'transformation_timestamp': datetime.now(timezone.utc).isoformat(),
                        'data_points': rate_data_points,
                        'country_code': 'US'  # FRED data is US-specific
                    }
                ]
                
                self.logger.info(f"Transformed FRED CPI data: {len(data_points)} index points, {len(rate_data_points)} rate points")
                return transformed_data
            else:
                # Get standardized mapping for other indicators
                mapping = self._get_indicator_mapping('fred', series_id)
                name = mapping['name']
                description = mapping['description']
                unit = self._extract_fred_unit(series_id)
            
            transformed_data = {
                'name': name,
                'source': 'fred',
                'source_identifier': series_id,
                'description': description, 
                'unit': unit,
                'frequency': 'monthly',  # FRED data requested as monthly
                'transformation_timestamp': datetime.now(timezone.utc).isoformat(),
                'data_points': data_points
            }
            
            # Add config information for country code determination
            if 'geo_filter' in mapping:
                transformed_data['geo_filter'] = mapping['geo_filter']
            if 'country_code' in mapping:
                transformed_data['country_code'] = mapping['country_code']
            
            self.logger.info(f"Transformed FRED data for {series_id}: {len(data_points)} data points")
            return transformed_data
            
        except Exception as e:
            self.logger.error(f"Failed to transform FRED data for {series_id}: {e}")
            raise e
    
    def _parse_eurostat_json(self, json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse Eurostat JSON data structure into list of data points."""
        data_points = []
        
        try:
            # Parse using the same logic as economic_data package
            time_mapping = json_data["dimension"]["time"]["category"]["index"]
            time_list = sorted(time_mapping.keys(), key=lambda x: time_mapping[x])
            available_indexes = set(map(int, json_data.get("value", {}).keys()))
            
            for i, time in enumerate(time_list):
                if i in available_indexes:
                    try:
                        # Convert Eurostat time format to date
                        parsed_date = self._parse_eurostat_date(time)
                        if parsed_date:
                            value = json_data["value"][str(i)]
                            if value is not None:
                                data_points.append({
                                    'date': parsed_date.isoformat(),
                                    'value': float(value)
                                })
                    except (ValueError, TypeError) as e:
                        self.logger.debug(f"Skipping invalid data point: {time}={json_data['value'].get(str(i))} ({e})")
                        continue
                        
        except Exception as e:
            self.logger.warning(f"Error parsing Eurostat JSON structure: {e}")
        
        return data_points
    
    def _parse_ecb_json(self, json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse ECB JSON data structure into list of data points."""
        data_points = []
        
        try:
            # Parse using the same logic as economic_data package
            time_periods_list = json_data["structure"]["dimensions"]["observation"][0]["values"]
            series_data = next(iter(json_data["dataSets"][0]["series"].values()))
            observations = series_data["observations"]
            
            for period_index_str, value_list in observations.items():
                try:
                    period_index = int(period_index_str)
                    time_period_obj = time_periods_list[period_index]
                    time_period = time_period_obj["id"]
                    
                    indicator_value = (
                        value_list[0] if value_list and value_list[0] is not None else None
                    )
                    
                    if indicator_value is not None:
                        # Convert ECB time format to date
                        parsed_date = self._parse_ecb_date(time_period)
                        if parsed_date:
                            data_points.append({
                                'date': parsed_date.isoformat(),
                                'value': float(indicator_value)
                            })
                            
                except (ValueError, TypeError, IndexError) as e:
                    self.logger.debug(f"Skipping invalid ECB observation: {period_index_str}={value_list} ({e})")
                    continue
                    
        except Exception as e:
            self.logger.warning(f"Error parsing ECB JSON structure: {e}")
        
        return data_points
    
    def _parse_fred_json(self, json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse FRED JSON data structure into list of data points."""
        data_points = []
        
        try:
            observations = json_data.get('observations', [])
            
            for obs in observations:
                try:
                    date_str = obs.get('date')
                    value_str = obs.get('value')
                    
                    # Skip missing values (FRED uses "." for missing data)
                    if not value_str or value_str == '.':
                        continue
                    
                    parsed_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    value = float(value_str)
                    
                    data_points.append({
                        'date': parsed_date.isoformat(),
                        'value': value
                    })
                    
                except (ValueError, TypeError) as e:
                    self.logger.debug(f"Skipping invalid FRED observation: {obs} ({e})")
                    continue
                    
        except Exception as e:
            self.logger.warning(f"Error parsing FRED JSON structure: {e}")
        
        return data_points
    
    def _parse_eurostat_date(self, time_key: str) -> Optional[date]:
        """Parse Eurostat time format (e.g., '2023M01', '2024-01') to date."""
        try:
            if 'M' in time_key:
                # Monthly format: 2023M01
                year, month = time_key.split('M')
                return date(int(year), int(month), 1)
            elif 'Q' in time_key:
                # Quarterly format: 2023Q1
                year, quarter = time_key.split('Q')
                month = (int(quarter) - 1) * 3 + 1
                return date(int(year), month, 1)
            elif '-' in time_key:
                # Monthly format: 2024-01
                year, month = time_key.split('-')
                return date(int(year), int(month), 1)
            elif len(time_key) == 4 and time_key.isdigit():
                # Annual format: 2023
                return date(int(time_key), 1, 1)
        except (ValueError, AttributeError):
            pass
        return None
    
    def _parse_ecb_date(self, time_period: str) -> Optional[date]:
        """Parse ECB time format to date."""
        try:
            if '-' in time_period:
                if time_period.count('-') == 2:
                    # Full date format: 2024-01-01
                    year, month, day = time_period.split('-')
                    return date(int(year), int(month), int(day))
                else:
                    # Month format: 2023-01
                    year, month = time_period.split('-')
                    return date(int(year), int(month), 1)
            elif len(time_period) == 4 and time_period.isdigit():
                # Annual format: 2023
                return date(int(time_period), 1, 1)
        except (ValueError, AttributeError):
            pass
        return None
    
    def _get_eurostat_indicator_name(self, data_code: str) -> str:
        """Get human-readable name for Eurostat indicator."""
        indicator_names = {
            'prc_hicp_midx': 'Harmonised Index of Consumer Prices (HICP)',
            'une_rt_m': 'Unemployment rate',
            'nama_10_gdp': 'Gross domestic product at market prices',
            'gov_10dd_edpt1': 'Government deficit/surplus, debt and associated data'
        }
        return indicator_names.get(data_code, f'Eurostat {data_code}')
    
    def _get_standardized_name(self, source: str, indicator_id: str, name: str) -> Optional[str]:
        """Get standardized name for economic indicator."""
        # Mapping from descriptive names to standardized names
        name_mapping = {
            "Harmonised Index of Consumer Prices (HICP)": "inflation_ea",
            "Consumer Price Index for All Urban Consumers: All Items in U.S. City Average": "inflation_index_us",
            "US Consumer Price Index (CPI)": "inflation_index_us",
            "Main Refinancing Operations rate": "interest_ea_daily", 
            "Effective Federal Funds Rate": "interest_us",
            "US Federal Funds Rate": "interest_us",
            "Unemployment rate": "unemployment_ea",
            "US Unemployment Rate": "unemployment_us"
        }
        
        # Also map by source + indicator_id for specific cases  
        source_id_mapping = {
            "eurostat_prc_hicp_midx": "inflation_ea",
            "fred_CPIAUCSL": "inflation_index_us", 
            "fred_UNRATE": "unemployment_us",
            "fred_DFF": "interest_us",
            "ecb_FM.D.U2.EUR.4F.KR.MRR_FR.LEV": "interest_ea_daily",
            "ecb_FM.B.U2.EUR.4F.KR.MRR_FR.LEV": "interest_ea",
            "eurostat_une_rt_m": "unemployment_ea"
        }
        
        # Try source_id mapping first (most specific)
        source_key = f"{source}_{indicator_id}"
        if source_key in source_id_mapping:
            return source_id_mapping[source_key]
            
        # Try name mapping
        if name in name_mapping:
            return name_mapping[name]
            
        return None
    
    def _get_ecb_indicator_name(self, dataflow_ref: str, series_key: str) -> str:
        """Get human-readable name for ECB indicator."""
        if dataflow_ref == 'FM' and 'MRR_FR' in series_key:
            return 'Main Refinancing Operations rate'
        elif dataflow_ref == 'BSI' and 'M3' in series_key:
            return 'M3 Money Supply'
        return f'ECB {dataflow_ref}.{series_key}'
    
    def _get_fred_indicator_name(self, series_id: str) -> str:
        """Get human-readable name for FRED indicator."""
        indicator_names = {
            'UNRATE': 'US Unemployment Rate',
            'CPIAUCSL': 'US Consumer Price Index (CPI)',
            'DFF': 'US Federal Funds Rate',
            'GDP': 'US Gross Domestic Product',
            'PAYEMS': 'US Total Nonfarm Payrolls'
        }
        return indicator_names.get(series_id, f'FRED {series_id}')
    
    def _extract_eurostat_unit(self, json_data: Dict[str, Any]) -> str:
        """Extract unit information from Eurostat JSON."""
        try:
            dimensions = json_data.get('dimension', {})
            unit_dim = dimensions.get('unit')
            if unit_dim and 'category' in unit_dim:
                categories = unit_dim['category'].get('label', {})
                if categories:
                    return list(categories.values())[0]
        except Exception:
            pass
        return 'Index'
    
    def _extract_eurostat_frequency(self, json_data: Dict[str, Any]) -> str:
        """Extract frequency information from Eurostat JSON."""
        try:
            dimensions = json_data.get('dimension', {})
            freq_dim = dimensions.get('freq')
            if freq_dim and 'category' in freq_dim:
                freq_codes = list(freq_dim['category'].get('label', {}).keys())
                if freq_codes:
                    freq_code = freq_codes[0]
                    return self._convert_frequency_code_to_string(freq_code)
        except Exception:
            pass
        return 'monthly'
    
    def _extract_ecb_unit(self, json_data: Dict[str, Any]) -> str:
        """Extract unit information from ECB JSON."""
        try:
            structure = json_data.get('structure', {})
            dimensions = structure.get('dimensions', {}).get('observation', [])
            for dim in dimensions:
                if dim.get('id') == 'UNIT_MEASURE' and 'values' in dim:
                    values = dim['values']
                    if values and len(values) > 0:
                        return values[0].get('name', 'Percent')
        except Exception:
            pass
        return 'Percent'
    
    def _extract_fred_unit(self, series_id: str) -> str:
        """Extract unit information from FRED series ID."""
        # FRED doesn't typically include unit info in observations response
        # Would need to make separate API call for series info
        unit_mapping = {
            'UNRATE': 'Percent',
            'CPIAUCSL': 'Index',
            'DFF': 'Percent',
            'GDP': 'Billions of Dollars'
        }
        return unit_mapping.get(series_id, 'Percent')
    
    def _convert_frequency_code_to_string(self, frequency_code: str) -> str:
        """Convert frequency code to string."""
        mapping = {
            'D': 'daily',
            'M': 'monthly',
            'Q': 'quarterly', 
            'A': 'yearly'
        }
        return mapping.get(frequency_code, 'monthly')
    
    def _get_indicator_mapping(self, source: str, source_identifier: str) -> Dict[str, str]:
        """
        Get standardized mapping for economic indicators.
        
        Uses YAML configuration if available, falls back to hardcoded values for backward compatibility.
        """
        # Try YAML configuration first (new approach)
        if config.economic_indicators:
            # Look for indicator by source and source_identifier
            for indicator_name, indicator_config in config.economic_indicators.items():
                if (indicator_config.get('source') == source and 
                    indicator_config.get('source_identifier') == source_identifier):
                    return {
                        'name': indicator_name,
                        'source': indicator_config['source'],
                        'source_identifier': indicator_config['source_identifier'],
                        'description': indicator_config.get('description', f'{source.upper()} indicator: {source_identifier}')
                    }
            
            # For ECB, try different identifier formats
            if source == 'ecb':
                # Try with semicolon format replacement
                alt_identifier_semicolon = source_identifier.replace('.', '; ', 1)
                alt_identifier_dot = source_identifier.replace('; ', '.', 1)
                
                for indicator_name, indicator_config in config.economic_indicators.items():
                    config_identifier = indicator_config.get('source_identifier', '')
                    if (indicator_config.get('source') == source and 
                        (config_identifier == alt_identifier_semicolon or config_identifier == alt_identifier_dot)):
                        return {
                            'name': indicator_name,
                            'source': indicator_config['source'],
                            'source_identifier': indicator_config['source_identifier'],
                            'description': indicator_config.get('description', f'{source.upper()} indicator: {source_identifier}')
                        }
        
        # No mapping found in YAML configuration
        self.logger.error(f"No economic indicator mapping found for {source}/{source_identifier}")
        self.logger.error("Available indicators in YAML config:")
        if config.economic_indicators:
            for name, conf in config.economic_indicators.items():
                self.logger.error(f"  - {name}: {conf.get('source')}/{conf.get('source_identifier')}")
        else:
            self.logger.error("  No indicators loaded from YAML configuration")
        
        raise ValueError(
            f"Economic indicator mapping not found: {source}/{source_identifier}. "
            f"Please add this indicator to config/economic_indicators.yaml or verify the source/identifier values."
        )
    
    def _calculate_inflation_rate(self, data_points: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Calculate year-over-year inflation rate from CPI index values.
        
        Args:
            data_points: List of data points with 'date' and 'value' keys
            
        Returns:
            List of data points with year-over-year inflation rate values
        """
        if len(data_points) < 13:  # Need at least 13 months for YoY calculation
            return []
        
        # Sort by date to ensure correct order
        sorted_points = sorted(data_points, key=lambda x: x['date'])
        inflation_rates = []
        
        # Start from month 12 (index 12) to compare with month 0 (12 months earlier)
        for i in range(12, len(sorted_points)):
            current = sorted_points[i]
            year_ago = sorted_points[i-12]
            
            if year_ago['value'] and year_ago['value'] != 0:
                # Calculate year-over-year percentage change
                inflation_rate = ((current['value'] - year_ago['value']) / year_ago['value']) * 100
                
                inflation_rates.append({
                    'date': current['date'],
                    'value': round(inflation_rate, 4)
                })
        
        return inflation_rates
    
    def transform_oecd_data(self, raw_data: Dict[str, Any], intended_indicator_name: str = None) -> Dict[str, Any]:
        """
        Transform raw OECD data into standardized format.
        
        Args:
            raw_data: Raw data from EconomicDataExtractor
            intended_indicator_name: The intended indicator name (e.g., "inflation_monthly_gb")
            
        Returns:
            Dictionary with transformed, standardized economic data
        """
        dataset = raw_data.get('dataset')
        country_code = raw_data.get('country_code')
        self.logger.info(f"Transforming OECD data for {dataset}/{country_code}")
        
        try:
            json_data = raw_data.get('raw_data', {})
            
            # Extract time series data from OECD SDMX-JSON structure
            data_points = self._parse_oecd_json(json_data)
            
            # Get standardized mapping for this indicator
            if intended_indicator_name:
                # Use the intended indicator name directly
                mapping = self._get_indicator_mapping_by_name(intended_indicator_name)
            else:
                # Fallback to old logic for backwards compatibility
                mapping = self._get_indicator_mapping('oecd', f"{dataset}_{country_code}")
            
            transformed_data = {
                'name': mapping['name'],
                'source': mapping['source'],
                'source_identifier': mapping['source_identifier'],
                'description': mapping['description'],
                'unit': 'percent',  # OECD inflation data is in percentage form
                'frequency': 'monthly',  # OECD CPI data is monthly
                'transformation_timestamp': datetime.now(timezone.utc).isoformat(),
                'data_points': data_points
            }
            
            # Add config information for country code determination
            if 'geo_filter' in mapping:
                transformed_data['geo_filter'] = mapping['geo_filter']
            if 'country_code' in mapping:
                transformed_data['country_code'] = mapping['country_code']
            
            self.logger.info(f"Transformed OECD data for {dataset}/{country_code}: {len(data_points)} data points")
            return transformed_data
            
        except Exception as e:
            self.logger.error(f"Failed to transform OECD data for {dataset}/{country_code}: {e}")
            raise e
    
    def _parse_oecd_json(self, json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse OECD SDMX-JSON data structure into list of data points."""
        data_points = []
        
        try:
            # OECD SDMX-JSON structure: dataSets[0].observations contains the actual data
            datasets = json_data.get('dataSets', [])
            if not datasets:
                self.logger.warning("No dataSets found in OECD JSON")
                return data_points
            
            observations = datasets[0].get('observations', {})
            
            # Get time dimension from structure
            structure = json_data.get('structure', {})
            dimensions = structure.get('dimensions', {}).get('observation', [])
            
            # Find time dimension
            time_dimension = None
            for dim in dimensions:
                if dim.get('id') == 'TIME_PERIOD':
                    time_dimension = dim.get('values', [])
                    break
            
            if not time_dimension:
                self.logger.warning("No TIME_PERIOD dimension found in OECD data")
                return data_points
            
            # Parse observations
            for obs_key, obs_value in observations.items():
                try:
                    # obs_key is typically "0:0:0:time_index" format
                    # Extract time index (last part)
                    time_index = int(obs_key.split(':')[-1])
                    
                    if time_index < len(time_dimension):
                        time_period = time_dimension[time_index]['id']
                        
                        # obs_value is a list, first element is the actual value
                        if obs_value and obs_value[0] is not None:
                            value = float(obs_value[0])
                            
                            # Parse OECD time format (YYYY-MM)
                            parsed_date = self._parse_oecd_date(time_period)
                            if parsed_date:
                                data_points.append({
                                    'date': parsed_date.isoformat(),
                                    'value': round(value, 4)
                                })
                                
                except (ValueError, TypeError, IndexError) as e:
                    self.logger.debug(f"Skipping invalid OECD observation: {obs_key}={obs_value} ({e})")
                    continue
                    
        except Exception as e:
            self.logger.warning(f"Error parsing OECD JSON structure: {e}")
        
        return data_points
    
    def _parse_oecd_date(self, time_period: str) -> Optional[date]:
        """Parse OECD time format to date."""
        try:
            # OECD typically uses YYYY-MM format
            if '-' in time_period and len(time_period) == 7:
                year, month = time_period.split('-')
                return date(int(year), int(month), 1)
            elif len(time_period) == 4 and time_period.isdigit():
                # Annual format: 2023
                return date(int(time_period), 1, 1)
        except (ValueError, AttributeError):
            pass
        return None
    
    def _get_indicator_mapping_by_name(self, indicator_name: str) -> Dict[str, str]:
        """
        Get indicator mapping by direct name lookup from config.
        
        Args:
            indicator_name: The indicator name to look up (e.g., 'inflation_monthly_sweden')
            
        Returns:
            Dictionary with mapping information
        """
        if config.economic_indicators and indicator_name in config.economic_indicators:
            indicator_config = config.economic_indicators[indicator_name]
            mapping = {
                'name': indicator_name,
                'source': indicator_config['source'],
                'source_identifier': indicator_config['source_identifier'],
                'description': indicator_config.get('description', f'Economic indicator: {indicator_name}')
            }
            
            # Include all config fields for country code determination
            if 'geo_filter' in indicator_config:
                mapping['geo_filter'] = indicator_config['geo_filter']
            if 'country_code' in indicator_config:
                mapping['country_code'] = indicator_config['country_code']
                
            return mapping
        else:
            raise ValueError(f"Indicator '{indicator_name}' not found in configuration")


class MonthlyValuationTransformer:
    """
    Pure transformer for monthly valuation metrics data.
    
    Responsibility: TRANSFORM ONLY
    - Transform extracted price and TTM data into monthly valuation metrics
    - Calculate P/E and P/S ratios for each month using median prices
    - Handle edge cases (null values for invalid ratios)
    - Clean and validate data
    - NO extraction or loading logic
    """
    
    def __init__(self):
        self.logger = get_logger(__name__)
    
    def transform_monthly_valuation_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw monthly valuation data into standardized monthly metrics.
        
        Args:
            raw_data: Raw data from MonthlyValuationExtractor containing:
                - monthly_price_data: List of monthly price records with median prices
                - ttm_timeline: List of TTM metric changes
                - instrument_info: Basic instrument information
                
        Returns:
            Dictionary with transformed monthly valuation metrics:
            {
                'ticker': str,
                'instrument_id': int,
                'start_date': date,
                'end_date': date,
                'transformation_timestamp': str,
                'monthly_metrics': List[Dict]  # Monthly valuation records
            }
        """
        ticker = raw_data.get('ticker')
        self.logger.info(f"Transforming monthly valuation data for {ticker}")
        
        try:
            # Extract components
            monthly_price_data = raw_data.get('monthly_price_data', [])
            ttm_timeline = raw_data.get('ttm_timeline', [])
            instrument_info = raw_data.get('instrument_info', {})
            
            if not monthly_price_data:
                self.logger.warning(f"No monthly price data available for {ticker}")
                return self._empty_result(raw_data)
            
            if not ttm_timeline:
                self.logger.warning(f"No TTM data available for {ticker}")
                return self._empty_result(raw_data)
            
            # Build monthly metrics by joining price data with TTM data
            monthly_metrics = self._build_monthly_metrics(
                ticker, monthly_price_data, ttm_timeline, instrument_info.get('instrument_id')
            )
            
            transformed_data = {
                'ticker': ticker,
                'instrument_id': instrument_info.get('instrument_id'),
                'start_date': raw_data.get('start_date'),
                'end_date': raw_data.get('end_date'),
                'transformation_timestamp': datetime.now(timezone.utc).isoformat(),
                'monthly_metrics': monthly_metrics
            }
            
            self.logger.info(
                f"Transformed monthly valuation data for {ticker}: "
                f"{len(monthly_metrics)} monthly records"
            )
            
            return transformed_data
            
        except Exception as e:
            self.logger.error(f"Failed to transform monthly valuation data for {ticker}: {e}")
            raise e
    
    def _build_monthly_metrics(
        self,
        ticker: str,
        monthly_price_data: List[Dict[str, Any]],
        ttm_timeline: List[Dict[str, Any]],
        instrument_id: Optional[int]
    ) -> List[Dict[str, Any]]:
        """
        Build monthly valuation metrics by joining price and TTM data.
        
        For each month, find the most recent TTM metrics and calculate ratios using median prices.
        """
        monthly_metrics = []
        
        if not ttm_timeline:
            return monthly_metrics
        
        # Sort TTM timeline by date (ensure chronological order)
        sorted_ttm = sorted(
            ttm_timeline, 
            key=lambda x: self._parse_date_string(x.get('ttm_as_of_date'))
        )
        
        # Process each monthly price record
        for price_record in monthly_price_data:
            try:
                price_date = self._parse_date_string(price_record.get('last_trading_day'))
                median_price = price_record.get('median_price')
                
                if not price_date or median_price is None:
                    continue
                
                # Find the most recent TTM data for this price date
                applicable_ttm = self._find_applicable_ttm(price_date, sorted_ttm)
                
                if not applicable_ttm:
                    # No TTM data available for this date - create record with nulls
                    monthly_metrics.append({
                        'instrument_id': instrument_id,
                        'date': price_date,
                        'median_monthly_price': median_price,
                        'ttm_revenue': None,
                        'ttm_net_income': None,
                        'ttm_eps': None,
                        'shares_diluted': None,
                        'pe_ratio': None,
                        'ps_ratio': None
                    })
                    continue
                
                # Calculate valuation ratios
                pe_ratio = self._calculate_pe_ratio(median_price, applicable_ttm.get('ttm_eps'))
                ps_ratio = self._calculate_ps_ratio(
                    median_price, 
                    applicable_ttm.get('shares_diluted'),
                    applicable_ttm.get('ttm_revenue')
                )
                
                # Create monthly metrics record
                monthly_metrics.append({
                    'instrument_id': instrument_id,
                    'date': price_date,
                    'median_monthly_price': median_price,
                    'ttm_revenue': applicable_ttm.get('ttm_revenue'),
                    'ttm_net_income': applicable_ttm.get('ttm_net_income'),
                    'ttm_eps': applicable_ttm.get('ttm_eps'),
                    'shares_diluted': applicable_ttm.get('shares_diluted'),
                    'pe_ratio': pe_ratio,
                    'ps_ratio': ps_ratio
                })
                
            except Exception as e:
                self.logger.debug(f"Error processing monthly metric for {ticker} on {price_record.get('last_trading_day')}: {e}")
                continue
        
        # Sort by date for consistency
        monthly_metrics.sort(key=lambda x: x['date'])
        return monthly_metrics
    
    def _find_applicable_ttm(
        self, 
        price_date: date, 
        sorted_ttm_timeline: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """
        Find the most recent TTM data that applies to the given price date.
        
        TTM data is valid from its as_of_date onwards until the next TTM period.
        """
        applicable_ttm = None
        
        for ttm_record in sorted_ttm_timeline:
            ttm_as_of_date = self._parse_date_string(ttm_record.get('ttm_as_of_date'))
            
            if ttm_as_of_date and ttm_as_of_date <= price_date:
                # This TTM period applies to our price date
                applicable_ttm = ttm_record
            else:
                # We've reached TTM data that's newer than our price date
                break
        
        return applicable_ttm
    
    def _calculate_pe_ratio(self, close_price: float, ttm_eps: Optional[float]) -> Optional[float]:
        """
        Calculate P/E ratio (Price per share / Earnings per share).
        
        Returns None if TTM EPS is None, zero, or negative (can't have meaningful P/E).
        """
        if ttm_eps is None or ttm_eps <= 0:
            return None
        
        if close_price is None or close_price <= 0:
            return None
        
        pe_ratio = close_price / ttm_eps
        return round(pe_ratio, 4)
    
    def _calculate_ps_ratio(
        self, 
        close_price: float, 
        shares_diluted: Optional[float], 
        ttm_revenue: Optional[float]
    ) -> Optional[float]:
        """
        Calculate P/S ratio (Market Cap / TTM Revenue).
        
        Market Cap = close_price * shares_diluted
        Returns None if revenue is None, zero, or negative.
        """
        if ttm_revenue is None or ttm_revenue <= 0:
            return None
        
        if close_price is None or close_price <= 0:
            return None
        
        if shares_diluted is None or shares_diluted <= 0:
            return None
        
        market_cap = close_price * shares_diluted
        ps_ratio = market_cap / ttm_revenue
        return round(ps_ratio, 4)
    
    def _parse_date_string(self, date_str: Any) -> Optional[date]:
        """Parse date string into date object."""
        if not date_str:
            return None
        
        # If already a date object
        if isinstance(date_str, date):
            return date_str
        
        # If it's a string, parse it
        if isinstance(date_str, str):
            try:
                # Try ISO format first
                return datetime.fromisoformat(date_str.replace('Z', '+00:00')).date()
            except ValueError:
                try:
                    # Try standard date format
                    return datetime.strptime(date_str, '%Y-%m-%d').date()
                except ValueError:
                    self.logger.debug(f"Could not parse date string: {date_str}")
                    return None
        
        return None
    
    def _empty_result(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Return empty result structure when no data can be transformed."""
        return {
            'ticker': raw_data.get('ticker'),
            'instrument_id': raw_data.get('instrument_info', {}).get('instrument_id'),
            'start_date': raw_data.get('start_date'),
            'end_date': raw_data.get('end_date'),
            'transformation_timestamp': datetime.now(timezone.utc).isoformat(),
            'monthly_metrics': []
        }