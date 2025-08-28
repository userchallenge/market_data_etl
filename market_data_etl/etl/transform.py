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