"""
Load phase - Pure data persistence to database.

This module is responsible ONLY for loading transformed data into the database.
No extraction or transformation logic should be here.
"""

from typing import Dict, Any, Optional, List, Union
from datetime import datetime, date, timezone
import pandas as pd
from sqlalchemy import text

from ..utils.logging import get_logger
from ..database.manager import DatabaseManager
from ..data.models import Instrument, Price, IncomeStatement, BalanceSheet, CashFlow, FinancialRatio, Event


class FinancialDataLoader:
    """
    Pure loader for financial data to database.
    
    Responsibility: LOAD ONLY
    - Persist transformed data to database
    - Handle database operations and transactions
    - NO extraction or transformation logic
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.logger = get_logger(__name__)
        self.db_manager = db_manager or DatabaseManager()
    
    def load_financial_data(self, transformed_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Load transformed financial data into database using the unified interface.
        
        Args:
            transformed_data: Transformed data from FinancialDataTransformer
            
        Returns:
            Dictionary with loading results and statistics
        """
        ticker = transformed_data.get('ticker')
        self.logger.info(f"Loading financial data for {ticker}")
        
        loading_results = {
            'ticker': ticker,
            'loading_timestamp': datetime.now(timezone.utc).isoformat(),
            'loaded_records': {},
            'errors': []
        }
        
        try:
            # Use DatabaseManager's unified store_financial_data method
            # This method expects the same format as returned by FinancialStatementFetcher
            company_info = transformed_data.get('company_info', {})
            financial_data_to_store = {
                'ticker': ticker,
                'currency': transformed_data.get('currency', 'USD'),
                'company_info': company_info,
                'instrument_info': company_info,  # DatabaseManager expects instrument_info
                'statements': transformed_data.get('statements', {}),
                'derived_metrics': transformed_data.get('derived_metrics', {}),
                'fetch_timestamp': transformed_data.get('transformation_timestamp')
            }
            
            # Store all data using the unified interface
            storage_counts = self.db_manager.store_financial_data(ticker, financial_data_to_store)
            
            # Store events data if available
            events_data = transformed_data.get('events', [])
            if events_data:
                events_count = self._load_events_data(ticker, events_data)
                storage_counts['events'] = events_count
            
            # Map the storage counts to our loading results format
            loading_results['loaded_records'] = storage_counts
            
            total_loaded = sum(storage_counts.values())
            self.logger.info(f"Successfully loaded {total_loaded} records for {ticker}")
            
            # Auto-calculate daily valuations (synchronous) with smart detection
            try:
                # For financial data, we need to check if new fundamental data was loaded
                # that might affect TTM calculations across a broader date range
                fundamental_data_range = self._extract_fundamental_data_range(transformed_data)
                self._auto_calculate_monthly_valuations(ticker, fundamental_data_range)
                loading_results['monthly_valuations_updated'] = True
            except Exception as e:
                self.logger.warning(f"Monthly valuation auto-calculation failed for {ticker}: {e}")
                loading_results['monthly_valuations_updated'] = False
            
        except Exception as e:
            error_msg = f"Failed to load financial data for {ticker}: {str(e)}"
            self.logger.error(error_msg)
            loading_results['errors'].append(error_msg)
            raise e
        
        return loading_results
    
    def load_price_data(self, transformed_price_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Load transformed price data into database.
        
        Args:
            transformed_price_data: Transformed price data from FinancialDataTransformer
            
        Returns:
            Dictionary with loading results and statistics
        """
        ticker = transformed_price_data.get('ticker')
        self.logger.info(f"Loading price data for {ticker}")
        
        loading_results = {
            'ticker': ticker,
            'loading_timestamp': datetime.now(timezone.utc).isoformat(),
            'loaded_records': 0,
            'errors': []
        }
        
        try:
            transformed_df = transformed_price_data.get('transformed_data')
            
            if transformed_df is None or transformed_df.empty:
                self.logger.warning(f"No price data to load for {ticker}")
                return loading_results
            
            # Extract instrument type and info from transformed data
            instrument_type = transformed_price_data.get('instrument_type')
            instrument_info = transformed_price_data.get('instrument_info', {})
            
            # Load price data to database
            loaded_count = self._load_price_dataframe(ticker, transformed_df, instrument_type, instrument_info)
            loading_results['loaded_records'] = loaded_count
            
            self.logger.info(f"Successfully loaded {loaded_count} price records for {ticker}")
            
            # Auto-calculate daily valuations (synchronous) with date range optimization
            try:
                # Extract date range from price data for incremental calculation
                price_data_range = self._extract_price_data_range(transformed_df)
                self._auto_calculate_monthly_valuations(ticker, price_data_range)
                loading_results['monthly_valuations_updated'] = True
            except Exception as e:
                self.logger.warning(f"Monthly valuation auto-calculation failed for {ticker}: {e}")
                loading_results['monthly_valuations_updated'] = False
            
        except Exception as e:
            error_msg = f"Failed to load price data for {ticker}: {str(e)}"
            self.logger.error(error_msg)
            loading_results['errors'].append(error_msg)
            raise e
        
        return loading_results
    
    
    def _load_price_dataframe(self, ticker: str, price_df: pd.DataFrame, instrument_type=None, instrument_info=None) -> int:
        """Load price DataFrame into database using unified interface."""
        if price_df.empty:
            return 0
        
        try:
            # Use DatabaseManager's unified store_price_data method with instrument type and info
            loaded_count = self.db_manager.store_price_data(ticker, price_df, instrument_type, instrument_info)
            return loaded_count
            
        except Exception as e:
            self.logger.error(f"Failed to load price data for {ticker}: {e}")
            return 0
    
    def _load_events_data(self, ticker: str, events_data: List[Dict[str, Any]]) -> int:
        """
        Load transformed events data into database.
        
        Args:
            ticker: Stock ticker symbol
            events_data: List of transformed event dictionaries
            
        Returns:
            Number of events loaded
        """
        if not events_data:
            return 0
        
        loaded_count = 0
        
        try:
            self.logger.info(f"Loading {len(events_data)} events for {ticker}")
            
            # Get or create instrument
            instrument = self.db_manager.get_or_create_instrument(ticker, {})
            
            # Use session context manager
            with self.db_manager.get_session() as session:
                for event_data in events_data:
                    try:
                        # Check for duplicate events (same ticker, type, date)
                        existing_event = session.query(Event).filter_by(
                            ticker_symbol=ticker,
                            event_type=event_data.get('event_type'),
                            event_date=event_data.get('event_date')
                        ).first()
                        
                        if not existing_event:
                            # Create Event object
                            event = Event(
                                instrument_id=instrument.id,
                                ticker_symbol=ticker,
                                event_type=event_data.get('event_type', 'unknown'),
                                event_date=event_data.get('event_date'),
                                event_time=event_data.get('event_time'),
                                description=event_data.get('description'),
                                estimated_eps=event_data.get('estimated_eps'),
                                reported_eps=event_data.get('reported_eps'),
                                eps_surprise=event_data.get('eps_surprise'),
                                dividend_amount=event_data.get('dividend_amount'),
                                dividend_currency=event_data.get('dividend_currency'),
                                split_ratio=event_data.get('split_ratio')
                            )
                            
                            session.add(event)
                            loaded_count += 1
                        else:
                            self.logger.debug(f"Event already exists: {event_data.get('event_type')} on {event_data.get('event_date')}")
                        
                    except Exception as e:
                        self.logger.warning(f"Failed to load event {event_data}: {e}")
                        continue
                
                # Commit all events
                session.commit()
            
            self.logger.info(f"Successfully loaded {loaded_count} events for {ticker}")
            return loaded_count
            
        except Exception as e:
            self.logger.error(f"Failed to load events data for {ticker}: {e}")
            return 0
    
    def _extract_price_data_range(self, price_df: pd.DataFrame) -> Optional[Dict[str, date]]:
        """
        Extract date range from price DataFrame for incremental calculation.
        
        Args:
            price_df: DataFrame with price data containing 'date' column
            
        Returns:
            Dictionary with start_date and end_date or None if no dates found
        """
        try:
            if price_df.empty or 'date' not in price_df.columns:
                return None
            
            # Get min and max dates from the DataFrame
            dates = price_df['date']
            min_date = dates.min()
            max_date = dates.max()
            
            # Convert to date objects if they're not already
            if hasattr(min_date, 'date'):
                min_date = min_date.date()
            if hasattr(max_date, 'date'):
                max_date = max_date.date()
            
            return {
                'start_date': min_date,
                'end_date': max_date
            }
            
        except Exception as e:
            self.logger.debug(f"Could not extract price data range: {e}")
            return None
    
    def _extract_fundamental_data_range(self, transformed_data: Dict[str, Any]) -> Optional[Dict[str, date]]:
        """
        Extract date range from financial data that might affect TTM calculations.
        
        Args:
            transformed_data: Transformed financial data containing statements
            
        Returns:
            Dictionary with start_date and end_date or None if no relevant data found
        """
        try:
            statements = transformed_data.get('statements', {})
            income_stmt = statements.get('income_stmt', {})
            quarterly_data = income_stmt.get('quarterly', {})
            
            if not quarterly_data:
                # No quarterly data means likely no TTM impact
                return None
            
            # Find date range of quarterly income statements
            quarter_dates = []
            for date_str, period_data in quarterly_data.items():
                try:
                    # Convert date string to date object
                    if isinstance(date_str, str):
                        quarter_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                        quarter_dates.append(quarter_date)
                except (ValueError, TypeError):
                    continue
            
            if not quarter_dates:
                return None
            
            return {
                'start_date': min(quarter_dates),
                'end_date': max(quarter_dates)
            }
            
        except Exception as e:
            self.logger.debug(f"Could not extract fundamental data range: {e}")
            return None
    
    def _auto_calculate_monthly_valuations(self, ticker: str, data_range: Optional[Dict[str, date]] = None) -> None:
        """
        Auto-calculate monthly valuations for a ticker when new data is loaded.
        
        This method intelligently determines whether to do incremental updates or
        full historical population based on the data range provided.
        
        Args:
            ticker: Stock ticker symbol
            data_range: Optional dict with 'start_date' and 'end_date' of newly loaded data
        """
        self.logger.info(f"Auto-calculating monthly valuations for {ticker}")
        
        try:
            # Create orchestrator
            valuation_etl = MonthlyValuationETLOrchestrator(self.db_manager)
            
            # Determine calculation strategy
            if data_range and data_range.get('start_date') and data_range.get('end_date'):
                # We have specific date range - use incremental calculation
                self.logger.info(
                    f"Using incremental calculation for {ticker}: "
                    f"{data_range['start_date']} to {data_range['end_date']}"
                )
                results = valuation_etl.run_incremental_valuation_etl(
                    ticker, 
                    data_range['start_date'], 
                    data_range['end_date']
                )
            else:
                # No date range info - fall back to full historical population
                self.logger.info(f"Using full historical population for {ticker}")
                results = valuation_etl.run_historical_population(ticker)
            
            # Log results
            if results['status'] == 'completed':
                loading_results = results.get('loading_results', {})
                records_processed = loading_results.get('records_processed', 0)
                mode = "incrementally" if results.get('incremental_mode') else "historically"
                
                self.logger.info(
                    f"Monthly valuations auto-calculated {mode} for {ticker}: "
                    f"{records_processed} records processed"
                )
            elif results['status'] == 'no_data':
                self.logger.debug(f"No data available for monthly valuation calculation: {ticker}")
            else:
                self.logger.warning(
                    f"Monthly valuation auto-calculation completed with issues for {ticker}: "
                    f"{results.get('error', 'unknown error')}"
                )
                
        except Exception as e:
            # Don't fail the main load operation if valuation calculation fails
            self.logger.warning(f"Failed to auto-calculate monthly valuations for {ticker}: {e}")


class ETLOrchestrator:
    """
    Orchestrates the complete ETL pipeline with proper separation of concerns.
    
    This class coordinates Extract, Transform, Load operations while maintaining
    clear separation between each phase.
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.logger = get_logger(__name__)
        self.db_manager = db_manager or DatabaseManager()
        
        # Import ETL components
        from .extract import FinancialDataExtractor, PriceDataExtractor
        from .transform import FinancialDataTransformer
        
        # Initialize ETL components
        self.financial_extractor = FinancialDataExtractor()
        self.price_extractor = PriceDataExtractor()
        self.transformer = FinancialDataTransformer()
        self.loader = FinancialDataLoader(self.db_manager)
    
    def run_financial_etl(self, ticker: str) -> Dict[str, Any]:
        """
        Run complete financial data ETL pipeline.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            ETL results with statistics from each phase
        """
        self.logger.info(f"Starting financial ETL pipeline for {ticker}")
        
        etl_results = {
            'ticker': ticker,
            'pipeline_start': datetime.now(timezone.utc).isoformat(),
            'phases': {}
        }
        
        try:
            # EXTRACT phase
            self.logger.info(f"Extract phase: extracting raw data for {ticker}")
            raw_data = self.financial_extractor.extract_financial_data(ticker)
            etl_results['phases']['extract'] = {
                'status': 'completed',
                'data_sources_count': len(raw_data.get('data_sources', {})),
                'timestamp': raw_data.get('extraction_timestamp')
            }
            
            # TRANSFORM phase
            self.logger.info(f"Transform phase: transforming data for {ticker}")
            transformed_data = self.transformer.transform_financial_data(raw_data)
            etl_results['phases']['transform'] = {
                'status': 'completed',
                'statements_count': len(transformed_data.get('statements', {})),
                'events_count': len(transformed_data.get('events', [])),
                'timestamp': transformed_data.get('transformation_timestamp')
            }
            
            # LOAD phase
            self.logger.info(f"Load phase: loading data for {ticker}")
            load_results = self.loader.load_financial_data(transformed_data)
            etl_results['phases']['load'] = {
                'status': 'completed',
                'loaded_records': load_results.get('loaded_records', {}),
                'errors': load_results.get('errors', []),
                'timestamp': load_results.get('loading_timestamp')
            }
            
            etl_results['pipeline_end'] = datetime.now(timezone.utc).isoformat()
            etl_results['status'] = 'completed'
            
            total_records = sum(load_results.get('loaded_records', {}).values())
            self.logger.info(f"Financial ETL pipeline completed for {ticker}: {total_records} records loaded")
            
        except Exception as e:
            etl_results['status'] = 'failed'
            etl_results['error'] = str(e)
            etl_results['pipeline_end'] = datetime.now(timezone.utc).isoformat()
            self.logger.error(f"Financial ETL pipeline failed for {ticker}: {e}")
            raise e
        
        return etl_results
    
    def run_price_etl(self, ticker: str, start_date, end_date=None, manual_instrument_type=None) -> Dict[str, Any]:
        """
        Run complete price data ETL pipeline.
        
        Args:
            ticker: Ticker symbol
            start_date: Start date for price data
            end_date: End date for price data (optional)
            manual_instrument_type: Manual instrument type override (optional)
            
        Returns:
            ETL results with statistics from each phase
        """
        self.logger.info(f"Starting price ETL pipeline for {ticker}")
        
        etl_results = {
            'ticker': ticker,
            'pipeline_start': datetime.now(timezone.utc).isoformat(),
            'phases': {}
        }
        
        try:
            # EXTRACT phase
            self.logger.info(f"Extract phase: extracting raw price data for {ticker}")
            raw_price_data = self.price_extractor.extract_price_data(ticker, start_date, end_date)
            
            # Apply manual instrument type override if specified
            if manual_instrument_type:
                self.logger.info(f"Applying manual instrument type override: {manual_instrument_type.value}")
                raw_price_data['instrument_type'] = manual_instrument_type
            
            etl_results['phases']['extract'] = {
                'status': 'completed',
                'shape': raw_price_data.get('shape'),
                'timestamp': raw_price_data.get('extraction_timestamp')
            }
            
            # TRANSFORM phase  
            self.logger.info(f"Transform phase: transforming price data for {ticker}")
            transformed_price_data = self.transformer.transform_price_data(raw_price_data)
            etl_results['phases']['transform'] = {
                'status': 'completed',
                'record_count': transformed_price_data.get('record_count', 0),
                'timestamp': transformed_price_data.get('transformation_timestamp')
            }
            
            # LOAD phase
            self.logger.info(f"Load phase: loading price data for {ticker}")
            load_results = self.loader.load_price_data(transformed_price_data)
            etl_results['phases']['load'] = {
                'status': 'completed',
                'loaded_records': load_results.get('loaded_records', 0),
                'errors': load_results.get('errors', []),
                'timestamp': load_results.get('loading_timestamp')
            }
            
            etl_results['pipeline_end'] = datetime.now(timezone.utc).isoformat()
            etl_results['status'] = 'completed'
            
            self.logger.info(
                f"Price ETL pipeline completed for {ticker}: "
                f"{load_results.get('loaded_records', 0)} records loaded"
            )
            
        except Exception as e:
            etl_results['status'] = 'failed'
            etl_results['error'] = str(e)
            etl_results['pipeline_end'] = datetime.now(timezone.utc).isoformat()
            self.logger.error(f"Price ETL pipeline failed for {ticker}: {e}")
            raise e
        
        return etl_results
    
class EconomicDataLoader:
    """
    Pure loader for economic data to database.
    
    Responsibility: LOAD ONLY
    - Persist transformed economic data to database
    - Handle database operations and transactions
    - NO extraction or transformation logic
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.logger = get_logger(__name__)
        self.db_manager = db_manager or DatabaseManager()
    
    def load_economic_data(self, transformed_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Load transformed economic data into database.
        
        Args:
            transformed_data: Transformed data from EconomicDataTransformer
            
        Returns:
            Dictionary with loading results and statistics
        """
        indicator_name = transformed_data.get('name')  # Use 'name' instead of 'indicator_id'
        source = transformed_data.get('source')
        
        self.logger.info(f"Loading economic data for {source}/{indicator_name}")
        
        loading_results = {
            'indicator_name': indicator_name,
            'source': source,
            'loading_timestamp': datetime.now(timezone.utc).isoformat(),
            'loaded_records': {
                'indicators': 0,
                'data_points': 0
            },
            'errors': []
        }
        
        try:
            # Store indicator and data points using database manager
            results = self.db_manager.store_economic_data(transformed_data)
            
            loading_results['loaded_records'] = results
            
            total_loaded = results.get('indicators', 0) + results.get('data_points', 0)
            self.logger.info(f"Successfully loaded {total_loaded} records for {source}/{indicator_name}")
            
        except Exception as e:
            error_msg = f"Failed to load economic data for {source}/{indicator_name}: {str(e)}"
            self.logger.error(error_msg)
            loading_results['errors'].append(error_msg)
            raise e
        
        return loading_results


class EconomicETLOrchestrator:
    """
    Orchestrates the complete economic data ETL pipeline.
    
    This class coordinates Extract, Transform, Load operations for economic data
    while maintaining clear separation between each phase.
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.logger = get_logger(__name__)
        self.db_manager = db_manager or DatabaseManager()
        
        # Import ETL components from unified locations
        from .extract import EconomicDataExtractor
        from .transform import EconomicDataTransformer
        
        # Initialize ETL components
        self.extractor = EconomicDataExtractor()
        self.transformer = EconomicDataTransformer()
        self.loader = EconomicDataLoader(self.db_manager)
    
    def run_eurostat_etl(self, data_code: str, from_date: str, to_date: str = None, geo_filter: str = None, indicator_name: str = None) -> Dict[str, Any]:
        """
        Run complete Eurostat data ETL pipeline.
        
        Args:
            data_code: Eurostat dataset code
            from_date: Start date for data
            to_date: End date for data (defaults to today if not specified)
            geo_filter: Geographic filter (e.g., "SE" for Sweden, None for default Euro Area)
            indicator_name: Intended indicator name (e.g., "inflation_monthly_sweden")
            
        Returns:
            ETL results with statistics from each phase
        """
        self.logger.info(f"Starting Eurostat ETL pipeline for {data_code}")
        
        etl_results = {
            'source': 'eurostat',
            'data_code': data_code,
            'pipeline_start': datetime.now(timezone.utc).isoformat(),
            'phases': {}
        }
        
        try:
            # EXTRACT phase
            self.logger.info(f"Extract phase: extracting Eurostat data for {data_code}")
            raw_data = self.extractor.extract_eurostat_data(data_code, from_date, to_date, geo_filter)
            etl_results['phases']['extract'] = {
                'status': 'completed',
                'timestamp': raw_data.get('extraction_timestamp')
            }
            
            # TRANSFORM phase
            self.logger.info(f"Transform phase: transforming Eurostat data for {data_code}")
            transformed_data = self.transformer.transform_eurostat_data(raw_data, indicator_name)
            etl_results['phases']['transform'] = {
                'status': 'completed',
                'data_points_count': len(transformed_data.get('data_points', [])),
                'timestamp': transformed_data.get('transformation_timestamp')
            }
            
            # LOAD phase
            self.logger.info(f"Load phase: loading Eurostat data for {data_code}")
            load_results = self.loader.load_economic_data(transformed_data)
            etl_results['phases']['load'] = {
                'status': 'completed',
                'loaded_records': load_results.get('loaded_records', {}),
                'errors': load_results.get('errors', []),
                'timestamp': load_results.get('loading_timestamp')
            }
            
            etl_results['pipeline_end'] = datetime.now(timezone.utc).isoformat()
            etl_results['status'] = 'completed'
            
            self.logger.info(f"Eurostat ETL pipeline completed for {data_code}")
            
        except Exception as e:
            etl_results['status'] = 'failed'
            etl_results['error'] = str(e)
            etl_results['pipeline_end'] = datetime.now(timezone.utc).isoformat()
            self.logger.error(f"Eurostat ETL pipeline failed for {data_code}: {e}")
            raise e
        
        return etl_results
    
    def run_oecd_etl(
        self, 
        series_key: str, 
        country_code: str, 
        from_date: str, 
        to_date: str = None,
        indicator_name: str = None
    ) -> Dict[str, Any]:
        """
        Run complete OECD data ETL pipeline.
        
        Args:
            series_key: OECD series key (e.g., "PRICES_CPI")  
            country_code: ISO country code (e.g., "GBR" for Great Britain)
            from_date: Start date for data
            to_date: End date for data (defaults to today if not specified)
            indicator_name: Intended indicator name (e.g., "inflation_monthly_gb")
            
        Returns:
            ETL results with statistics from each phase
        """
        indicator_id = f"{series_key}_{country_code}"
        self.logger.info(f"Starting OECD ETL pipeline for {indicator_id}")
        
        etl_results = {
            'source': 'oecd',
            'series_key': series_key,
            'country_code': country_code,
            'indicator_id': indicator_id,
            'pipeline_start': datetime.now(timezone.utc).isoformat(),
            'phases': {}
        }
        
        try:
            # EXTRACT phase
            self.logger.info(f"Extract phase: extracting OECD data for {indicator_id}")
            raw_data = self.extractor.extract_oecd_data(series_key, country_code, from_date, to_date)
            etl_results['phases']['extract'] = {
                'status': 'completed',
                'timestamp': raw_data.get('extraction_timestamp')
            }
            
            # TRANSFORM phase
            self.logger.info(f"Transform phase: transforming OECD data for {indicator_id}")
            transformed_data = self.transformer.transform_oecd_data(raw_data, indicator_name)
            etl_results['phases']['transform'] = {
                'status': 'completed',
                'records_processed': len(transformed_data.get('data_points', []))
            }
            
            # LOAD phase
            self.logger.info(f"Load phase: storing OECD data for {indicator_id}")
            load_results = self.loader.load_economic_data(transformed_data)
            etl_results['phases']['load'] = {
                'status': 'completed',
                'loaded_records': load_results.get('loaded_records', {}),
                'errors': load_results.get('errors', []),
                'timestamp': load_results.get('loading_timestamp')
            }
            
            etl_results['status'] = 'completed'
            etl_results['pipeline_end'] = datetime.now(timezone.utc).isoformat()
            
            total_records = load_results.get('loaded_records', {}).get('data_points', 0)
            self.logger.info(f"✅ OECD ETL pipeline completed for {indicator_id}: {total_records} records processed")
            
        except Exception as e:
            etl_results['status'] = 'failed'
            etl_results['error'] = str(e)
            etl_results['pipeline_end'] = datetime.now(timezone.utc).isoformat()
            self.logger.error(f"OECD ETL pipeline failed for {indicator_id}: {e}")
            raise e
        
        return etl_results
    
    def run_ecb_etl(
        self, 
        dataflow_ref: str, 
        series_key: str, 
        from_date: str, 
        to_date: str
    ) -> Dict[str, Any]:
        """
        Run complete ECB data ETL pipeline.
        
        Args:
            dataflow_ref: ECB dataflow reference
            series_key: ECB series key
            from_date: Start date for data
            to_date: End date for data
            
        Returns:
            ETL results with statistics from each phase
        """
        indicator_id = f"{dataflow_ref}.{series_key}"
        self.logger.info(f"Starting ECB ETL pipeline for {indicator_id}")
        
        etl_results = {
            'source': 'ecb',
            'indicator_id': indicator_id,
            'pipeline_start': datetime.now(timezone.utc).isoformat(),
            'phases': {}
        }
        
        try:
            # EXTRACT phase
            self.logger.info(f"Extract phase: extracting ECB data for {indicator_id}")
            raw_data = self.extractor.extract_ecb_data(dataflow_ref, series_key, from_date, to_date)
            etl_results['phases']['extract'] = {
                'status': 'completed',
                'timestamp': raw_data.get('extraction_timestamp')
            }
            
            # TRANSFORM phase
            self.logger.info(f"Transform phase: transforming ECB data for {indicator_id}")
            transformed_data = self.transformer.transform_ecb_data(raw_data)
            etl_results['phases']['transform'] = {
                'status': 'completed',
                'data_points_count': len(transformed_data.get('data_points', [])),
                'timestamp': transformed_data.get('transformation_timestamp')
            }
            
            # LOAD phase
            self.logger.info(f"Load phase: loading ECB data for {indicator_id}")
            load_results = self.loader.load_economic_data(transformed_data)
            etl_results['phases']['load'] = {
                'status': 'completed',
                'loaded_records': load_results.get('loaded_records', {}),
                'errors': load_results.get('errors', []),
                'timestamp': load_results.get('loading_timestamp')
            }
            
            etl_results['pipeline_end'] = datetime.now(timezone.utc).isoformat()
            etl_results['status'] = 'completed'
            
            self.logger.info(f"ECB ETL pipeline completed for {indicator_id}")
            
        except Exception as e:
            etl_results['status'] = 'failed'
            etl_results['error'] = str(e)
            etl_results['pipeline_end'] = datetime.now(timezone.utc).isoformat()
            self.logger.error(f"ECB ETL pipeline failed for {indicator_id}: {e}")
            raise e
        
        return etl_results
    
    def run_fred_etl(
        self, 
        series_id: str, 
        api_key: str, 
        from_date: str, 
        to_date: str
    ) -> Dict[str, Any]:
        """
        Run complete FRED data ETL pipeline.
        
        Args:
            series_id: FRED series ID
            api_key: FRED API key
            from_date: Start date for data
            to_date: End date for data
            
        Returns:
            ETL results with statistics from each phase
        """
        self.logger.info(f"Starting FRED ETL pipeline for {series_id}")
        
        etl_results = {
            'source': 'fred',
            'series_id': series_id,
            'pipeline_start': datetime.now(timezone.utc).isoformat(),
            'phases': {}
        }
        
        try:
            # EXTRACT phase
            self.logger.info(f"Extract phase: extracting FRED data for {series_id}")
            raw_data = self.extractor.extract_fred_data(series_id, api_key, from_date, to_date)
            etl_results['phases']['extract'] = {
                'status': 'completed',
                'timestamp': raw_data.get('extraction_timestamp')
            }
            
            # TRANSFORM phase
            self.logger.info(f"Transform phase: transforming FRED data for {series_id}")
            transformed_data = self.transformer.transform_fred_data(raw_data)
            
            # Handle case where transform returns multiple indicators (e.g., CPI index + rate)
            if isinstance(transformed_data, list):
                total_data_points = sum(len(data.get('data_points', [])) for data in transformed_data)
                etl_results['phases']['transform'] = {
                    'status': 'completed',
                    'data_points_count': total_data_points,
                    'indicators_count': len(transformed_data),
                    'timestamp': transformed_data[0].get('transformation_timestamp') if transformed_data else None
                }
            else:
                etl_results['phases']['transform'] = {
                    'status': 'completed',
                    'data_points_count': len(transformed_data.get('data_points', [])),
                    'timestamp': transformed_data.get('transformation_timestamp')
                }
            
            # LOAD phase
            self.logger.info(f"Load phase: loading FRED data for {series_id}")
            if isinstance(transformed_data, list):
                # Load multiple indicators
                all_load_results = []
                for data in transformed_data:
                    load_result = self.loader.load_economic_data(data)
                    all_load_results.append(load_result)
                
                # Combine results
                total_loaded = sum(result.get('loaded_records', {}).get('data_points', 0) for result in all_load_results)
                load_results = {
                    'loaded_records': {'data_points': total_loaded, 'indicators': len(transformed_data)},
                    'errors': [],
                    'loading_timestamp': all_load_results[0].get('loading_timestamp') if all_load_results else None
                }
            else:
                load_results = self.loader.load_economic_data(transformed_data)
            etl_results['phases']['load'] = {
                'status': 'completed',
                'loaded_records': load_results.get('loaded_records', {}),
                'errors': load_results.get('errors', []),
                'timestamp': load_results.get('loading_timestamp')
            }
            
            etl_results['pipeline_end'] = datetime.now(timezone.utc).isoformat()
            etl_results['status'] = 'completed'
            
            self.logger.info(f"FRED ETL pipeline completed for {series_id}")
            
        except Exception as e:
            etl_results['status'] = 'failed'
            etl_results['error'] = str(e)
            etl_results['pipeline_end'] = datetime.now(timezone.utc).isoformat()
            self.logger.error(f"FRED ETL pipeline failed for {series_id}: {e}")
            raise e
        
        return etl_results


class AlignedDataETLOrchestrator:
    """
    Orchestrates the complete aligned data ETL pipeline.
    
    This class coordinates the process of:
    1. Extracting price and economic data from existing database
    2. Transforming economic data with forward-fill to trading calendar
    3. Loading aligned data into the unified aligned_daily_data table
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.logger = get_logger(__name__)
        self.db_manager = db_manager or DatabaseManager()
        
        # Import transformer here to avoid circular imports
        from ..data.forward_fill import forward_fill_transformer
        from ..utils.trading_calendar import trading_calendar
        
        self.transformer = forward_fill_transformer
        self.trading_calendar = trading_calendar
    
    def rebuild_aligned_data(
        self,
        tickers: Optional[List[str]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        clear_existing: bool = True
    ) -> Dict[str, Any]:
        """
        Rebuild aligned data for specified tickers and date range.
        
        Args:
            tickers: List of ticker symbols to process (None for all)
            start_date: Start date for alignment (None for all available)
            end_date: End date for alignment (None for today)
            clear_existing: Whether to clear existing aligned data
            
        Returns:
            Dictionary with rebuild results and statistics
        """
        self.logger.info(f"Starting aligned data rebuild for {len(tickers) if tickers else 'all'} tickers")
        
        rebuild_results = {
            'pipeline_start': datetime.now(timezone.utc).isoformat(),
            'tickers_processed': 0,
            'total_records_created': 0,
            'errors': [],
            'statistics': {}
        }
        
        try:
            # Get list of tickers to process
            if not tickers:
                # Get all tickers with price data
                tickers = self._get_all_tickers_with_data()
            
            if not tickers:
                self.logger.warning("No tickers found to process")
                return rebuild_results
            
            # Get economic data once for all tickers
            economic_data = self._get_all_economic_data(start_date, end_date)
            
            # Process each ticker
            for ticker in tickers:
                try:
                    ticker_results = self._rebuild_ticker_aligned_data(
                        ticker, start_date, end_date, economic_data, clear_existing
                    )
                    
                    rebuild_results['tickers_processed'] += 1
                    rebuild_results['total_records_created'] += ticker_results['records_created']
                    rebuild_results['statistics'][ticker] = ticker_results
                    
                    if ticker_results['records_created'] > 0:
                        self.logger.info(
                            f"✅ {ticker}: {ticker_results['records_created']} aligned records created"
                        )
                    
                except Exception as e:
                    error_msg = f"Failed to rebuild aligned data for {ticker}: {str(e)}"
                    self.logger.error(error_msg)
                    rebuild_results['errors'].append(error_msg)
            
            rebuild_results['pipeline_end'] = datetime.now(timezone.utc).isoformat()
            
            # Log summary
            self.logger.info(
                f"Aligned data rebuild complete: {rebuild_results['tickers_processed']} tickers, "
                f"{rebuild_results['total_records_created']} records, "
                f"{len(rebuild_results['errors'])} errors"
            )
            
            return rebuild_results
            
        except Exception as e:
            rebuild_results['pipeline_end'] = datetime.now(timezone.utc).isoformat()
            error_msg = f"Aligned data rebuild failed: {str(e)}"
            self.logger.error(error_msg)
            rebuild_results['errors'].append(error_msg)
            raise e
    
    def _rebuild_ticker_aligned_data(
        self,
        ticker: str,
        start_date: Optional[date],
        end_date: Optional[date],
        economic_data: Dict[str, List[Dict[str, Any]]],
        clear_existing: bool
    ) -> Dict[str, Any]:
        """Rebuild aligned data for a single ticker."""
        
        # Get instrument info
        instrument_info = self.db_manager.get_instrument_info(ticker)
        if not instrument_info:
            raise ValueError(f"Instrument not found for ticker {ticker}")
        
        instrument_id = instrument_info['instrument_id']
        
        # Detect trading calendar for this ticker
        exchange = self.trading_calendar.detect_exchange_from_ticker(ticker)
        
        # Get price data from database
        price_data = self.db_manager.get_price_data(ticker, start_date, end_date)
        
        if price_data.empty:
            self.logger.warning(f"No price data found for {ticker}")
            return {'records_created': 0, 'trading_days': 0, 'exchange': exchange}
        
        # Determine date range from available data
        # CRITICAL: Allow alignment beyond price data range to forward-fill economic indicators
        if price_data.empty:
            actual_start_date = start_date or date(2020, 1, 1)
            actual_end_date = end_date or date.today()
        else:
            price_start = price_data.index.min().date() if hasattr(price_data.index.min(), 'date') else price_data.index.min()
            price_end = price_data.index.max().date() if hasattr(price_data.index.max(), 'date') else price_data.index.max()
            
            # Start date: constrained by price data availability
            actual_start_date = max(start_date or price_start, price_start)
            
            # End date: EXTEND beyond price data to forward-fill economic indicators
            # This allows economic indicators to be forward-filled to today's date
            # even if price data is outdated
            if end_date:
                actual_end_date = end_date  # Use requested end date
            else:
                actual_end_date = max(price_end, date.today())  # Extend to today if no explicit end date
        
        # Get trading calendar for this ticker
        # Extended range allows forward-filling economic indicators beyond price data
        trading_days = self.transformer.get_date_range_for_instrument(
            ticker, actual_start_date, actual_end_date, exchange
        )
        
        self.logger.info(
            f"Date range for {ticker}: {actual_start_date} to {actual_end_date} "
            f"({len(trading_days)} trading days, price data through {price_end if not price_data.empty else 'N/A'})"
        )
        
        if not trading_days:
            return {'records_created': 0, 'trading_days': 0, 'exchange': exchange}
        
        # Forward-fill economic data to trading calendar
        aligned_economic_df = self.transformer.forward_fill_economic_data(
            economic_data, trading_days, exchange
        )
        
        # Combine price data with forward-filled economic data
        aligned_df = self.transformer.align_price_with_economic_data(
            price_data, aligned_economic_df, ticker
        )
        
        if aligned_df.empty:
            return {'records_created': 0, 'trading_days': len(trading_days), 'exchange': exchange}
        
        # Create database records
        aligned_records = self.transformer.create_aligned_daily_records(
            ticker, aligned_df, instrument_id, exchange
        )
        
        # Store in database
        records_created = 0
        if aligned_records:
            records_created = self.db_manager.store_aligned_daily_data(
                aligned_records, clear_existing=clear_existing
            )
        
        return {
            'records_created': records_created,
            'trading_days': len(trading_days),
            'exchange': exchange,
            'date_range': {
                'start': actual_start_date,
                'end': actual_end_date
            }
        }
    
    def _get_all_tickers_with_data(self) -> List[str]:
        """Get all tickers that have price data in database."""
        try:
            instruments_info = self.db_manager.get_all_instruments_info()
            
            tickers_with_data = []
            for info in instruments_info:
                ticker = info['ticker_symbol']
                # Quick check if ticker has any price data
                price_count = self.db_manager.get_price_data_count(ticker)
                if price_count > 0:
                    tickers_with_data.append(ticker)
            
            self.logger.info(f"Found {len(tickers_with_data)} tickers with price data")
            return tickers_with_data
            
        except Exception as e:
            self.logger.error(f"Failed to get tickers with data: {e}")
            return []
    
    def _get_all_economic_data(
        self, 
        start_date: Optional[date], 
        end_date: Optional[date]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Get all economic indicator data for the date range."""
        
        economic_data = {}
        
        try:
            # Get all economic indicators
            indicators = self.db_manager.get_all_economic_indicators()
            
            for indicator in indicators:
                indicator_name = indicator['name']
                
                # Get data for this indicator
                indicator_data = self.db_manager.get_economic_data(
                    indicator_name, start_date, end_date
                )
                
                if not indicator_data.empty:
                    # Convert DataFrame to list of dictionaries
                    data_points = []
                    # get_economic_data returns DataFrame with 'date' and 'value' columns
                    for _, row in indicator_data.iterrows():
                        date_val = row['date']
                        value = row['value']
                        if pd.notna(value):
                            # Handle different date formats
                            if hasattr(date_val, 'date'):
                                date_obj = date_val.date()
                            elif isinstance(date_val, str):
                                date_obj = datetime.strptime(date_val, '%Y-%m-%d').date()
                            else:
                                date_obj = date_val
                            
                            data_points.append({
                                'date': date_obj,
                                'value': float(value)
                            })
                    
                    if data_points:
                        economic_data[indicator_name] = data_points
                        self.logger.debug(f"Loaded {len(data_points)} points for {indicator_name}")
            
            self.logger.info(f"Loaded economic data for {len(economic_data)} indicators")
            return economic_data
            
        except Exception as e:
            self.logger.error(f"Failed to get economic data: {e}")
            return {}


class MonthlyValuationLoader:
    """
    Pure loader for monthly valuation metrics to database.
    
    Responsibility: LOAD ONLY
    - Persist transformed monthly valuation data to database
    - Handle database operations and transactions
    - NO extraction or transformation logic
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.logger = get_logger(__name__)
        self.db_manager = db_manager or DatabaseManager()
    
    def load_monthly_valuation_data(self, transformed_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Load transformed monthly valuation metrics into database.
        
        Args:
            transformed_data: Transformed data from MonthlyValuationTransformer
            
        Returns:
            Dictionary with loading results and statistics
        """
        ticker = transformed_data.get('ticker')
        self.logger.info(f"Loading monthly valuation data for {ticker}")
        
        loading_results = {
            'ticker': ticker,
            'loading_timestamp': datetime.now(timezone.utc).isoformat(),
            'records_processed': 0,
            'records_inserted': 0,
            'records_updated': 0,
            'errors': []
        }
        
        try:
            monthly_metrics = transformed_data.get('monthly_metrics', [])
            
            if not monthly_metrics:
                self.logger.info(f"No monthly valuation metrics to load for {ticker}")
                return loading_results
            
            # Load in batch for efficiency
            results = self._batch_load_monthly_metrics(monthly_metrics)
            
            loading_results.update({
                'records_processed': len(monthly_metrics),
                'records_inserted': results['inserted'],
                'records_updated': results['updated']
            })
            
            self.logger.info(
                f"Successfully loaded monthly valuation data for {ticker}: "
                f"{results['inserted']} inserted, {results['updated']} updated"
            )
            
        except Exception as e:
            error_msg = f"Failed to load monthly valuation data for {ticker}: {e}"
            self.logger.error(error_msg)
            loading_results['errors'].append(error_msg)
        
        return loading_results
    
    def _batch_load_monthly_metrics(self, monthly_metrics: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Load monthly metrics in batch using database manager.
        
        Returns:
            Dictionary with insert/update counts
        """
        from ..data.models import MonthlyValuationMetrics
        
        inserted_count = 0
        updated_count = 0
        
        with self.db_manager.get_session() as session:
            for metric_data in monthly_metrics:
                try:
                    # Check if record already exists
                    existing = session.query(MonthlyValuationMetrics).filter(
                        MonthlyValuationMetrics.instrument_id == metric_data['instrument_id'],
                        MonthlyValuationMetrics.date == metric_data['date']
                    ).first()
                    
                    if existing:
                        # Update existing record
                        for key, value in metric_data.items():
                            if key not in ['instrument_id', 'date']:  # Don't update key fields
                                setattr(existing, key, value)
                        existing.updated_at = datetime.utcnow()
                        updated_count += 1
                    else:
                        # Create new record
                        new_metric = MonthlyValuationMetrics(**metric_data)
                        session.add(new_metric)
                        inserted_count += 1
                    
                    # Commit in batches for performance
                    if (inserted_count + updated_count) % 1000 == 0:
                        session.commit()
                        
                except Exception as e:
                    self.logger.debug(f"Error loading monthly metric: {e}")
                    session.rollback()
                    continue
            
            # Final commit
            session.commit()
        
        return {'inserted': inserted_count, 'updated': updated_count}


class MonthlyValuationETLOrchestrator:
    """
    Orchestrator for monthly valuation metrics ETL process.
    
    Coordinates extract → transform → load pipeline for monthly valuations.
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.logger = get_logger(__name__)
        self.db_manager = db_manager or DatabaseManager()
        
        # Initialize ETL components
        from .extract import MonthlyValuationExtractor
        from .transform import MonthlyValuationTransformer
        
        self.extractor = MonthlyValuationExtractor()
        self.transformer = MonthlyValuationTransformer()
        self.loader = MonthlyValuationLoader(db_manager)
    
    def run_monthly_valuation_etl(
        self,
        ticker: str,
        start_date: date,
        end_date: Optional[date] = None
    ) -> Dict[str, Any]:
        """
        Run complete monthly valuation ETL pipeline for a ticker.
        
        Args:
            ticker: Stock ticker symbol
            start_date: Start date for valuation calculation
            end_date: End date for valuation calculation
            
        Returns:
            Dictionary with ETL results
        """
        self.logger.info(f"Starting monthly valuation ETL for {ticker} from {start_date}")
        
        etl_results = {
            'ticker': ticker,
            'start_date': start_date,
            'end_date': end_date,
            'status': 'started',
            'extraction_results': None,
            'transformation_results': None,
            'loading_results': None,
            'error': None
        }
        
        try:
            # Extract
            raw_data = self.extractor.extract_monthly_valuation_data(ticker, start_date, end_date)
            etl_results['extraction_results'] = {
                'monthly_price_records': len(raw_data.get('monthly_price_data', [])),
                'ttm_periods': len(raw_data.get('ttm_timeline', []))
            }
            
            # Transform
            transformed_data = self.transformer.transform_monthly_valuation_data(raw_data)
            etl_results['transformation_results'] = {
                'monthly_metrics_count': len(transformed_data.get('monthly_metrics', []))
            }
            
            # Load
            loading_results = self.loader.load_monthly_valuation_data(transformed_data)
            etl_results['loading_results'] = loading_results
            
            # Determine overall status
            if loading_results.get('errors'):
                etl_results['status'] = 'completed_with_errors'
            else:
                etl_results['status'] = 'completed'
            
            self.logger.info(f"Monthly valuation ETL completed for {ticker}")
            
        except Exception as e:
            error_msg = f"Monthly valuation ETL failed for {ticker}: {e}"
            self.logger.error(error_msg)
            etl_results['status'] = 'failed'
            etl_results['error'] = error_msg
        
        return etl_results
    
    def run_incremental_valuation_etl(
        self,
        ticker: str,
        start_date: date,
        end_date: Optional[date] = None,
        recalculate_affected: bool = True
    ) -> Dict[str, Any]:
        """
        Run incremental monthly valuation ETL for specific date ranges.
        
        This is more efficient than historical population as it only calculates
        monthly valuations for the specified date range instead of all historical data.
        
        Args:
            ticker: Stock ticker symbol
            start_date: Start date for incremental calculation
            end_date: End date for incremental calculation (defaults to today)
            recalculate_affected: If True, also recalculate months where TTM may have changed
            
        Returns:
            Dictionary with ETL results
        """
        if end_date is None:
            end_date = date.today()
            
        self.logger.info(f"Starting incremental monthly valuation ETL for {ticker} from {start_date} to {end_date}")
        
        etl_results = {
            'ticker': ticker,
            'start_date': start_date,
            'end_date': end_date,
            'status': 'started',
            'incremental_mode': True,
            'extraction_results': None,
            'transformation_results': None,
            'loading_results': None,
            'error': None
        }
        
        try:
            # For incremental updates, we might need to extend the date range
            # if new fundamental data affects historical monthly TTM calculations
            calculation_start_date = start_date
            calculation_end_date = end_date
            
            if recalculate_affected:
                # Check if we have new fundamental data that affects TTM calculations
                affected_range = self._get_ttm_affected_date_range(ticker, start_date, end_date)
                if affected_range:
                    # Ensure affected dates are date objects for comparison
                    affected_start = self._ensure_date_object(affected_range['start_date'])
                    affected_end = self._ensure_date_object(affected_range['end_date'])
                    
                    calculation_start_date = min(start_date, affected_start)
                    calculation_end_date = max(end_date, affected_end)
                    
                    self.logger.info(
                        f"Extending calculation range due to TTM changes: "
                        f"{calculation_start_date} to {calculation_end_date}"
                    )
            
            # Run ETL for the determined date range
            return self.run_monthly_valuation_etl(ticker, calculation_start_date, calculation_end_date)
            
        except Exception as e:
            error_msg = f"Incremental valuation ETL failed for {ticker}: {e}"
            self.logger.error(error_msg)
            etl_results['status'] = 'failed'
            etl_results['error'] = error_msg
            return etl_results
    
    def run_historical_population(self, ticker: str) -> Dict[str, Any]:
        """
        Run daily valuation ETL for all available historical data for a ticker.
        
        Finds the earliest date with both price and fundamental data.
        """
        self.logger.info(f"Starting historical daily valuation population for {ticker}")
        
        try:
            # Get date range with available data
            date_range = self._get_available_data_range(ticker)
            
            if not date_range:
                self.logger.warning(f"No data available for {ticker}")
                return {
                    'ticker': ticker,
                    'status': 'no_data',
                    'error': 'No price or fundamental data found'
                }
            
            start_date = date_range['start_date']
            end_date = date_range['end_date']
            
            self.logger.info(
                f"Running historical population for {ticker} from {start_date} to {end_date}"
            )
            
            # Run ETL for full historical range
            return self.run_monthly_valuation_etl(ticker, start_date, end_date)
            
        except Exception as e:
            error_msg = f"Historical population failed for {ticker}: {e}"
            self.logger.error(error_msg)
            return {
                'ticker': ticker,
                'status': 'failed',
                'error': error_msg
            }
    
    def _get_available_data_range(self, ticker: str) -> Optional[Dict[str, date]]:
        """Get the date range where both price and fundamental data are available."""
        with self.db_manager.get_session() as session:
            # Get price data range
            price_query = text("""
            SELECT MIN(p.date) as min_date, MAX(p.date) as max_date
            FROM instruments ins
            JOIN prices p ON ins.id = p.instrument_id
            WHERE ins.ticker_symbol = :ticker
            """)
            price_result = session.execute(price_query, {'ticker': ticker}).fetchone()
            
            # Get fundamental data range (from quarterly income statements)
            fundamental_query = text("""
            SELECT MIN(inc.period_end_date) as min_date, MAX(inc.period_end_date) as max_date
            FROM instruments ins
            JOIN income_statements inc ON ins.id = inc.instrument_id
            WHERE ins.ticker_symbol = :ticker
                AND inc.period_type = 'quarterly'
                AND inc.total_revenue IS NOT NULL
                AND inc.net_income IS NOT NULL
            """)
            fundamental_result = session.execute(fundamental_query, {'ticker': ticker}).fetchone()
            
            if not price_result or not fundamental_result:
                return None
            
            price_min, price_max = price_result[0], price_result[1]
            fund_min, fund_max = fundamental_result[0], fundamental_result[1]
            
            if not all([price_min, price_max, fund_min, fund_max]):
                return None
            
            # Use the latest start date (when both types become available)
            # and the earliest end date (when either type ends)
            start_date = max(price_min, fund_min)
            end_date = min(price_max, fund_max)
            
            if start_date > end_date:
                return None
            
            return {
                'start_date': start_date,
                'end_date': end_date
            }
    
    def _get_ttm_affected_date_range(
        self, 
        ticker: str, 
        new_data_start: date, 
        new_data_end: date
    ) -> Optional[Dict[str, date]]:
        """
        Determine if new fundamental data affects existing TTM calculations.
        
        When new quarterly data is added, it can change TTM calculations going back
        up to 4 quarters, affecting existing daily valuations.
        
        Args:
            ticker: Stock ticker symbol
            new_data_start: Start date of newly added data
            new_data_end: End date of newly added data
            
        Returns:
            Dictionary with affected date range or None if no impact
        """
        try:
            with self.db_manager.get_session() as session:
                # Check if any new quarterly income statements were added in this date range
                new_quarters_query = text("""
                SELECT MIN(inc.period_end_date) as earliest_new_quarter
                FROM instruments ins
                JOIN income_statements inc ON ins.id = inc.instrument_id
                WHERE ins.ticker_symbol = :ticker
                    AND inc.period_type = 'quarterly'
                    AND inc.period_end_date >= :start_date
                    AND inc.period_end_date <= :end_date
                    AND inc.total_revenue IS NOT NULL
                    AND inc.net_income IS NOT NULL
                """)
                
                result = session.execute(
                    new_quarters_query, 
                    {'ticker': ticker, 'start_date': new_data_start, 'end_date': new_data_end}
                ).fetchone()
                
                if not result or not result[0]:
                    # No new quarterly data, no TTM impact
                    return None
                
                earliest_new_quarter = result[0]
                
                # New quarterly data affects TTM calculations from that quarter forward
                # But we want to be conservative and recalculate from the start of that quarter
                affected_start = earliest_new_quarter
                
                # Affected range goes to the end of our price data
                price_end_query = text("""
                SELECT MAX(p.date) as max_price_date
                FROM instruments ins
                JOIN prices p ON ins.id = p.instrument_id
                WHERE ins.ticker_symbol = :ticker
                """)
                
                price_result = session.execute(price_end_query, {'ticker': ticker}).fetchone()
                affected_end = price_result[0] if price_result and price_result[0] else new_data_end
                
                self.logger.info(
                    f"New quarterly data detected for {ticker}: affects valuations from {affected_start}"
                )
                
                return {
                    'start_date': affected_start,
                    'end_date': affected_end
                }
                
        except Exception as e:
            self.logger.warning(f"Error determining TTM affected range for {ticker}: {e}")
            return None
    
    def _ensure_date_object(self, date_value: Union[str, date, datetime]) -> date:
        """
        Ensure a value is converted to a date object.
        
        Args:
            date_value: Date value that might be a string, datetime, or date
            
        Returns:
            date object
        """
        if isinstance(date_value, date):
            return date_value
        elif isinstance(date_value, datetime):
            return date_value.date()
        elif isinstance(date_value, str):
            try:
                # Try parsing ISO format date string
                return datetime.fromisoformat(date_value.replace('Z', '+00:00')).date()
            except ValueError:
                try:
                    # Try standard date format
                    return datetime.strptime(date_value, '%Y-%m-%d').date()
                except ValueError:
                    raise ValueError(f"Cannot convert date string to date object: {date_value}")
        else:
            raise TypeError(f"Cannot convert {type(date_value)} to date object: {date_value}")