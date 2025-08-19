"""
Load phase - Pure data persistence to database.

This module is responsible ONLY for loading transformed data into the database.
No extraction or transformation logic should be here.
"""

from typing import Dict, Any, Optional, List
from datetime import datetime, date, timezone
import pandas as pd

from ..utils.logging import get_logger
from ..database.manager import DatabaseManager
from ..data.models import Instrument, Price, IncomeStatement, BalanceSheet, CashFlow, FinancialRatio


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
            financial_data_to_store = {
                'ticker': ticker,
                'currency': transformed_data.get('currency', 'USD'),
                'company_info': transformed_data.get('company_info', {}),
                'statements': transformed_data.get('statements', {}),
                'derived_metrics': transformed_data.get('derived_metrics', {}),
                'fetch_timestamp': transformed_data.get('transformation_timestamp')
            }
            
            # Store all data using the unified interface
            storage_counts = self.db_manager.store_financial_data(ticker, financial_data_to_store)
            
            # Map the storage counts to our loading results format
            loading_results['loaded_records'] = storage_counts
            
            total_loaded = sum(storage_counts.values())
            self.logger.info(f"Successfully loaded {total_loaded} records for {ticker}")
            
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
    
    def load_economic_data(self, transformed_data: Dict[str, Any], auto_extend_to_today: bool = False) -> Dict[str, Any]:
        """
        Load transformed economic data into database.
        
        Args:
            transformed_data: Transformed data from EconomicDataTransformer
            auto_extend_to_today: Whether to forward-fill data to today's date when no end date specified
            
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
            results = self.db_manager.store_economic_data(transformed_data, auto_extend_to_today)
            
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
    
    def run_eurostat_etl(self, data_code: str, from_date: str, to_date: str = None, auto_extend_to_today: bool = False) -> Dict[str, Any]:
        """
        Run complete Eurostat data ETL pipeline.
        
        Args:
            data_code: Eurostat dataset code
            from_date: Start date for data
            to_date: End date for data (defaults to today if not specified)
            auto_extend_to_today: Whether to forward-fill data to today when no to_date specified
            
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
            raw_data = self.extractor.extract_eurostat_data(data_code, from_date, to_date)
            etl_results['phases']['extract'] = {
                'status': 'completed',
                'timestamp': raw_data.get('extraction_timestamp')
            }
            
            # TRANSFORM phase
            self.logger.info(f"Transform phase: transforming Eurostat data for {data_code}")
            transformed_data = self.transformer.transform_eurostat_data(raw_data)
            etl_results['phases']['transform'] = {
                'status': 'completed',
                'data_points_count': len(transformed_data.get('data_points', [])),
                'timestamp': transformed_data.get('transformation_timestamp')
            }
            
            # LOAD phase
            self.logger.info(f"Load phase: loading Eurostat data for {data_code}")
            load_results = self.loader.load_economic_data(transformed_data, auto_extend_to_today)
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
    
    def run_ecb_etl(
        self, 
        dataflow_ref: str, 
        series_key: str, 
        from_date: str, 
        to_date: str,
        auto_extend_to_today: bool = False
    ) -> Dict[str, Any]:
        """
        Run complete ECB data ETL pipeline.
        
        Args:
            dataflow_ref: ECB dataflow reference
            series_key: ECB series key
            from_date: Start date for data
            to_date: End date for data
            auto_extend_to_today: Whether to forward-fill data to today when no end date specified
            
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
            load_results = self.loader.load_economic_data(transformed_data, auto_extend_to_today)
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
        to_date: str,
        auto_extend_to_today: bool = False
    ) -> Dict[str, Any]:
        """
        Run complete FRED data ETL pipeline.
        
        Args:
            series_id: FRED series ID
            api_key: FRED API key
            from_date: Start date for data
            to_date: End date for data
            auto_extend_to_today: Whether to forward-fill data to today when no end date specified
            
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
                    load_result = self.loader.load_economic_data(data, auto_extend_to_today)
                    all_load_results.append(load_result)
                
                # Combine results
                total_loaded = sum(result.get('loaded_records', {}).get('data_points', 0) for result in all_load_results)
                load_results = {
                    'loaded_records': {'data_points': total_loaded, 'indicators': len(transformed_data)},
                    'errors': [],
                    'loading_timestamp': all_load_results[0].get('loading_timestamp') if all_load_results else None
                }
            else:
                load_results = self.loader.load_economic_data(transformed_data, auto_extend_to_today)
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