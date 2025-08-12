"""
Load phase - Pure data persistence to database.

This module is responsible ONLY for loading transformed data into the database.
No extraction or transformation logic should be here.
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
import pandas as pd

from ..utils.logging import get_logger
from ..database.manager import DatabaseManager
from ..data.models import Company, Price, IncomeStatement, BalanceSheet, CashFlow, FinancialRatio


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
            'loading_timestamp': datetime.utcnow().isoformat(),
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
            'loading_timestamp': datetime.utcnow().isoformat(),
            'loaded_records': 0,
            'errors': []
        }
        
        try:
            transformed_df = transformed_price_data.get('transformed_data')
            
            if transformed_df is None or transformed_df.empty:
                self.logger.warning(f"No price data to load for {ticker}")
                return loading_results
            
            # Load price data to database
            loaded_count = self._load_price_dataframe(ticker, transformed_df)
            loading_results['loaded_records'] = loaded_count
            
            self.logger.info(f"Successfully loaded {loaded_count} price records for {ticker}")
            
        except Exception as e:
            error_msg = f"Failed to load price data for {ticker}: {str(e)}"
            self.logger.error(error_msg)
            loading_results['errors'].append(error_msg)
            raise e
        
        return loading_results
    
    
    def _load_price_dataframe(self, ticker: str, price_df: pd.DataFrame) -> int:
        """Load price DataFrame into database using unified interface."""
        if price_df.empty:
            return 0
        
        try:
            # Use DatabaseManager's unified store_price_data method
            loaded_count = self.db_manager.store_price_data(ticker, price_df)
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
            'pipeline_start': datetime.utcnow().isoformat(),
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
            
            etl_results['pipeline_end'] = datetime.utcnow().isoformat()
            etl_results['status'] = 'completed'
            
            total_records = sum(load_results.get('loaded_records', {}).values())
            self.logger.info(f"Financial ETL pipeline completed for {ticker}: {total_records} records loaded")
            
        except Exception as e:
            etl_results['status'] = 'failed'
            etl_results['error'] = str(e)
            etl_results['pipeline_end'] = datetime.utcnow().isoformat()
            self.logger.error(f"Financial ETL pipeline failed for {ticker}: {e}")
            raise e
        
        return etl_results
    
    def run_price_etl(self, ticker: str, start_date, end_date=None) -> Dict[str, Any]:
        """
        Run complete price data ETL pipeline.
        
        Args:
            ticker: Stock ticker symbol
            start_date: Start date for price data
            end_date: End date for price data (optional)
            
        Returns:
            ETL results with statistics from each phase
        """
        self.logger.info(f"Starting price ETL pipeline for {ticker}")
        
        etl_results = {
            'ticker': ticker,
            'pipeline_start': datetime.utcnow().isoformat(),
            'phases': {}
        }
        
        try:
            # EXTRACT phase
            self.logger.info(f"Extract phase: extracting raw price data for {ticker}")
            raw_price_data = self.price_extractor.extract_price_data(ticker, start_date, end_date)
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
            
            etl_results['pipeline_end'] = datetime.utcnow().isoformat()
            etl_results['status'] = 'completed'
            
            self.logger.info(
                f"Price ETL pipeline completed for {ticker}: "
                f"{load_results.get('loaded_records', 0)} records loaded"
            )
            
        except Exception as e:
            etl_results['status'] = 'failed'
            etl_results['error'] = str(e)
            etl_results['pipeline_end'] = datetime.utcnow().isoformat()
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
        indicator_id = transformed_data.get('indicator_id')
        source = transformed_data.get('source')
        
        self.logger.info(f"Loading economic data for {source}/{indicator_id}")
        
        loading_results = {
            'indicator_id': indicator_id,
            'source': source,
            'loading_timestamp': datetime.utcnow().isoformat(),
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
            self.logger.info(f"Successfully loaded {total_loaded} records for {source}/{indicator_id}")
            
        except Exception as e:
            error_msg = f"Failed to load economic data for {source}/{indicator_id}: {str(e)}"
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
    
    def run_eurostat_etl(self, data_code: str, from_date: str) -> Dict[str, Any]:
        """
        Run complete Eurostat data ETL pipeline.
        
        Args:
            data_code: Eurostat dataset code
            from_date: Start date for data
            
        Returns:
            ETL results with statistics from each phase
        """
        self.logger.info(f"Starting Eurostat ETL pipeline for {data_code}")
        
        etl_results = {
            'source': 'eurostat',
            'data_code': data_code,
            'pipeline_start': datetime.utcnow().isoformat(),
            'phases': {}
        }
        
        try:
            # EXTRACT phase
            self.logger.info(f"Extract phase: extracting Eurostat data for {data_code}")
            raw_data = self.extractor.extract_eurostat_data(data_code, from_date)
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
            load_results = self.loader.load_economic_data(transformed_data)
            etl_results['phases']['load'] = {
                'status': 'completed',
                'loaded_records': load_results.get('loaded_records', {}),
                'errors': load_results.get('errors', []),
                'timestamp': load_results.get('loading_timestamp')
            }
            
            etl_results['pipeline_end'] = datetime.utcnow().isoformat()
            etl_results['status'] = 'completed'
            
            self.logger.info(f"Eurostat ETL pipeline completed for {data_code}")
            
        except Exception as e:
            etl_results['status'] = 'failed'
            etl_results['error'] = str(e)
            etl_results['pipeline_end'] = datetime.utcnow().isoformat()
            self.logger.error(f"Eurostat ETL pipeline failed for {data_code}: {e}")
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
            'pipeline_start': datetime.utcnow().isoformat(),
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
            
            etl_results['pipeline_end'] = datetime.utcnow().isoformat()
            etl_results['status'] = 'completed'
            
            self.logger.info(f"ECB ETL pipeline completed for {indicator_id}")
            
        except Exception as e:
            etl_results['status'] = 'failed'
            etl_results['error'] = str(e)
            etl_results['pipeline_end'] = datetime.utcnow().isoformat()
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
            'pipeline_start': datetime.utcnow().isoformat(),
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
            etl_results['phases']['transform'] = {
                'status': 'completed',
                'data_points_count': len(transformed_data.get('data_points', [])),
                'timestamp': transformed_data.get('transformation_timestamp')
            }
            
            # LOAD phase
            self.logger.info(f"Load phase: loading FRED data for {series_id}")
            load_results = self.loader.load_economic_data(transformed_data)
            etl_results['phases']['load'] = {
                'status': 'completed',
                'loaded_records': load_results.get('loaded_records', {}),
                'errors': load_results.get('errors', []),
                'timestamp': load_results.get('loading_timestamp')
            }
            
            etl_results['pipeline_end'] = datetime.utcnow().isoformat()
            etl_results['status'] = 'completed'
            
            self.logger.info(f"FRED ETL pipeline completed for {series_id}")
            
        except Exception as e:
            etl_results['status'] = 'failed'
            etl_results['error'] = str(e)
            etl_results['pipeline_end'] = datetime.utcnow().isoformat()
            self.logger.error(f"FRED ETL pipeline failed for {series_id}: {e}")
            raise e
        
        return etl_results