"""
Unified data transformation utilities to eliminate code duplication.

This module provides common transformation patterns used throughout
the ETL pipeline, reducing code duplication and improving consistency.
"""

from datetime import datetime, date
from typing import Dict, Any, Optional, Union, List
import logging

from ..utils.logging import get_logger

logger = get_logger(__name__)


def create_timestamp() -> str:
    """
    Create standardized timestamp string.
    
    This replaces the 30+ occurrences of datetime.utcnow().isoformat()
    throughout the codebase with a single, consistent implementation.
    
    Returns:
        ISO formatted timestamp string
        
    Usage:
        timestamp = create_timestamp()
        # Returns: "2024-08-19T15:30:45.123456"
    """
    return datetime.utcnow().isoformat()


def create_extraction_metadata(
    source: str,
    operation: str,
    **additional_fields
) -> Dict[str, Any]:
    """
    Create standardized extraction metadata structure.
    
    This consolidates the repeated pattern of creating metadata
    dictionaries with timestamps and source information.
    
    Args:
        source: Data source name (e.g., "yahoo", "eurostat", "ecb", "fred")
        operation: Operation being performed (e.g., "fetch_prices", "fetch_economic")
        **additional_fields: Additional metadata fields
        
    Returns:
        Dictionary with standardized metadata structure
        
    Usage:
        metadata = create_extraction_metadata(
            source="fred",
            operation="fetch_economic",
            series_id="UNRATE",
            url=url
        )
    """
    metadata = {
        'source': source,
        'operation': operation,
        'extraction_timestamp': create_timestamp()
    }
    
    # Add any additional fields provided
    metadata.update(additional_fields)
    
    return metadata


def create_transformation_metadata(
    transformer: str,
    record_count: int,
    **additional_fields
) -> Dict[str, Any]:
    """
    Create standardized transformation metadata structure.
    
    Args:
        transformer: Name of the transformer class
        record_count: Number of records processed
        **additional_fields: Additional metadata fields
        
    Returns:
        Dictionary with transformation metadata
        
    Usage:
        metadata = create_transformation_metadata(
            transformer="EconomicDataTransformer",
            record_count=15,
            indicator_name="unemployment_rate"
        )
    """
    metadata = {
        'transformer': transformer,
        'transformation_timestamp': create_timestamp(),
        'record_count': record_count
    }
    
    metadata.update(additional_fields)
    return metadata


def create_loading_metadata(
    loader: str,
    loaded_records: int,
    **additional_fields
) -> Dict[str, Any]:
    """
    Create standardized loading metadata structure.
    
    Args:
        loader: Name of the loader class
        loaded_records: Number of records loaded
        **additional_fields: Additional metadata fields
        
    Returns:
        Dictionary with loading metadata
        
    Usage:
        metadata = create_loading_metadata(
            loader="EconomicDataLoader",
            loaded_records=10,
            target_table="economic_indicator_data"
        )
    """
    metadata = {
        'loader': loader,
        'loading_timestamp': create_timestamp(),
        'loaded_records': loaded_records
    }
    
    metadata.update(additional_fields)
    return metadata


def create_pipeline_metadata(
    pipeline_name: str,
    status: str = "started",
    **additional_fields
) -> Dict[str, Any]:
    """
    Create standardized ETL pipeline metadata structure.
    
    Args:
        pipeline_name: Name of the ETL pipeline
        status: Pipeline status ("started", "completed", "failed")
        **additional_fields: Additional metadata fields
        
    Returns:
        Dictionary with pipeline metadata
        
    Usage:
        metadata = create_pipeline_metadata(
            pipeline_name="price_etl",
            status="started",
            ticker="AAPL"
        )
    """
    metadata = {
        'pipeline': pipeline_name,
        'status': status,
        'pipeline_start': create_timestamp()
    }
    
    if status in ['completed', 'failed']:
        metadata['pipeline_end'] = create_timestamp()
    
    metadata.update(additional_fields)
    return metadata


def format_date_for_api(date_value: Union[date, str]) -> str:
    """
    Format date for API requests consistently.
    
    Args:
        date_value: Date as date object or string
        
    Returns:
        Formatted date string
        
    Usage:
        api_date = format_date_for_api(date(2024, 1, 1))
        # Returns: "2024-01-01"
    """
    if isinstance(date_value, date):
        return date_value.isoformat()
    elif isinstance(date_value, str):
        return date_value  # Assume already formatted
    else:
        raise ValueError(f"Unsupported date type: {type(date_value)}")


def safe_parse_date(date_string: str, format_string: str = "%Y-%m-%d") -> Optional[date]:
    """
    Safely parse date string with consistent error handling.
    
    Args:
        date_string: Date string to parse
        format_string: Expected date format
        
    Returns:
        Parsed date object or None if parsing fails
        
    Usage:
        parsed_date = safe_parse_date("2024-01-01")
        if parsed_date:
            print(f"Parsed: {parsed_date}")
    """
    try:
        return datetime.strptime(date_string.strip(), format_string).date()
    except (ValueError, AttributeError) as e:
        logger.debug(f"Failed to parse date '{date_string}' with format '{format_string}': {e}")
        return None


def safe_parse_numeric(value: Any, default: Optional[float] = None) -> Optional[float]:
    """
    Safely parse numeric value with consistent error handling.
    
    Args:
        value: Value to parse as numeric
        default: Default value if parsing fails
        
    Returns:
        Parsed numeric value or default
        
    Usage:
        price = safe_parse_numeric(price_string, 0.0)
    """
    if value is None or (isinstance(value, str) and value.strip() in ['', '.', 'N/A']):
        return default
    
    try:
        return float(value)
    except (ValueError, TypeError) as e:
        logger.debug(f"Failed to parse numeric value '{value}': {e}")
        return default


def normalize_ticker_symbol(ticker: str) -> str:
    """
    Normalize ticker symbol consistently.
    
    Args:
        ticker: Raw ticker symbol
        
    Returns:
        Normalized ticker symbol
        
    Usage:
        normalized = normalize_ticker_symbol(" aapl ")
        # Returns: "AAPL"
    """
    return ticker.strip().upper()


def create_data_point(
    date_value: Union[date, str],
    value: Union[float, str, None],
    **additional_fields
) -> Dict[str, Any]:
    """
    Create standardized data point structure.
    
    This consolidates the repeated pattern of creating data point
    dictionaries with date and value information.
    
    Args:
        date_value: Date of the data point
        value: Value of the data point
        **additional_fields: Additional fields for the data point
        
    Returns:
        Dictionary with standardized data point structure
        
    Usage:
        data_point = create_data_point(
            date_value=date(2024, 1, 1),
            value=3.8,
            unit="percent"
        )
    """
    data_point = {
        'date': format_date_for_api(date_value),
        'value': safe_parse_numeric(value)
    }
    
    data_point.update(additional_fields)
    return data_point


def extract_api_response_data(
    response_data: Dict[str, Any],
    data_path: List[str],
    default: Any = None
) -> Any:
    """
    Safely extract data from nested API response structure.
    
    Args:
        response_data: API response dictionary
        data_path: List of keys to navigate to the data
        default: Default value if path doesn't exist
        
    Returns:
        Extracted data or default value
        
    Usage:
        observations = extract_api_response_data(
            fred_response,
            ['observations'],
            default=[]
        )
    """
    current = response_data
    
    for key in data_path:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            logger.debug(f"Key '{key}' not found in API response at path {data_path}")
            return default
    
    return current


class DataTransformationContext:
    """
    Context manager for data transformation operations.
    
    Provides consistent logging and error tracking for transformation operations.
    """
    
    def __init__(self, transformer_name: str, operation: str):
        self.transformer_name = transformer_name
        self.operation = operation
        self.start_time = None
        self.records_processed = 0
        self.errors = []
    
    def __enter__(self):
        self.start_time = datetime.utcnow()
        logger.info(f"{self.transformer_name}: Starting {self.operation}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = datetime.utcnow() - self.start_time
        
        if exc_type is None:
            logger.info(f"{self.transformer_name}: Completed {self.operation} - {self.records_processed} records in {duration.total_seconds():.2f}s")
        else:
            logger.error(f"{self.transformer_name}: Failed {self.operation} after {duration.total_seconds():.2f}s - {exc_val}")
    
    def add_processed_record(self):
        """Increment the processed record counter."""
        self.records_processed += 1
    
    def add_error(self, error_msg: str):
        """Add an error to the transformation context."""
        self.errors.append(error_msg)
        logger.warning(f"{self.transformer_name}: {error_msg}")
    
    def get_summary(self) -> Dict[str, Any]:
        """Get transformation summary."""
        return {
            'transformer': self.transformer_name,
            'operation': self.operation,
            'records_processed': self.records_processed,
            'errors': len(self.errors),
            'timestamp': create_timestamp()
        }


# Convenience functions for common transformation patterns
def transform_price_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform price data with standardized metadata."""
    return {
        **create_transformation_metadata("PriceDataTransformer", len(raw_data.get('prices', []))),
        'transformed_data': raw_data
    }


def transform_financial_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform financial data with standardized metadata."""
    return {
        **create_transformation_metadata("FinancialDataTransformer", len(raw_data.get('statements', []))),
        'transformed_data': raw_data
    }


def transform_economic_data(raw_data: Dict[str, Any], indicator_name: str) -> Dict[str, Any]:
    """Transform economic data with standardized metadata."""
    return {
        **create_transformation_metadata("EconomicDataTransformer", len(raw_data.get('data_points', [])), indicator_name=indicator_name),
        'transformed_data': raw_data
    }