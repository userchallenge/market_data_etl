"""
Unified error handling utilities to eliminate code duplication.

This module provides consistent error handling patterns used throughout
the application, reducing code duplication and improving maintainability.
"""

import logging
from typing import Callable, Any, Optional, Dict, Type, Union
from functools import wraps
import requests

from ..utils.exceptions import YahooFinanceError, ValidationError
from ..utils.logging import get_logger

logger = get_logger(__name__)

# Exit codes
SUCCESS_EXIT_CODE = 0
ERROR_EXIT_CODE = 1


def handle_api_request_errors(source_name: str):
    """
    Decorator for unified API request error handling.
    
    Handles common patterns:
    - requests.exceptions.RequestException
    - JSON decode errors 
    - Generic exceptions with proper error propagation
    
    Args:
        source_name: Name of the data source for error messages (e.g., "Eurostat", "ECB", "FRED")
        
    Usage:
        @handle_api_request_errors("FRED")
        def fetch_data(self):
            response = requests.get(url)
            return response.json()
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except requests.exceptions.RequestException as e:
                raise YahooFinanceError(f"{source_name} API request failed: {e}")
            except Exception as e:
                if "JSONDecodeError" in str(type(e)):
                    raise YahooFinanceError(f"{source_name} JSON decode error: {e}")
                raise e
        return wrapper
    return decorator


def handle_cli_command_errors(command_name: str):
    """
    Decorator for unified CLI command error handling.
    
    Handles common patterns:
    - ValidationError → print error + return ERROR_EXIT_CODE
    - YahooFinanceError → print specific error + return ERROR_EXIT_CODE  
    - Generic Exception → log + print + return ERROR_EXIT_CODE
    
    Args:
        command_name: Name of the CLI command for logging
        
    Usage:
        @handle_cli_command_errors("fetch_prices_command")
        def fetch_prices_command(ticker: str) -> int:
            # Command implementation
            return SUCCESS_EXIT_CODE
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except ValidationError as e:
                logger.error(f"Validation error: {e}")
                return ERROR_EXIT_CODE
            except YahooFinanceError as e:
                logger.error(f"Yahoo Finance error: {str(e)}")
                return ERROR_EXIT_CODE
            except Exception as e:
                logger.error(f"Unexpected error in {command_name}: {e}", exc_info=True)
                return ERROR_EXIT_CODE
        return wrapper
    return decorator


def handle_database_errors(operation_name: str):
    """
    Decorator for unified database operation error handling.
    
    Handles common patterns:
    - Database-specific exceptions
    - Generic exceptions with proper logging
    - Consistent error propagation
    
    Args:
        operation_name: Name of the database operation for logging
        
    Usage:
        @handle_database_errors("store_price_data")
        def store_price_data(self, data):
            # Database operation
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Get logger from the class instance if available
                instance = args[0] if args else None
                if hasattr(instance, 'logger'):
                    instance.logger.error(f"Error in {operation_name}: {e}", exc_info=True)
                else:
                    logger.error(f"Error in {operation_name}: {e}", exc_info=True)
                raise
        return wrapper
    return decorator


def safe_execute(
    operation: Callable,
    error_message: str = "Operation failed",
    default_return: Any = None,
    log_errors: bool = True
) -> Any:
    """
    Safely execute an operation with consistent error handling.
    
    Args:
        operation: Function to execute
        error_message: Custom error message for logging
        default_return: Value to return if operation fails
        log_errors: Whether to log errors
        
    Returns:
        Operation result or default_return on failure
        
    Usage:
        result = safe_execute(
            lambda: risky_operation(),
            error_message="Failed to process data",
            default_return=[]
        )
    """
    try:
        return operation()
    except Exception as e:
        if log_errors:
            logger.error(f"{error_message}: {e}", exc_info=True)
        return default_return


def create_error_context(
    operation: str,
    details: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Create consistent error context information.
    
    Args:
        operation: Name of the operation that failed
        details: Additional context details
        
    Returns:
        Dictionary with error context information
        
    Usage:
        error_context = create_error_context("fetch_data", {"ticker": "AAPL"})
    """
    context = {
        "operation": operation,
        "timestamp": logger.handlers[0].formatter.formatTime(logging.LogRecord(
            name="", level=0, pathname="", lineno=0, msg="", args=(), exc_info=None
        )) if logger.handlers else None
    }
    
    if details:
        context.update(details)
    
    return context


class ErrorAggregator:
    """
    Aggregate errors from batch operations.
    
    Useful for operations that process multiple items and need to
    collect all errors rather than failing on the first one.
    """
    
    def __init__(self):
        self.errors: List[Dict[str, Any]] = []
    
    def add_error(self, operation: str, error: Exception, context: Optional[Dict[str, Any]] = None):
        """Add an error to the collection."""
        error_info = {
            "operation": operation,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "context": context or {}
        }
        self.errors.append(error_info)
    
    def has_errors(self) -> bool:
        """Check if any errors were collected."""
        return len(self.errors) > 0
    
    def get_error_summary(self) -> str:
        """Get a summary of all collected errors."""
        if not self.errors:
            return "No errors"
        
        summary = f"Collected {len(self.errors)} errors:\n"
        for i, error in enumerate(self.errors, 1):
            summary += f"  {i}. {error['operation']}: {error['error_message']}\n"
        
        return summary.strip()
    
    def clear(self):
        """Clear all collected errors."""
        self.errors.clear()