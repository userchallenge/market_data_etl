"""
Validation utilities for market data ETL operations.

This module provides reusable validation functions to ensure
data integrity and consistency across the application.
"""

import re
from datetime import date, datetime
from typing import List, Optional

from .exceptions import ValidationError


# Constants
MIN_TICKER_LENGTH = 1
MAX_TICKER_LENGTH = 20
VALID_TICKER_CHARS = r'^[A-Z0-9.-]+$'


def validate_ticker(ticker: str) -> str:
    """
    Validate and normalize ticker format.
    
    Args:
        ticker: Ticker symbol to validate
        
    Returns:
        Validated and normalized ticker symbol
        
    Raises:
        ValidationError: If ticker format is invalid
    """
    if not ticker or not ticker.strip():
        raise ValidationError("Ticker cannot be empty")
    
    ticker = ticker.strip().upper()
    
    # Check length
    if len(ticker) < MIN_TICKER_LENGTH or len(ticker) > MAX_TICKER_LENGTH:
        raise ValidationError(f"Ticker length must be between {MIN_TICKER_LENGTH} and {MAX_TICKER_LENGTH} characters")
    
    # Check format - allow alphanumeric and common ticker separators
    if not re.match(VALID_TICKER_CHARS, ticker):
        raise ValidationError(f"Invalid ticker format: {ticker}. Only letters, numbers, dots, and hyphens are allowed")
    
    return ticker


def validate_date_string(date_string: str, field_name: str = "date") -> date:
    """
    Parse and validate date string in YYYY-MM-DD format.
    
    Args:
        date_string: Date string to parse
        field_name: Name of the field for error messages
        
    Returns:
        Parsed date object
        
    Raises:
        ValidationError: If date format is invalid or date is unreasonable
    """
    if not date_string or not date_string.strip():
        raise ValidationError(f"{field_name} cannot be empty")
    
    try:
        parsed_date = datetime.strptime(date_string.strip(), '%Y-%m-%d').date()
    except ValueError:
        raise ValidationError(f"Invalid {field_name} format: {date_string}. Expected YYYY-MM-DD format")
    
    # Validate date is reasonable (not too far in past/future)
    today = date.today()
    min_date = date(1900, 1, 1)
    max_date = date(today.year + 1, 12, 31)
    
    if parsed_date < min_date or parsed_date > max_date:
        raise ValidationError(f"{field_name} must be between {min_date} and {max_date}")
    
    return parsed_date


def validate_date_range(start_date: date, end_date: date) -> None:
    """
    Validate that date range is logical.
    
    Args:
        start_date: Start date of range
        end_date: End date of range
        
    Raises:
        ValidationError: If date range is invalid
    """
    if start_date > end_date:
        raise ValidationError("Start date cannot be after end date")
    
    if end_date > date.today():
        raise ValidationError("End date cannot be in the future")
    
    # Check for reasonable range (not too large)
    max_range_days = 3650  # ~10 years
    if (end_date - start_date).days > max_range_days:
        raise ValidationError(f"Date range cannot exceed {max_range_days} days")


def validate_years_parameter(years: int, min_years: int = 1, max_years: int = 20) -> None:
    """
    Validate years parameter for financial summaries.
    
    Args:
        years: Number of years to validate
        min_years: Minimum allowed years
        max_years: Maximum allowed years
        
    Raises:
        ValidationError: If years parameter is invalid
    """
    if not isinstance(years, int):
        raise ValidationError("Years must be an integer")
    
    if years < min_years or years > max_years:
        raise ValidationError(f"Years must be between {min_years} and {max_years}")


def sanitize_sql_input(value: str) -> str:
    """
    Sanitize input to prevent SQL injection.
    
    Args:
        value: Input string to sanitize
        
    Returns:
        Sanitized string safe for database operations
    """
    if not isinstance(value, str):
        return str(value)
    
    # Remove or escape potentially dangerous characters
    # Note: This is additional protection - SQLAlchemy ORM provides primary protection
    dangerous_chars = ["'", '"', ';', '--', '/*', '*/']
    sanitized = value
    
    for char in dangerous_chars:
        sanitized = sanitized.replace(char, '')
    
    return sanitized.strip()


def validate_currency_code(currency: str) -> str:
    """
    Validate currency code format.
    
    Args:
        currency: Currency code to validate
        
    Returns:
        Validated currency code
        
    Raises:
        ValidationError: If currency code is invalid
    """
    if not currency or not currency.strip():
        return 'USD'  # Default currency
    
    currency = currency.strip().upper()
    
    # Basic validation - should be 3 uppercase letters
    if not re.match(r'^[A-Z]{3}$', currency):
        raise ValidationError(f"Invalid currency code: {currency}. Must be 3 uppercase letters")
    
    return currency