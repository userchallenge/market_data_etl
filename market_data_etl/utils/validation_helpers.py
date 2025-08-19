"""
Unified validation utilities to eliminate code duplication.

This module provides common validation patterns used throughout
CLI commands and other components, reducing code duplication.
"""

from datetime import date
from typing import Tuple, Optional
from dataclasses import dataclass

from .validation import validate_ticker, validate_date_string, validate_date_range, validate_years_parameter


@dataclass
class ValidationResult:
    """Result of a validation operation."""
    ticker: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    years: Optional[int] = None


def validate_ticker_and_date_range(
    ticker: str,
    from_date: str,
    to_date: Optional[str] = None,
    default_end_date: Optional[date] = None
) -> ValidationResult:
    """
    Validate ticker and date range - the most common validation pattern.
    
    This pattern is repeated 5+ times across CLI commands:
    - validate_ticker(ticker)
    - validate_date_string(from_date, "from_date")  
    - validate_date_string(to_date, "to_date") or default
    - validate_date_range(start_date, end_date)
    
    Args:
        ticker: Ticker symbol to validate
        from_date: Start date string in YYYY-MM-DD format
        to_date: End date string in YYYY-MM-DD format (optional)
        default_end_date: Default end date if to_date not provided (default: today)
        
    Returns:
        ValidationResult with validated ticker, start_date, and end_date
        
    Raises:
        ValidationError: If any validation fails
        
    Usage:
        result = validate_ticker_and_date_range(
            ticker="AAPL",
            from_date="2024-01-01",
            to_date="2024-12-31"
        )
        print(f"Validated: {result.ticker} from {result.start_date} to {result.end_date}")
    """
    # Validate ticker
    validated_ticker = validate_ticker(ticker)
    
    # Validate dates
    start_date = validate_date_string(from_date, "from_date")
    end_date = (
        validate_date_string(to_date, "to_date") if to_date 
        else default_end_date or date.today()
    )
    
    # Validate date range
    validate_date_range(start_date, end_date)
    
    return ValidationResult(
        ticker=validated_ticker,
        start_date=start_date,
        end_date=end_date
    )


def validate_ticker_only(ticker: str) -> ValidationResult:
    """
    Validate ticker only - another common pattern.
    
    This pattern is repeated 8+ times across CLI commands.
    
    Args:
        ticker: Ticker symbol to validate
        
    Returns:
        ValidationResult with validated ticker
        
    Raises:
        ValidationError: If ticker validation fails
        
    Usage:
        result = validate_ticker_only("AAPL")
        print(f"Validated ticker: {result.ticker}")
    """
    validated_ticker = validate_ticker(ticker)
    
    return ValidationResult(ticker=validated_ticker)


def validate_ticker_and_years(
    ticker: str, 
    years: int,
    min_years: int = 1,
    max_years: int = 20
) -> ValidationResult:
    """
    Validate ticker and years parameter.
    
    Args:
        ticker: Ticker symbol to validate
        years: Number of years to validate
        min_years: Minimum allowed years
        max_years: Maximum allowed years
        
    Returns:
        ValidationResult with validated ticker and years
        
    Raises:
        ValidationError: If validation fails
        
    Usage:
        result = validate_ticker_and_years("AAPL", 5)
        print(f"Validated: {result.ticker} for {result.years} years")
    """
    validated_ticker = validate_ticker(ticker)
    validate_years_parameter(years, min_years, max_years)
    
    return ValidationResult(
        ticker=validated_ticker,
        years=years
    )


def validate_portfolio_ticker_and_date_range(
    tickers: list[str],
    from_date: Optional[str] = None,
    to_date: Optional[str] = None
) -> Tuple[list[str], Optional[date], Optional[date]]:
    """
    Validate multiple tickers and optional date range for portfolio operations.
    
    Args:
        tickers: List of ticker symbols to validate
        from_date: Optional start date string
        to_date: Optional end date string
        
    Returns:
        Tuple of (validated_tickers, start_date, end_date)
        
    Raises:
        ValidationError: If any validation fails
        
    Usage:
        tickers, start, end = validate_portfolio_ticker_and_date_range(
            ["AAPL", "GOOGL"], 
            "2024-01-01", 
            "2024-12-31"
        )
    """
    # Validate all tickers
    validated_tickers = []
    for ticker in tickers:
        validated_tickers.append(validate_ticker(ticker))
    
    # Validate dates if provided
    start_date = None
    end_date = None
    
    if from_date:
        start_date = validate_date_string(from_date, "from_date")
        
    if to_date:
        end_date = validate_date_string(to_date, "to_date")
    elif start_date:
        # If start_date provided but not end_date, default to today
        end_date = date.today()
    
    # Validate date range if both dates exist
    if start_date and end_date:
        validate_date_range(start_date, end_date)
    
    return validated_tickers, start_date, end_date


class ValidationContext:
    """
    Context manager for batch validation operations.
    
    Collects all validation errors and provides a summary,
    useful for operations that validate multiple items.
    """
    
    def __init__(self):
        self.errors = []
        self.validated_items = []
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # If there were validation errors collected, raise summary
        if self.errors:
            error_summary = "\n".join([f"- {error}" for error in self.errors])
            from .exceptions import ValidationError
            raise ValidationError(f"Multiple validation errors:\n{error_summary}")
    
    def safe_validate_ticker(self, ticker: str) -> Optional[str]:
        """Safely validate a ticker, collecting errors instead of raising."""
        try:
            validated = validate_ticker(ticker)
            self.validated_items.append(validated)
            return validated
        except Exception as e:
            self.errors.append(f"Invalid ticker '{ticker}': {e}")
            return None
    
    def safe_validate_date(self, date_string: str, field_name: str) -> Optional[date]:
        """Safely validate a date, collecting errors instead of raising."""
        try:
            validated = validate_date_string(date_string, field_name)
            return validated
        except Exception as e:
            self.errors.append(f"Invalid {field_name} '{date_string}': {e}")
            return None
    
    def has_errors(self) -> bool:
        """Check if any validation errors were collected."""
        return len(self.errors) > 0
    
    def get_validated_items(self) -> list:
        """Get all successfully validated items."""
        return self.validated_items.copy()


# Convenience functions for common validation scenarios
def validate_price_fetch_params(
    ticker: str,
    from_date: str,
    to_date: Optional[str] = None
) -> ValidationResult:
    """Validate parameters for price fetching operations."""
    return validate_ticker_and_date_range(
        ticker=ticker,
        from_date=from_date,
        to_date=to_date,
        default_end_date=date.today()
    )


def validate_financial_fetch_params(ticker: str) -> ValidationResult:
    """Validate parameters for financial data fetching operations."""
    return validate_ticker_only(ticker)


def validate_summary_params(ticker: str, years: int = 5) -> ValidationResult:
    """Validate parameters for financial summary operations."""
    return validate_ticker_and_years(ticker, years)


def validate_portfolio_fetch_params(
    tickers: list[str],
    from_date: Optional[str] = None,
    to_date: Optional[str] = None
) -> Tuple[list[str], Optional[date], Optional[date]]:
    """Validate parameters for portfolio operations."""
    return validate_portfolio_ticker_and_date_range(tickers, from_date, to_date)