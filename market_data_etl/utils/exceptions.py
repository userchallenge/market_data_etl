"""
Custom exceptions for the market data ETL package.

This module defines package-specific exceptions that provide
clear error messages and help with debugging.
"""


class MarketDataETLError(Exception):
    """Base exception class for all market data ETL errors."""
    pass


class YahooFinanceError(MarketDataETLError):
    """Exception raised when Yahoo Finance API operations fail."""
    pass


class DatabaseError(MarketDataETLError):
    """Exception raised when database operations fail."""
    pass


class ValidationError(MarketDataETLError):
    """Exception raised when input validation fails."""
    pass


class ConfigurationError(MarketDataETLError):
    """Exception raised when configuration is invalid or missing."""
    pass