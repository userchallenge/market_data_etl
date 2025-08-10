"""
Configuration management for the market data ETL package.

This module provides configuration settings and environment
variable handling for the application.
"""

import os
from pathlib import Path
from typing import Optional
from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    path: str = "market_data.db"
    echo: bool = False
    
    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """Create config from environment variables."""
        return cls(
            path=os.getenv("MARKET_DATA_DB_PATH", "market_data.db"),
            echo=os.getenv("MARKET_DATA_DB_ECHO", "false").lower() == "true"
        )


@dataclass
class RetryConfig:
    """Retry configuration for API calls."""
    max_retries: int = 5
    initial_backoff: float = 1.0
    backoff_multiplier: float = 2.0
    
    @classmethod
    def from_env(cls) -> "RetryConfig":
        """Create config from environment variables."""
        return cls(
            max_retries=int(os.getenv("MARKET_DATA_MAX_RETRIES", "5")),
            initial_backoff=float(os.getenv("MARKET_DATA_INITIAL_BACKOFF", "1.0")),
            backoff_multiplier=float(os.getenv("MARKET_DATA_BACKOFF_MULTIPLIER", "2.0"))
        )


@dataclass
class Config:
    """Main configuration class."""
    database: DatabaseConfig
    retry: RetryConfig
    log_level: str = "INFO"
    log_file: Optional[str] = None
    
    @classmethod
    def from_env(cls) -> "Config":
        """Create config from environment variables."""
        return cls(
            database=DatabaseConfig.from_env(),
            retry=RetryConfig.from_env(),
            log_level=os.getenv("MARKET_DATA_LOG_LEVEL", "INFO"),
            log_file=os.getenv("MARKET_DATA_LOG_FILE")
        )
    
    @classmethod
    def default(cls) -> "Config":
        """Create default configuration."""
        return cls(
            database=DatabaseConfig(),
            retry=RetryConfig()
        )


# Global configuration instance
config = Config.from_env()