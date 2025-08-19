"""
Configuration management for the market data ETL package.

This module provides configuration settings and environment
variable handling for the application.
"""

import os
from pathlib import Path
from typing import Optional, Dict, Any
from dataclasses import dataclass
import logging


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
class APIConfig:
    """API configuration settings."""
    fred_api_key: Optional[str] = None
    eurostat_base_url: str = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
    ecb_base_url: str = "https://sdw-wsrest.ecb.europa.eu/service/data"
    fred_base_url: str = "https://api.stlouisfed.org/fred"
    
    @classmethod
    def from_env(cls) -> "APIConfig":
        """Create config from environment variables."""
        return cls(
            fred_api_key=os.getenv("FRED_API_KEY"),
            eurostat_base_url=os.getenv("EUROSTAT_BASE_URL", cls.eurostat_base_url),
            ecb_base_url=os.getenv("ECB_BASE_URL", cls.ecb_base_url),
            fred_base_url=os.getenv("FRED_BASE_URL", cls.fred_base_url)
        )


@dataclass
class Config:
    """Main configuration class."""
    database: DatabaseConfig
    retry: RetryConfig
    api: APIConfig
    log_level: str = "INFO"
    log_file: Optional[str] = None
    economic_indicators: Optional[Dict[str, Any]] = None
    
    @classmethod
    def from_env(cls) -> "Config":
        """Create config from environment variables (backward compatibility)."""
        return cls(
            database=DatabaseConfig.from_env(),
            retry=RetryConfig.from_env(),
            api=APIConfig.from_env(),
            log_level=os.getenv("MARKET_DATA_LOG_LEVEL", "INFO"),
            log_file=os.getenv("MARKET_DATA_LOG_FILE")
        )
    
    @classmethod
    def default(cls) -> "Config":
        """Create default configuration."""
        return cls(
            database=DatabaseConfig(),
            retry=RetryConfig(),
            api=APIConfig()
        )
    
    @classmethod
    def load(cls) -> "Config":
        """
        Load configuration from YAML files (required).
        
        Environment variables can still override individual settings,
        but YAML configuration files must be present for economic indicators
        and other structured configuration.
        """
        try:
            # Load from YAML files (required)
            yaml_config = cls._try_load_yaml_config()
            if yaml_config:
                return yaml_config
            else:
                # YAML config files not found - this is now required
                logger = logging.getLogger(__name__)
                logger.error("YAML configuration files are required but not found")
                logger.error("Expected config files:")
                logger.error("  - config/app_config.yaml (or config/app_config.yml)")
                logger.error("  - config/economic_indicators.yaml")
                raise FileNotFoundError(
                    "YAML configuration files are required. "
                    "Please ensure config/app_config.yaml and config/economic_indicators.yaml exist."
                )
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to load required YAML configuration: {e}")
            raise
    
    @classmethod
    def _try_load_yaml_config(cls) -> Optional["Config"]:
        """Try to load configuration from YAML files."""
        try:
            import yaml
        except ImportError:
            # yaml not available, fall back to env config
            return None
        
        # Look for config files in standard locations
        config_paths = [
            Path("config/app_config.yaml"),
            Path("config/app_config.yml"),
            Path("app_config.yaml"),
            Path("app_config.yml")
        ]
        
        app_config_path = None
        for path in config_paths:
            if path.exists():
                app_config_path = path
                break
        
        if not app_config_path:
            return None
        
        # Load main app config
        with open(app_config_path, 'r') as f:
            app_config = yaml.safe_load(f)
        
        # Load economic indicators config
        indicators_config_path = app_config_path.parent / "economic_indicators.yaml"
        economic_indicators = None
        if indicators_config_path.exists():
            with open(indicators_config_path, 'r') as f:
                indicators_data = yaml.safe_load(f)
                economic_indicators = indicators_data.get('indicators', {})
        
        # Create config objects from YAML data with env var overrides
        database_config = DatabaseConfig(
            path=os.getenv("MARKET_DATA_DB_PATH", app_config.get('database', {}).get('path', 'market_data.db')),
            echo=os.getenv("MARKET_DATA_DB_ECHO", str(app_config.get('database', {}).get('echo', False))).lower() == 'true'
        )
        
        retry_config = RetryConfig(
            max_retries=int(os.getenv("MARKET_DATA_MAX_RETRIES", str(app_config.get('retry', {}).get('max_retries', 5)))),
            initial_backoff=float(os.getenv("MARKET_DATA_INITIAL_BACKOFF", str(app_config.get('retry', {}).get('initial_backoff', 1.0)))),
            backoff_multiplier=float(os.getenv("MARKET_DATA_BACKOFF_MULTIPLIER", str(app_config.get('retry', {}).get('backoff_multiplier', 2.0))))
        )
        
        api_endpoints = app_config.get('api_endpoints', {})
        api_config = APIConfig(
            fred_api_key=os.getenv("FRED_API_KEY"),  # Always from env for security
            eurostat_base_url=os.getenv("EUROSTAT_BASE_URL", api_endpoints.get('eurostat', APIConfig.eurostat_base_url)),
            ecb_base_url=os.getenv("ECB_BASE_URL", api_endpoints.get('ecb', APIConfig.ecb_base_url)),
            fred_base_url=os.getenv("FRED_BASE_URL", api_endpoints.get('fred', APIConfig.fred_base_url))
        )
        
        return cls(
            database=database_config,
            retry=retry_config,
            api=api_config,
            log_level=os.getenv("MARKET_DATA_LOG_LEVEL", app_config.get('logging', {}).get('level', 'INFO')),
            log_file=os.getenv("MARKET_DATA_LOG_FILE", app_config.get('logging', {}).get('file')),
            economic_indicators=economic_indicators
        )
    
    def get_economic_indicator_config(self, indicator_name: str) -> Optional[Dict[str, Any]]:
        """
        Get configuration for a specific economic indicator.
        
        Args:
            indicator_name: Name of the economic indicator
            
        Returns:
            Dictionary with indicator configuration or None if not found
        """
        if self.economic_indicators and indicator_name in self.economic_indicators:
            return self.economic_indicators[indicator_name]
        return None
    
    def list_economic_indicators(self) -> list[str]:
        """Get list of all available economic indicators."""
        if self.economic_indicators:
            return list(self.economic_indicators.keys())
        return []


# Global configuration instance with YAML support and backward compatibility
config = Config.load()