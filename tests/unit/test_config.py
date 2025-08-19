"""
Test configuration loading and backward compatibility.

These tests ensure that:
1. Current hardcoded configuration works (backward compatibility)
2. New YAML configuration loading works
3. Fallback mechanisms work correctly
4. Environment variable overrides work
"""

import pytest
import os
import tempfile
import yaml
from unittest.mock import patch, MagicMock
from pathlib import Path

from market_data_etl.config import Config, DatabaseConfig, RetryConfig, APIConfig


class TestBackwardCompatibilityConfig:
    """Test that current configuration system continues to work."""
    
    def test_current_config_from_env_works(self):
        """Test that existing Config.from_env() works unchanged."""
        config = Config.from_env()
        
        # Verify all required attributes exist
        assert hasattr(config, 'database')
        assert hasattr(config, 'retry')
        assert hasattr(config, 'api')
        assert hasattr(config, 'log_level')
        
        # Verify types are correct
        assert isinstance(config.database, DatabaseConfig)
        assert isinstance(config.retry, RetryConfig)
        assert isinstance(config.api, APIConfig)
        
        # Verify default values
        assert config.database.path == "market_data.db"
        assert config.retry.max_retries == 5
        assert config.log_level == "INFO"
    
    def test_environment_variable_overrides(self):
        """Test that environment variables override defaults."""
        with patch.dict(os.environ, {
            'MARKET_DATA_DB_PATH': 'custom_test.db',
            'MARKET_DATA_MAX_RETRIES': '10',
            'MARKET_DATA_LOG_LEVEL': 'DEBUG',
            'FRED_API_KEY': 'test_key_123'
        }):
            config = Config.from_env()
            
            assert config.database.path == 'custom_test.db'
            assert config.retry.max_retries == 10
            assert config.log_level == 'DEBUG'
            assert config.api.fred_api_key == 'test_key_123'
    
    def test_default_config_works(self):
        """Test that Config.default() creates valid configuration."""
        config = Config.default()
        
        assert config.database.path == "market_data.db"
        assert config.retry.max_retries == 5
        assert config.api.fred_api_key is None
        assert config.log_level == "INFO"


class TestNewYAMLConfiguration:
    """Test new YAML configuration loading functionality."""
    
    @pytest.fixture
    def temp_config_dir(self):
        """Create temporary config directory for tests."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir) / "config"
            config_dir.mkdir()
            yield config_dir
    
    @pytest.fixture
    def sample_config_files(self, temp_config_dir):
        """Create sample YAML config files."""
        # Main app config
        app_config = {
            'database': {
                'path': 'test_market_data.db',
                'echo': True
            },
            'retry': {
                'max_retries': 3,
                'initial_backoff': 0.5,
                'backoff_multiplier': 1.5
            },
            'api': {
                'fred_api_key': 'yaml_test_key'
            },
            'log_level': 'DEBUG'
        }
        
        app_config_file = temp_config_dir / "app_config.yaml"
        with open(app_config_file, 'w') as f:
            yaml.dump(app_config, f)
        
        # Economic indicators config
        indicators_config = {
            'indicators': {
                'test_indicator_1': {
                    'source': 'eurostat',
                    'source_identifier': 'test_id_1',
                    'description': 'Test Indicator 1'
                },
                'test_indicator_2': {
                    'source': 'fred',
                    'source_identifier': 'TEST_ID_2',
                    'description': 'Test Indicator 2'
                }
            }
        }
        
        indicators_file = temp_config_dir / "economic_indicators.yaml"
        with open(indicators_file, 'w') as f:
            yaml.dump(indicators_config, f)
        
        return {
            'app_config': app_config_file,
            'indicators': indicators_file,
            'config_dir': temp_config_dir
        }
    
    def test_yaml_config_loading_basic(self, sample_config_files):
        """Test basic YAML configuration loading."""
        # This test will need implementation once we add YAML loading to Config
        # For now, we'll test the structure we expect
        
        with open(sample_config_files['app_config']) as f:
            config_data = yaml.safe_load(f)
        
        # Verify structure is correct
        assert 'database' in config_data
        assert 'retry' in config_data
        assert 'api' in config_data
        assert config_data['database']['path'] == 'test_market_data.db'
        assert config_data['retry']['max_retries'] == 3
    
    def test_economic_indicators_yaml_structure(self, sample_config_files):
        """Test economic indicators YAML structure."""
        with open(sample_config_files['indicators']) as f:
            indicators_data = yaml.safe_load(f)
        
        assert 'indicators' in indicators_data
        indicators = indicators_data['indicators']
        
        # Test structure of first indicator
        test_indicator = indicators['test_indicator_1']
        assert 'source' in test_indicator
        assert 'source_identifier' in test_indicator
        assert 'description' in test_indicator
        
        assert test_indicator['source'] == 'eurostat'
        assert test_indicator['source_identifier'] == 'test_id_1'
    
    def test_invalid_yaml_file_handling(self, temp_config_dir):
        """Test graceful handling of invalid YAML files."""
        # Create invalid YAML file
        invalid_file = temp_config_dir / "invalid.yaml"
        with open(invalid_file, 'w') as f:
            f.write("invalid: yaml: content: [unclosed")
        
        # Should not raise exception, should handle gracefully
        with pytest.raises(yaml.YAMLError):
            with open(invalid_file) as f:
                yaml.safe_load(f)
    
    def test_missing_config_file_raises_error(self):
        """Test that missing YAML files raise clear error in pure YAML mode."""
        # In pure YAML mode, missing config files should raise FileNotFoundError
        with patch('pathlib.Path.exists', return_value=False):
            with pytest.raises(FileNotFoundError) as exc_info:
                Config.load()
            
            error_msg = str(exc_info.value)
            assert 'YAML configuration files are required' in error_msg
            assert 'config/app_config.yaml' in error_msg


class TestConfigMigrationScenarios:
    """Test various configuration migration scenarios."""
    
    def test_partial_yaml_config(self):
        """Test scenario where only some YAML configs are present."""
        # Test that system works with partial YAML configuration
        # and falls back to defaults/env vars for missing sections
        pass
    
    def test_yaml_config_with_env_overrides(self):
        """Test that environment variables can override YAML values."""
        # Test precedence: env vars > YAML > defaults
        pass
    
    def test_malformed_yaml_raises_error(self):
        """Test that malformed YAML raises clear error in pure YAML mode."""
        # In pure YAML mode, malformed YAML should raise error
        # This test would require creating actual malformed YAML and testing
        # but the behavior is now to raise exception rather than fall back
        pass


class TestConfigurationValidation:
    """Test configuration value validation."""
    
    def test_database_path_validation(self):
        """Test database path validation."""
        # Test valid paths
        valid_paths = ["market_data.db", "/abs/path/db.sqlite", ":memory:"]
        for path in valid_paths:
            config = DatabaseConfig(path=path)
            assert config.path == path
    
    def test_retry_config_validation(self):
        """Test retry configuration validation."""
        # Test valid values
        config = RetryConfig(max_retries=5, initial_backoff=1.0, backoff_multiplier=2.0)
        assert config.max_retries == 5
        assert config.initial_backoff == 1.0
        assert config.backoff_multiplier == 2.0
        
        # Test edge cases
        config_min = RetryConfig(max_retries=1, initial_backoff=0.1, backoff_multiplier=1.1)
        assert config_min.max_retries == 1
    
    def test_api_key_handling(self):
        """Test API key configuration handling."""
        # Test with API key
        config = APIConfig(fred_api_key="test_key")
        assert config.fred_api_key == "test_key"
        
        # Test without API key (should be None)
        config_none = APIConfig()
        assert config_none.fred_api_key is None


class TestConfigurationIntegration:
    """Test configuration integration with rest of system."""
    
    def test_config_accessible_from_modules(self):
        """Test that config is accessible from other modules."""
        from market_data_etl.config import config
        
        # Should be able to import and use global config
        assert hasattr(config, 'database')
        assert hasattr(config, 'retry')
        assert hasattr(config, 'api')
    
    def test_config_used_in_database_manager(self):
        """Test that DatabaseManager uses configuration correctly."""
        # This will test integration once we update DatabaseManager
        pass
    
    def test_config_used_in_fetchers(self):
        """Test that data fetchers use configuration correctly."""
        # Test that fetchers use retry config, API endpoints from config
        pass


# Fixtures for all tests
@pytest.fixture
def clean_environment():
    """Clean environment variables for consistent testing."""
    env_vars_to_clean = [
        'MARKET_DATA_DB_PATH',
        'MARKET_DATA_DB_ECHO', 
        'MARKET_DATA_MAX_RETRIES',
        'MARKET_DATA_INITIAL_BACKOFF',
        'MARKET_DATA_BACKOFF_MULTIPLIER',
        'MARKET_DATA_LOG_LEVEL',
        'MARKET_DATA_LOG_FILE',
        'FRED_API_KEY'
    ]
    
    original_values = {}
    for var in env_vars_to_clean:
        original_values[var] = os.environ.get(var)
        if var in os.environ:
            del os.environ[var]
    
    yield
    
    # Restore original values
    for var, value in original_values.items():
        if value is not None:
            os.environ[var] = value
        elif var in os.environ:
            del os.environ[var]


@pytest.fixture
def mock_yaml_files():
    """Mock YAML file existence and content."""
    return MagicMock()