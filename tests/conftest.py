"""
Pytest configuration and shared fixtures for market_data_etl tests.

This file contains:
- Global pytest configuration
- Shared fixtures used across multiple test modules
- Test database setup and cleanup
- Mock data generators
- Common test utilities
"""

import pytest
import tempfile
import os
from pathlib import Path
import pandas as pd
from datetime import date, datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import yaml
import json

from market_data_etl.data.models import Base
from market_data_etl.database.manager import DatabaseManager


# Global pytest configuration
def pytest_configure(config):
    """Configure pytest with custom markers and settings."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "unit: mark test as unit test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "external_api: mark test as requiring external API access"
    )


# Database fixtures
@pytest.fixture(scope="session")
def temp_db_engine():
    """Create temporary database engine for testing session."""
    engine = create_engine('sqlite:///:memory:', echo=False)
    
    # Enable foreign key constraints for test database
    from sqlalchemy import event
    @event.listens_for(engine, "connect")
    def enable_foreign_keys(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()
    
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def temp_db_session(temp_db_engine):
    """Create temporary database session for individual tests."""
    Session = sessionmaker(bind=temp_db_engine)
    session = Session()
    
    yield session
    
    # Cleanup - rollback any uncommitted changes
    session.rollback()
    session.close()


@pytest.fixture
def temp_db_file():
    """Create temporary database file for tests that need persistent storage."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        temp_path = f.name
    
    # Create database with proper schema
    engine = create_engine(f'sqlite:///{temp_path}')
    Base.metadata.create_all(engine)
    engine.dispose()
    
    yield temp_path
    
    # Cleanup
    Path(temp_path).unlink(missing_ok=True)


@pytest.fixture
def test_db_manager(temp_db_file):
    """Create DatabaseManager instance with temporary database."""
    return DatabaseManager(db_path=temp_db_file)


# Configuration fixtures
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
    
    # Store original values
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
def temp_config_dir():
    """Create temporary config directory with test configurations."""
    with tempfile.TemporaryDirectory() as temp_dir:
        config_dir = Path(temp_dir) / "config"
        config_dir.mkdir()
        
        # Create test config files
        app_config = {
            'database': {
                'path': ':memory:',
                'echo': False
            },
            'retry': {
                'max_retries': 3,
                'initial_backoff': 0.5,
                'backoff_multiplier': 1.5
            },
            'api': {
                'fred_api_key': 'test_key'
            },
            'log_level': 'DEBUG'
        }
        
        app_config_file = config_dir / "app_config.yaml"
        with open(app_config_file, 'w') as f:
            yaml.dump(app_config, f)
        
        # Create test economic indicators config
        indicators_config = {
            'indicators': {
                'test_inflation_euro': {
                    'source': 'eurostat',
                    'source_identifier': 'prc_hicp_test',
                    'description': 'Test Eurozone Inflation'
                },
                'test_unemployment_us': {
                    'source': 'fred',
                    'source_identifier': 'UNRATE_TEST',
                    'description': 'Test US Unemployment Rate'
                }
            }
        }
        
        indicators_file = config_dir / "economic_indicators.yaml"
        with open(indicators_file, 'w') as f:
            yaml.dump(indicators_config, f)
        
        yield config_dir


# Mock data fixtures
@pytest.fixture
def sample_price_data():
    """Generate sample price data for testing."""
    dates = pd.date_range('2024-01-01', periods=10, freq='D')
    
    return pd.DataFrame({
        'Open': [100.0 + i for i in range(10)],
        'High': [105.0 + i for i in range(10)],
        'Low': [95.0 + i for i in range(10)],
        'Close': [102.0 + i for i in range(10)],
        'Adj Close': [102.0 + i for i in range(10)],
        'Volume': [1000000 + i * 10000 for i in range(10)]
    }, index=dates)


@pytest.fixture
def sample_instrument_data():
    """Generate sample instrument data for testing."""
    return {
        'ticker_symbol': 'TEST',
        'instrument_name': 'Test Company Inc.',
        'instrument_type': 'stock',
        'sector': 'Technology',
        'industry': 'Software',
        'country': 'United States',
        'currency': 'USD',
        'market_cap': 1000000000,
        'employees': 10000
    }


@pytest.fixture
def sample_economic_data():
    """Generate sample economic indicator data for testing."""
    return {
        'name': 'test_inflation',
        'source': 'test_source',
        'source_identifier': 'TEST_INFLATION',
        'description': 'Test Inflation Indicator',
        'unit': 'percent',
        'frequency': 'monthly',
        'data_points': [
            {'date': '2024-01-01', 'value': 2.5},
            {'date': '2024-02-01', 'value': 2.7},
            {'date': '2024-03-01', 'value': 2.6}
        ]
    }


@pytest.fixture
def sample_portfolio_data():
    """Generate sample portfolio data for testing."""
    return {
        'name': 'Test Portfolio',
        'description': 'Portfolio for testing purposes',
        'holdings': ['AAPL', 'MSFT', 'GOOGL', 'TSLA']
    }


@pytest.fixture 
def sample_yahoo_response():
    """Mock Yahoo Finance API response data."""
    return {
        'info': {
            'longName': 'Apple Inc.',
            'shortName': 'Apple',
            'sector': 'Technology',
            'industry': 'Consumer Electronics', 
            'country': 'United States',
            'currency': 'USD',
            'marketCap': 3000000000000,
            'fullTimeEmployees': 150000,
            'exchange': 'NMS',
            'quoteType': 'EQUITY'
        },
        'history': pd.DataFrame({
            'Open': [150.0, 151.0, 152.0],
            'High': [155.0, 156.0, 157.0],
            'Low': [148.0, 149.0, 150.0],
            'Close': [153.0, 154.0, 155.0],
            'Adj Close': [153.0, 154.0, 155.0],
            'Volume': [50000000, 52000000, 48000000]
        }, index=pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']))
    }


@pytest.fixture
def sample_eurostat_response():
    """Mock Eurostat API response data."""
    return {
        'dimension': {
            'time': {
                'category': {
                    'index': {
                        '2024M01': 0,
                        '2024M02': 1,
                        '2024M03': 2
                    }
                }
            }
        },
        'value': {
            '0': 2.5,
            '1': 2.7,
            '2': 2.6
        }
    }


@pytest.fixture
def sample_fred_response():
    """Mock FRED API response data."""
    return {
        'observations': [
            {'date': '2024-01-01', 'value': '3.8'},
            {'date': '2024-02-01', 'value': '3.7'},
            {'date': '2024-03-01', 'value': '3.9'}
        ]
    }


@pytest.fixture
def sample_ecb_response():
    """Mock ECB API response data."""
    return {
        'structure': {
            'dimensions': {
                'observation': [{
                    'values': [
                        {'id': '2024-01', 'name': '2024-01'},
                        {'id': '2024-02', 'name': '2024-02'},
                        {'id': '2024-03', 'name': '2024-03'}
                    ]
                }]
            }
        },
        'dataSets': [{
            'series': {
                '0:0:0:0:0:0:0': {
                    'observations': {
                        '0': [4.25],
                        '1': [4.50],
                        '2': [4.25]
                    }
                }
            }
        }]
    }


# File fixtures
@pytest.fixture
def temp_portfolio_file(sample_portfolio_data):
    """Create temporary portfolio JSON file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(sample_portfolio_data, f)
        temp_file = f.name
    
    yield temp_file
    
    Path(temp_file).unlink(missing_ok=True)


@pytest.fixture
def temp_csv_file():
    """Create temporary CSV file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write('date,ticker,quantity,price,transaction_type\n')
        f.write('2024-01-01,AAPL,100,150.00,buy\n')
        f.write('2024-01-02,MSFT,50,300.00,buy\n')
        temp_file = f.name
    
    yield temp_file
    
    Path(temp_file).unlink(missing_ok=True)


# Utility fixtures and functions
@pytest.fixture
def capture_stdout():
    """Capture stdout for testing CLI output."""
    import sys
    from io import StringIO
    
    old_stdout = sys.stdout
    captured_output = StringIO()
    sys.stdout = captured_output
    
    yield captured_output
    
    sys.stdout = old_stdout


def create_test_instruments(session, count=5):
    """Utility function to create test instruments in database."""
    from market_data_etl.data.models import Instrument, InstrumentType
    
    instruments = []
    for i in range(count):
        instrument = Instrument(
            ticker_symbol=f'TEST{i:03d}',
            instrument_name=f'Test Company {i}',
            instrument_type=InstrumentType.STOCK,
            currency='USD',
            sector='Technology',
            industry='Software',
            country='United States'
        )
        instruments.append(instrument)
    
    session.add_all(instruments)
    session.commit()
    return instruments


def create_test_prices(session, instrument, days=10):
    """Utility function to create test price data."""
    from market_data_etl.data.models import Price
    import random
    
    prices = []
    for i in range(days):
        price = Price(
            instrument_id=instrument.id,
            date=date(2024, 1, i + 1),
            open=100.0 + random.uniform(-5, 5),
            high=105.0 + random.uniform(-5, 5),
            low=95.0 + random.uniform(-5, 5),
            close=102.0 + random.uniform(-5, 5),
            adj_close=102.0 + random.uniform(-5, 5),
            volume=random.randint(100000, 1000000)
        )
        prices.append(price)
    
    session.add_all(prices)
    session.commit()
    return prices


# Performance testing utilities
@pytest.fixture
def performance_timer():
    """Timer fixture for performance testing."""
    import time
    
    class Timer:
        def __init__(self):
            self.start_time = None
            self.end_time = None
        
        def start(self):
            self.start_time = time.time()
        
        def stop(self):
            self.end_time = time.time()
        
        @property
        def elapsed(self):
            if self.start_time and self.end_time:
                return self.end_time - self.start_time
            return None
    
    return Timer()


# Custom assertions
def assert_valid_instrument_data(instrument_data):
    """Assert that instrument data has valid structure."""
    required_fields = ['ticker_symbol', 'instrument_name', 'instrument_type', 'currency']
    
    for field in required_fields:
        assert field in instrument_data, f"Missing required field: {field}"
        assert instrument_data[field], f"Empty value for required field: {field}"
    
    # Validate currency format
    assert len(instrument_data['currency']) == 3, "Currency should be 3-letter code"
    assert instrument_data['currency'].isupper(), "Currency should be uppercase"


def assert_valid_price_data(price_df):
    """Assert that price DataFrame has valid structure."""
    required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    
    for col in required_columns:
        assert col in price_df.columns, f"Missing required column: {col}"
    
    # Check data types
    numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    for col in numeric_columns:
        assert pd.api.types.is_numeric_dtype(price_df[col]), f"Column {col} should be numeric"
    
    # Check for valid price relationships
    assert (price_df['High'] >= price_df['Low']).all(), "High prices should be >= Low prices"
    assert (price_df['High'] >= price_df['Open']).all(), "High prices should be >= Open prices"
    assert (price_df['High'] >= price_df['Close']).all(), "High prices should be >= Close prices"
    assert (price_df['Low'] <= price_df['Open']).all(), "Low prices should be <= Open prices"
    assert (price_df['Low'] <= price_df['Close']).all(), "Low prices should be <= Close prices"


def assert_valid_economic_data(economic_data):
    """Assert that economic indicator data has valid structure."""
    required_fields = ['name', 'source', 'source_identifier', 'description', 'data_points']
    
    for field in required_fields:
        assert field in economic_data, f"Missing required field: {field}"
    
    # Validate data points structure
    data_points = economic_data['data_points']
    assert isinstance(data_points, list), "data_points should be a list"
    
    for point in data_points:
        assert 'date' in point, "Data point missing date"
        assert 'value' in point, "Data point missing value"
        assert isinstance(point['value'], (int, float)), "Value should be numeric"


# Test markers for organization
pytestmark = pytest.mark.unit  # Default marker for this conftest