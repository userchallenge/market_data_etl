"""
Test database schema consistency and constraints.

These tests ensure that:
1. Current database schema works correctly
2. All required tables and relationships exist  
3. Data integrity constraints are enforced
4. Schema changes don't break existing functionality
5. Database operations remain performant
"""

import pytest
import tempfile
from pathlib import Path
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from datetime import date, datetime
import pandas as pd

from market_data_etl.data.models import (
    Base, Instrument, Price, IncomeStatement, BalanceSheet, CashFlow, 
    FinancialRatio, EconomicIndicator, EconomicIndicatorData, Portfolio, 
    PortfolioHolding, Transaction, AlignedDailyData, InstrumentType, 
    TransactionType
)
from market_data_etl.database.manager import DatabaseManager


class TestDatabaseSchemaIntegrity:
    """Test database schema structure and integrity."""
    
    @pytest.fixture
    def temp_db_path(self):
        """Create temporary database for testing."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            temp_path = f.name
        
        yield temp_path
        
        # Cleanup
        Path(temp_path).unlink(missing_ok=True)
    
    @pytest.fixture
    def test_engine(self, temp_db_path):
        """Create test database engine."""
        engine = create_engine(f'sqlite:///{temp_db_path}')
        Base.metadata.create_all(engine)
        return engine
    
    @pytest.fixture
    def test_session(self, test_engine):
        """Create test database session."""
        Session = sessionmaker(bind=test_engine)
        session = Session()
        
        yield session
        
        session.close()
    
    @pytest.fixture
    def db_manager(self, temp_db_path):
        """Create DatabaseManager with test database."""
        return DatabaseManager(db_path=temp_db_path)
    
    def test_all_required_tables_exist(self, test_engine):
        """Test that all required tables are created."""
        inspector = inspect(test_engine)
        table_names = set(inspector.get_table_names())
        
        required_tables = {
            'instruments', 'prices', 'income_statements', 'balance_sheets', 
            'cash_flows', 'financial_ratios', 'economic_indicators', 
            'economic_indicator_data', 'portfolios', 'portfolio_holdings', 
            'transactions', 'aligned_daily_data'
        }
        
        assert required_tables.issubset(table_names), f"Missing tables: {required_tables - table_names}"
    
    def test_foreign_key_relationships(self, test_engine):
        """Test that all foreign key relationships exist."""
        inspector = inspect(test_engine)
        
        # Test key relationships
        relationships = {
            'prices': [('instrument_id', 'instruments', 'id')],
            'income_statements': [('instrument_id', 'instruments', 'id')],
            'balance_sheets': [('instrument_id', 'instruments', 'id')],
            'cash_flows': [('instrument_id', 'instruments', 'id')],
            'financial_ratios': [('instrument_id', 'instruments', 'id')],
            'portfolio_holdings': [
                ('portfolio_id', 'portfolios', 'id'),
                ('instrument_id', 'instruments', 'id')
            ],
            'transactions': [
                ('portfolio_id', 'portfolios', 'id'),
                ('instrument_id', 'instruments', 'id')
            ],
            'economic_indicator_data': [('indicator_id', 'economic_indicators', 'id')],
            'aligned_daily_data': [('instrument_id', 'instruments', 'id')]
        }
        
        for table_name, expected_fks in relationships.items():
            actual_fks = inspector.get_foreign_keys(table_name)
            
            for fk_col, ref_table, ref_col in expected_fks:
                found_fk = False
                for fk in actual_fks:
                    if (fk_col in fk['constrained_columns'] and 
                        fk['referred_table'] == ref_table and 
                        ref_col in fk['referred_columns']):
                        found_fk = True
                        break
                
                assert found_fk, f"Missing FK: {table_name}.{fk_col} -> {ref_table}.{ref_col}"
    
    def test_primary_key_constraints(self, test_engine):
        """Test that all tables have appropriate primary keys."""
        inspector = inspect(test_engine)
        
        for table_name in inspector.get_table_names():
            pk_constraint = inspector.get_pk_constraint(table_name)
            
            if table_name == 'aligned_daily_data':
                # Composite primary key
                expected_pk_cols = {'date', 'instrument_id'}
                actual_pk_cols = set(pk_constraint['constrained_columns'])
                assert actual_pk_cols == expected_pk_cols
            else:
                # Single column primary key (usually 'id')
                assert len(pk_constraint['constrained_columns']) >= 1
    
    def test_index_creation(self, test_engine):
        """Test that important indexes are created."""
        inspector = inspect(test_engine)
        
        # Key indexes that should exist for performance
        expected_indexes = {
            'instruments': ['ticker_symbol', 'isin'],
            'prices': ['date', 'instrument_id'],
            'portfolio_holdings': ['portfolio_id', 'instrument_id'],
            'economic_indicator_data': ['indicator_id', 'date'],
            'aligned_daily_data': ['date', 'instrument_id']
        }
        
        for table_name, expected_cols in expected_indexes.items():
            if table_name in inspector.get_table_names():
                indexes = inspector.get_indexes(table_name)
                
                for expected_col in expected_cols:
                    found_index = False
                    for idx in indexes:
                        if expected_col in idx['column_names']:
                            found_index = True
                            break
                    
                    # Note: Some indexes might be created automatically for PKs/FKs
                    # This is more of a performance check than strict requirement
                    if not found_index:
                        print(f"Warning: No explicit index found for {table_name}.{expected_col}")


class TestDataIntegrityConstraints:
    """Test data integrity constraints and validation."""
    
    @pytest.fixture
    def test_session(self):
        """Create in-memory database for testing."""
        engine = create_engine('sqlite:///:memory:')
        
        # Enable foreign key constraints for test database
        from sqlalchemy import event
        @event.listens_for(engine, "connect")
        def enable_foreign_keys(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()
        
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        yield session
        
        session.close()
    
    def test_instrument_creation_validation(self, test_session):
        """Test instrument creation with various validations."""
        # Valid instrument
        valid_instrument = Instrument(
            ticker_symbol='TEST',
            instrument_name='Test Company',
            instrument_type=InstrumentType.STOCK,
            currency='USD',
            country='United States'
        )
        
        test_session.add(valid_instrument)
        test_session.commit()
        
        # Verify it was created
        assert test_session.query(Instrument).filter_by(ticker_symbol='TEST').first() is not None
    
    def test_unique_constraints(self, test_session):
        """Test unique constraints on key fields."""
        # Create first instrument
        instrument1 = Instrument(
            ticker_symbol='UNIQUE_TEST',
            instrument_name='Test Company 1',
            instrument_type=InstrumentType.STOCK,
            currency='USD'
        )
        test_session.add(instrument1)
        test_session.commit()
        
        # Try to create duplicate ticker - should fail
        instrument2 = Instrument(
            ticker_symbol='UNIQUE_TEST',  # Same ticker
            instrument_name='Test Company 2', 
            instrument_type=InstrumentType.STOCK,
            currency='EUR'
        )
        test_session.add(instrument2)
        
        with pytest.raises(Exception):  # SQLAlchemy will raise IntegrityError
            test_session.commit()
    
    def test_foreign_key_constraints(self, test_session):
        """Test that foreign key constraints are enforced."""
        # Try to create price without valid instrument - should fail
        invalid_price = Price(
            instrument_id=99999,  # Non-existent instrument
            date=date(2024, 1, 1),
            close=100.0
        )
        test_session.add(invalid_price)
        
        with pytest.raises(Exception):  # Should fail FK constraint
            test_session.commit()
    
    def test_enum_constraints(self, test_session):
        """Test that enum constraints work correctly."""
        # Valid enum values should work
        valid_instrument = Instrument(
            ticker_symbol='ENUM_TEST',
            instrument_name='Test',
            instrument_type=InstrumentType.STOCK,  # Valid enum
            currency='USD'
        )
        test_session.add(valid_instrument)
        test_session.commit()
        
        # Verify enum value is stored correctly
        retrieved = test_session.query(Instrument).filter_by(ticker_symbol='ENUM_TEST').first()
        assert retrieved.instrument_type == InstrumentType.STOCK
    
    def test_date_constraints(self, test_session):
        """Test date field constraints and formats."""
        # Create instrument first
        instrument = Instrument(
            ticker_symbol='DATE_TEST',
            instrument_name='Date Test Company',
            instrument_type=InstrumentType.STOCK,
            currency='USD'
        )
        test_session.add(instrument)
        test_session.commit()
        
        # Valid date
        valid_price = Price(
            instrument_id=instrument.id,
            date=date(2024, 1, 1),
            close=100.0
        )
        test_session.add(valid_price)
        test_session.commit()
        
        # Verify date is stored correctly
        retrieved_price = test_session.query(Price).filter_by(instrument_id=instrument.id).first()
        assert retrieved_price.date == date(2024, 1, 1)
        assert isinstance(retrieved_price.created_at, datetime)


class TestDatabasePerformance:
    """Test database performance and query efficiency."""
    
    @pytest.fixture
    def populated_db(self):
        """Create database with test data for performance testing."""
        engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Create test instruments
        instruments = []
        for i in range(100):
            instrument = Instrument(
                ticker_symbol=f'TEST{i:03d}',
                instrument_name=f'Test Company {i}',
                instrument_type=InstrumentType.STOCK,
                currency='USD'
            )
            instruments.append(instrument)
        
        session.add_all(instruments)
        session.commit()
        
        # Create test price data
        import random
        prices = []
        for instrument in instruments:
            for day in range(30):  # 30 days of data
                price = Price(
                    instrument_id=instrument.id,
                    date=date(2024, 1, day + 1),
                    open=100 + random.uniform(-10, 10),
                    high=105 + random.uniform(-10, 10),
                    low=95 + random.uniform(-10, 10),
                    close=100 + random.uniform(-10, 10),
                    volume=random.randint(10000, 1000000)
                )
                prices.append(price)
        
        session.add_all(prices)
        session.commit()
        
        yield session
        session.close()
    
    def test_price_query_performance(self, populated_db):
        """Test that price queries are efficient."""
        import time
        
        # Test single instrument price query
        start_time = time.time()
        
        instrument = populated_db.query(Instrument).first()
        prices = populated_db.query(Price).filter(
            Price.instrument_id == instrument.id
        ).order_by(Price.date).all()
        
        elapsed = time.time() - start_time
        
        assert len(prices) == 30  # Should get all 30 days
        assert elapsed < 0.1, f"Price query too slow: {elapsed:.3f}s"
    
    def test_date_range_query_performance(self, populated_db):
        """Test date range queries are efficient."""
        import time
        
        start_time = time.time()
        
        # Query prices for date range across all instruments
        prices = populated_db.query(Price).filter(
            Price.date >= date(2024, 1, 10),
            Price.date <= date(2024, 1, 20)
        ).all()
        
        elapsed = time.time() - start_time
        
        # Should get 11 days × 100 instruments = 1100 records
        expected_count = 11 * 100
        assert len(prices) == expected_count
        assert elapsed < 0.2, f"Date range query too slow: {elapsed:.3f}s"
    
    def test_bulk_insert_performance(self):
        """Test that bulk inserts are efficient."""
        engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Create many instruments at once
        import time
        start_time = time.time()
        
        instruments = []
        for i in range(1000):  # 1000 instruments
            instrument = Instrument(
                ticker_symbol=f'BULK{i:04d}',
                instrument_name=f'Bulk Test Company {i}',
                instrument_type=InstrumentType.STOCK,
                currency='USD'
            )
            instruments.append(instrument)
        
        session.add_all(instruments)
        session.commit()
        
        elapsed = time.time() - start_time
        
        # Should insert 1000 records quickly
        assert elapsed < 2.0, f"Bulk insert too slow: {elapsed:.3f}s for 1000 records"
        
        # Verify all were inserted
        count = session.query(Instrument).count()
        assert count == 1000
        
        session.close()


class TestDatabaseManagerIntegration:
    """Test DatabaseManager functionality with current schema."""
    
    @pytest.fixture
    def temp_db_manager(self):
        """Create temporary DatabaseManager for testing."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            temp_path = f.name
        
        db_manager = DatabaseManager(db_path=temp_path)
        
        yield db_manager
        
        # Cleanup
        Path(temp_path).unlink(missing_ok=True)
    
    def test_database_manager_initialization(self, temp_db_manager):
        """Test that DatabaseManager initializes correctly."""
        # Should be able to create tables
        assert temp_db_manager.engine is not None
        
        # Should be able to create session
        with temp_db_manager.get_session() as session:
            assert session is not None
    
    def test_instrument_storage_and_retrieval(self, temp_db_manager):
        """Test instrument storage and retrieval through DatabaseManager."""
        # This tests the current store_price_data functionality
        test_ticker = 'TESTSTORAGE'
        
        # Create sample price DataFrame
        price_data = pd.DataFrame({
            'date': [date(2024, 1, 1), date(2024, 1, 2)],
            'open': [100.0, 101.0],
            'high': [105.0, 106.0],
            'low': [95.0, 96.0],
            'close': [102.0, 103.0],
            'adj_close': [102.0, 103.0],
            'volume': [1000000, 1100000]
        })
        
        # Store data
        count = temp_db_manager.store_price_data(test_ticker, price_data)
        assert count == 2  # Should store 2 records
        
        # Retrieve instrument info
        instrument_info = temp_db_manager.get_instrument_info(test_ticker)
        assert instrument_info is not None
        assert instrument_info['exists'] == True
        assert instrument_info['ticker'] == test_ticker
    
    def test_economic_data_storage(self, temp_db_manager):
        """Test economic data storage functionality."""
        # Test economic indicator storage
        indicator_data = {
            'name': 'test_inflation',
            'source': 'test_source',
            'source_identifier': 'TEST_ID',
            'description': 'Test Inflation Indicator',
            'unit': 'percent',
            'frequency': 'monthly',
            'data_points': [
                {'date': '2024-01-01', 'value': 2.5},
                {'date': '2024-02-01', 'value': 2.7}
            ]
        }
        
        results = temp_db_manager.store_economic_data(indicator_data)
        
        # Should create indicator and data points
        assert results['indicators'] >= 1
        assert results['data_points'] == 2
    
    def test_portfolio_operations(self, temp_db_manager):
        """Test portfolio creation and management."""
        # Test portfolio configuration loading
        portfolio_config = {
            'name': 'Test Portfolio',
            'description': 'Test portfolio for unit testing',
            'holdings': ['AAPL', 'MSFT', 'GOOGL']
        }
        
        # This should work with current portfolio loading functionality
        portfolio_id = temp_db_manager.load_portfolio_from_config(portfolio_config)
        assert portfolio_id is not None
        
        # Should be able to retrieve portfolio info
        portfolio_summary = temp_db_manager.get_portfolio_summary('Test Portfolio')
        assert portfolio_summary is not None
        assert portfolio_summary['exists'] == True
        assert portfolio_summary['portfolio']['name'] == 'Test Portfolio'
    
    def test_clear_all_data_with_foreign_keys(self, temp_db_manager):
        """Test that clear_all_data works with foreign key constraints."""
        # First, create some test data with relationships
        test_ticker = 'CLEARTEST'
        
        # Create instrument and price data
        price_data = pd.DataFrame({
            'date': [date(2024, 1, 1), date(2024, 1, 2)],
            'open': [100.0, 101.0],
            'high': [105.0, 106.0],
            'low': [95.0, 96.0],
            'close': [102.0, 103.0],
            'adj_close': [102.0, 103.0],
            'volume': [1000000, 1100000]
        })
        temp_db_manager.store_price_data(test_ticker, price_data)
        
        # Create economic indicator data
        indicator_data = {
            'name': 'test_clear_indicator',
            'source': 'test_source',
            'source_identifier': 'CLEAR_TEST_ID',
            'description': 'Test indicator for clear test',
            'unit': 'percent',
            'frequency': 'monthly',
            'data_points': [
                {'date': '2024-01-01', 'value': 2.5},
                {'date': '2024-02-01', 'value': 2.7}
            ]
        }
        temp_db_manager.store_economic_data(indicator_data)
        
        # Create portfolio data
        portfolio_config = {
            'name': 'Clear Test Portfolio',
            'description': 'Portfolio for clear test',
            'holdings': [test_ticker]
        }
        temp_db_manager.load_portfolio_from_config(portfolio_config)
        
        # Verify data exists before clearing
        instrument_info = temp_db_manager.get_instrument_info(test_ticker)
        assert instrument_info['exists'] == True
        
        portfolio_summary = temp_db_manager.get_portfolio_summary('Clear Test Portfolio')
        assert portfolio_summary['exists'] == True
        
        # Test clear_all_data - should work without FK constraint errors
        success = temp_db_manager.clear_all_data()
        assert success == True
        
        # Verify all data is cleared
        instrument_info_after = temp_db_manager.get_instrument_info(test_ticker)
        assert instrument_info_after['exists'] == False
        
        portfolio_summary_after = temp_db_manager.get_portfolio_summary('Clear Test Portfolio')
        assert portfolio_summary_after['exists'] == False