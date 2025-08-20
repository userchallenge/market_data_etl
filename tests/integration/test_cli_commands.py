"""
Integration tests for CLI commands.

These tests ensure that:
1. All CLI commands work end-to-end
2. Command argument parsing works correctly
3. Commands integrate properly with database
4. Error handling and validation work in CLI context
5. Output formatting is consistent
"""

import pytest
import tempfile
import json
import csv
from pathlib import Path
from unittest.mock import patch, MagicMock
from io import StringIO
import sys

from market_data_etl.cli.commands import (
    fetch_prices_command, fetch_fundamentals_command, fetch_economic_indicator_command,
    fetch_portfolio_prices_command, load_portfolio_command,
    economic_info_command, db_info_command, portfolio_info_command
)
from market_data_etl.database.manager import DatabaseManager


class TestPriceCommands:
    """Test price-related CLI commands."""
    
    @pytest.fixture
    def temp_db_path(self):
        """Create temporary database for testing."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            temp_path = f.name
        
        yield temp_path
        
        Path(temp_path).unlink(missing_ok=True)
    
    @patch('market_data_etl.cli.commands.DatabaseManager')
    @patch('market_data_etl.data.fetchers.PriceFetcher.fetch_price_data')
    def test_fetch_prices_command_success(self, mock_fetch, mock_db_manager, temp_db_path):
        """Test successful price fetching command."""
        # Mock successful price fetching
        mock_fetch.return_value = {
            'info': {'longName': 'Apple Inc.'},
            'history': MagicMock()  # Mock DataFrame
        }
        
        # Mock database operations
        mock_db_instance = MagicMock()
        mock_db_manager.return_value = mock_db_instance
        mock_db_instance.store_price_data.return_value = 10  # 10 records stored
        
        # Mock instrument info for the enhanced fetch_prices_command behavior
        mock_db_instance.get_instrument_info.return_value = {
            'instrument_type': 'fund',  # Use fund to avoid automatic financial fetch
            'instrument_id': 1
        }
        
        # Capture stdout
        captured_output = StringIO()
        
        with patch('sys.stdout', captured_output):
            result = fetch_prices_command(
                ticker='AAPL',
                from_date='2024-01-01',
                to_date='2024-01-31',
                instrument_type=None
            )
        
        # Should return success
        assert result == 0  # SUCCESS_EXIT_CODE
        
        # Should have success message
        output = captured_output.getvalue()
        assert 'successfully' in output.lower() or '✅' in output
        assert 'AAPL' in output
    
    def test_fetch_prices_command_invalid_ticker(self):
        """Test price command with invalid ticker format."""
        result = fetch_prices_command(
            ticker='',  # Empty ticker
            from_date='2024-01-01',
            to_date='2024-01-31',
            instrument_type=None
        )
        
        # Should return error code
        assert result == 1  # ERROR_EXIT_CODE
    
    def test_fetch_prices_command_invalid_date(self):
        """Test price command with invalid date format."""
        result = fetch_prices_command(
            ticker='AAPL',
            from_date='invalid-date',
            to_date='2024-01-31',
            instrument_type=None
        )
        
        # Should return error code
        assert result == 1  # ERROR_EXIT_CODE
    
    @patch('market_data_etl.data.fetchers.PriceFetcher.fetch_price_data')
    def test_fetch_prices_command_api_error(self, mock_fetch):
        """Test price command when API fails."""
        mock_fetch.side_effect = Exception("Yahoo Finance API Error")
        
        result = fetch_prices_command(
            ticker='INVALID_TICKER',
            from_date='2024-01-01',
            to_date='2024-01-31',
            instrument_type=None
        )
        
        # Should handle error gracefully
        assert result == 1  # ERROR_EXIT_CODE


class TestFundamentalCommands:
    """Test fundamental data CLI commands."""
    
    @patch('market_data_etl.cli.commands.DatabaseManager')
    @patch('market_data_etl.etl.load.ETLOrchestrator')
    def test_fetch_fundamentals_command_success(self, mock_orchestrator, mock_db_manager):
        """Test successful fundamentals fetching."""
        # Mock database manager
        mock_db_instance = MagicMock()
        mock_db_manager.return_value = mock_db_instance
        mock_db_instance.get_instrument_info.return_value = {
            'exists': True,
            'instrument_type': 'stock'  # Valid InstrumentType enum value
        }
        
        # Mock successful ETL execution
        mock_etl_instance = MagicMock()
        mock_orchestrator.return_value = mock_etl_instance
        mock_etl_instance.run_financial_etl.return_value = {
            'status': 'completed',
            'phases': {'load': {'loaded_records': {'income_statements': 4}}}
        }
        
        captured_output = StringIO()
        
        with patch('sys.stdout', captured_output):
            result = fetch_fundamentals_command(ticker='AAPL')
        
        assert result == 0
        output = captured_output.getvalue()
        assert 'AAPL' in output
        assert ('successfully' in output.lower() or '✅' in output)
    
    @patch('market_data_etl.etl.load.ETLOrchestrator')
    def test_fetch_fundamentals_command_etl_failure(self, mock_orchestrator):
        """Test fundamentals command when ETL fails."""
        mock_etl_instance = MagicMock()
        mock_orchestrator.return_value = mock_etl_instance
        mock_etl_instance.run_financial_etl.side_effect = Exception("ETL Pipeline Error")
        
        result = fetch_fundamentals_command(ticker='FAIL_TICKER')
        
        assert result == 1


class TestEconomicCommands:
    """Test economic data CLI commands."""
    
    @patch('market_data_etl.cli.commands.DatabaseManager')
    @patch('market_data_etl.etl.load.EconomicETLOrchestrator')
    def test_fetch_economic_command_eurostat(self, mock_orchestrator, mock_db_manager):
        """Test fetching Eurostat economic data."""
        # Mock successful ETL execution
        mock_etl_instance = MagicMock()
        mock_orchestrator.return_value = mock_etl_instance
        mock_etl_instance.run_eurostat_etl.return_value = {
            'status': 'completed',
            'phases': {'load': {'loaded_records': {'data_points': 12}}}
        }
        
        captured_output = StringIO()
        
        with patch('sys.stdout', captured_output):
            result = fetch_economic_indicator_command(
                name='inflation_monthly_euro',
                from_date='2024-01-01'
            )
        
        assert result == 0
        output = captured_output.getvalue()
        assert 'inflation' in output.lower()
    
    @patch('market_data_etl.cli.commands.DatabaseManager')
    @patch('market_data_etl.etl.load.EconomicETLOrchestrator')
    def test_fetch_economic_command_fred(self, mock_orchestrator, mock_db_manager):
        """Test fetching FRED economic data."""
        mock_etl_instance = MagicMock()
        mock_orchestrator.return_value = mock_etl_instance
        mock_etl_instance.run_fred_etl.return_value = {
            'status': 'completed',
            'phases': {'load': {'loaded_records': {'data_points': 24}}}
        }
        
        with patch.dict('os.environ', {'FRED_API_KEY': 'test_key'}):
            result = fetch_economic_indicator_command(
                name='unemployment_monthly_rate_us',
                from_date='2024-01-01'
            )
        
        assert result == 0
    
    @patch('market_data_etl.config.config')
    def test_fetch_economic_command_missing_fred_key(self, mock_config):
        """Test FRED command without API key."""
        # Mock config to return None for FRED API key
        mock_config.api.fred_api_key = None
        
        result = fetch_economic_indicator_command(
            name='unemployment_monthly_rate_us',
            from_date='2024-01-01'
        )
        
        assert result == 1  # Should fail without API key
    
    def test_fetch_economic_command_invalid_indicator(self):
        """Test economic command with invalid indicator."""
        result = fetch_economic_indicator_command(
            name='invalid_indicator',
            from_date='2024-01-01'
        )
        
        assert result == 1


class TestPortfolioCommands:
    """Test portfolio-related CLI commands."""
    
    @pytest.fixture
    def temp_portfolio_file(self):
        """Create temporary portfolio JSON file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            portfolio_data = {
                'name': 'Test Portfolio',
                'description': 'Test portfolio for CLI testing',
                'holdings': ['AAPL', 'MSFT', 'GOOGL']
            }
            json.dump(portfolio_data, f)
            temp_file = f.name
        
        yield temp_file
        
        Path(temp_file).unlink(missing_ok=True)
    
    @patch('market_data_etl.cli.commands.DatabaseManager')
    def test_load_portfolio_command_success(self, mock_db_manager, temp_portfolio_file):
        """Test successful portfolio loading."""
        mock_db_instance = MagicMock()
        mock_db_manager.return_value = mock_db_instance
        # Mock portfolio object
        mock_portfolio = MagicMock()
        mock_portfolio.name = 'Test Portfolio'
        mock_db_instance.load_portfolio_from_config.return_value = mock_portfolio
        
        captured_output = StringIO()
        
        with patch('sys.stdout', captured_output):
            result = load_portfolio_command(file_path=temp_portfolio_file)
        
        assert result == 0
        output = captured_output.getvalue()
        assert 'Test Portfolio' in output
        assert ('successfully' in output.lower() or '✅' in output)
    
    def test_load_portfolio_command_missing_file(self):
        """Test portfolio loading with missing file."""
        result = load_portfolio_command(file_path='nonexistent.json')
        
        assert result == 1
    
    def test_load_portfolio_command_invalid_json(self):
        """Test portfolio loading with invalid JSON."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write('invalid json content')
            invalid_file = f.name
        
        try:
            result = load_portfolio_command(file_path=invalid_file)
            assert result == 1
        finally:
            Path(invalid_file).unlink(missing_ok=True)
    
    @patch('market_data_etl.cli.commands.DatabaseManager')
    @patch('market_data_etl.etl.load.ETLOrchestrator')
    def test_fetch_portfolio_prices_command(self, mock_orchestrator, mock_db_manager, temp_portfolio_file):
        """Test fetching prices for portfolio holdings."""
        # Mock database manager
        mock_db_instance = MagicMock()
        mock_db_manager.return_value = mock_db_instance
        mock_db_instance.get_portfolio_by_name.return_value = {
            'id': 1,
            'name': 'Test Portfolio',
            'holdings': ['AAPL', 'MSFT', 'GOOGL']
        }
        
        # Mock ETL orchestrator
        mock_etl_instance = MagicMock()
        mock_orchestrator.return_value = mock_etl_instance
        mock_etl_instance.run_price_etl.return_value = {
            'status': 'completed',
            'phases': {'load': {'loaded_records': 30}}
        }
        
        # Load portfolio first
        load_portfolio_command(file_path=temp_portfolio_file)
        
        captured_output = StringIO()
        
        with patch('sys.stdout', captured_output):
            result = fetch_portfolio_prices_command(
                portfolio_name='Test Portfolio',
                from_date='2024-01-01'
            )
        
        assert result == 0
        output = captured_output.getvalue()
        assert 'Test Portfolio' in output


class TestInfoCommands:
    """Test info/query CLI commands."""
    
    @patch('market_data_etl.cli.commands.DatabaseManager')
    def test_ticker_info_command_success(self, mock_db_manager):
        """Test ticker info command."""
        mock_db_instance = MagicMock()
        mock_db_manager.return_value = mock_db_instance
        # Mock the correct method that the code actually calls
        mock_db_instance.get_instrument_info.return_value = {
            'exists': True,
            'instrument': {
                'ticker_symbol': 'AAPL',
                'name': 'Apple Inc.',
                'sector': 'Technology',
                'industry': 'Consumer Electronics',
                'country': 'US',
                'currency': 'USD',
                'market_cap': 2500000000000,  # $2.5T as plain int
                'created_at': '2024-01-01 00:00:00'
            },
            'price_data': {
                'count': 250,
                'date_range': ('2024-01-01', '2024-12-31'),  # Tuple as expected
                'latest_price': 175.50
            },
            'financial_statements': {
                'income_statements': 12,
                'balance_sheets': 12,
                'cash_flows': 12,
                'financial_ratios': 48
            }
        }
        
        captured_output = StringIO()
        
        with patch('sys.stdout', captured_output):
            result = db_info_command(ticker='AAPL')
        
        assert result == 0
        output = captured_output.getvalue()
        assert 'AAPL' in output
        assert 'Apple Inc.' in output
    
    @patch('market_data_etl.cli.commands.DatabaseManager')
    def test_ticker_info_command_not_found(self, mock_db_manager):
        """Test ticker info for non-existent ticker."""
        mock_db_instance = MagicMock()
        mock_db_manager.return_value = mock_db_instance
        mock_db_instance.get_instrument_info.return_value = None
        
        result = db_info_command(ticker='NONEXISTENT')
        
        assert result == 1
    
    @patch('market_data_etl.cli.commands.DatabaseManager')
    def test_economic_info_command_success(self, mock_db_manager):
        """Test economic indicator info command."""
        mock_db_instance = MagicMock()
        mock_db_manager.return_value = mock_db_instance
        mock_db_instance.get_economic_indicator_info.return_value = {
            'name': 'inflation_monthly_euro',
            'source': 'eurostat',
            'description': 'Eurozone Inflation Rate',
            'data_point_count': 120
        }
        
        captured_output = StringIO()
        
        with patch('sys.stdout', captured_output):
            result = economic_info_command(indicator_name='inflation_monthly_euro')
        
        assert result == 0
        output = captured_output.getvalue()
        assert 'inflation_monthly_euro' in output
        assert 'eurostat' in output.lower()
    
    @patch('market_data_etl.cli.commands.DatabaseManager')
    def test_portfolio_info_command_success(self, mock_db_manager):
        """Test portfolio info command."""
        mock_db_instance = MagicMock()
        mock_db_manager.return_value = mock_db_instance
        mock_db_instance.get_portfolio_summary.return_value = {
            'exists': True,
            'portfolio': {
                'name': 'Test Portfolio',
                'description': 'Test portfolio for demonstration',
                'currency': 'USD',
                'created_date': '2024-01-01',
                'created_at': '2024-01-01T00:00:00'
            },
            'holdings': {
                'total_count': 3,
                'breakdown': {'stock': 2, 'etf': 1}
            },
            'transactions': {
                'count': 10
            }
        }
        
        captured_output = StringIO()
        
        with patch('sys.stdout', captured_output):
            result = portfolio_info_command(portfolio_name='Test Portfolio')
        
        assert result == 0
        output = captured_output.getvalue()
        assert 'Test Portfolio' in output
        assert '3' in output  # Holdings count


class TestCLIErrorHandling:
    """Test error handling across CLI commands."""
    
    def test_validation_error_handling(self):
        """Test that validation errors are handled gracefully."""
        # Test with invalid date format
        result = fetch_prices_command(
            ticker='AAPL',
            from_date='not-a-date',
            to_date='2024-01-31',
            instrument_type=None
        )
        
        assert result == 1
    
    def test_database_error_handling(self):
        """Test handling of database errors."""
        with patch('market_data_etl.cli.commands.DatabaseManager') as mock_db:
            mock_db.side_effect = Exception("Database connection failed")
            
            result = db_info_command(ticker='AAPL')
            
            assert result == 1
    
    @patch('market_data_etl.cli.commands.DatabaseManager')
    def test_empty_result_handling(self, mock_db_manager):
        """Test handling when database returns empty results."""
        mock_db_instance = MagicMock()
        mock_db_manager.return_value = mock_db_instance
        mock_db_instance.get_instrument_info.return_value = {}  # Empty result
        
        result = db_info_command(ticker='EMPTY')
        
        # Should handle empty result gracefully (may return success or error depending on implementation)
        assert result in [0, 1]


class TestCLIOutputFormatting:
    """Test consistent output formatting across commands."""
    
    @patch('market_data_etl.cli.commands.DatabaseManager')
    def test_success_message_format(self, mock_db_manager):
        """Test that success messages follow consistent format."""
        mock_db_instance = MagicMock()
        mock_db_manager.return_value = mock_db_instance
        mock_db_instance.get_instrument_info.return_value = {
            'exists': True,
            'instrument': {
                'ticker_symbol': 'TEST',
                'name': 'Test Company',
                'sector': 'Technology',
                'industry': 'Software',
                'country': 'US',
                'currency': 'USD',
                'market_cap': 1000000000,
                'created_at': '2024-01-01T00:00:00'
            },
            'price_data': {
                'count': 100,
                'date_range': ('2024-01-01', '2024-12-31')
            },
            'financial_statements': {
                'income_statements': 4,
                'balance_sheets': 4,
                'cash_flows': 4,
                'financial_ratios': 16
            }
        }
        
        captured_output = StringIO()
        
        with patch('sys.stdout', captured_output):
            db_info_command(ticker='TEST')
        
        output = captured_output.getvalue()
        
        # Should have consistent formatting (check for key elements)
        assert 'TEST' in output
        assert 'Test Company' in output
        # Could check for specific formatting patterns like "✅" or consistent spacing
    
    def test_error_message_format(self):
        """Test that error messages follow consistent format."""
        captured_output = StringIO()
        
        with patch('sys.stdout', captured_output):
            result = db_info_command(ticker='')  # Invalid empty ticker
        
        output = captured_output.getvalue()
        
        # Error messages should be clear and consistent
        if output.strip():  # If there's output
            assert 'error' in output.lower() or 'invalid' in output.lower() or '❌' in output


class TestCLIIntegrationScenarios:
    """Test realistic CLI usage scenarios."""
    
    @pytest.fixture
    def temp_db_path(self):
        """Create temporary database for integration testing."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            temp_path = f.name
        
        yield temp_path
        
        Path(temp_path).unlink(missing_ok=True)
    
    def test_complete_workflow_scenario(self, temp_db_path):
        """Test complete workflow: load portfolio -> fetch prices -> get info."""
        # This would test a realistic user workflow
        # Create portfolio file
        portfolio_data = {
            'name': 'Integration Test Portfolio',
            'holdings': ['AAPL', 'MSFT']
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(portfolio_data, f)
            portfolio_file = f.name
        
        try:
            # Mock all external dependencies
            with patch('market_data_etl.cli.commands.DatabaseManager') as mock_db:
                with patch('market_data_etl.etl.load.ETLOrchestrator') as mock_etl:
                    # Setup mocks
                    mock_db_instance = MagicMock()
                    mock_db.return_value = mock_db_instance
                    # Mock portfolio object with name attribute
                    mock_portfolio = MagicMock()
                    mock_portfolio.name = 'Integration Test Portfolio'
                    mock_db_instance.load_portfolio_from_config.return_value = mock_portfolio
                    mock_db_instance.get_portfolio_by_name.return_value = {
                        'name': 'Integration Test Portfolio',
                        'holdings': ['AAPL', 'MSFT']
                    }
                    mock_db_instance.get_portfolio_summary.return_value = {
                        'exists': True,
                        'portfolio': {
                            'name': 'Integration Test Portfolio',
                            'description': 'Test portfolio',
                            'currency': 'USD',
                            'created_date': '2024-01-01',
                            'created_at': '2024-01-01T00:00:00'
                        },
                        'holdings': {'total_count': 2, 'breakdown': {'stock': 2}},
                        'transactions': {'count': 0}
                    }
                    
                    mock_etl_instance = MagicMock()
                    mock_etl.return_value = mock_etl_instance
                    mock_etl_instance.run_price_etl.return_value = {
                        'status': 'completed',
                        'phases': {'load': {'loaded_records': 60}}
                    }
                    
                    # Execute workflow
                    step1 = load_portfolio_command(file_path=portfolio_file)
                    assert step1 == 0
                    
                    step2 = fetch_portfolio_prices_command(
                        portfolio_name='Integration Test Portfolio',
                        from_date='2024-01-01'
                    )
                    assert step2 == 0
                    
                    step3 = portfolio_info_command(portfolio_name='Integration Test Portfolio')
                    assert step3 == 0
        
        finally:
            Path(portfolio_file).unlink(missing_ok=True)
    
    def test_error_recovery_scenario(self):
        """Test that CLI can recover from errors gracefully."""
        # Test scenario where first command fails but subsequent commands work
        
        # First command fails
        result1 = fetch_prices_command(
            ticker='INVALID',
            from_date='bad-date',
            to_date='2024-01-31',
            instrument_type=None
        )
        assert result1 == 1
        
        # Second command with valid data should still work
        with patch('market_data_etl.cli.commands.DatabaseManager') as mock_db:
            mock_db_instance = MagicMock()
            mock_db.return_value = mock_db_instance
            mock_db_instance.get_instrument_info.return_value = {
                'exists': True,
                'instrument': {
                    'ticker_symbol': 'VALID',
                    'name': 'Valid Company',
                    'sector': 'Technology',
                    'industry': 'Software',
                    'country': 'US',
                    'currency': 'USD',
                    'market_cap': 1000000000,
                    'employees': 50000,
                    'created_at': '2024-01-01T00:00:00'
                },
                'price_data': {'count': 50, 'date_range': ('2024-01-01', '2024-12-31')},
                'financial_statements': {
                    'income_statements': 4,
                    'balance_sheets': 4,
                    'cash_flows': 4,
                    'financial_ratios': 16
                }
            }
            
            result2 = db_info_command(ticker='VALID')
            assert result2 == 0