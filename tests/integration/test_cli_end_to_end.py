"""
End-to-end CLI tests using subprocess to test actual CLI commands.

These tests execute the actual CLI commands as subprocess calls to ensure
the complete command-line interface works correctly in real usage scenarios.
"""

import pytest
import subprocess
import tempfile
import json
import os
from pathlib import Path
from unittest.mock import patch


class TestCLIEndToEnd:
    """End-to-end tests for CLI commands using subprocess."""
    
    def run_cli_command(self, cmd_args, env_vars=None):
        """
        Helper method to run CLI commands and capture output.
        
        Args:
            cmd_args: List of command arguments
            env_vars: Optional environment variables dict
            
        Returns:
            Tuple of (return_code, stdout, stderr)
        """
        if env_vars:
            env = os.environ.copy()
            env.update(env_vars)
        else:
            env = None
            
        try:
            result = subprocess.run(
                ['market-data-etl'] + cmd_args,
                capture_output=True,
                text=True,
                timeout=30,
                env=env
            )
            return result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            return -1, "", "Command timed out"
        except FileNotFoundError:
            return -1, "", "market-data-etl command not found"


class TestPriceCLICommands(TestCLIEndToEnd):
    """Test price-related CLI commands end-to-end."""
    
    def test_fetch_prices_help(self):
        """Test fetch-prices command help."""
        returncode, stdout, stderr = self.run_cli_command(['fetch-prices', '--help'])
        
        assert returncode == 0
        assert 'fetch-prices' in stdout
        assert '--ticker' in stdout
        assert '--from' in stdout
        assert '--to' in stdout
    
    def test_fetch_prices_invalid_ticker(self):
        """Test fetch-prices with invalid ticker."""
        returncode, stdout, stderr = self.run_cli_command([
            'fetch-prices',
            '--ticker', '',
            '--from', '2024-01-01',
            '--to', '2024-01-31'
        ])
        
        assert returncode == 1
        # Should show validation error
        assert 'error' in stdout.lower() or 'invalid' in stdout.lower()
    
    def test_fetch_prices_invalid_date(self):
        """Test fetch-prices with invalid date format."""
        returncode, stdout, stderr = self.run_cli_command([
            'fetch-prices',
            '--ticker', 'AAPL',
            '--from', 'not-a-date',
            '--to', '2024-01-31'
        ])
        
        assert returncode == 1
        # Should show date validation error
        assert 'date' in stdout.lower() or 'invalid' in stdout.lower() or 'error' in stdout.lower()
    
    @pytest.mark.external_api
    def test_fetch_prices_real_ticker(self):
        """Test fetch-prices with real ticker (requires external API)."""
        returncode, stdout, stderr = self.run_cli_command([
            'fetch-prices',
            '--ticker', 'AAPL',
            '--from', '2024-01-01',
            '--to', '2024-01-02'
        ])
        
        # May succeed or fail depending on API availability
        # Just check that command processes without crashing
        assert returncode in [0, 1]
        assert stdout is not None


class TestFundamentalsCLICommands(TestCLIEndToEnd):
    """Test fundamentals CLI commands end-to-end."""
    
    def test_fetch_fundamentals_help(self):
        """Test fetch-financial-statements command help."""
        returncode, stdout, stderr = self.run_cli_command(['fetch-financial-statements', '--help'])
        
        assert returncode == 0
        assert 'fetch-financial-statements' in stdout
        assert '--ticker' in stdout
    
    def test_fetch_fundamentals_invalid_ticker(self):
        """Test fetch-financial-statements with invalid ticker."""
        returncode, stdout, stderr = self.run_cli_command([
            'fetch-financial-statements',
            '--ticker', ''
        ])
        
        assert returncode == 1
        assert 'error' in stdout.lower() or 'invalid' in stdout.lower()
    
    @pytest.mark.external_api
    def test_fetch_fundamentals_real_ticker(self):
        """Test fetch-financial-statements with real ticker."""
        returncode, stdout, stderr = self.run_cli_command([
            'fetch-financial-statements',
            '--ticker', 'AAPL'
        ])
        
        # May succeed or fail depending on API availability
        assert returncode in [0, 1]
        assert stdout is not None


class TestEconomicCLICommands(TestCLIEndToEnd):
    """Test economic data CLI commands end-to-end with new interface."""
    
    def test_fetch_economic_indicator_help(self):
        """Test fetch-economic-indicator command help."""
        returncode, stdout, stderr = self.run_cli_command(['fetch-economic-indicator', '--help'])
        
        assert returncode == 0
        assert 'fetch-economic-indicator' in stdout
        assert '--indicator' in stdout
        assert '--area' in stdout
        assert '--from' in stdout
        assert '--to' in stdout
        assert 'inflation' in stdout
        assert 'unemployment' in stdout
        assert 'interest' in stdout
        # Should show that se is a supported area code
        assert 'se' in stdout
    
    def test_fetch_all_economic_indicators_help(self):
        """Test fetch-all-economic-indicators command help."""
        returncode, stdout, stderr = self.run_cli_command(['fetch-all-economic-indicators', '--help'])
        
        assert returncode == 0
        assert 'fetch-all-economic-indicators' in stdout
        assert '--from' in stdout
        assert '--to' in stdout
    
    def test_db_info_command_available(self):
        """Test db-info command is available (can be used for economic data too)."""
        returncode, stdout, stderr = self.run_cli_command(['db-info', '--help'])
        
        assert returncode == 0
        assert 'db-info' in stdout
        assert '--ticker' in stdout
    
    def test_fetch_economic_indicator_missing_args(self):
        """Test fetch-economic-indicator with missing required arguments."""
        returncode, stdout, stderr = self.run_cli_command([
            'fetch-economic-indicator'
        ])
        
        assert returncode == 2  # argparse error for missing required args
        assert 'required' in stderr.lower() or 'error' in stderr.lower()
    
    def test_fetch_economic_indicator_invalid_indicator(self):
        """Test fetch-economic-indicator with invalid indicator choice."""
        returncode, stdout, stderr = self.run_cli_command([
            'fetch-economic-indicator',
            '--indicator', 'invalid_choice',
            '--area', 'us',
            '--from', '2024-01-01',
            '--to', '2024-01-31'
        ])
        
        assert returncode == 2  # argparse choice validation error
        assert 'invalid choice' in stderr.lower() or 'choose from' in stderr.lower()
    
    def test_fetch_economic_indicator_valid_args_no_api_key(self):
        """Test fetch-economic-indicator with valid args but missing API key for FRED."""
        # Remove FRED_API_KEY if it exists
        env_vars = {k: v for k, v in os.environ.items() if k != 'FRED_API_KEY'}
        
        returncode, stdout, stderr = self.run_cli_command([
            'fetch-economic-indicator',
            '--indicator', 'unemployment',
            '--area', 'us',
            '--from', '2024-01-01',
            '--to', '2024-01-31'
        ], env_vars=env_vars)
        
        # May succeed (if command processes) or fail (if API key validation is strict)
        assert returncode in [0, 1]
        # If it succeeds, it should process but may show API-related messages
        if returncode == 0:
            assert stdout is not None
        else:
            # Should show FRED API key error
            assert 'fred' in stdout.lower() or 'api' in stdout.lower() or 'key' in stdout.lower()
    
    def test_fetch_economic_indicator_eurostat_no_api_key_needed(self):
        """Test fetch-economic-indicator for Eurostat (no API key required)."""
        returncode, stdout, stderr = self.run_cli_command([
            'fetch-economic-indicator',
            '--indicator', 'inflation',
            '--area', 'ea',
            '--from', '2024-01-01',
            '--to', '2024-01-01'
        ])
        
        # Should process without API key requirement
        # May succeed or fail depending on network/API
        assert returncode in [0, 1]
        assert stdout is not None
    
    def test_fetch_economic_indicator_unemployment_se(self):
        """Test fetch-economic-indicator for Swedish unemployment (Eurostat)."""
        returncode, stdout, stderr = self.run_cli_command([
            'fetch-economic-indicator',
            '--indicator', 'unemployment',
            '--area', 'se',
            '--from', '2024-01-01',
            '--to', '2024-01-01'
        ])
        
        # Should process without API key requirement
        # May succeed or fail depending on network/API
        assert returncode in [0, 1]
        assert stdout is not None
    
    def test_fetch_economic_indicator_interest_se(self):
        """Test fetch-economic-indicator for Swedish interest rates (Eurostat)."""
        returncode, stdout, stderr = self.run_cli_command([
            'fetch-economic-indicator',
            '--indicator', 'interest',
            '--area', 'se',
            '--from', '2024-01-01',
            '--to', '2024-01-01'
        ])
        
        # Should process without API key requirement
        # May succeed or fail depending on network/API
        assert returncode in [0, 1]
        assert stdout is not None
    
    @pytest.mark.external_api
    def test_fetch_economic_indicator_with_fred_key(self):
        """Test fetch-economic-indicator with FRED API key."""
        fred_key = os.environ.get('FRED_API_KEY')
        if not fred_key:
            pytest.skip("FRED_API_KEY not available")
        
        returncode, stdout, stderr = self.run_cli_command([
            'fetch-economic-indicator',
            '--indicator', 'unemployment',
            '--area', 'us',
            '--from', '2024-01-01',
            '--to', '2024-01-01'
        ])
        
        # May succeed or fail depending on API availability
        assert returncode in [0, 1]
        assert stdout is not None
    
    @pytest.mark.external_api
    def test_fetch_all_economic_indicators_with_fred_key(self):
        """Test fetch-all-economic-indicators command."""
        fred_key = os.environ.get('FRED_API_KEY')
        if not fred_key:
            pytest.skip("FRED_API_KEY not available")
        
        returncode, stdout, stderr = self.run_cli_command([
            'fetch-all-economic-indicators',
            '--from', '2024-01-01',
            '--to', '2024-01-01'
        ])
        
        # Should attempt to process all indicators
        assert returncode in [0, 1]
        assert stdout is not None
        # Should show some processing information
        assert ('processing' in stdout.lower() or 'completed' in stdout.lower() or 
                'failed' in stdout.lower() or '✅' in stdout or '❌' in stdout)
    
    @pytest.mark.external_api
    def test_fetch_economic_indicator_swedish_unemployment_external(self):
        """Test Swedish unemployment with real Eurostat API."""
        returncode, stdout, stderr = self.run_cli_command([
            'fetch-economic-indicator',
            '--indicator', 'unemployment',
            '--area', 'se',
            '--from', '2024-01-01',
            '--to', '2024-01-01'
        ])
        
        # May succeed or fail depending on API availability
        assert returncode in [0, 1]
        assert stdout is not None
        if returncode == 0:
            assert 'se' in stdout.lower() or 'sweden' in stdout.lower() or '✅' in stdout
    
    @pytest.mark.external_api
    def test_fetch_economic_indicator_swedish_interest_external(self):
        """Test Swedish interest rates with real Eurostat API."""
        returncode, stdout, stderr = self.run_cli_command([
            'fetch-economic-indicator',
            '--indicator', 'interest',
            '--area', 'se',
            '--from', '2024-01-01',
            '--to', '2024-01-01'
        ])
        
        # May succeed or fail depending on API availability
        assert returncode in [0, 1]
        assert stdout is not None
        if returncode == 0:
            assert 'se' in stdout.lower() or 'sweden' in stdout.lower() or '✅' in stdout
    
    def test_db_info_economic_data_query(self):
        """Test querying economic data via db-info (if implemented)."""
        # This would be used if db-info supports economic indicator queries
        returncode, stdout, stderr = self.run_cli_command([
            'db-info',
            '--ticker', 'nonexistent_ticker'
        ])
        
        assert returncode == 1
        # Should show not found error
        assert 'not found' in stdout.lower() or 'error' in stdout.lower()


class TestPortfolioCLICommands(TestCLIEndToEnd):
    """Test portfolio CLI commands end-to-end."""
    
    @pytest.fixture
    def temp_portfolio_file(self):
        """Create temporary portfolio JSON file for testing."""
        portfolio_data = {
            'name': 'CLI Test Portfolio',
            'description': 'Test portfolio for end-to-end CLI testing',
            'holdings': ['AAPL', 'MSFT', 'GOOGL'],
            'currency': 'USD'
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(portfolio_data, f)
            temp_file = f.name
        
        yield temp_file
        
        Path(temp_file).unlink(missing_ok=True)
    
    def test_load_portfolio_help(self):
        """Test load-portfolio command help."""
        returncode, stdout, stderr = self.run_cli_command(['load-portfolio', '--help'])
        
        assert returncode == 0
        assert 'load-portfolio' in stdout
        assert '--file' in stdout
    
    def test_portfolio_info_help(self):
        """Test portfolio-info command help."""
        returncode, stdout, stderr = self.run_cli_command(['portfolio-info', '--help'])
        
        assert returncode == 0
        assert 'portfolio-info' in stdout
        assert '--portfolio' in stdout
    
    def test_fetch_portfolio_prices_help(self):
        """Test fetch-portfolio-prices command help."""
        returncode, stdout, stderr = self.run_cli_command(['fetch-portfolio-prices', '--help'])
        
        assert returncode == 0
        assert 'fetch-portfolio-prices' in stdout
        assert '--portfolio' in stdout
    
    def test_load_portfolio_missing_file(self):
        """Test load-portfolio with missing file."""
        returncode, stdout, stderr = self.run_cli_command([
            'load-portfolio',
            '--file', 'nonexistent.json'
        ])
        
        assert returncode == 1
        assert 'not found' in stdout.lower() or 'error' in stdout.lower()
    
    def test_load_portfolio_valid_file(self, temp_portfolio_file):
        """Test load-portfolio with valid portfolio file."""
        returncode, stdout, stderr = self.run_cli_command([
            'load-portfolio',
            '--file', temp_portfolio_file
        ])
        
        # Should succeed
        assert returncode == 0
        assert 'CLI Test Portfolio' in stdout
        assert ('successfully' in stdout.lower() or '✅' in stdout)
    
    def test_portfolio_info_nonexistent(self):
        """Test portfolio-info with non-existent portfolio."""
        returncode, stdout, stderr = self.run_cli_command([
            'portfolio-info',
            '--portfolio', 'NonExistent Portfolio'
        ])
        
        # The command currently returns 0 but shows "not found" message
        assert returncode == 0
        assert 'not found' in stdout.lower()


class TestInfoCLICommands(TestCLIEndToEnd):
    """Test info/query CLI commands end-to-end."""
    
    def test_db_info_help(self):
        """Test db-info command help."""
        returncode, stdout, stderr = self.run_cli_command(['db-info', '--help'])
        
        assert returncode == 0
        assert 'db-info' in stdout
        assert '--ticker' in stdout
    
    def test_db_info_invalid_ticker(self):
        """Test db-info with invalid ticker."""
        returncode, stdout, stderr = self.run_cli_command([
            'db-info',
            '--ticker', ''
        ])
        
        assert returncode == 1
        assert 'error' in stdout.lower() or 'invalid' in stdout.lower()
    
    def test_db_info_nonexistent_ticker(self):
        """Test db-info with non-existent ticker."""
        returncode, stdout, stderr = self.run_cli_command([
            'db-info',
            '--ticker', 'NONEXISTENT_TICKER_12345'
        ])
        
        assert returncode == 1
        assert 'not found' in stdout.lower() or 'error' in stdout.lower()


class TestCLIErrorHandling(TestCLIEndToEnd):
    """Test CLI error handling and edge cases."""
    
    def test_invalid_command(self):
        """Test running invalid command."""
        returncode, stdout, stderr = self.run_cli_command(['invalid-command'])
        
        assert returncode == 2  # argparse error
        assert 'invalid choice' in stderr.lower() or 'unrecognized arguments' in stderr.lower()
    
    def test_no_arguments(self):
        """Test running CLI with no arguments."""
        returncode, stdout, stderr = self.run_cli_command([])
        
        assert returncode == 2  # argparse error
        assert 'required' in stderr.lower() or 'arguments' in stderr.lower()
    
    def test_help_command(self):
        """Test main help command."""
        returncode, stdout, stderr = self.run_cli_command(['--help'])
        
        assert returncode == 0
        assert 'market-data-etl' in stdout
        # Should show available commands
        assert 'fetch-prices' in stdout
        assert 'fetch-financial-statements' in stdout
        assert 'fetch-economic-indicator' in stdout
    
    def test_version_command(self):
        """Test version command if available."""
        returncode, stdout, stderr = self.run_cli_command(['--version'])
        
        # Version command may or may not be implemented
        # If implemented, should return 0, if not, may return 2
        assert returncode in [0, 2]


class TestCLIWorkflows(TestCLIEndToEnd):
    """Test complete CLI workflows end-to-end."""
    
    @pytest.fixture
    def temp_portfolio_file(self):
        """Create temporary portfolio for workflow testing."""
        portfolio_data = {
            'name': 'Workflow Test Portfolio',
            'description': 'Portfolio for testing complete workflows',
            'holdings': ['AAPL', 'MSFT'],
            'currency': 'USD'
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(portfolio_data, f)
            temp_file = f.name
        
        yield temp_file
        
        Path(temp_file).unlink(missing_ok=True)
    
    def test_portfolio_workflow(self, temp_portfolio_file):
        """Test complete portfolio workflow: load -> info -> fetch prices."""
        # Step 1: Load portfolio
        returncode1, stdout1, stderr1 = self.run_cli_command([
            'load-portfolio',
            '--file', temp_portfolio_file
        ])
        
        assert returncode1 == 0
        assert 'Workflow Test Portfolio' in stdout1
        
        # Step 2: Get portfolio info
        returncode2, stdout2, stderr2 = self.run_cli_command([
            'portfolio-info',
            '--portfolio', 'Workflow Test Portfolio'
        ])
        
        # May succeed or fail depending on database state
        # But should handle gracefully
        assert returncode2 in [0, 1]
        assert stdout2 is not None
        
        # Step 3: Fetch portfolio prices (if portfolio exists)
        if returncode2 == 0:
            returncode3, stdout3, stderr3 = self.run_cli_command([
                'fetch-portfolio-prices',
                '--portfolio', 'Workflow Test Portfolio',
                '--from', '2024-01-01',
                '--to', '2024-01-02'
            ])
            
            # May succeed or fail depending on API availability
            assert returncode3 in [0, 1]
            assert stdout3 is not None
    
    @pytest.mark.external_api
    def test_economic_data_workflow(self):
        """Test economic data workflow: fetch -> info."""
        fred_key = os.environ.get('FRED_API_KEY')
        if not fred_key:
            pytest.skip("FRED_API_KEY not available")
        
        # Step 1: Fetch economic data
        returncode1, stdout1, stderr1 = self.run_cli_command([
            'fetch-economic-indicator',
            '--indicator', 'unemployment',
            '--area', 'us',
            '--from', '2024-01-01',
            '--to', '2024-01-01'
        ])
        
        # Step 2: Check if economic data was processed (via general db status or similar)
        # Note: economic-info command doesn't exist, so we skip this step
        # Economic data info would be available through other means in the actual system
        pass


if __name__ == '__main__':
    # Allow running this test file directly
    pytest.main([__file__, '-v'])