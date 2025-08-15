"""
CLI command implementations for market data ETL operations.

This module contains the actual command logic separated from
argument parsing for better testability and organization.
"""

from typing import List, Optional, Dict, Any
from datetime import date, datetime
import json
import csv
import os

from ..etl.load import ETLOrchestrator
from ..database.manager import DatabaseManager
from ..utils.logging import get_logger
from ..utils.exceptions import YahooFinanceError, ValidationError, DatabaseError
from ..utils.validation import validate_ticker, validate_date_string, validate_date_range, validate_years_parameter
from ..data.models import InstrumentType

logger = get_logger(__name__)


# Constants
SUCCESS_EXIT_CODE = 0
ERROR_EXIT_CODE = 1

# Status messages
PERIOD_TYPE_ANNUAL = 'annual'
PERIOD_TYPE_QUARTERLY = 'quarterly'


def find_missing_dates_in_range(
    existing_dates: List[date], 
    range_start_date: date, 
    range_end_date: date
) -> List[date]:
    """
    Calculate which dates are missing from existing data.
    
    Args:
        existing_dates: List of dates that already exist
        range_start_date: Start date of requested range
        range_end_date: End date of requested range
        
    Returns:
        List of missing dates that need to be fetched
    """
    existing_dates_set = set(existing_dates)
    
    # Generate all dates in the range
    all_dates_in_range = []
    current_date = range_start_date
    while current_date <= range_end_date:
        # Include all dates - Yahoo Finance will filter out non-trading days
        all_dates_in_range.append(current_date)
        current_date = date.fromordinal(current_date.toordinal() + 1)
    
    return [d for d in all_dates_in_range if d not in existing_dates_set]


def fetch_prices_command(
    ticker: str,
    from_date: str,
    to_date: Optional[str] = None
) -> int:
    """
    Handle fetch-prices command using proper ETL pattern.
    
    Args:
        ticker: Stock ticker symbol
        from_date: Start date in YYYY-MM-DD format
        to_date: End date in YYYY-MM-DD format (optional)
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        ticker = validate_ticker(ticker)
        start_date = validate_date_string(from_date, "from_date")
        end_date = validate_date_string(to_date, "to_date") if to_date else date.today()
        
        validate_date_range(start_date, end_date)
        
        print(f"Running price ETL pipeline for {ticker} from {start_date} to {end_date}...")
        
        # Initialize ETL orchestrator
        etl = ETLOrchestrator()
        
        # Check existing data to avoid unnecessary fetches
        db = DatabaseManager()
        existing_dates = db.get_existing_price_dates(ticker)
        missing_dates = find_missing_dates_in_range(existing_dates, start_date, end_date)
        
        if not missing_dates:
            print("Database already contains data for the full range. Nothing to fetch.")
            return SUCCESS_EXIT_CODE
        
        print(f"Found {len(existing_dates)} days already in database, processing {len(missing_dates)} missing days...")
        
        # Run ETL pipeline
        earliest_missing = min(missing_dates)
        latest_missing = max(missing_dates)
        
        etl_results = etl.run_price_etl(ticker, earliest_missing, latest_missing)
        
        # Report results
        if etl_results['status'] == 'completed':
            loaded_records = etl_results['phases']['load']['loaded_records']
            print(f"\n✅ ETL Pipeline completed successfully!")
            print(f"📊 Pipeline Summary:")
            print(f"  • Extract: {etl_results['phases']['extract']['shape']} records")
            print(f"  • Transform: {etl_results['phases']['transform']['record_count']} records")
            print(f"  • Load: {loaded_records} records stored")
            print(f"Operation completed successfully.")
        else:
            print(f"ERROR: ETL pipeline failed - {etl_results.get('error', 'Unknown error')}")
            return ERROR_EXIT_CODE
        
        return SUCCESS_EXIT_CODE
        
    except ValidationError as e:
        print(f"ERROR: {e}")
        return ERROR_EXIT_CODE
    except YahooFinanceError as e:
        print(f"ERROR: Failed to fetch prices for {ticker} from Yahoo Finance.")
        print(str(e))
        return ERROR_EXIT_CODE
    except Exception as e:
        logger.error(f"Unexpected error in fetch_prices_command: {e}", exc_info=True)
        print(f"ERROR: Unexpected error: {e}")
        return ERROR_EXIT_CODE


def fetch_fundamentals_command(ticker: str) -> int:
    """
    Handle fetch-fundamentals command (legacy - redirects to financial statements).
    
    Args:
        ticker: Stock ticker symbol
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    print(f"⚠️  Legacy command detected!")
    print(f"The 'fetch-fundamentals' command is deprecated.")
    print(f"For structured financial analysis, use: fetch-financial-statements --ticker {ticker}")
    print(f"Automatically redirecting to the new command...\n")
    
    # Redirect to the proper financial statements command
    return fetch_financial_statements_command(ticker, quarterly=True)


def db_info_command(ticker: str) -> int:
    """
    Handle db-info command.
    
    Args:
        ticker: Stock ticker symbol
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        ticker = validate_ticker(ticker)
        
        # Initialize database
        db = DatabaseManager()
        
        # Get ticker info
        info = db.get_ticker_info(ticker)
        
        print(f"Database information for {ticker}:")
        print("-" * 40)
        
        if not info['exists']:
            print("No data found for this ticker.")
            return 0
        
        # Company info
        company = info['company']
        print(f"Company: {company['name'] or 'N/A'}")
        print(f"Sector: {company['sector'] or 'N/A'}")
        print(f"Industry: {company['industry'] or 'N/A'}")
        print(f"Country: {company['country'] or 'N/A'}")
        print(f"Currency: {company['currency']}")
        if company['market_cap']:
            print(f"Market Cap: {company['currency']} {company['market_cap']:,.0f}")
        print(f"Created: {company['created_at']}")
        
        # Price data info
        price_data = info['price_data']
        print(f"\nPrice data:")
        print(f"  Records: {price_data['count']}")
        
        if price_data['date_range']:
            min_date, max_date = price_data['date_range']
            print(f"  Date range: {min_date} to {max_date}")
        else:
            print("  Date range: No price data")
        
        # Financial statement data info
        statements = info['financial_statements']
        print(f"\nFinancial statements:")
        print(f"  Income Statements: {statements['income_statements']}")
        print(f"  Balance Sheets: {statements['balance_sheets']}")
        print(f"  Cash Flow Statements: {statements['cash_flows']}")
        print(f"  Financial Ratios: {statements['financial_ratios']}")
        
        total_financial = sum([
            statements['income_statements'],
            statements['balance_sheets'], 
            statements['cash_flows'],
            statements['financial_ratios']
        ])
        
        if total_financial > 0:
            print(f"\n✅ Financial analysis ready with {total_financial} total records")
        else:
            print(f"\n💡 Use 'fetch-financial-statements' command to get structured financial data")
        
        return 0
        
    except ValidationError as e:
        print(f"ERROR: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error in db_info_command: {e}", exc_info=True)
        print(f"ERROR: Unexpected error: {e}")
        return 1


def fetch_financial_statements_command(
    ticker: str,
    quarterly: bool = True
) -> int:
    """
    Handle fetch-financial-statements command using proper ETL pattern.
    
    Args:
        ticker: Stock ticker symbol
        quarterly: Whether to include quarterly data (default True)
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        ticker = validate_ticker(ticker)
        
        print(f"Running financial ETL pipeline for {ticker}...")
        if quarterly:
            print("Including both annual and quarterly data for comprehensive analysis.")
        else:
            print("Fetching annual data only.")
        
        # Initialize ETL orchestrator
        etl = ETLOrchestrator()
        
        # Run ETL pipeline
        etl_results = etl.run_financial_etl(ticker)
        
        # Report results
        if etl_results['status'] == 'completed':
            load_results = etl_results['phases']['load']['loaded_records']
            
            print(f"\n✅ ETL Pipeline completed successfully!")
            print(f"📊 Pipeline Summary:")
            print(f"  • Extract: {etl_results['phases']['extract']['data_sources_count']} data sources")
            print(f"  • Transform: {etl_results['phases']['transform']['statements_count']} statement types")
            
            # Show detailed load results
            print(f"  • Load Results:")
            total_records = sum(load_results.values())
            for record_type, count in load_results.items():
                print(f"    - {record_type.replace('_', ' ').title()}: {count} records")
            
            print(f"\n🎉 Operation completed successfully! Stored {total_records} total records.")
            print("✅ Data is now ready for rigorous financial analysis.")
        else:
            print(f"ERROR: ETL pipeline failed - {etl_results.get('error', 'Unknown error')}")
            if etl_results['phases'].get('load', {}).get('errors'):
                print("Load errors:")
                for error in etl_results['phases']['load']['errors']:
                    print(f"  - {error}")
            return 1
        
        return 0
        
    except ValidationError as e:
        print(f"ERROR: {e}")
        return 1
    except YahooFinanceError as e:
        print(f"ERROR: Failed to fetch financial statements for {ticker} from Yahoo Finance.")
        print(str(e))
        return 1
    except Exception as e:
        logger.error(f"Unexpected error in fetch_financial_statements_command: {e}", exc_info=True)
        print(f"ERROR: Unexpected error: {e}")
        return 1


def financial_summary_command(
    ticker: str,
    years: int = 5
) -> int:
    """
    Handle financial-summary command to show comprehensive financial overview.
    
    Args:
        ticker: Stock ticker symbol
        years: Number of recent years to include (default 5)
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        ticker = validate_ticker(ticker)
        
        validate_years_parameter(years, 1, 20)
        
        print(f"Generating {years}-year financial summary for {ticker}...")
        
        # Initialize database manager
        db = DatabaseManager()
        
        # Get comprehensive financial summary
        summary = db.get_company_financial_summary(ticker, years)
        
        if not summary:
            print(f"No financial data found for {ticker}.")
            print("Try running: market-data-etl fetch-financial-statements --ticker {ticker}")
            return 0
        
        company = summary['company']
        data_summary = summary['data_summary']
        latest = summary['latest_data']
        
        # Display company information
        print(f"\n🏢 {company['name']} ({company['ticker']})")
        print("=" * 60)
        print(f"Sector: {company['sector'] or 'N/A'}")
        print(f"Industry: {company['industry'] or 'N/A'}")
        print(f"Country: {company['country'] or 'N/A'}")
        print(f"Currency: {company['currency']}")
        if company['market_cap']:
            print(f"Market Cap: {company['currency']} {company['market_cap']:,.0f}")
        if company['employees']:
            print(f"Employees: {company['employees']:,}")
        
        # Display data availability
        print(f"\n📈 Financial Data Summary ({years} years)")
        print("-" * 40)
        print(f"Date Range: {data_summary['date_range']['from']} to {data_summary['date_range']['to']}")
        print(f"Income Statements: {data_summary['income_statements']}")
        print(f"Balance Sheets: {data_summary['balance_sheets']}")
        print(f"Cash Flow Statements: {data_summary['cash_flows']}")
        print(f"Financial Ratios: {data_summary['financial_ratios']}")
        
        # Display latest financial highlights
        if latest['income_statement']:
            income = latest['income_statement']
            print(f"\n💰 Latest Financial Highlights ({income.period_end_date})")
            print("-" * 50)
            if income.total_revenue:
                print(f"Total Revenue: {income.currency} {income.total_revenue:,.0f}")
            if income.operating_income:
                print(f"Operating Income: {income.currency} {income.operating_income:,.0f}")
            if income.net_income:
                print(f"Net Income: {income.currency} {income.net_income:,.0f}")
            if income.basic_eps:
                print(f"Earnings Per Share: {income.basic_eps:.2f}")
        
        if latest['balance_sheet']:
            balance = latest['balance_sheet']
            print(f"\n🏦 Balance Sheet Highlights ({balance.period_end_date})")
            print("-" * 50)
            if balance.total_assets:
                print(f"Total Assets: {balance.currency} {balance.total_assets:,.0f}")
            if balance.cash_and_equivalents:
                print(f"Cash & Equivalents: {balance.currency} {balance.cash_and_equivalents:,.0f}")
            if balance.total_debt:
                print(f"Total Debt: {balance.currency} {balance.total_debt:,.0f}")
            if balance.total_shareholders_equity:
                print(f"Shareholders Equity: {balance.currency} {balance.total_shareholders_equity:,.0f}")
        
        if latest['financial_ratios']:
            ratios = latest['financial_ratios']
            print(f"\n📊 Key Financial Ratios ({ratios.period_end_date})")
            print("-" * 50)
            if ratios.net_profit_margin:
                print(f"Net Profit Margin: {ratios.net_profit_margin:.2%}")
            if ratios.return_on_equity:
                print(f"Return on Equity: {ratios.return_on_equity:.2%}")
            if ratios.return_on_assets:
                print(f"Return on Assets: {ratios.return_on_assets:.2%}")
            if ratios.current_ratio:
                print(f"Current Ratio: {ratios.current_ratio:.2f}")
            if ratios.debt_to_equity:
                print(f"Debt to Equity: {ratios.debt_to_equity:.2f}")
        
        print(f"\n✅ Financial summary complete for {ticker}")
        return 0
        
    except ValidationError as e:
        print(f"ERROR: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error in financial_summary_command: {e}", exc_info=True)
        print(f"ERROR: Unexpected error: {e}")
        return 1


def clear_database_command(
    ticker: Optional[str] = None,
    clear_all: bool = False,
    confirm: bool = False
) -> int:
    """
    Handle clear-database command for development/testing.
    
    Args:
        ticker: Specific ticker to clear (optional)
        clear_all: Clear all data from database
        confirm: Skip confirmation prompt
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        # Validate ticker if provided
        if ticker:
            ticker = validate_ticker(ticker)
        
        # Initialize database manager
        db = DatabaseManager()
        
        # Determine what to clear
        if clear_all:
            operation = "all data from the database"
        else:
            operation = f"all data for ticker {ticker}"
        
        # Confirmation prompt unless --confirm flag is used
        if not confirm:
            print(f"⚠️  WARNING: This will permanently delete {operation}!")
            response = input("Are you sure you want to continue? (yes/no): ").lower().strip()
            if response not in ['yes', 'y']:
                print("Operation cancelled.")
                return SUCCESS_EXIT_CODE
        
        print(f"Clearing {operation}...")
        
        if clear_all:
            # Clear all data
            result = db.clear_all_data()
            if result:
                print("✅ Successfully cleared all data from database")
            else:
                print("❌ Failed to clear database")
                return ERROR_EXIT_CODE
        else:
            # Clear data for specific ticker
            result = db.clear_ticker_data(ticker)
            if result:
                print(f"✅ Successfully cleared all data for {ticker}")
            else:
                print(f"❌ No data found for {ticker} or failed to clear")
                return ERROR_EXIT_CODE
        
        return SUCCESS_EXIT_CODE
        
    except ValidationError as e:
        print(f"ERROR: {e}")
        return ERROR_EXIT_CODE
    except Exception as e:
        logger.error(f"Unexpected error in clear_database_command: {e}", exc_info=True)
        print(f"ERROR: Unexpected error: {e}")
        return ERROR_EXIT_CODE


# =============================================================================
# PORTFOLIO MANAGEMENT COMMANDS
# =============================================================================

def load_portfolio_command(file_path: str) -> int:
    """
    Handle load-portfolio command to load portfolio from JSON configuration.
    
    Args:
        file_path: Path to portfolio JSON configuration file
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        # Validate file path
        if not os.path.exists(file_path):
            print(f"ERROR: Portfolio configuration file not found: {file_path}")
            return ERROR_EXIT_CODE
        
        if not file_path.endswith('.json'):
            print(f"ERROR: Portfolio configuration file must be a JSON file: {file_path}")
            return ERROR_EXIT_CODE
        
        # Load and validate JSON
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                portfolio_config = json.load(f)
        except json.JSONDecodeError as e:
            print(f"ERROR: Invalid JSON in portfolio configuration file: {e}")
            return ERROR_EXIT_CODE
        except Exception as e:
            print(f"ERROR: Failed to read portfolio configuration file: {e}")
            return ERROR_EXIT_CODE
        
        # Validate required fields
        required_fields = ['name', 'currency', 'created_date', 'holdings']
        for field in required_fields:
            if field not in portfolio_config:
                print(f"ERROR: Missing required field in portfolio configuration: {field}")
                return ERROR_EXIT_CODE
        
        if not portfolio_config['holdings']:
            print("ERROR: Portfolio must contain at least one holding")
            return ERROR_EXIT_CODE
        
        print(f"Loading portfolio configuration: {portfolio_config['name']}")
        print(f"Holdings count: {len(portfolio_config['holdings'])}")
        
        # Initialize database manager
        db = DatabaseManager()
        
        # Load portfolio
        portfolio = db.load_portfolio_from_config(portfolio_config)
        
        print(f"✅ Successfully loaded portfolio: {portfolio.name}")
        print(f"📊 Holdings: {len(portfolio_config['holdings'])} instruments")
        
        # Show holdings breakdown
        holdings_by_type = {}
        for holding_info in portfolio_config['holdings'].values():
            instrument_type = holding_info.get('type', 'stock')
            holdings_by_type[instrument_type] = holdings_by_type.get(instrument_type, 0) + 1
        
        for instrument_type, count in holdings_by_type.items():
            print(f"  • {instrument_type.title()}: {count}")
        
        return SUCCESS_EXIT_CODE
        
    except DatabaseError as e:
        print(f"ERROR: Database error: {e}")
        return ERROR_EXIT_CODE
    except ValidationError as e:
        print(f"ERROR: {e}")
        return ERROR_EXIT_CODE
    except Exception as e:
        logger.error(f"Unexpected error in load_portfolio_command: {e}", exc_info=True)
        print(f"ERROR: Unexpected error: {e}")
        return ERROR_EXIT_CODE


def load_transactions_command(file_path: str, portfolio_name: Optional[str] = None) -> int:
    """
    Handle load-transactions command to load transactions from CSV file.
    
    Args:
        file_path: Path to transactions CSV file
        portfolio_name: Optional portfolio name to associate transactions with
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        # Validate file path
        if not os.path.exists(file_path):
            print(f"ERROR: Transactions CSV file not found: {file_path}")
            return ERROR_EXIT_CODE
        
        if not file_path.endswith('.csv'):
            print(f"ERROR: Transactions file must be a CSV file: {file_path}")
            return ERROR_EXIT_CODE
        
        # Load and validate CSV
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                transactions_data = list(reader)
        except Exception as e:
            print(f"ERROR: Failed to read transactions CSV file: {e}")
            return ERROR_EXIT_CODE
        
        if not transactions_data:
            print("ERROR: Transactions CSV file is empty")
            return ERROR_EXIT_CODE
        
        # Validate required columns
        required_columns = ['date', 'ticker', 'transaction_type', 'quantity', 'price_per_unit', 'currency']
        first_row = transactions_data[0]
        for column in required_columns:
            if column not in first_row:
                print(f"ERROR: Missing required column in CSV: {column}")
                return ERROR_EXIT_CODE
        
        print(f"Loading {len(transactions_data)} transactions from CSV")
        if portfolio_name:
            print(f"Associating with portfolio: {portfolio_name}")
        
        # Initialize database manager
        db = DatabaseManager()
        
        # Load transactions
        loaded_count = db.load_transactions_from_csv(transactions_data, portfolio_name)
        
        print(f"✅ Successfully loaded {loaded_count} transactions")
        
        return SUCCESS_EXIT_CODE
        
    except DatabaseError as e:
        print(f"ERROR: Database error: {e}")
        return ERROR_EXIT_CODE
    except ValidationError as e:
        print(f"ERROR: {e}")
        return ERROR_EXIT_CODE
    except Exception as e:
        logger.error(f"Unexpected error in load_transactions_command: {e}", exc_info=True)
        print(f"ERROR: Unexpected error: {e}")
        return ERROR_EXIT_CODE


def fetch_portfolio_prices_command(
    portfolio_name: str,
    from_date: str,
    to_date: Optional[str] = None
) -> int:
    """
    Handle fetch-portfolio-prices command to fetch price data for all portfolio holdings.
    
    Args:
        portfolio_name: Portfolio name
        from_date: Start date in YYYY-MM-DD format
        to_date: End date in YYYY-MM-DD format (optional)
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        # Validate dates
        start_date = validate_date_string(from_date, "from_date")
        end_date = validate_date_string(to_date, "to_date") if to_date else date.today()
        validate_date_range(start_date, end_date)
        
        print(f"Fetching portfolio prices for: {portfolio_name}")
        print(f"Date range: {start_date} to {end_date}")
        
        # Initialize components
        db = DatabaseManager()
        etl = ETLOrchestrator()
        
        # Get portfolio summary
        portfolio_summary = db.get_portfolio_summary(portfolio_name)
        if not portfolio_summary['exists']:
            print(f"ERROR: Portfolio '{portfolio_name}' not found")
            return ERROR_EXIT_CODE
        
        # Get portfolio holdings with ticker information
        with db.get_session() as session:
            from ..data.models import Portfolio, PortfolioHolding, Company
            
            portfolio = session.query(Portfolio).filter(
                Portfolio.name == portfolio_name
            ).first()
            
            holdings = session.query(PortfolioHolding, Company).join(
                Company, PortfolioHolding.company_id == Company.id
            ).filter(PortfolioHolding.portfolio_id == portfolio.id).all()
            
            if not holdings:
                print("ERROR: Portfolio has no holdings")
                return ERROR_EXIT_CODE
        
        print(f"Processing {len(holdings)} holdings...")
        
        # Track results and errors
        results = []
        errors = []
        
        for holding, company in holdings:
            ticker = company.ticker_symbol
            
            # Skip instruments without yahoo ticker
            if not ticker or ticker == 'None':
                if company.isin:
                    print(f"⚠️  Skipping {company.company_name} ({company.isin}): No Yahoo ticker available")
                    errors.append(f"No Yahoo ticker for {company.company_name}")
                else:
                    print(f"⚠️  Skipping {company.company_name}: No ticker or ISIN available")
                    errors.append(f"No ticker for {company.company_name}")
                continue
            
            try:
                print(f"📈 Fetching {ticker} ({company.company_name})...")
                
                # Check for existing data
                existing_dates = db.get_existing_price_dates(ticker)
                missing_dates = find_missing_dates_in_range(existing_dates, start_date, end_date)
                
                if not missing_dates:
                    print(f"  ✓ All data already exists for {ticker}")
                    results.append({'ticker': ticker, 'status': 'up_to_date', 'records': len(existing_dates)})
                    continue
                
                print(f"  • Processing {len(missing_dates)} missing days...")
                
                # Run ETL pipeline
                earliest_missing = min(missing_dates)
                latest_missing = max(missing_dates)
                etl_results = etl.run_price_etl(ticker, earliest_missing, latest_missing)
                
                if etl_results['status'] == 'completed':
                    loaded_records = etl_results['phases']['load']['loaded_records']
                    print(f"  ✓ Loaded {loaded_records} records for {ticker}")
                    results.append({'ticker': ticker, 'status': 'success', 'records': loaded_records})
                else:
                    error_msg = etl_results.get('error', 'Unknown error')
                    print(f"  ❌ Failed to fetch {ticker}: {error_msg}")
                    errors.append(f"{ticker}: {error_msg}")
                
            except Exception as e:
                error_msg = str(e)
                print(f"  ❌ Error processing {ticker}: {error_msg}")
                errors.append(f"{ticker}: {error_msg}")
        
        # Report final results
        print(f"\n📊 Portfolio Price Fetch Summary:")
        print(f"Holdings processed: {len(holdings)}")
        
        successful = [r for r in results if r['status'] in ['success', 'up_to_date']]
        if successful:
            total_records = sum(r['records'] for r in successful)
            print(f"✅ Successful: {len(successful)} holdings, {total_records} total records")
        
        if errors:
            print(f"❌ Errors: {len(errors)}")
            for error in errors:
                print(f"  • {error}")
            return ERROR_EXIT_CODE
        
        print("🎉 Portfolio price fetch completed successfully!")
        return SUCCESS_EXIT_CODE
        
    except ValidationError as e:
        print(f"ERROR: {e}")
        return ERROR_EXIT_CODE
    except DatabaseError as e:
        print(f"ERROR: Database error: {e}")
        return ERROR_EXIT_CODE
    except Exception as e:
        logger.error(f"Unexpected error in fetch_portfolio_prices_command: {e}", exc_info=True)
        print(f"ERROR: Unexpected error: {e}")
        return ERROR_EXIT_CODE


def fetch_portfolio_fundamentals_command(portfolio_name: str) -> int:
    """
    Handle fetch-portfolio-fundamentals command to fetch fundamental data for stocks in portfolio.
    Automatically skips funds and ETFs.
    
    Args:
        portfolio_name: Portfolio name
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        print(f"Fetching portfolio fundamentals for: {portfolio_name}")
        print("Note: Funds and ETFs will be automatically skipped")
        
        # Initialize components
        db = DatabaseManager()
        etl = ETLOrchestrator()
        
        # Get portfolio summary
        portfolio_summary = db.get_portfolio_summary(portfolio_name)
        if not portfolio_summary['exists']:
            print(f"ERROR: Portfolio '{portfolio_name}' not found")
            return ERROR_EXIT_CODE
        
        # Get portfolio holdings - only stocks
        with db.get_session() as session:
            from ..data.models import Portfolio, PortfolioHolding, Company
            
            portfolio = session.query(Portfolio).filter(
                Portfolio.name == portfolio_name
            ).first()
            
            # Filter for stocks only
            stock_holdings = session.query(PortfolioHolding, Company).join(
                Company, PortfolioHolding.company_id == Company.id
            ).filter(
                PortfolioHolding.portfolio_id == portfolio.id,
                Company.instrument_type == InstrumentType.STOCK
            ).all()
            
            total_holdings = session.query(PortfolioHolding).filter(
                PortfolioHolding.portfolio_id == portfolio.id
            ).count()
        
        print(f"Found {len(stock_holdings)} stocks out of {total_holdings} total holdings")
        
        if not stock_holdings:
            print("No stocks found in portfolio for fundamental data fetching")
            return SUCCESS_EXIT_CODE
        
        # Track results and errors
        results = []
        errors = []
        
        for holding, company in stock_holdings:
            ticker = company.ticker_symbol
            
            # Skip instruments without yahoo ticker
            if not ticker or ticker == 'None':
                print(f"⚠️  Skipping {company.company_name}: No Yahoo ticker available")
                errors.append(f"No Yahoo ticker for {company.company_name}")
                continue
            
            try:
                print(f"📊 Fetching fundamentals for {ticker} ({company.company_name})...")
                
                # Run financial ETL pipeline
                etl_results = etl.run_financial_etl(ticker)
                
                if etl_results['status'] == 'completed':
                    load_results = etl_results['phases']['load']['loaded_records']
                    total_records = sum(load_results.values())
                    print(f"  ✓ Loaded {total_records} financial records for {ticker}")
                    results.append({'ticker': ticker, 'status': 'success', 'records': total_records})
                else:
                    error_msg = etl_results.get('error', 'Unknown error')
                    print(f"  ❌ Failed to fetch {ticker}: {error_msg}")
                    errors.append(f"{ticker}: {error_msg}")
                
            except Exception as e:
                error_msg = str(e)
                print(f"  ❌ Error processing {ticker}: {error_msg}")
                errors.append(f"{ticker}: {error_msg}")
        
        # Report final results
        print(f"\n📊 Portfolio Fundamentals Fetch Summary:")
        print(f"Stocks processed: {len(stock_holdings)}")
        
        successful = [r for r in results if r['status'] == 'success']
        if successful:
            total_records = sum(r['records'] for r in successful)
            print(f"✅ Successful: {len(successful)} stocks, {total_records} total records")
        
        if errors:
            print(f"❌ Errors: {len(errors)}")
            for error in errors:
                print(f"  • {error}")
            return ERROR_EXIT_CODE
        
        print("🎉 Portfolio fundamentals fetch completed successfully!")
        return SUCCESS_EXIT_CODE
        
    except DatabaseError as e:
        print(f"ERROR: Database error: {e}")
        return ERROR_EXIT_CODE
    except Exception as e:
        logger.error(f"Unexpected error in fetch_portfolio_fundamentals_command: {e}", exc_info=True)
        print(f"ERROR: Unexpected error: {e}")
        return ERROR_EXIT_CODE


def portfolio_info_command(portfolio_name: str) -> int:
    """
    Handle portfolio-info command to show portfolio information and holdings summary.
    
    Args:
        portfolio_name: Portfolio name
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        # Initialize database manager
        db = DatabaseManager()
        
        # Get portfolio summary
        portfolio_summary = db.get_portfolio_summary(portfolio_name)
        if not portfolio_summary['exists']:
            print(f"Portfolio '{portfolio_name}' not found.")
            return SUCCESS_EXIT_CODE
        
        portfolio = portfolio_summary['portfolio']
        holdings = portfolio_summary['holdings']
        transactions = portfolio_summary['transactions']
        
        print(f"Portfolio Information: {portfolio['name']}")
        print("=" * 50)
        print(f"Description: {portfolio['description'] or 'N/A'}")
        print(f"Currency: {portfolio['currency']}")
        print(f"Created: {portfolio['created_date']}")
        print(f"Last Updated: {portfolio['created_at']}")
        
        print(f"\n📈 Holdings Summary:")
        print(f"Total Holdings: {holdings['total_count']}")
        
        if holdings['breakdown']:
            print("Breakdown by Type:")
            for instrument_type, count in holdings['breakdown'].items():
                print(f"  • {instrument_type.title()}: {count}")
        
        print(f"\n💰 Transactions:")
        print(f"Total Transactions: {transactions['count']}")
        
        # Get detailed holdings information
        with db.get_session() as session:
            from ..data.models import Portfolio, PortfolioHolding, Company
            
            portfolio_db = session.query(Portfolio).filter(
                Portfolio.name == portfolio_name
            ).first()
            
            detailed_holdings = session.query(PortfolioHolding, Company).join(
                Company, PortfolioHolding.company_id == Company.id
            ).filter(PortfolioHolding.portfolio_id == portfolio_db.id).all()
            
            if detailed_holdings:
                print(f"\n🏢 Holdings Details:")
                print("-" * 80)
                print(f"{'Ticker':<15} {'Name':<30} {'Type':<10} {'Country':<8} {'Sector':<20}")
                print("-" * 80)
                
                for holding, company in detailed_holdings:
                    ticker = company.ticker_symbol or company.isin or 'N/A'
                    name = company.company_name[:28] + '..' if len(company.company_name) > 30 else company.company_name
                    instrument_type = company.instrument_type.value if company.instrument_type else 'N/A'
                    country = company.country or 'N/A'
                    sector = (company.sector or '')[:18] + '..' if len(company.sector or '') > 20 else (company.sector or 'N/A')
                    
                    print(f"{ticker:<15} {name:<30} {instrument_type:<10} {country:<8} {sector:<20}")
        
        return SUCCESS_EXIT_CODE
        
    except DatabaseError as e:
        print(f"ERROR: Database error: {e}")
        return ERROR_EXIT_CODE
    except Exception as e:
        logger.error(f"Unexpected error in portfolio_info_command: {e}", exc_info=True)
        print(f"ERROR: Unexpected error: {e}")
        return ERROR_EXIT_CODE


def fetch_economic_command(
    source: str,
    indicator: str,
    from_date: str,
    to_date: Optional[str] = None,
    api_key: Optional[str] = None
) -> int:
    """
    Handle fetch-economic command to fetch economic data from various sources.
    
    Args:
        source: Data source (eurostat, ecb, fred)
        indicator: Indicator code or ID
        from_date: Start date in YYYY-MM-DD format
        to_date: End date in YYYY-MM-DD format (required for ECB and FRED)
        api_key: API key (required for FRED)
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        # Import economic ETL components
        from ..etl.load import EconomicETLOrchestrator
        
        # Validate dates
        start_date_obj = validate_date_string(from_date, "from_date")
        if to_date:
            end_date_obj = validate_date_string(to_date, "to_date")
            validate_date_range(start_date_obj, end_date_obj)
        
        # Validate source
        valid_sources = ['eurostat', 'ecb', 'fred']
        if source.lower() not in valid_sources:
            print(f"ERROR: Invalid source '{source}'. Valid sources: {', '.join(valid_sources)}")
            return ERROR_EXIT_CODE
        
        source = source.lower()
        
        # Source-specific validation
        if source in ['ecb', 'fred'] and not to_date:
            print(f"ERROR: --to date is required for {source.upper()} data source")
            return ERROR_EXIT_CODE
        
        # For FRED, use environment variable if api_key not provided
        if source == 'fred':
            if not api_key:
                from ..config import config
                api_key = config.api.fred_api_key
                if not api_key:
                    print("ERROR: FRED API key required but not found")
                    print("Either:")
                    print("  1. Set FRED_API_KEY environment variable, or")
                    print("  2. Use --api-key parameter")
                    print("Get your free API key from: https://fred.stlouisfed.org/docs/api/api_key.html")
                    return ERROR_EXIT_CODE
        
        print(f"Fetching {source.upper()} economic data for indicator: {indicator}")
        print(f"Date range: {from_date} to {to_date or 'present'}")
        
        # Initialize ETL orchestrator
        etl = EconomicETLOrchestrator()
        
        # Run appropriate ETL pipeline based on source
        if source == 'eurostat':
            etl_results = etl.run_eurostat_etl(indicator, from_date)
        elif source == 'ecb':
            # ECB requires parsing the indicator into dataflow_ref and series_key
            if '.' not in indicator:
                print("ERROR: ECB indicator must be in format 'dataflow_ref.series_key' (e.g., 'FM.B.U2.EUR.4F.KR.MRR_FR.LEV')")
                return ERROR_EXIT_CODE
            
            parts = indicator.split('.', 1)  # Split on first dot only
            dataflow_ref = parts[0]
            series_key = parts[1]
            etl_results = etl.run_ecb_etl(dataflow_ref, series_key, from_date, to_date)
        elif source == 'fred':
            etl_results = etl.run_fred_etl(indicator, api_key, from_date, to_date)
        
        # Report results
        if etl_results['status'] == 'completed':
            load_phase = etl_results['phases']['load']
            loaded_records = load_phase['loaded_records']
            indicators_count = loaded_records.get('indicators', 0)
            data_points_count = loaded_records.get('data_points', 0)
            
            print(f"✅ Successfully processed {source.upper()} data for {indicator}")
            print(f"📊 Loaded: {indicators_count} indicator(s), {data_points_count} data points")
            
            # Show data range if available
            transform_phase = etl_results['phases']['transform']
            if transform_phase.get('data_points_count', 0) > 0:
                print(f"📈 Data points retrieved: {transform_phase['data_points_count']}")
        else:
            error_msg = etl_results.get('error', 'Unknown error')
            print(f"❌ Failed to fetch {source.upper()} data for {indicator}")
            print(f"Error: {error_msg}")
            return ERROR_EXIT_CODE
        
        return SUCCESS_EXIT_CODE
        
    except ValidationError as e:
        print(f"ERROR: {e}")
        return ERROR_EXIT_CODE
    except Exception as e:
        logger.error(f"Unexpected error in fetch_economic_command: {e}", exc_info=True)
        print(f"ERROR: Unexpected error: {e}")
        return ERROR_EXIT_CODE


def economic_info_command(indicator_name: str) -> int:
    """
    Handle economic-info command to show information about an economic indicator.
    
    Args:
        indicator_name: Economic indicator name (standardized identifier)
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        from ..database.manager import DatabaseManager
        
        print(f"Economic indicator information for: {indicator_name}")
        
        # Initialize database manager
        db = DatabaseManager()
        
        # Get indicator info
        indicator_info = db.get_economic_indicator_info(indicator_name)
        
        if not indicator_info['exists']:
            print(f"Economic indicator '{indicator_name}' not found in database.")
            print("Use 'fetch-economic' command to retrieve data first.")
            return SUCCESS_EXIT_CODE
        
        # Display indicator information
        indicator = indicator_info['indicator']
        data_info = indicator_info['data_points']
        thresholds = indicator_info.get('thresholds', [])
        
        print("=" * 60)
        print(f"Name: {indicator['name']}")
        print(f"Description: {indicator['description'] or 'N/A'}")
        print(f"Source: {indicator['source'].upper()}")
        print(f"Source Identifier: {indicator['source_identifier']}")
        print(f"Unit: {indicator['unit']}")
        print(f"Frequency: {indicator['frequency']}")
        print(f"Created: {indicator['created_at']}")
        print(f"Last Updated: {indicator['updated_at']}")
        
        print(f"\n📊 Data Points:")
        print(f"Total Records: {data_info['count']}")
        if data_info['count'] > 0 and data_info['date_range']:
            earliest_date, latest_date = data_info['date_range']
            print(f"Date Range: {earliest_date} to {latest_date}")
        
        # Show thresholds if available
        if thresholds:
            print(f"\n🎯 Analysis Thresholds:")
            for threshold in thresholds:
                category = threshold['category'].title()
                min_val = f"{threshold['min_value']:.2f}" if threshold['min_value'] is not None else "None"
                max_val = f"{threshold['max_value']:.2f}" if threshold['max_value'] is not None else "None"
                print(f"  • {category}: {min_val} to {max_val}")
        
        # Get recent data points
        df = db.get_economic_data(indicator_name, from_date=None)
        if not df.empty:
            print(f"\n📈 Recent Data (last 10 points):")
            print("-" * 30)
            print(f"{'Date':<12} {'Value':<10}")
            print("-" * 30)
            
            # Sort by date descending for recent data
            recent_data = df.sort_values('date', ascending=False).head(10)
            for _, row in recent_data.iterrows():
                date_str = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])
                value_str = f"{row['value']:.2f}" if row['value'] is not None else 'N/A'
                print(f"{date_str:<12} {value_str:<10}")
        
        return SUCCESS_EXIT_CODE
        
    except Exception as e:
        logger.error(f"Unexpected error in economic_info_command: {e}", exc_info=True)
        print(f"ERROR: Unexpected error: {e}")
        return ERROR_EXIT_CODE


def load_price_csv_command(file_path: str, ticker: str) -> int:
    """
    Handle load-price-csv command to import price data from CSV file.
    
    Args:
        file_path: Path to CSV file with price data
        ticker: Ticker symbol for the data
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        from ..database.manager import DatabaseManager
        import pandas as pd
        import os
        
        print(f"Loading price data from CSV for {ticker}")
        print(f"File: {file_path}")
        
        # Validate file exists
        if not os.path.exists(file_path):
            print(f"ERROR: File not found: {file_path}")
            return ERROR_EXIT_CODE
        
        # Initialize database
        db = DatabaseManager()
        
        # Read and validate CSV
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Read CSV file with {len(df)} rows")
        except Exception as e:
            print(f"ERROR: Failed to read CSV file: {e}")
            return ERROR_EXIT_CODE
        
        # Validate required columns
        required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"ERROR: Missing required columns: {missing_columns}")
            print(f"Required columns: {required_columns}")
            print(f"Found columns: {list(df.columns)}")
            return ERROR_EXIT_CODE
        
        # Convert date column to proper format
        try:
            df['date'] = pd.to_datetime(df['date']).dt.date
        except Exception as e:
            print(f"ERROR: Failed to parse date column: {e}")
            return ERROR_EXIT_CODE
        
        # Validate data types and ranges
        numeric_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_columns:
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            except Exception as e:
                print(f"ERROR: Failed to convert {col} to numeric: {e}")
                return ERROR_EXIT_CODE
        
        # Check for missing values after conversion
        if df[required_columns].isnull().any().any():
            print("ERROR: Found missing or invalid values in required columns")
            return ERROR_EXIT_CODE
        
        # Store data using existing infrastructure
        inserted_count = db.store_price_data(ticker, df)
        
        print(f"✅ Successfully loaded {inserted_count} price records for {ticker}")
        print("Operation completed successfully.")
        
        return SUCCESS_EXIT_CODE
        
    except Exception as e:
        logger.error(f"Unexpected error in load_price_csv_command: {e}", exc_info=True)
        print(f"ERROR: Unexpected error: {e}")
        return ERROR_EXIT_CODE


def generate_price_csv_template_command(ticker: str, output_file: str) -> int:
    """
    Generate a CSV template file for manual price data entry.
    
    Args:
        ticker: Ticker symbol (for filename reference)
        output_file: Output CSV file path
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        import pandas as pd
        from datetime import date, timedelta
        
        logger = get_logger(__name__)
        
        print(f"Generating CSV template for {ticker}")
        print(f"Output: {output_file}")
        
        # Create sample data with recent dates
        today = date.today()
        dates = [today - timedelta(days=i) for i in range(5, 0, -1)]
        
        # Create template DataFrame with example data
        template_data = {
            'date': [d.strftime('%Y-%m-%d') for d in dates],
            'open': [100.00, 101.50, 99.75, 102.25, 103.10],
            'high': [102.00, 103.25, 101.50, 104.00, 105.50],
            'low': [99.50, 100.75, 98.25, 101.50, 102.75],
            'close': [101.50, 99.75, 102.25, 103.10, 104.85],
            'volume': [1000000, 1250000, 875000, 1500000, 1100000]
        }
        
        df = pd.DataFrame(template_data)
        
        # Write CSV file
        try:
            df.to_csv(output_file, index=False)
            logger.info(f"Generated CSV template at {output_file}")
        except Exception as e:
            print(f"ERROR: Failed to write CSV file: {e}")
            return ERROR_EXIT_CODE
        
        print(f"✅ CSV template generated successfully!")
        print(f"\nTemplate format:")
        print(f"  - date: YYYY-MM-DD format")
        print(f"  - open, high, low, close: Price values")
        print(f"  - volume: Number of shares traded")
        print(f"\nEdit the file with your data and use:")
        print(f"  market-data-etl load-price-csv --file {output_file} --ticker {ticker}")
        
        return SUCCESS_EXIT_CODE
        
    except Exception as e:
        logger = get_logger(__name__)
        logger.error(f"Unexpected error in generate_price_csv_template_command: {e}", exc_info=True)
        print(f"ERROR: Unexpected error: {e}")
        return ERROR_EXIT_CODE