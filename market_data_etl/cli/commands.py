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
import pandas as pd

from ..etl.load import ETLOrchestrator
from ..database.manager import DatabaseManager
from ..utils.logging import get_logger
from ..utils.exceptions import YahooFinanceError, ValidationError, DatabaseError
from ..utils.validation import validate_ticker, validate_date_string, validate_date_range, validate_years_parameter
from ..utils.error_handlers import SUCCESS_EXIT_CODE, ERROR_EXIT_CODE
from ..data.models import InstrumentType
from ..data.fetchers import detect_instrument_type, detect_from_symbol_pattern
import yfinance as yf

logger = get_logger(__name__)

# Status messages
PERIOD_TYPE_ANNUAL = 'annual'
PERIOD_TYPE_QUARTERLY = 'quarterly'


# =============================================================================
# BUSINESS LOGIC FUNCTIONS
# =============================================================================

def should_fetch_fundamentals(instrument_type: InstrumentType) -> bool:
    """
    Determine if fundamental data should be fetched for an instrument type.
    
    Only fetch fundamentals for stocks as indices, funds, ETFs, and 
    commodities don't have traditional fundamental data.
    
    Args:
        instrument_type: The type of financial instrument
        
    Returns:
        True if fundamentals should be fetched, False otherwise
    """
    return instrument_type == InstrumentType.STOCK


def should_fetch_constituents(instrument_type: InstrumentType) -> bool:
    """
    Determine if constituent data should be fetched for an instrument type.
    
    Fetch constituents for indices and some ETFs that track specific 
    underlying assets.
    
    Args:
        instrument_type: The type of financial instrument
        
    Returns:
        True if constituents should be fetched, False otherwise
    """
    return instrument_type in [InstrumentType.INDEX, InstrumentType.ETF]


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
    to_date: Optional[str] = None,
    instrument_type: Optional[str] = None,
    prices_only: bool = False
) -> int:
    """
    Handle fetch-prices command using proper ETL pattern.
    
    Args:
        ticker: Ticker symbol
        from_date: Start date in YYYY-MM-DD format
        to_date: End date in YYYY-MM-DD format (optional)
        instrument_type: Manual instrument type override (optional)
        prices_only: Only fetch price data, skip automatic financial statements (optional)
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        ticker = validate_ticker(ticker)
        start_date = validate_date_string(from_date, "from_date")
        end_date = validate_date_string(to_date, "to_date") if to_date else date.today()
        
        validate_date_range(start_date, end_date)
        
        # Handle manual instrument type override
        manual_instrument_type = None
        if instrument_type:
            try:
                manual_instrument_type = InstrumentType(instrument_type)
                logger.info(f"Manual override: Setting instrument type to {manual_instrument_type.value}")
            except ValueError:
                logger.error(f"Invalid instrument type '{instrument_type}'. Valid options: {[t.value for t in InstrumentType]}")
                return ERROR_EXIT_CODE
        
        logger.info(f"Starting price fetch for {ticker} from {start_date} to {end_date}")
        
        # Initialize ETL orchestrator
        etl = ETLOrchestrator()
        
        # Check existing data to avoid unnecessary fetches
        db = DatabaseManager()
        existing_dates = db.get_existing_price_dates(ticker)
        missing_dates = find_missing_dates_in_range(existing_dates, start_date, end_date)
        
        if not missing_dates:
            logger.info("Database already contains data for the full range. Nothing to fetch.")
            return SUCCESS_EXIT_CODE
        
        logger.info(f"Processing {len(missing_dates)} missing days for {ticker}")
        
        # Run ETL pipeline
        earliest_missing = min(missing_dates)
        latest_missing = max(missing_dates)
        
        etl_results = etl.run_price_etl(ticker, earliest_missing, latest_missing, manual_instrument_type)
        
        # Report results
        if etl_results['status'] == 'completed':
            loaded_records = etl_results['phases']['load']['loaded_records']
            logger.info(f"Fetched {loaded_records} prices for {ticker}, stored {loaded_records}")
            print(f"\n✅ Price ETL Pipeline completed successfully for {ticker}!")
            print(f"📊 Price Pipeline Summary:")
            print(f"  • Extract: {etl_results['phases']['extract']['shape']} records")
            print(f"  • Transform: {etl_results['phases']['transform']['record_count']} records")
            print(f"  • Load: {loaded_records} records stored")
            
            # Automatically fetch financial statements for stocks (unless prices_only is True)
            if not prices_only:
                # Get instrument info to check if it's a stock
                db = DatabaseManager()
                instrument_info = db.get_instrument_info(ticker)
                
                if instrument_info and instrument_info.get('instrument_type'):
                    instrument_type_enum = InstrumentType(instrument_info['instrument_type'])
                    
                    if should_fetch_fundamentals(instrument_type_enum):
                        logger.info(f"Fetching financial statements for {ticker}")
                        
                        fin_result = fetch_financial_statements_command(ticker=ticker, quarterly=True)
                        
                        if fin_result == SUCCESS_EXIT_CODE:
                            logger.info(f"Updated financial statements for {ticker}")
                        else:
                            logger.warning(f"Financial statements update failed for {ticker}, but price data was successful")
                    else:
                        logger.info(f"Skipping financial statements for {ticker} ({instrument_type_enum.value}) - not applicable")
                else:
                    logger.warning(f"Could not determine instrument type for automatic financial data fetch")
            else:
                logger.info(f"Skipping automatic financial statements (--prices-only flag used)")
            
            logger.info(f"Completed price fetch for {ticker}")
        else:
            logger.error(f"ETL pipeline failed - {etl_results.get('error', 'Unknown error')}")
            return ERROR_EXIT_CODE
        
        return SUCCESS_EXIT_CODE
        
    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        return ERROR_EXIT_CODE
    except YahooFinanceError as e:
        logger.error(f"Failed to fetch prices for {ticker} from Yahoo Finance: {e}")
        return ERROR_EXIT_CODE
    except Exception as e:
        logger.error(f"Unexpected error in fetch_prices_command: {e}", exc_info=True)
        return ERROR_EXIT_CODE


def fetch_fundamentals_command(ticker: str) -> int:
    """
    Handle fetch-fundamentals command (legacy - redirects to financial statements).
    
    Args:
        ticker: Stock ticker symbol
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    logger.warning(f"Legacy command detected")
    logger.warning(f"The 'fetch-fundamentals' command is deprecated")
    logger.info(f"For structured financial analysis, use: fetch-financial-statements --ticker {ticker}")
    logger.info(f"Automatically redirecting to the new command")
    
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
        
        # Get instrument info
        info = db.get_instrument_info(ticker)
        
        print(f"Database information for {ticker}:")
        print("-" * 40)
        
        if not info['exists']:
            logger.info("No data found for this ticker")
            print("No data found for this ticker.")
            return 0
        
        # Instrument info
        instrument = info['instrument']
        print(f"Instrument: {instrument['name'] or 'N/A'}")
        print(f"Sector: {instrument['sector'] or 'N/A'}")
        print(f"Industry: {instrument['industry'] or 'N/A'}")
        print(f"Country: {instrument['country'] or 'N/A'}")
        print(f"Currency: {instrument['currency']}")
        if instrument['market_cap']:
            print(f"Market Cap: {instrument['currency']} {instrument['market_cap']:,.0f}")
        print(f"Created: {instrument['created_at']}")
        
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
            logger.info(f"Financial analysis ready with {total_financial} total records")
            print(f"\n✅ Financial analysis ready with {total_financial} total records")
        else:
            print(f"\n💡 Use 'fetch-financial-statements' command to get structured financial data")
        
        return 0
        
    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error in db_info_command: {e}", exc_info=True)
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
        
        # Check if this instrument type should have fundamentals fetched
        db = DatabaseManager()
        instrument_info = db.get_instrument_info(ticker)
        
        if instrument_info and instrument_info.get('instrument_type'):
            instrument_type = InstrumentType(instrument_info['instrument_type'])
            if not should_fetch_fundamentals(instrument_type):
                logger.warning(f"Skipping fundamental data for {ticker} (instrument type: {instrument_type.value})")
                print("Fundamental data is only available for stocks.")
                print("Indices, funds, ETFs, and commodities don't have traditional fundamental data.")
                return SUCCESS_EXIT_CODE
        
        logger.info(f"Starting financial data fetch for {ticker}")
        if quarterly:
            logger.info("Including both annual and quarterly data for comprehensive analysis")
        else:
            logger.info("Fetching annual data only")
        
        # Initialize ETL orchestrator
        etl = ETLOrchestrator()
        
        # Run ETL pipeline
        etl_results = etl.run_financial_etl(ticker)
        
        # Report results
        if etl_results['status'] == 'completed':
            load_results = etl_results['phases']['load']['loaded_records']
            
            logger.info(f"Completed financial data fetch for {ticker}")
            print(f"📊 Pipeline Summary:")
            print(f"  • Extract: {etl_results['phases']['extract']['data_sources_count']} data sources")
            print(f"  • Transform: {etl_results['phases']['transform']['statements_count']} statement types")
            
            # Show detailed load results
            print(f"  • Load Results:")
            total_records = sum(load_results.values())
            for record_type, count in load_results.items():
                print(f"    - {record_type.replace('_', ' ').title()}: {count} records")
            
            logger.info(f"Stored {total_records} financial records for {ticker}")
            print(f"✅ Data is now ready for rigorous financial analysis of {ticker}.")
        else:
            logger.error(f"ETL pipeline failed - {etl_results.get('error', 'Unknown error')}")
            if etl_results['phases'].get('load', {}).get('errors'):
                logger.error("Load errors:")
                for error in etl_results['phases']['load']['errors']:
                    logger.error(f"  - {error}")
            return 1
        
        return 0
        
    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        return 1
    except YahooFinanceError as e:
        logger.error(f"Failed to fetch financial statements for {ticker} from Yahoo Finance: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error in fetch_financial_statements_command: {e}", exc_info=True)
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
        summary = db.get_instrument_financial_summary(ticker, years)
        
        if not summary:
            print(f"No financial data found for {ticker}.")
            print("Try running: market-data-etl fetch-financial-statements --ticker {ticker}")
            return 0
        
        instrument = summary['instrument']
        data_summary = summary['data_summary']
        latest = summary['latest_data']
        
        # Display instrument information
        print(f"\n🏢 {instrument['name']} ({instrument['ticker']})")
        print("=" * 60)
        print(f"Sector: {instrument['sector'] or 'N/A'}")
        print(f"Industry: {instrument['industry'] or 'N/A'}")
        print(f"Country: {instrument['country'] or 'N/A'}")
        print(f"Currency: {instrument['currency']}")
        if instrument['market_cap']:
            print(f"Market Cap: {instrument['currency']} {instrument['market_cap']:,.0f}")
        if instrument['employees']:
            print(f"Employees: {instrument['employees']:,}")
        
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
        required_fields = ['name', 'holdings']
        for field in required_fields:
            if field not in portfolio_config:
                print(f"ERROR: Missing required field in portfolio configuration: {field}")
                return ERROR_EXIT_CODE
        
        if not portfolio_config['holdings'] or not isinstance(portfolio_config['holdings'], list):
            print("ERROR: Portfolio must contain at least one ticker in holdings array")
            return ERROR_EXIT_CODE
        
        print(f"Loading portfolio configuration: {portfolio_config['name']}")
        print(f"Holdings count: {len(portfolio_config['holdings'])}")
        
        # Initialize database manager
        db = DatabaseManager()
        
        # Load portfolio
        portfolio = db.load_portfolio_from_config(portfolio_config)
        
        print(f"✅ Successfully loaded portfolio: {portfolio.name}")
        print(f"📊 Holdings: {len(portfolio_config['holdings'])} instruments")
        
        # Show tickers list
        tickers = portfolio_config['holdings']
        if len(tickers) <= 10:
            print(f"Tickers: {', '.join(tickers)}")
        else:
            print(f"Tickers: {', '.join(tickers[:8])}, ... and {len(tickers)-8} more")
        
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
            from ..data.models import Portfolio, PortfolioHolding, Instrument
            
            portfolio = session.query(Portfolio).filter(
                Portfolio.name == portfolio_name
            ).first()
            
            holdings = session.query(PortfolioHolding, Instrument).join(
                Instrument, PortfolioHolding.instrument_id == Instrument.id
            ).filter(PortfolioHolding.portfolio_id == portfolio.id).all()
            
            if not holdings:
                print("ERROR: Portfolio has no holdings")
                return ERROR_EXIT_CODE
        
        print(f"Processing {len(holdings)} holdings...")
        
        # Track results and errors
        results = []
        errors = []
        
        # Portfolio prices don't override instrument type
        manual_instrument_type = None
        
        for holding, instrument in holdings:
            ticker = instrument.ticker_symbol
            
            # Skip instruments without yahoo ticker
            if not ticker or ticker == 'None':
                if instrument.isin:
                    print(f"⚠️  Skipping {instrument.instrument_name} ({instrument.isin}): No Yahoo ticker available")
                    errors.append(f"No Yahoo ticker for {instrument.instrument_name}")
                else:
                    print(f"⚠️  Skipping {instrument.instrument_name}: No ticker or ISIN available")
                    errors.append(f"No ticker for {instrument.instrument_name}")
                continue
            
            try:
                print(f"📈 Fetching {ticker} ({instrument.instrument_name})...")
                
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
                etl_results = etl.run_price_etl(ticker, earliest_missing, latest_missing, manual_instrument_type)
                
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
            from ..data.models import Portfolio, PortfolioHolding, Instrument
            
            portfolio = session.query(Portfolio).filter(
                Portfolio.name == portfolio_name
            ).first()
            
            # Filter for stocks only
            stock_holdings = session.query(PortfolioHolding, Instrument).join(
                Instrument, PortfolioHolding.instrument_id == Instrument.id
            ).filter(
                PortfolioHolding.portfolio_id == portfolio.id,
                Instrument.instrument_type == InstrumentType.STOCK
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
        
        for holding, instrument in stock_holdings:
            ticker = instrument.ticker_symbol
            
            # Skip instruments without yahoo ticker
            if not ticker or ticker == 'None':
                print(f"⚠️  Skipping {instrument.instrument_name}: No Yahoo ticker available")
                errors.append(f"No Yahoo ticker for {instrument.instrument_name}")
                continue
            
            try:
                print(f"📊 Fetching fundamentals for {ticker} ({instrument.instrument_name})...")
                
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
            from ..data.models import Portfolio, PortfolioHolding, Instrument
            
            portfolio_db = session.query(Portfolio).filter(
                Portfolio.name == portfolio_name
            ).first()
            
            detailed_holdings = session.query(PortfolioHolding, Instrument).join(
                Instrument, PortfolioHolding.instrument_id == Instrument.id
            ).filter(PortfolioHolding.portfolio_id == portfolio_db.id).all()
            
            if detailed_holdings:
                print(f"\n🏢 Holdings Details:")
                print("-" * 80)
                print(f"{'Ticker':<15} {'Name':<30} {'Type':<10} {'Country':<8} {'Sector':<20}")
                print("-" * 80)
                
                for holding, instrument in detailed_holdings:
                    ticker = instrument.ticker_symbol or instrument.isin or 'N/A'
                    name = instrument.instrument_name[:28] + '..' if len(instrument.instrument_name) > 30 else instrument.instrument_name
                    instrument_type = instrument.instrument_type.value if instrument.instrument_type else 'N/A'
                    country = instrument.country or 'N/A'
                    sector = (instrument.sector or '')[:18] + '..' if len(instrument.sector or '') > 20 else (instrument.sector or 'N/A')
                    
                    print(f"{ticker:<15} {name:<30} {instrument_type:<10} {country:<8} {sector:<20}")
        
        return SUCCESS_EXIT_CODE
        
    except DatabaseError as e:
        print(f"ERROR: Database error: {e}")
        return ERROR_EXIT_CODE
    except Exception as e:
        logger.error(f"Unexpected error in portfolio_info_command: {e}", exc_info=True)
        print(f"ERROR: Unexpected error: {e}")
        return ERROR_EXIT_CODE


def fetch_economic_indicator_command(
    name: str,
    from_date: str,
    to_date: Optional[str] = None
) -> int:
    """
    Handle fetch-economic-indicator command to fetch economic data using standardized names.
    
    Args:
        name: Standardized economic indicator name (e.g., unemployment_monthly_rate_us)
        from_date: Start date in YYYY-MM-DD format
        to_date: End date in YYYY-MM-DD format (optional)
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        from ..etl.load import EconomicETLOrchestrator
        from ..config import config
        
        # Validate dates
        validate_date_string(from_date, "from_date")
        if to_date:
            validate_date_string(to_date, "to_date")
        
        # Map name to source info
        mapping = _get_indicator_reverse_mapping()
        if name not in mapping:
            print(f"ERROR: Unknown indicator '{name}'")
            print(f"Available: {', '.join(sorted(mapping.keys()))}")
            return ERROR_EXIT_CODE
        
        source, source_identifier, description = mapping[name]
        
        # Note: All economic data sources fetch data through specified date range
        
        # Get FRED API key if needed
        api_key = None
        if source == 'fred':
            api_key = config.api.fred_api_key
            if not api_key:
                print("ERROR: Set FRED_API_KEY environment variable")
                return ERROR_EXIT_CODE
        
        print(f"Fetching {description}")
        
        # Track if to_date was user-specified vs auto-extended
        if not to_date:
            to_date = date.today().strftime('%Y-%m-%d')
            print(f"   Fetching through: {to_date} (default: today)")
        
        # Run ETL pipeline
        etl = EconomicETLOrchestrator()
        if source == 'eurostat':
            results = etl.run_eurostat_etl(source_identifier, from_date, to_date)
        elif source == 'ecb':
            parts = source_identifier.split('.', 1)
            results = etl.run_ecb_etl(parts[0], parts[1], from_date, to_date)
        elif source == 'fred':
            results = etl.run_fred_etl(source_identifier, api_key, from_date, to_date)
        
        # Report results
        if results['status'] == 'completed':
            load_stats = results['phases']['load']['loaded_records']
            print(f"✅ Success: {load_stats.get('data_points', 0)} data points")
            return SUCCESS_EXIT_CODE
        else:
            print(f"❌ Failed: {results.get('error_message', 'Unknown error')}")
            return ERROR_EXIT_CODE
        
    except ValidationError as e:
        print(f"ERROR: {e}")
        return ERROR_EXIT_CODE
    except Exception as e:
        logger = get_logger(__name__)
        logger.error(f"Unexpected error in fetch_economic_indicator_command: {e}", exc_info=True)
        print(f"ERROR: Unexpected error: {e}")
        return ERROR_EXIT_CODE


def fetch_all_economic_indicators_command(
    from_date: str,
    to_date: Optional[str] = None
) -> int:
    """
    Handle fetch-all-economic-indicators command to fetch all available economic indicators.
    
    Args:
        from_date: Start date in YYYY-MM-DD format
        to_date: End date in YYYY-MM-DD format (optional, defaults to today)
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        # Validate dates
        validate_date_string(from_date, "from_date")
        if to_date:
            validate_date_string(to_date, "to_date")
        
        print(f"🔄 Fetching all economic indicators...")
        print(f"   Date range: {from_date} to {to_date or 'today (auto-extend)'}")
        print()
        
        # Get all available indicators
        mapping = _get_indicator_reverse_mapping()
        total_indicators = len(mapping)
        
        # Track results
        successful_fetches = []
        failed_fetches = []
        total_data_points = 0
        
        # Process each indicator
        for indicator_name, (source, source_identifier, description) in mapping.items():
            try:
                print(f"Fetching {description}")
                
                # Call the existing fetch logic
                if to_date:
                    result = fetch_economic_indicator_command(indicator_name, from_date, to_date)
                else:
                    result = fetch_economic_indicator_command(indicator_name, from_date)
                
                if result == SUCCESS_EXIT_CODE:
                    successful_fetches.append(indicator_name)
                    # Note: We can't easily get data points count from existing function
                    print(f"✅ {indicator_name}: Success")
                else:
                    failed_fetches.append(indicator_name)
                    print(f"❌ {indicator_name}: Failed")
                    
            except Exception as e:
                failed_fetches.append(indicator_name)
                print(f"❌ {indicator_name}: Error - {str(e)}")
        
        print()
        print("📊 Summary:")
        print(f"- Successfully fetched: {len(successful_fetches)}/{total_indicators} indicators")
        if successful_fetches:
            print(f"- Successful: {', '.join(successful_fetches)}")
        if failed_fetches:
            print(f"- Failed: {', '.join(failed_fetches)}")
        
        if failed_fetches:
            print(f"⚠️  {len(failed_fetches)} indicators failed to fetch")
            return ERROR_EXIT_CODE
        else:
            print("✅ All economic indicators fetched successfully")
            return SUCCESS_EXIT_CODE
            
    except ValidationError as e:
        print(f"ERROR: {e}")
        return ERROR_EXIT_CODE
    except Exception as e:
        logger = get_logger(__name__)
        logger.error(f"Unexpected error in fetch_all_economic_indicators_command: {e}", exc_info=True)
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


def update_instrument_types_command(dry_run: bool = False) -> int:
    """
    Update existing companies table with correct instrument types using auto-detection.
    
    Args:
        dry_run: If True, only show what would be changed without making updates
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        print("🔍 Analyzing existing instruments for type detection...")
        if dry_run:
            print("📋 DRY RUN MODE: No changes will be made")
        
        db = DatabaseManager()
        
        # Get all instruments from database
        with db.get_session() as session:
            from ..data.models import Instrument
            instruments = session.query(Instrument).all()
        
        if not instruments:
            print("No instruments found in database.")
            return SUCCESS_EXIT_CODE
        
        print(f"Found {len(instruments)} instruments to analyze")
        
        updates_needed = []
        detection_stats = {
            'total': len(instruments),
            'correct': 0,
            'needs_update': 0,
            'errors': 0
        }
        
        for instrument in instruments:
            try:
                # Detect current instrument type
                ticker = instrument.ticker_symbol
                print(f"Analyzing {ticker}...", end=' ')
                
                # Try to get Yahoo Finance info for detection
                try:
                    yf_ticker = yf.Ticker(ticker)
                    info = yf_ticker.info
                    detected_type = detect_instrument_type(ticker, info)
                except Exception:
                    # Fallback to pattern-based detection if Yahoo fails
                    detected_type = detect_from_symbol_pattern(ticker)
                
                current_type = instrument.instrument_type
                
                if current_type == detected_type:
                    print(f"✅ {current_type.value} (correct)")
                    detection_stats['correct'] += 1
                else:
                    print(f"❌ {current_type.value} → {detected_type.value} (needs update)")
                    detection_stats['needs_update'] += 1
                    updates_needed.append({
                        'ticker': ticker,
                        'current': current_type,
                        'detected': detected_type,
                        'instrument_id': instrument.id
                    })
                
            except Exception as e:
                print(f"⚠️  Error analyzing {ticker}: {e}")
                detection_stats['errors'] += 1
        
        # Print summary
        print(f"\n📊 Detection Summary:")
        print(f"  Total instruments: {detection_stats['total']}")
        print(f"  Already correct: {detection_stats['correct']}")
        print(f"  Need updates: {detection_stats['needs_update']}")
        print(f"  Errors: {detection_stats['errors']}")
        
        if not updates_needed:
            print("✅ All instruments have correct types!")
            return SUCCESS_EXIT_CODE
        
        if dry_run:
            print(f"\n📋 Would update {len(updates_needed)} instruments:")
            for update in updates_needed:
                print(f"  {update['ticker']}: {update['current'].value} → {update['detected'].value}")
            print("\nRun without --dry-run to apply these changes.")
            return SUCCESS_EXIT_CODE
        
        # Apply updates
        print(f"\n🔧 Applying {len(updates_needed)} updates...")
        
        with db.get_session() as session:
            updated_count = 0
            for update in updates_needed:
                try:
                    instrument = session.query(Instrument).filter(Instrument.id == update['instrument_id']).first()
                    if instrument:
                        instrument.instrument_type = update['detected']
                        print(f"  Updated {update['ticker']}: {update['current'].value} → {update['detected'].value}")
                        updated_count += 1
                except Exception as e:
                    print(f"  ⚠️  Failed to update {update['ticker']}: {e}")
            
            session.commit()
        
        print(f"\n✅ Successfully updated {updated_count} instrument types!")
        return SUCCESS_EXIT_CODE
        
    except Exception as e:
        logger.error(f"Unexpected error in update_instrument_types_command: {e}", exc_info=True)
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


def _get_indicator_reverse_mapping() -> Dict[str, tuple]:
    """Get reverse mapping from standardized names to source info."""
    from ..config import config
    
    # Build reverse mapping from economic_indicators.yaml config
    mapping = {}
    
    if config.economic_indicators:
        for indicator_name, indicator_config in config.economic_indicators.items():
            source = indicator_config.get('source')
            source_identifier = indicator_config.get('source_identifier')
            description = indicator_config.get('description', f'{source.upper()} indicator: {source_identifier}')
            
            if source and source_identifier:
                mapping[indicator_name] = (source, source_identifier, description)
    
    return mapping


# =============================================================================
# DATA ALIGNMENT COMMANDS
# =============================================================================

def align_data_command(
    ticker: str,
    economic_indicator: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    alignment_method: str = "last_of_period",
    output_format: str = "summary"
) -> int:
    """
    Align price data with economic indicators for analysis.
    
    Args:
        ticker: Instrument ticker symbol
        economic_indicator: Economic indicator name
        from_date: Start date (YYYY-MM-DD format, optional)
        to_date: End date (YYYY-MM-DD format, optional)
        alignment_method: Alignment method (last_of_period, first_of_period, forward_fill, nearest)
        output_format: Output format (summary, detailed, csv)
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        # Validate inputs
        validate_ticker(ticker)
        
        start_date = None
        end_date = None
        
        if from_date:
            start_date = validate_date_string(from_date, "from_date")
        
        if to_date:
            end_date = validate_date_string(to_date, "to_date")
        
        if start_date and end_date and start_date > end_date:
            raise ValidationError("Start date must be before end date")
        
        # Valid alignment methods
        valid_methods = ["last_of_period", "first_of_period", "forward_fill", "nearest"]
        if alignment_method not in valid_methods:
            raise ValidationError(f"Invalid alignment method. Choose from: {', '.join(valid_methods)}")
        
        # Valid output formats
        valid_formats = ["summary", "detailed", "csv"]
        if output_format not in valid_formats:
            raise ValidationError(f"Invalid output format. Choose from: {', '.join(valid_formats)}")
        
        print(f"📊 Aligning {ticker} price data with {economic_indicator}")
        print(f"   Method: {alignment_method}")
        if start_date:
            print(f"   From: {start_date}")
        if end_date:
            print(f"   To: {end_date}")
        print()
        
        # Initialize database manager
        db = DatabaseManager()
        
        # Get aligned data
        aligned_data = db.get_aligned_price_economic_data(
            instrument_ticker=ticker,
            economic_indicator_name=economic_indicator,
            start_date=start_date,
            end_date=end_date,
            alignment_method=alignment_method
        )
        
        if not aligned_data:
            print(f"❌ No aligned data found for {ticker} and {economic_indicator}")
            print(f"   Check that both instruments have data in the specified date range.")
            return ERROR_EXIT_CODE
        
        # Display results based on output format
        if output_format == "summary":
            _display_alignment_summary(aligned_data, ticker, economic_indicator)
        elif output_format == "detailed":
            _display_alignment_detailed(aligned_data, ticker, economic_indicator)
        elif output_format == "csv":
            _export_alignment_csv(aligned_data, ticker, economic_indicator)
        
        return SUCCESS_EXIT_CODE
        
    except ValidationError as e:
        print(f"ERROR: {e}")
        return ERROR_EXIT_CODE
    except Exception as e:
        logger.error(f"Unexpected error in align_data_command: {e}", exc_info=True)
        print(f"ERROR: Unexpected error: {e}")
        return ERROR_EXIT_CODE


def alignment_info_command() -> int:
    """
    Show information about available data alignment capabilities.
    
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        print("📊 Data Alignment System Information")
        print("=" * 50)
        
        # Initialize database manager
        db = DatabaseManager()
        
        # Get alignment summary
        summary = db.get_alignment_data_summary()
        
        print(f"📈 Available Data:")
        print(f"   Instruments with price data: {summary['instruments_with_price_data']}")
        print(f"   Economic indicators with data: {summary['economic_indicators_with_data']}")
        print(f"   Potential alignment pairs: {summary['potential_alignment_pairs']}")
        print()
        
        print(f"📅 Date Ranges:")
        price_range = summary['price_data_date_range']
        economic_range = summary['economic_data_date_range']
        
        if price_range['start'] and price_range['end']:
            print(f"   Price data: {price_range['start']} to {price_range['end']}")
        
        if economic_range['start'] and economic_range['end']:
            print(f"   Economic data: {economic_range['start']} to {economic_range['end']}")
        print()
        
        print("🔧 Available Alignment Methods:")
        print("   • last_of_period  - Use last trading day of each month")
        print("   • first_of_period - Use first trading day of each month")
        print("   • forward_fill    - Fill daily prices with latest economic data")
        print("   • nearest         - Use nearest economic data point")
        print()
        
        print("💡 Example Commands:")
        print("   market-data-etl align-data --ticker AAPL --economic-indicator inflation_monthly_us")
        print("   market-data-etl align-data --ticker MSFT --economic-indicator unemployment_monthly_rate_us --from 2024-01-01 --method forward_fill")
        print("   market-data-etl alignment-pairs  # Show all available combinations")
        
        return SUCCESS_EXIT_CODE
        
    except Exception as e:
        logger.error(f"Unexpected error in alignment_info_command: {e}", exc_info=True)
        print(f"ERROR: Unexpected error: {e}")
        return ERROR_EXIT_CODE


def alignment_pairs_command(limit: int = 20) -> int:
    """
    Show available instrument-economic indicator pairs for alignment.
    
    Args:
        limit: Maximum number of pairs to show
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        print("📊 Available Data Alignment Pairs")
        print("=" * 80)
        
        # Initialize database manager
        db = DatabaseManager()
        
        # Get available pairs
        pairs = db.get_available_alignment_pairs()
        
        if not pairs:
            print("❌ No alignment pairs available")
            print("   Make sure you have both price data and economic indicators in the database.")
            return ERROR_EXIT_CODE
        
        print(f"Found {len(pairs)} total alignment pairs. Showing first {min(limit, len(pairs))}:")
        print()
        
        # Display header
        print(f"{'Ticker':<12} {'Instrument':<25} {'Type':<8} {'Economic Indicator':<25} {'Source':<8} {'Freq':<8}")
        print("-" * 86)
        
        # Display pairs
        for i, pair in enumerate(pairs[:limit]):
            ticker = pair['instrument_ticker'][:11]
            name = pair['instrument_name'][:24] if pair['instrument_name'] else 'N/A'
            inst_type = pair['instrument_type'][:7]
            indicator = pair['economic_indicator'][:24]
            source = pair['indicator_source'][:7]
            frequency = pair['indicator_frequency'][:7]
            
            print(f"{ticker:<12} {name:<25} {inst_type:<8} {indicator:<25} {source:<8} {frequency:<8}")
        
        if len(pairs) > limit:
            print(f"\n... and {len(pairs) - limit} more pairs")
            print(f"Use 'alignment-pairs --limit {len(pairs)}' to see all pairs")
        
        print(f"\n💡 Use 'align-data --ticker <TICKER> --economic-indicator <INDICATOR>' to align specific data")
        
        return SUCCESS_EXIT_CODE
        
    except Exception as e:
        logger.error(f"Unexpected error in alignment_pairs_command: {e}", exc_info=True)
        print(f"ERROR: Unexpected error: {e}")
        return ERROR_EXIT_CODE


# =============================================================================
# ALIGNMENT HELPER FUNCTIONS
# =============================================================================

def _display_alignment_summary(data: List[Dict[str, Any]], ticker: str, indicator: str) -> None:
    """Display summary of aligned data."""
    print(f"✅ Successfully aligned {len(data)} data points")
    print()
    
    if data:
        first_point = data[0]
        last_point = data[-1]
        
        print(f"📅 Date Range: {first_point['date']} to {last_point['date']}")
        print(f"📊 Sample Data (first and last points):")
        print()
        
        # Display first point
        print(f"First Point ({first_point['date']}):")
        if 'daily_close' in first_point:
            print(f"   {ticker} Close: ${first_point['daily_close']:,.2f}")
        if 'monthly_value' in first_point:
            print(f"   {indicator}: {first_point['monthly_value']}")
        print()
        
        # Display last point
        print(f"Last Point ({last_point['date']}):")
        if 'daily_close' in last_point:
            print(f"   {ticker} Close: ${last_point['daily_close']:,.2f}")
        if 'monthly_value' in last_point:
            print(f"   {indicator}: {last_point['monthly_value']}")
        print()
        
        print("💡 Use --output detailed to see all data points")
        print("💡 Use --output csv to export to CSV format")


def _display_alignment_detailed(data: List[Dict[str, Any]], ticker: str, indicator: str) -> None:
    """Display detailed aligned data."""
    print(f"✅ Aligned Data: {ticker} vs {indicator}")
    print("=" * 80)
    
    # Display header
    print(f"{'Date':<12} {'Close':<10} {'Volume':<12} {'Indicator':<15} {'Method':<15}")
    print("-" * 64)
    
    # Display each data point
    for point in data:
        date_str = point['date']
        close_price = f"${point.get('daily_close', 0):,.2f}" if point.get('daily_close') else "N/A"
        volume = f"{point.get('daily_volume', 0):,}" if point.get('daily_volume') else "N/A"
        indicator_value = f"{point.get('monthly_value', 0):.3f}" if point.get('monthly_value') else "N/A"
        method = point.get('alignment_method', 'N/A')
        
        print(f"{date_str:<12} {close_price:<10} {volume:<12} {indicator_value:<15} {method:<15}")


def _export_alignment_csv(data: List[Dict[str, Any]], ticker: str, indicator: str) -> None:
    """Export aligned data to CSV format."""
    import csv
    import io
    
    # Generate filename
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"aligned_data_{ticker}_{indicator}_{timestamp}.csv"
    
    # Write CSV
    output = io.StringIO()
    if data:
        fieldnames = list(data[0].keys())
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
        
        # Save to file
        with open(filename, 'w', newline='') as f:
            f.write(output.getvalue())
        
        print(f"✅ Exported {len(data)} aligned data points to: {filename}")
    else:
        print("❌ No data to export")


# =============================================================================
# ALIGNED DATA COMMANDS
# =============================================================================

def rebuild_aligned_data_command(
    tickers: Optional[List[str]] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    clear_existing: bool = True
) -> int:
    """
    Handle rebuild-aligned-data command.
    
    Args:
        tickers: List of ticker symbols to rebuild (None for all)
        from_date: Start date for rebuild (YYYY-MM-DD format)
        to_date: End date for rebuild (YYYY-MM-DD format)
        clear_existing: Whether to clear existing aligned data
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        print("🔄 Rebuilding aligned daily data...")
        
        # Validate and parse dates
        start_date = None
        end_date = None
        
        if from_date:
            start_date = validate_date_string(from_date, "from_date")
            print(f"   Start date: {start_date}")
        
        if to_date:
            end_date = validate_date_string(to_date, "to_date")
            print(f"   End date: {end_date}")
        
        if start_date and end_date:
            validate_date_range(start_date, end_date)
        
        # Validate tickers if provided
        validated_tickers = None
        if tickers:
            validated_tickers = []
            for ticker in tickers:
                validated_tickers.append(validate_ticker(ticker))
            print(f"   Tickers: {', '.join(validated_tickers)}")
        else:
            print("   Tickers: All tickers with price data")
        
        print(f"   Clear existing: {'Yes' if clear_existing else 'No'}")
        print()
        
        # Import and run ETL orchestrator
        from ..etl.load import AlignedDataETLOrchestrator
        
        etl = AlignedDataETLOrchestrator()
        results = etl.rebuild_aligned_data(
            tickers=validated_tickers,
            start_date=start_date,
            end_date=end_date,
            clear_existing=clear_existing
        )
        
        # Display results
        print("📊 Rebuild Results:")
        print("=" * 50)
        print(f"Tickers processed: {results['tickers_processed']}")
        print(f"Total records created: {results['total_records_created']:,}")
        print(f"Errors encountered: {len(results['errors'])}")
        
        # Display per-ticker statistics
        if results['statistics']:
            print("\n📈 Per-Ticker Results:")
            for ticker, stats in results['statistics'].items():
                if stats['records_created'] > 0:
                    print(f"   ✅ {ticker}: {stats['records_created']:,} records ({stats['exchange']} calendar)")
                else:
                    print(f"   ⚠️  {ticker}: No records created")
        
        # Display errors if any
        if results['errors']:
            print("\n❌ Errors:")
            for error in results['errors']:
                print(f"   • {error}")
        
        if results['total_records_created'] > 0:
            print(f"\n✅ Successfully rebuilt {results['total_records_created']:,} aligned daily records")
            return SUCCESS_EXIT_CODE
        else:
            print("\n⚠️  No aligned records were created")
            return ERROR_EXIT_CODE
            
    except ValidationError as e:
        print(f"ERROR: {e}")
        return ERROR_EXIT_CODE
    except Exception as e:
        print(f"ERROR: Failed to rebuild aligned data: {e}")
        logger.error(f"rebuild_aligned_data_command failed: {e}", exc_info=True)
        return ERROR_EXIT_CODE


def query_aligned_data_command(
    ticker: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    indicators: Optional[List[str]] = None,
    output_format: str = 'summary'
) -> int:
    """
    Handle query-aligned-data command.
    
    Args:
        ticker: Ticker symbol to query
        from_date: Start date filter (YYYY-MM-DD format)
        to_date: End date filter (YYYY-MM-DD format)
        indicators: List of specific indicators to include
        output_format: Output format ('summary', 'detailed', 'csv')
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        # Validate inputs
        ticker = validate_ticker(ticker)
        
        start_date = None
        end_date = None
        
        if from_date:
            start_date = validate_date_string(from_date, "from_date")
        
        if to_date:
            end_date = validate_date_string(to_date, "to_date")
        
        if start_date and end_date:
            validate_date_range(start_date, end_date)
        
        print(f"📊 Querying aligned data for {ticker}")
        if start_date or end_date:
            print(f"   Date range: {start_date or 'earliest'} to {end_date or 'latest'}")
        if indicators:
            print(f"   Indicators: {', '.join(indicators)}")
        print()
        
        # Query aligned data
        db = DatabaseManager()
        aligned_df = db.get_aligned_daily_data(
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
            indicators=indicators
        )
        
        if aligned_df.empty:
            print(f"❌ No aligned data found for {ticker}")
            print("💡 Try rebuilding aligned data first: rebuild-aligned-data --ticker", ticker)
            return ERROR_EXIT_CODE
        
        # Display results based on format
        if output_format == 'csv':
            _export_aligned_data_csv(aligned_df, ticker)
        elif output_format == 'detailed':
            _display_aligned_data_detailed(aligned_df, ticker)
        else:  # summary
            _display_aligned_data_summary(aligned_df, ticker)
        
        return SUCCESS_EXIT_CODE
        
    except ValidationError as e:
        print(f"ERROR: {e}")
        return ERROR_EXIT_CODE
    except Exception as e:
        print(f"ERROR: Failed to query aligned data: {e}")
        logger.error(f"query_aligned_data_command failed: {e}", exc_info=True)
        return ERROR_EXIT_CODE


def aligned_data_info_command() -> int:
    """
    Handle aligned-data-info command.
    
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        print("📊 Aligned Data System Information")
        print("=" * 50)
        
        db = DatabaseManager()
        
        # Get coverage statistics
        coverage = db.get_aligned_data_coverage()
        
        if coverage['total_records'] == 0:
            print("❌ No aligned data found in database")
            print("💡 Run 'rebuild-aligned-data' to create aligned data")
            return SUCCESS_EXIT_CODE
        
        # Display basic statistics
        print(f"📈 Total Records: {coverage['total_records']:,}")
        print(f"📅 Date Range: {coverage['date_range']['start']} to {coverage['date_range']['end']}")
        print()
        
        # Display field coverage
        print("🔧 Field Coverage:")
        field_coverage = coverage.get('field_coverage', {})
        
        # Group by category
        price_fields = [f for f in field_coverage.keys() if 'price' in f or f == 'volume']
        economic_fields = [f for f in field_coverage.keys() if f not in price_fields]
        
        if price_fields:
            print("   Price Data:")
            for field in sorted(price_fields):
                stats = field_coverage[field]
                print(f"      {field}: {stats['coverage_percentage']:.1f}% ({stats['records_with_data']:,} records)")
        
        if economic_fields:
            print("   Economic Indicators:")
            for field in sorted(economic_fields):
                stats = field_coverage[field]
                print(f"      {field}: {stats['coverage_percentage']:.1f}% ({stats['records_with_data']:,} records)")
        
        print()
        print("💡 Example Commands:")
        print("   query-aligned-data --ticker AAPL --from 2024-01-01")
        print("   query-aligned-data --ticker ESSITY-B.ST --indicators inflation_monthly_euro")
        print("   rebuild-aligned-data --ticker AAPL --from 2024-01-01")
        
        return SUCCESS_EXIT_CODE
        
    except Exception as e:
        print(f"ERROR: Failed to get aligned data info: {e}")
        logger.error(f"aligned_data_info_command failed: {e}", exc_info=True)
        return ERROR_EXIT_CODE


# =============================================================================
# ALIGNED DATA DISPLAY FUNCTIONS  
# =============================================================================

def _display_aligned_data_summary(aligned_df, ticker: str) -> None:
    """Display summary of aligned data."""
    print(f"✅ Aligned Data Summary: {ticker}")
    print("=" * 50)
    
    # Basic statistics
    total_records = len(aligned_df)
    min_date = aligned_df.index.min()
    max_date = aligned_df.index.max()
    
    # Handle different date formats
    if hasattr(min_date, 'date'):
        date_range = f"{min_date.date()} to {max_date.date()}"
    else:
        date_range = f"{min_date} to {max_date}"
    
    print(f"📊 Records: {total_records:,}")
    print(f"📅 Date Range: {date_range}")
    
    # Trading calendar info
    if 'trading_calendar' in aligned_df.columns:
        calendars = aligned_df['trading_calendar'].unique()
        print(f"📈 Trading Calendar: {', '.join(calendars)}")
    
    print()
    
    # Price data coverage
    price_cols = ['open', 'high', 'low', 'close', 'volume']
    price_coverage = []
    for col in price_cols:
        if col in aligned_df.columns:
            non_null_count = aligned_df[col].count()
            coverage_pct = (non_null_count / total_records) * 100
            price_coverage.append(f"{col}: {coverage_pct:.1f}%")
    
    if price_coverage:
        print("📈 Price Data Coverage:")
        for coverage in price_coverage:
            print(f"   {coverage}")
        print()
    
    # Economic indicators coverage
    economic_cols = [col for col in aligned_df.columns 
                    if col not in price_cols + ['trading_calendar'] 
                    and not col.endswith('_price')]
    
    if economic_cols:
        print("🌍 Economic Indicators Coverage:")
        for col in sorted(economic_cols):
            non_null_count = aligned_df[col].count()
            coverage_pct = (non_null_count / total_records) * 100
            print(f"   {col}: {coverage_pct:.1f}% ({non_null_count:,} records)")
        print()
    
    # Sample data
    if total_records > 0:
        print("📄 Sample Data (first 3 rows):")
        sample_df = aligned_df.head(3)
        
        # Display key columns only
        display_cols = ['close', 'volume'] + [col for col in economic_cols[:3]]
        display_cols = [col for col in display_cols if col in aligned_df.columns]
        
        if display_cols:
            print(sample_df[display_cols].to_string())
        print()
    
    print("💡 Use --output detailed to see all data")
    print("💡 Use --output csv to export data")


def _display_aligned_data_detailed(aligned_df, ticker: str) -> None:
    """Display detailed aligned data."""
    print(f"✅ Detailed Aligned Data: {ticker}")
    print("=" * 80)
    
    # Display all data with formatting
    pd_options = {
        'display.max_rows': 50,
        'display.max_columns': 10,
        'display.width': 120,
        'display.precision': 2
    }
    
    with pd.option_context(*[item for pair in pd_options.items() for item in pair]):
        print(aligned_df.to_string())
    
    if len(aligned_df) > 50:
        print(f"\n... showing first 50 of {len(aligned_df)} records")
        print("💡 Use --output csv to export all data")


def _export_aligned_data_csv(aligned_df, ticker: str) -> None:
    """Export aligned data to CSV."""
    from datetime import datetime
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"aligned_data_{ticker}_{timestamp}.csv"
    
    # Export with date as column
    export_df = aligned_df.reset_index()
    export_df.to_csv(filename, index=False)
    
    print(f"✅ Exported {len(aligned_df)} aligned records to: {filename}")
    print(f"📁 File location: {os.path.abspath(filename)}")


# =============================================================================
# FETCH-ALL COMMAND - Update all data from latest dates
# =============================================================================

def fetch_all_command(
    dry_run: bool = False,
    prices_only: bool = False,
    economic_only: bool = False
) -> int:
    """
    Handle fetch-all command to update all data from latest dates to today.
    
    Args:
        dry_run: Show what would be updated without actually fetching
        prices_only: Only update price data
        economic_only: Only update economic data
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        from datetime import date, timedelta
        
        db = DatabaseManager()
        today = date.today()
        
        logger.info("Starting fetch-all command to update all data from latest dates to today")
        logger.info(f"Target date: {today}")
        
        # Track overall results
        total_operations = 0
        successful_operations = 0
        failed_operations = []
        
        # =============================================================================
        # 1. UPDATE PRICE DATA FOR ALL INSTRUMENTS
        # =============================================================================
        
        if not economic_only:
            logger.info("Updating price data for all instruments")
            
            # Get all instruments
            instruments = db.get_all_instruments_info()
            logger.info(f"Found {len(instruments)} instruments in database")
            
            for instrument in instruments:
                ticker = instrument['ticker_symbol']
                total_operations += 1
                
                try:
                    # Get latest price date
                    date_range = db.get_price_date_range(ticker)
                    
                    if date_range is None:
                        logger.warning(f"{ticker}: No existing price data - skipping (use fetch-prices first)")
                        continue
                        
                    min_date, max_date = date_range
                    next_date = max_date + timedelta(days=1)
                    
                    if next_date > today:
                        logger.info(f"{ticker}: Already up to date (latest: {max_date})")
                        successful_operations += 1
                        continue
                    
                    if dry_run:
                        logger.info(f"{ticker}: Would fetch prices from {next_date} to {today}")
                        successful_operations += 1
                        continue
                    
                    logger.info(f"{ticker}: Fetching prices from {next_date} to {today}")
                    
                    # Call existing fetch_prices_command
                    result = fetch_prices_command(
                        ticker=ticker,
                        from_date=next_date.strftime('%Y-%m-%d'),
                        to_date=today.strftime('%Y-%m-%d')
                    )
                    
                    if result == SUCCESS_EXIT_CODE:
                        logger.info(f"{ticker}: Price update successful")
                        successful_operations += 1
                        
                        # Also update financial data for stocks
                        if not prices_only and instrument['instrument_type'] == 'stock':
                            logger.info(f"{ticker}: Updating financial statements")
                            fin_result = fetch_financial_statements_command(ticker=ticker, quarterly=True)
                            if fin_result == SUCCESS_EXIT_CODE:
                                logger.info(f"{ticker}: Financial statements updated")
                            else:
                                logger.warning(f"{ticker}: Financial statements update failed")
                    else:
                        failed_operations.append(f"{ticker} (prices)")
                        logger.error(f"{ticker}: Price update failed")
                        
                except Exception as e:
                    failed_operations.append(f"{ticker} (prices): {str(e)}")
                    logger.error(f"{ticker}: Error - {str(e)}")
            
            print()
        
        # =============================================================================
        # 2. UPDATE ECONOMIC INDICATORS
        # =============================================================================
        
        if not prices_only:
            logger.info("Updating economic indicators")
            
            # Get all economic indicators
            indicators = db.get_all_economic_indicators()
            print(f"Found {len(indicators)} economic indicators in database")
            
            for indicator in indicators:
                indicator_name = indicator['name']
                indicator_id = indicator['id']
                total_operations += 1
                
                try:
                    # Get latest economic data date
                    latest_date = db.get_latest_economic_indicator_date(indicator_id)
                    
                    if latest_date is None:
                        print(f"⚠️  {indicator_name}: No existing data - skipping")
                        continue
                    
                    next_date = latest_date + timedelta(days=1)
                    
                    if next_date > today:
                        print(f"✅ {indicator_name}: Already up to date (latest: {latest_date})")
                        successful_operations += 1
                        continue
                    
                    if dry_run:
                        print(f"📋 {indicator_name}: Would fetch from {next_date} to {today}")
                        successful_operations += 1
                        continue
                    
                    print(f"🔄 {indicator_name}: Fetching from {next_date} to {today}")
                    
                    # Call existing fetch_economic_indicator_command
                    result = fetch_economic_indicator_command(
                        name=indicator_name,
                        from_date=next_date.strftime('%Y-%m-%d'),
                        to_date=today.strftime('%Y-%m-%d')
                    )
                    
                    if result == SUCCESS_EXIT_CODE:
                        print(f"✅ {indicator_name}: Update successful")
                        successful_operations += 1
                    else:
                        failed_operations.append(f"{indicator_name} (economic)")
                        print(f"❌ {indicator_name}: Update failed")
                        
                except Exception as e:
                    failed_operations.append(f"{indicator_name} (economic): {str(e)}")
                    print(f"❌ {indicator_name}: Error - {str(e)}")
            
            print()
        
        # =============================================================================
        # SUMMARY REPORT
        # =============================================================================
        
        print("📋 FETCH-ALL SUMMARY")
        print("=" * 50)
        print(f"📊 Total operations: {total_operations}")
        print(f"✅ Successful: {successful_operations}")
        print(f"❌ Failed: {len(failed_operations)}")
        
        if failed_operations:
            print(f"\n❌ Failed operations:")
            for failure in failed_operations:
                print(f"  • {failure}")
        
        success_rate = (successful_operations / total_operations * 100) if total_operations > 0 else 0
        print(f"\n📈 Success rate: {success_rate:.1f}%")
        
        if dry_run:
            print("\n📋 This was a dry run - no data was actually fetched")
        elif len(failed_operations) == 0:
            print("\n🎉 All updates completed successfully!")
        elif success_rate >= 80:
            print("\n✅ Most updates completed successfully")
        else:
            print("\n⚠️  Many updates failed - check individual error messages above")
        
        # Return success if at least 80% succeeded, or if it was just a dry run
        if dry_run or success_rate >= 80:
            return SUCCESS_EXIT_CODE
        else:
            return ERROR_EXIT_CODE
            
    except Exception as e:
        print(f"ERROR: Fetch-all command failed: {e}")
        return ERROR_EXIT_CODE