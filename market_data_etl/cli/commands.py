"""
CLI command implementations for market data ETL operations.

This module contains the actual command logic separated from
argument parsing for better testability and organization.
"""

from typing import List
from datetime import date, datetime

from ..etl.load import ETLOrchestrator
from ..database.manager import DatabaseManager
from ..utils.logging import get_logger
from ..utils.exceptions import YahooFinanceError, ValidationError

logger = get_logger(__name__)


def validate_ticker(ticker: str) -> str:
    """
    Validate ticker format.
    
    Args:
        ticker: Ticker symbol
        
    Returns:
        Validated ticker symbol
        
    Raises:
        ValidationError: If ticker format is invalid
    """
    if not ticker or not ticker.strip():
        raise ValidationError("Ticker cannot be empty")
    
    ticker = ticker.strip().upper()
    
    # Basic validation - allow alphanumeric and common ticker separators
    if not all(c.isalnum() or c in '.-' for c in ticker):
        raise ValidationError(f"Invalid ticker format: {ticker}")
    
    return ticker


def parse_date(date_string: str) -> date:
    """
    Parse date string in YYYY-MM-DD format.
    
    Args:
        date_string: Date in YYYY-MM-DD format
        
    Returns:
        Parsed date object
        
    Raises:
        ValidationError: If date format is invalid
    """
    try:
        return datetime.strptime(date_string, '%Y-%m-%d').date()
    except ValueError:
        raise ValidationError(f"Invalid date format: {date_string}. Expected YYYY-MM-DD format.")


def calculate_missing_dates(
    existing_dates: List[date], 
    from_date: date, 
    to_date: date
) -> List[date]:
    """
    Calculate which dates are missing from existing data.
    
    Args:
        existing_dates: List of dates that already exist
        from_date: Start date of requested range
        to_date: End date of requested range
        
    Returns:
        List of missing dates that need to be fetched
    """
    existing_set = set(existing_dates)
    
    # Generate all dates in the range (excluding weekends for efficiency)
    all_dates = []
    current = from_date
    while current <= to_date:
        # Include all dates - Yahoo Finance will filter out non-trading days
        all_dates.append(current)
        current = date.fromordinal(current.toordinal() + 1)
    
    return [d for d in all_dates if d not in existing_set]


def fetch_prices_command(
    ticker: str,
    from_date: str,
    to_date: str = None
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
        start_date = parse_date(from_date)
        end_date = parse_date(to_date) if to_date else date.today()
        
        if start_date > end_date:
            print("ERROR: from_date cannot be after to_date")
            return 1
        
        if end_date > date.today():
            print("ERROR: to_date cannot be in the future")
            return 1
        
        print(f"Running price ETL pipeline for {ticker} from {start_date} to {end_date}...")
        
        # Initialize ETL orchestrator
        etl = ETLOrchestrator()
        
        # Check existing data to avoid unnecessary fetches
        db = DatabaseManager()
        existing_dates = db.get_existing_price_dates(ticker)
        missing_dates = calculate_missing_dates(existing_dates, start_date, end_date)
        
        if not missing_dates:
            print("Database already contains data for the full range. Nothing to fetch.")
            return 0
        
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
            return 1
        
        return 0
        
    except ValidationError as e:
        print(f"ERROR: {e}")
        return 1
    except YahooFinanceError as e:
        print(f"ERROR: Failed to fetch prices for {ticker} from Yahoo Finance.")
        print(str(e))
        return 1
    except Exception as e:
        logger.error(f"Unexpected error in fetch_prices_command: {e}", exc_info=True)
        print(f"ERROR: Unexpected error: {e}")
        return 1


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
        
        if years < 1 or years > 20:
            print("ERROR: Years must be between 1 and 20")
            return 1
        
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