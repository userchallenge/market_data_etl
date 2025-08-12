"""
Main CLI entry point for market data ETL system.

This module provides the command-line interface with argument parsing
and dispatching to the appropriate command handlers.
"""

import argparse
import sys
from typing import NoReturn

from ..utils.logging import setup_logging
from ..config import config
from .commands import (
    fetch_prices_command, 
    fetch_fundamentals_command, 
    db_info_command,
    fetch_financial_statements_command,
    financial_summary_command,
    clear_database_command,
    load_portfolio_command,
    load_transactions_command,
    fetch_portfolio_prices_command,
    fetch_portfolio_fundamentals_command,
    portfolio_info_command,
    fetch_economic_command,
    economic_info_command
)


def create_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        description="Market Data ETL System - Extract and store market data from Yahoo Finance",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s fetch-prices --ticker AAPL --from 2024-01-01 --to 2024-01-31
  %(prog)s fetch-fundamentals --ticker MSFT
  %(prog)s fetch-financial-statements --ticker AAPL
  %(prog)s financial-summary --ticker AAPL --years 3
  %(prog)s fetch-economic --source eurostat --indicator prc_hicp_midx --from 2024-01-01
  %(prog)s fetch-economic --source fred --indicator UNRATE --from 2024-01-01 --to 2024-12-31 --api-key YOUR_KEY
  %(prog)s economic-info --indicator prc_hicp_midx
  %(prog)s db-info --ticker VOLV-B.ST
  %(prog)s clear-database --all
  %(prog)s load-portfolio --file ./portfolios/my_portfolio.json
  %(prog)s fetch-portfolio-prices --portfolio "My Portfolio" --from 2024-01-01
  
Environment Variables:
  MARKET_DATA_DB_PATH         Database file path (default: market_data.db)
  MARKET_DATA_LOG_LEVEL       Log level (default: INFO)
  MARKET_DATA_LOG_FILE        Log file path (optional)
  MARKET_DATA_MAX_RETRIES     Max API retries (default: 5)
        """
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging (DEBUG level)'
    )
    
    parser.add_argument(
        '--log-file',
        help='Log file path (overrides environment variable)'
    )
    
    subparsers = parser.add_subparsers(
        dest='command',
        help='Available commands',
        required=True
    )
    
    # fetch-prices command
    prices_parser = subparsers.add_parser(
        'fetch-prices',
        help='Fetch historical price data for a ticker'
    )
    prices_parser.add_argument(
        '--ticker',
        required=True,
        help='Stock ticker symbol (e.g., AAPL, ERIC-B.ST)'
    )
    prices_parser.add_argument(
        '--from',
        dest='from_date',
        required=True,
        help='Start date in YYYY-MM-DD format'
    )
    prices_parser.add_argument(
        '--to',
        dest='to_date',
        help='End date in YYYY-MM-DD format (defaults to today)'
    )
    
    # fetch-fundamentals command
    fundamentals_parser = subparsers.add_parser(
        'fetch-fundamentals',
        help='Fetch fundamental data for a ticker'
    )
    fundamentals_parser.add_argument(
        '--ticker',
        required=True,
        help='Stock ticker symbol (e.g., AAPL, ERIC-B.ST)'
    )
    
    # fetch-financial-statements command
    financial_parser = subparsers.add_parser(
        'fetch-financial-statements',
        help='Fetch structured financial statements for comprehensive analysis'
    )
    financial_parser.add_argument(
        '--ticker',
        required=True,
        help='Stock ticker symbol (e.g., AAPL, ERIC-B.ST)'
    )
    financial_parser.add_argument(
        '--no-quarterly',
        action='store_true',
        help='Fetch annual data only (exclude quarterly data)'
    )
    
    # financial-summary command
    summary_parser = subparsers.add_parser(
        'financial-summary',
        help='Show comprehensive financial summary for a company'
    )
    summary_parser.add_argument(
        '--ticker',
        required=True,
        help='Stock ticker symbol (e.g., AAPL, ERIC-B.ST)'
    )
    summary_parser.add_argument(
        '--years',
        type=int,
        default=5,
        help='Number of recent years to include (default: 5)'
    )
    
    # db-info command
    info_parser = subparsers.add_parser(
        'db-info',
        help='Show database information for a ticker'
    )
    info_parser.add_argument(
        '--ticker',
        required=True,
        help='Stock ticker symbol (e.g., AAPL, ERIC-B.ST)'
    )
    
    # clear-database command
    clear_parser = subparsers.add_parser(
        'clear-database',
        help='Clear database data for development/testing'
    )
    clear_group = clear_parser.add_mutually_exclusive_group(required=True)
    clear_group.add_argument(
        '--ticker',
        help='Clear data for specific ticker only'
    )
    clear_group.add_argument(
        '--all',
        action='store_true',
        help='Clear all data from database'
    )
    clear_parser.add_argument(
        '--confirm',
        action='store_true',
        help='Skip confirmation prompt'
    )
    
    # load-portfolio command
    load_portfolio_parser = subparsers.add_parser(
        'load-portfolio',
        help='Load portfolio configuration from JSON file'
    )
    load_portfolio_parser.add_argument(
        '--file',
        required=True,
        help='Path to portfolio JSON configuration file'
    )
    
    # load-transactions command
    load_transactions_parser = subparsers.add_parser(
        'load-transactions',
        help='Load transactions from CSV file'
    )
    load_transactions_parser.add_argument(
        '--file',
        required=True,
        help='Path to transactions CSV file'
    )
    load_transactions_parser.add_argument(
        '--portfolio',
        help='Portfolio name to associate transactions with'
    )
    
    # fetch-portfolio-prices command
    portfolio_prices_parser = subparsers.add_parser(
        'fetch-portfolio-prices',
        help='Fetch price data for all holdings in a portfolio'
    )
    portfolio_prices_parser.add_argument(
        '--portfolio',
        required=True,
        help='Portfolio name'
    )
    portfolio_prices_parser.add_argument(
        '--from',
        dest='from_date',
        required=True,
        help='Start date in YYYY-MM-DD format'
    )
    portfolio_prices_parser.add_argument(
        '--to',
        dest='to_date',
        help='End date in YYYY-MM-DD format (defaults to today)'
    )
    
    # fetch-portfolio-fundamentals command
    portfolio_fundamentals_parser = subparsers.add_parser(
        'fetch-portfolio-fundamentals',
        help='Fetch fundamental data for stocks in a portfolio (skips funds)'
    )
    portfolio_fundamentals_parser.add_argument(
        '--portfolio',
        required=True,
        help='Portfolio name'
    )
    
    # portfolio-info command
    portfolio_info_parser = subparsers.add_parser(
        'portfolio-info',
        help='Show portfolio information and holdings summary'
    )
    portfolio_info_parser.add_argument(
        '--portfolio',
        required=True,
        help='Portfolio name'
    )
    
    # fetch-economic command
    economic_parser = subparsers.add_parser(
        'fetch-economic',
        help='Fetch economic data from Eurostat, ECB, or FRED APIs'
    )
    economic_parser.add_argument(
        '--source',
        required=True,
        choices=['eurostat', 'ecb', 'fred'],
        help='Data source (eurostat, ecb, fred)'
    )
    economic_parser.add_argument(
        '--indicator',
        required=True,
        help='Economic indicator code/ID (e.g., prc_hicp_midx, UNRATE, FM.B.U2.EUR.4F.KR.MRR_FR.LEV)'
    )
    economic_parser.add_argument(
        '--from',
        dest='from_date',
        required=True,
        help='Start date in YYYY-MM-DD format'
    )
    economic_parser.add_argument(
        '--to',
        dest='to_date',
        help='End date in YYYY-MM-DD format (required for ECB and FRED)'
    )
    economic_parser.add_argument(
        '--api-key',
        dest='api_key',
        help='API key (required for FRED data source)'
    )
    
    # economic-info command
    economic_info_parser = subparsers.add_parser(
        'economic-info',
        help='Show information about an economic indicator'
    )
    economic_info_parser.add_argument(
        '--indicator',
        required=True,
        help='Economic indicator ID'
    )
    
    return parser


def main() -> NoReturn:
    """Main CLI entry point."""
    parser = create_parser()
    args = parser.parse_args()
    
    # Setup logging
    log_file = args.log_file or config.log_file
    setup_logging(
        level=config.log_level,
        log_file=log_file,
        verbose=args.verbose
    )
    
    # Execute command
    try:
        if args.command == 'fetch-prices':
            exit_code = fetch_prices_command(
                ticker=args.ticker,
                from_date=args.from_date,
                to_date=args.to_date
            )
        elif args.command == 'fetch-fundamentals':
            exit_code = fetch_fundamentals_command(ticker=args.ticker)
        elif args.command == 'fetch-financial-statements':
            exit_code = fetch_financial_statements_command(
                ticker=args.ticker,
                quarterly=not args.no_quarterly
            )
        elif args.command == 'financial-summary':
            exit_code = financial_summary_command(
                ticker=args.ticker,
                years=args.years
            )
        elif args.command == 'db-info':
            exit_code = db_info_command(ticker=args.ticker)
        elif args.command == 'clear-database':
            exit_code = clear_database_command(
                ticker=args.ticker,
                clear_all=args.all,
                confirm=args.confirm
            )
        elif args.command == 'load-portfolio':
            exit_code = load_portfolio_command(file_path=args.file)
        elif args.command == 'load-transactions':
            exit_code = load_transactions_command(
                file_path=args.file,
                portfolio_name=args.portfolio
            )
        elif args.command == 'fetch-portfolio-prices':
            exit_code = fetch_portfolio_prices_command(
                portfolio_name=args.portfolio,
                from_date=args.from_date,
                to_date=args.to_date
            )
        elif args.command == 'fetch-portfolio-fundamentals':
            exit_code = fetch_portfolio_fundamentals_command(
                portfolio_name=args.portfolio
            )
        elif args.command == 'portfolio-info':
            exit_code = portfolio_info_command(portfolio_name=args.portfolio)
        elif args.command == 'fetch-economic':
            exit_code = fetch_economic_command(
                source=args.source,
                indicator=args.indicator,
                from_date=args.from_date,
                to_date=args.to_date,
                api_key=args.api_key
            )
        elif args.command == 'economic-info':
            exit_code = economic_info_command(indicator_id=args.indicator)
        else:
            print(f"ERROR: Unknown command: {args.command}")
            exit_code = 1
        
        sys.exit(exit_code)
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Unexpected error occurred: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()