"""
Main CLI entry point for market data ETL system.

This module provides the command-line interface with argument parsing
and dispatching to the appropriate command handlers.
"""

import argparse
import sys

from ..utils.logging import setup_logging
from ..config import config
from .commands import (
    fetch_prices_command, 
    fetch_fundamentals_command, 
    db_info_command,
    fetch_financial_statements_command,
    financial_summary_command
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
  %(prog)s db-info --ticker VOLV-B.ST
  
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
    
    return parser


def main():
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