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
 
    db_info_command,
    fetch_financial_statements_command,
    clear_database_command,
    load_portfolio_command,
    load_transactions_command,
    fetch_portfolio_prices_command,
    fetch_portfolio_fundamentals_command,
    portfolio_info_command,
    fetch_economic_indicator_command,
    load_price_csv_command,
    generate_price_csv_template_command,
    update_instrument_types_command,
    align_data_command,
    alignment_info_command,
    alignment_pairs_command,
    rebuild_aligned_data_command,
    query_aligned_data_command,
    aligned_data_info_command
)


def create_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        description="Market Data ETL System - Extract and store market data from Yahoo Finance",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s fetch-prices --ticker AAPL --from 2024-01-01 --to 2024-01-31
  %(prog)s fetch-financial-statements --ticker AAPL
  %(prog)s fetch-economic-indicator --name unemployment_monthly_rate_us --from 2024-01-01 --to 2024-12-31
  %(prog)s fetch-economic-indicator --name inflation_monthly_euro --from 2024-01-01
  %(prog)s db-info --ticker VOLV-B.ST
  %(prog)s clear-database --all
  %(prog)s load-portfolio --file ./portfolios/my_portfolio.json
  %(prog)s fetch-portfolio-prices --portfolio "My Portfolio" --from 2024-01-01
  %(prog)s generate-price-csv-template --ticker ^OMXS30 --output omxs30_template.csv
  %(prog)s load-price-csv --file omxs30_data.csv --ticker ^OMXS30
  %(prog)s align-data --ticker AAPL --economic-indicator inflation_monthly_us
  %(prog)s align-data --ticker MSFT --economic-indicator unemployment_monthly_rate_us --from 2024-01-01 --method forward_fill
  %(prog)s alignment-info
  %(prog)s alignment-pairs --limit 10
  %(prog)s rebuild-aligned-data --ticker AAPL --from 2024-01-01
  %(prog)s rebuild-aligned-data --from 2024-01-01 --to 2024-12-31
  %(prog)s query-aligned-data --ticker AAPL --from 2024-01-01 --output detailed
  %(prog)s query-aligned-data --ticker ESSITY-B.ST --indicators inflation_monthly_euro
  %(prog)s aligned-data-info
  
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
    prices_parser.add_argument(
        '--instrument-type',
        dest='instrument_type',
        choices=['stock', 'fund', 'etf', 'index', 'commodity', 'currency', 'cryptocurrency', 'unknown'],
        help='Manually specify instrument type (overrides auto-detection)'
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
    
    # fetch-economic-indicator command
    economic_parser = subparsers.add_parser(
        'fetch-economic-indicator',
        help='Fetch economic data using standardized indicator names'
    )
    economic_parser.add_argument(
        '--name',
        required=True,
        help='Standardized economic indicator name (e.g., unemployment_monthly_rate_us, inflation_monthly_euro)'
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
        help='End date in YYYY-MM-DD format'
    )
    
    
    # load-price-csv command
    load_csv_parser = subparsers.add_parser(
        'load-price-csv',
        help='Load price data from CSV file'
    )
    load_csv_parser.add_argument(
        '--file',
        required=True,
        help='Path to CSV file with price data'
    )
    load_csv_parser.add_argument(
        '--ticker',
        required=True,
        help='Ticker symbol for the data'
    )
    
    # generate-price-csv-template command
    generate_csv_parser = subparsers.add_parser(
        'generate-price-csv-template',
        help='Generate CSV template for manual price data entry'
    )
    generate_csv_parser.add_argument(
        '--ticker',
        required=True,
        help='Ticker symbol (for reference)'
    )
    generate_csv_parser.add_argument(
        '--output',
        required=True,
        help='Output CSV file path'
    )
    
    # update-instrument-types command
    update_types_parser = subparsers.add_parser(
        'update-instrument-types',
        help='Update existing instruments with correct types using auto-detection'
    )
    update_types_parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be changed without making updates'
    )
    
    # align-data command
    align_parser = subparsers.add_parser(
        'align-data',
        help='Align price data with economic indicators for analysis'
    )
    align_parser.add_argument(
        '--ticker',
        required=True,
        help='Instrument ticker symbol (e.g., AAPL, MSFT)'
    )
    align_parser.add_argument(
        '--economic-indicator',
        required=True,
        dest='economic_indicator',
        help='Economic indicator name (e.g., inflation_monthly_us, unemployment_monthly_rate_us)'
    )
    align_parser.add_argument(
        '--from',
        dest='from_date',
        help='Start date in YYYY-MM-DD format (optional)'
    )
    align_parser.add_argument(
        '--to',
        dest='to_date',
        help='End date in YYYY-MM-DD format (optional)'
    )
    align_parser.add_argument(
        '--method',
        dest='alignment_method',
        choices=['last_of_period', 'first_of_period', 'forward_fill', 'nearest'],
        default='last_of_period',
        help='Alignment method (default: last_of_period)'
    )
    align_parser.add_argument(
        '--output',
        dest='output_format',
        choices=['summary', 'detailed', 'csv'],
        default='summary',
        help='Output format (default: summary)'
    )
    
    # alignment-info command
    alignment_info_parser = subparsers.add_parser(
        'alignment-info',
        help='Show data alignment system information and capabilities'
    )
    
    # alignment-pairs command
    alignment_pairs_parser = subparsers.add_parser(
        'alignment-pairs',
        help='Show available instrument-economic indicator pairs for alignment'
    )
    alignment_pairs_parser.add_argument(
        '--limit',
        type=int,
        default=20,
        help='Maximum number of pairs to show (default: 20)'
    )
    
    # rebuild-aligned-data command
    rebuild_aligned_parser = subparsers.add_parser(
        'rebuild-aligned-data',
        help='Rebuild trading-day aligned data with forward-filled economic indicators'
    )
    rebuild_aligned_parser.add_argument(
        '--ticker',
        action='append',
        dest='tickers',
        help='Ticker symbol(s) to rebuild (can be used multiple times, default: all tickers)'
    )
    rebuild_aligned_parser.add_argument(
        '--from',
        dest='from_date',
        help='Start date in YYYY-MM-DD format (optional)'
    )
    rebuild_aligned_parser.add_argument(
        '--to',
        dest='to_date', 
        help='End date in YYYY-MM-DD format (optional)'
    )
    rebuild_aligned_parser.add_argument(
        '--no-clear',
        action='store_false',
        dest='clear_existing',
        help='Do not clear existing aligned data before rebuilding'
    )
    
    # query-aligned-data command
    query_aligned_parser = subparsers.add_parser(
        'query-aligned-data',
        help='Query trading-day aligned data for analysis'
    )
    query_aligned_parser.add_argument(
        '--ticker',
        required=True,
        help='Instrument ticker symbol (e.g., AAPL, ESSITY-B.ST)'
    )
    query_aligned_parser.add_argument(
        '--from',
        dest='from_date',
        help='Start date in YYYY-MM-DD format (optional)'
    )
    query_aligned_parser.add_argument(
        '--to',
        dest='to_date',
        help='End date in YYYY-MM-DD format (optional)'
    )
    query_aligned_parser.add_argument(
        '--indicators',
        nargs='+',
        help='Specific economic indicators to include (e.g., inflation_monthly_us unemployment_monthly_rate_us)'
    )
    query_aligned_parser.add_argument(
        '--output',
        dest='output_format',
        choices=['summary', 'detailed', 'csv'],
        default='summary',
        help='Output format (default: summary)'
    )
    
    # aligned-data-info command
    aligned_data_info_parser = subparsers.add_parser(
        'aligned-data-info',
        help='Show aligned data system information and coverage statistics'
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
                to_date=args.to_date,
                instrument_type=getattr(args, 'instrument_type', None)
            )
        elif args.command == 'fetch-financial-statements':
            exit_code = fetch_financial_statements_command(
                ticker=args.ticker,
                quarterly=not args.no_quarterly
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
        elif args.command == 'fetch-economic-indicator':
            exit_code = fetch_economic_indicator_command(
                name=args.name,
                from_date=args.from_date,
                to_date=args.to_date
            )
        elif args.command == 'load-price-csv':
            exit_code = load_price_csv_command(
                file_path=args.file,
                ticker=args.ticker
            )
        elif args.command == 'generate-price-csv-template':
            exit_code = generate_price_csv_template_command(
                ticker=args.ticker,
                output_file=args.output
            )
        elif args.command == 'update-instrument-types':
            exit_code = update_instrument_types_command(
                dry_run=args.dry_run
            )
        elif args.command == 'align-data':
            exit_code = align_data_command(
                ticker=args.ticker,
                economic_indicator=args.economic_indicator,
                from_date=getattr(args, 'from_date', None),
                to_date=getattr(args, 'to_date', None),
                alignment_method=args.alignment_method,
                output_format=args.output_format
            )
        elif args.command == 'alignment-info':
            exit_code = alignment_info_command()
        elif args.command == 'alignment-pairs':
            exit_code = alignment_pairs_command(
                limit=args.limit
            )
        elif args.command == 'rebuild-aligned-data':
            exit_code = rebuild_aligned_data_command(
                tickers=getattr(args, 'tickers', None),
                from_date=getattr(args, 'from_date', None),
                to_date=getattr(args, 'to_date', None),
                clear_existing=getattr(args, 'clear_existing', True)
            )
        elif args.command == 'query-aligned-data':
            exit_code = query_aligned_data_command(
                ticker=args.ticker,
                from_date=getattr(args, 'from_date', None),
                to_date=getattr(args, 'to_date', None),
                indicators=getattr(args, 'indicators', None),
                output_format=getattr(args, 'output_format', 'summary')
            )
        elif args.command == 'aligned-data-info':
            exit_code = aligned_data_info_command()
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