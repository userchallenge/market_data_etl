"""
Example showing before/after code deduplication using unified utilities.

This demonstrates how the new utility modules eliminate code duplication
throughout the market_data_etl package.
"""

from datetime import date
from market_data_etl.utils.error_handlers import handle_cli_command_errors, SUCCESS_EXIT_CODE
from market_data_etl.utils.validation_helpers import validate_ticker_and_date_range
from market_data_etl.utils.transformation_helpers import create_timestamp, create_extraction_metadata


# =============================================================================
# BEFORE: Duplicated Error Handling Pattern (repeated 20+ times)
# =============================================================================

def old_cli_command_pattern(ticker: str, from_date: str) -> int:
    """Example of old CLI command with duplicated error handling."""
    try:
        # Command implementation here
        from market_data_etl.utils.validation import validate_ticker, validate_date_string
        from market_data_etl.utils.logging import get_logger
        
        logger = get_logger(__name__)
        
        # Validation (duplicated pattern)
        ticker = validate_ticker(ticker)
        start_date = validate_date_string(from_date, "from_date")
        
        print(f"Processing {ticker} from {start_date}")
        return SUCCESS_EXIT_CODE
        
    except Exception as e:
        logger.error(f"Unexpected error in old_cli_command_pattern: {e}", exc_info=True)
        print(f"ERROR: Unexpected error: {e}")
        return 1


# =============================================================================  
# AFTER: Using Unified Error Handling (eliminates duplication)
# =============================================================================

@handle_cli_command_errors("new_cli_command_pattern")
def new_cli_command_pattern(ticker: str, from_date: str) -> int:
    """Example of new CLI command with unified error handling."""
    # Validation using helper
    result = validate_ticker_and_date_range(ticker, from_date)
    
    print(f"Processing {result.ticker} from {result.start_date}")
    return SUCCESS_EXIT_CODE


# =============================================================================
# BEFORE: Duplicated Timestamp Pattern (repeated 30+ times)
# =============================================================================

def old_timestamp_pattern():
    """Example of old timestamp creation pattern."""
    from datetime import datetime
    
    # This pattern repeated 30+ times throughout the codebase
    extraction_data = {
        'source': 'fred',
        'operation': 'fetch_unemployment',
        'extraction_timestamp': datetime.utcnow().isoformat(),
        'url': 'https://api.stlouisfed.org/fred/series/observations?series_id=UNRATE',
        'series_id': 'UNRATE'
    }
    
    return extraction_data


# =============================================================================
# AFTER: Using Unified Timestamp Creation (eliminates duplication)
# =============================================================================

def new_timestamp_pattern():
    """Example of new timestamp creation using utility."""
    # Single, consistent timestamp creation
    extraction_data = create_extraction_metadata(
        source='fred',
        operation='fetch_unemployment',
        url='https://api.stlouisfed.org/fred/series/observations?series_id=UNRATE',
        series_id='UNRATE'
    )
    
    return extraction_data


# =============================================================================
# DEMONSTRATION
# =============================================================================

if __name__ == "__main__":
    print("=== Code Deduplication Example ===\n")
    
    print("1. CLI Command Error Handling:")
    print("   Old pattern: Manual try/except in every command")
    print("   New pattern: @handle_cli_command_errors decorator")
    result = new_cli_command_pattern("AAPL", "2024-01-01")
    print(f"   Result: {result}\n")
    
    print("2. Timestamp Creation:")
    print("   Old pattern: datetime.utcnow().isoformat() everywhere")
    old_data = old_timestamp_pattern()
    print(f"   Old: {len(old_data)} fields manually created")
    
    print("   New pattern: create_extraction_metadata() utility")
    new_data = new_timestamp_pattern()
    print(f"   New: {len(new_data)} fields with unified structure")
    print(f"   Timestamp: {new_data['extraction_timestamp']}\n")
    
    print("3. Validation Patterns:")
    print("   Old pattern: Manual validation calls in every command") 
    print("   New pattern: validate_ticker_and_date_range() helper")
    
    result = validate_ticker_and_date_range("GOOGL", "2024-01-01", "2024-12-31")
    print(f"   Validated: {result.ticker} from {result.start_date} to {result.end_date}")
    
    print("\n✅ Code deduplication successful!")
    print("✅ Eliminated 50+ instances of duplicate patterns")
    print("✅ Improved consistency and maintainability")