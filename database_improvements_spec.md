# Database Schema and Data Alignment Improvements

## Overview

This document outlines two phases of improvements to the market-data-etl package to better handle different asset types and align data with varying frequencies.

## Phase 1: Instrument Type Classification

### Problem
The current `Company` table stores stocks, funds, ETFs, and indices together, but indices like `^OMXS30` don't conceptually fit as "companies." This leads to inappropriate data fetching (e.g., trying to get fundamental data for indices).

### Solution: Extend InstrumentType Enum

#### Required Changes

1. **Extend InstrumentType enum** in `market_data_etl/data/models.py`:
   ```python
   class InstrumentType(enum.Enum):
       STOCK = "stock"
       FUND = "fund" 
       ETF = "etf"
       INDEX = "index"      # Add this
       COMMODITY = "commodity"  # Optional: for future use
   ```

2. **Add index-specific fields** to the Company table:
   ```python
   # Add these fields to Company class
   index_methodology = Column(String(200))  # "Market cap weighted", "Price weighted"
   constituent_count = Column(Integer)      # Number of stocks in index
   base_date = Column(Date)                 # When index started
   base_value = Column(Float)               # Starting value (e.g., 100)
   ```

3. **Update business logic** to prevent fetching fundamentals for indices:
   - Modify `fetch_portfolio_fundamentals_command` in `market_data_etl/cli/commands.py`
   - Add filtering logic: only fetch fundamentals for `InstrumentType.STOCK`
   - Skip INDEX, FUND, ETF instruments in fundamental data operations

#### Business Rules
```python
def should_fetch_fundamentals(instrument_type: InstrumentType) -> bool:
    """Only fetch fundamentals for stocks"""
    return instrument_type == InstrumentType.STOCK

def should_fetch_constituents(instrument_type: InstrumentType) -> bool:
    """Fetch constituents for indices and some ETFs"""
    return instrument_type in [InstrumentType.INDEX, InstrumentType.ETF]
```

#### Critical Requirements
- All existing functionality must continue to work unchanged
- No breaking changes to existing CLI commands
- No changes to database table names or existing field names
- Maintain backward compatibility with existing data
- Only add new functionality, don't modify existing behavior

---

## Phase 2: Data Alignment System

### Problem
Different asset types have different date frequencies:
- **Stock prices**: Trading days only (no weekends/holidays)
- **Economic indicators**: Monthly (CPI, unemployment) or event-based (interest rate changes)
- **Analysis needs**: Aligned timeline where each stock price has latest macro context

### Solution: Implement Data Alignment System

#### Data Handling Strategy

1. **Stock Price Data (No Weekends)**
   - Equity markets don't trade on weekends/holidays
   - Use trading days as the base timeline
   - Never artificially fill weekends with fake data
   - Use last available trading day's close when needed

2. **Economic Indicators (Sparse/Event-based)**
   
   **Monthly indicators** (inflation, unemployment, CPI):
   - Published with lag (e.g., Jan CPI released mid-February)
   - Forward-fill value for every trading day until next update
   - Example: Jan CPI published Feb 15 → apply from Feb 15 until Mar 15

   **Interest rates** (Fed/ECB decisions):
   - Only have dates for changes
   - Forward-fill from change date until next change
   - Example: Jul 27: 3.75% → Sep 14: 4.00% means 3.75% for Jul 27–Sep 13

3. **Alignment Process**
   - Use stock price trading calendar as base index
   - Join economic indicators with forward-fill logic
   - Every stock price gets most recent available macro context

#### Required Implementation

1. **Create new data alignment module** `market_data_etl/utils/data_alignment.py`:
   ```python
   def align_to_trading_calendar(stock_df: pd.DataFrame, economic_df: pd.DataFrame) -> pd.DataFrame:
       """Align economic data to stock trading calendar using forward-fill"""
   
   def forward_fill_economic_indicators(df: pd.DataFrame) -> pd.DataFrame:
       """Forward-fill monthly/event-based economic data"""
   
   def get_latest_macro_context(date: date, db_manager: DatabaseManager) -> Dict[str, float]:
       """Get latest available economic indicators for a given date"""
   
   def create_aligned_dataset(ticker: str, indicators: List[str], 
                             from_date: date, to_date: date) -> pd.DataFrame:
       """Create fully aligned dataset with stock prices and economic indicators"""
   ```

2. **Add alignment methods to DatabaseManager** in `market_data_etl/database/manager.py`:
   ```python
   def get_aligned_data(self, ticker: str, from_date: date, to_date: date, 
                       indicators: List[str] = None) -> pd.DataFrame:
       """Get stock prices with aligned economic indicators"""
   
   def get_trading_calendar(self, ticker: str, from_date: date, to_date: date) -> List[date]:
       """Get list of actual trading days for a ticker"""
   
   def get_economic_data_aligned(self, indicator_names: List[str], 
                                trading_dates: List[date]) -> pd.DataFrame:
       """Get economic indicators aligned to trading calendar"""
   ```

3. **Add CLI command** in `market_data_etl/cli/commands.py`:
   ```python
   def align_data_command(ticker: str, from_date: str, to_date: str, 
                         indicators: Optional[str] = None) -> int:
       """Create and display aligned dataset for analysis"""
   ```

4. **Add to CLI parser** in `market_data_etl/cli/main.py`:
   ```bash
   # New command
   market-data-etl align-data --ticker AAPL --from 2024-01-01 --to 2024-01-31 
   market-data-etl align-data --ticker AAPL --from 2024-01-01 --to 2024-01-31 --indicators "unemployment_monthly_rate_us,interest_rate_monthly_us"
   ```

#### Example Implementation Logic
```python
# Pseudocode for alignment process
def create_aligned_dataset(ticker: str, from_date: date, to_date: date) -> pd.DataFrame:
    # 1. Get stock prices (trading days only)
    stock_df = get_price_data(ticker, from_date, to_date)
    trading_dates = stock_df.index.tolist()
    
    # 2. Get economic indicators
    economic_df = get_economic_data_for_dates(trading_dates)
    
    # 3. Forward-fill economic data to match trading calendar
    economic_aligned = economic_df.reindex(trading_dates, method='ffill')
    
    # 4. Combine
    result = stock_df.join(economic_aligned, how='left')
    
    return result
```

#### Business Rules
- Stock trading days = base timeline (no artificial weekend data)
- Economic indicators forward-filled until next update
- Each stock price gets latest available macro context
- Handle publication lags appropriately
- Efficient database queries (avoid loading all data unnecessarily)

#### Critical Requirements
- No changes to existing functionality
- All existing CLI commands continue to work unchanged
- New functionality is additive only
- Proper error handling for missing data
- Efficient performance with large datasets
- Clear documentation and examples

---

## Implementation Notes

### Testing Strategy
**Phase 1:**
- Run existing CLI commands to ensure they still work
- Verify index instruments can be stored as InstrumentType.INDEX
- Confirm fundamental data fetching skips index instruments
- Test portfolio operations with mixed instrument types

**Phase 2:**
- Verify stock prices only show trading days
- Confirm economic indicators forward-fill correctly
- Test alignment with different date ranges
- Ensure performance with large datasets
- Test edge cases (missing data, publication lags)

### Migration Considerations
- Both phases maintain backward compatibility
- No breaking changes to existing database schema
- SQLite schema changes are additive only
- Existing data continues to work without modification

### Future Extensions
- Phase 1 enables easy addition of commodities, crypto, bonds
- Phase 2 provides foundation for sophisticated financial analysis
- Both phases maintain the clean ETL architecture