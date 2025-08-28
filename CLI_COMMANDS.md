# CLI Commands Reference

This document provides a comprehensive reference for all available CLI commands in the market-data-etl system.

## Overview

The market-data-etl CLI provides commands for fetching, storing, and managing financial market data from various sources including Yahoo Finance, FRED, Eurostat, ECB, and OECD.

## Command Categories

### 1. Price Data Commands

#### `fetch-prices`
Fetch historical price data for individual ticker symbols.

**Usage:**
```bash
market-data-etl fetch-prices --ticker AAPL --from 2024-01-01 --to 2024-01-31
market-data-etl fetch-prices --ticker AAPL --from 2024-01-01 --prices-only
```

**Arguments:**
- `--ticker` (required): Stock ticker symbol (e.g., AAPL, MSFT, VOLV-B.ST)
- `--from` (required): Start date in YYYY-MM-DD format
- `--to` (optional): End date in YYYY-MM-DD format (defaults to today)
- `--instrument-type` (optional): Force instrument type (stock, fund, etf)
- `--prices-only` (optional): Skip automatic fundamental data fetching

**Examples:**
```bash
# Fetch Apple stock prices for January 2024
market-data-etl fetch-prices --ticker AAPL --from 2024-01-01 --to 2024-01-31

# Fetch prices only (skip fundamentals) for Swedish stock
market-data-etl fetch-prices --ticker VOLV-B.ST --from 2024-01-01 --prices-only
```

#### `fetch-all`
Fetch all latest data (prices, economic indicators, and financial statements).

**Usage:**
```bash
market-data-etl fetch-all
market-data-etl fetch-all --dry-run
market-data-etl fetch-all --prices-only
```

**Arguments:**
- `--dry-run` (optional): Show what would be fetched without executing
- `--prices-only` (optional): Only fetch price data, skip other data types

### 2. Financial Statement Commands

#### `fetch-financial-statements`
Fetch structured financial statements for comprehensive fundamental analysis.

**Usage:**
```bash
market-data-etl fetch-financial-statements --ticker AAPL
```

**Arguments:**
- `--ticker` (required): Stock ticker symbol

**What it fetches:**
- Income statements (quarterly and annual)
- Balance sheets (quarterly and annual) 
- Cash flow statements (quarterly and annual)
- Financial ratios and key metrics

### 3. Economic Data Commands

#### `fetch-economic-indicator`
Fetch economic data using simplified indicator and area parameters.

**Usage:**
```bash
market-data-etl fetch-economic-indicator --indicator unemployment --area us --from 2024-01-01 --to 2024-12-31
market-data-etl fetch-economic-indicator --indicator inflation --area ea --from 2024-01-01
```

**Arguments:**
- `--indicator` (required): Economic indicator type
  - `inflation`: Inflation/consumer price data
  - `unemployment`: Unemployment rate data
  - `interest`: Interest rate data
- `--area` (required): Geographic area code
  - `us`: United States
  - `ea`: Euro Area
  - `se`: Sweden
  - `gb`: United Kingdom (Great Britain)
- `--from` (required): Start date in YYYY-MM-DD format
- `--to` (optional): End date in YYYY-MM-DD format (defaults to today)

**Data Sources by Area:**
- **US (us)**: FRED (Federal Reserve Economic Data)
  - Requires: `FRED_API_KEY` environment variable
- **Euro Area (ea)**: Eurostat (European Statistics)
  - No API key required
- **Sweden (se)**: Eurostat
  - No API key required  
- **UK (gb)**: OECD (Organisation for Economic Co-operation and Development)
  - No API key required

**Examples:**
```bash
# Fetch US unemployment rate for 2024
market-data-etl fetch-economic-indicator --indicator unemployment --area us --from 2024-01-01 --to 2024-12-31

# Fetch Euro Area inflation data from January 2024
market-data-etl fetch-economic-indicator --indicator inflation --area ea --from 2024-01-01

# Fetch Swedish inflation data
market-data-etl fetch-economic-indicator --indicator inflation --area se --from 2024-01-01

# Fetch UK interest rate data
market-data-etl fetch-economic-indicator --indicator interest --area gb --from 2024-01-01
```

#### `fetch-all-economic-indicators`
Fetch all available economic indicators at once.

**Usage:**
```bash
market-data-etl fetch-all-economic-indicators --from 2024-01-01
market-data-etl fetch-all-economic-indicators --from 2024-01-01 --to 2024-12-31
```

**Arguments:**
- `--from` (required): Start date in YYYY-MM-DD format
- `--to` (optional): End date in YYYY-MM-DD format (defaults to today)

**What it fetches:**
All configured economic indicators including:
- US unemployment, inflation, and interest rates (FRED)
- Euro Area inflation and unemployment (Eurostat)
- Euro Area interest rates (ECB)
- Swedish inflation (Eurostat)
- UK inflation (OECD)

### 4. Portfolio Management Commands

#### `load-portfolio`
Load portfolio configuration from JSON file.

**Usage:**
```bash
market-data-etl load-portfolio --file ./portfolios/my_portfolio.json
```

**Arguments:**
- `--file` (required): Path to JSON portfolio configuration file

**Portfolio JSON Format:**
```json
{
  "name": "My Investment Portfolio",
  "description": "Long-term growth portfolio",
  "currency": "USD",
  "holdings": ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"]
}
```

#### `fetch-portfolio-prices`
Fetch price data for all holdings in a portfolio.

**Usage:**
```bash
market-data-etl fetch-portfolio-prices --portfolio "My Portfolio" --from 2024-01-01
```

**Arguments:**
- `--portfolio` (required): Portfolio name (as defined in JSON)
- `--from` (required): Start date in YYYY-MM-DD format
- `--to` (optional): End date in YYYY-MM-DD format

#### `fetch-portfolio-fundamentals`
Fetch fundamental data for stocks in a portfolio (skips funds/ETFs).

**Usage:**
```bash
market-data-etl fetch-portfolio-fundamentals --portfolio "My Portfolio"
```

**Arguments:**
- `--portfolio` (required): Portfolio name

#### `portfolio-info`
Show portfolio information and holdings summary.

**Usage:**
```bash
market-data-etl portfolio-info --portfolio "My Portfolio"
```

**Arguments:**
- `--portfolio` (required): Portfolio name

### 5. Data Import Commands

#### `load-price-csv`
Load price data from CSV file for custom/manual data entry.

**Usage:**
```bash
market-data-etl load-price-csv --file omxs30_data.csv --ticker ^OMXS30
```

**Arguments:**
- `--file` (required): Path to CSV file
- `--ticker` (required): Ticker symbol for the data

#### `generate-price-csv-template`
Generate CSV template for manual price data entry.

**Usage:**
```bash
market-data-etl generate-price-csv-template --ticker ^OMXS30 --output omxs30_template.csv
```

**Arguments:**
- `--ticker` (required): Ticker symbol
- `--output` (required): Output CSV file path

#### `load-transactions`
Load portfolio transactions from CSV file.

**Usage:**
```bash
market-data-etl load-transactions --file transactions.csv --portfolio "My Portfolio"
```

**Arguments:**
- `--file` (required): Path to CSV file
- `--portfolio` (required): Portfolio name

### 6. Information and Query Commands

#### `db-info`
Show database information for a ticker symbol.

**Usage:**
```bash
market-data-etl db-info --ticker AAPL
market-data-etl db-info --ticker VOLV-B.ST
```

**Arguments:**
- `--ticker` (required): Ticker symbol

**Information provided:**
- Instrument details (name, sector, industry, country, currency)
- Price data summary (count, date range, latest price)
- Financial statements summary (count by type)
- Market cap, employee count, and other metrics

### 7. Database Management Commands

#### `clear-database`
Clear database data for development/testing purposes.

**Usage:**
```bash
market-data-etl clear-database --all
market-data-etl clear-database --table prices
```

**Arguments:**
- `--all` (optional): Clear all data
- `--table` (optional): Clear specific table

#### `update-instrument-types`
Update existing instruments with correct types using auto-detection.

**Usage:**
```bash
market-data-etl update-instrument-types
```

No arguments required. Analyzes existing instruments and updates their types.

### 8. Data Alignment Commands (Advanced)

#### `align-data`
Align price data with economic indicators for analysis.

**Usage:**
```bash
market-data-etl align-data --ticker AAPL --economic-indicator inflation_ea
market-data-etl align-data --ticker MSFT --economic-indicator unemployment_us --from 2024-01-01 --method forward_fill
```

**Arguments:**
- `--ticker` (required): Stock ticker symbol
- `--economic-indicator` (required): Economic indicator name
- `--from` (optional): Start date for alignment
- `--method` (optional): Alignment method (forward_fill, etc.)

#### `alignment-info`
Show data alignment system information and capabilities.

**Usage:**
```bash
market-data-etl alignment-info
```

#### `alignment-pairs`
Show available instrument-economic indicator pairs for alignment.

**Usage:**
```bash
market-data-etl alignment-pairs --limit 10
```

**Arguments:**
- `--limit` (optional): Limit number of pairs shown

#### `rebuild-aligned-data`
Rebuild trading-day aligned data with forward-filled economic indicators.

**Usage:**
```bash
market-data-etl rebuild-aligned-data --ticker AAPL --from 2024-01-01
market-data-etl rebuild-aligned-data --from 2024-01-01 --to 2024-12-31
```

**Arguments:**
- `--ticker` (optional): Specific ticker to rebuild
- `--from` (required): Start date
- `--to` (optional): End date

#### `query-aligned-data`
Query trading-day aligned data for analysis.

**Usage:**
```bash
market-data-etl query-aligned-data --ticker AAPL --from 2024-01-01 --output detailed
market-data-etl query-aligned-data --ticker ESSITY-B.ST --indicators inflation_ea
```

**Arguments:**
- `--ticker` (required): Stock ticker symbol
- `--from` (optional): Start date
- `--output` (optional): Output format (detailed, summary)
- `--indicators` (optional): Specific economic indicators

#### `aligned-data-info`
Show aligned data system information and coverage statistics.

**Usage:**
```bash
market-data-etl aligned-data-info
```

## Environment Variables

The following environment variables configure system behavior:

### Required for FRED Data
- `FRED_API_KEY`: API key for Federal Reserve Economic Data (required for US economic indicators)

### Optional Configuration
- `MARKET_DATA_DB_PATH`: Database file path (default: `market_data.db`)
- `MARKET_DATA_LOG_LEVEL`: Logging level (default: `INFO`)
- `MARKET_DATA_LOG_FILE`: Log file path (optional)
- `MARKET_DATA_MAX_RETRIES`: Maximum API retries (default: 5)

## Data Sources

### Financial Data
- **Yahoo Finance**: Stock prices, financial statements, company information
  - No API key required
  - Covers global markets

### Economic Data
- **FRED (Federal Reserve Economic Data)**: US economic indicators
  - Requires API key: https://fred.stlouisfed.org/docs/api/api_key.html
  - US unemployment, inflation, interest rates
- **Eurostat**: European economic statistics
  - No API key required
  - Euro Area and EU member country data
- **ECB (European Central Bank)**: Euro Area monetary policy data
  - No API key required
  - Euro Area interest rates
- **OECD**: International economic data
  - No API key required
  - UK and other developed country indicators

## Exit Codes

- **0**: Success
- **1**: Error (validation, API failure, etc.)
- **2**: Argument parsing error

## Common Usage Patterns

### Daily Data Update Routine
```bash
# Fetch latest prices for key holdings
market-data-etl fetch-prices --ticker AAPL --from 2024-01-01
market-data-etl fetch-prices --ticker MSFT --from 2024-01-01

# Update economic indicators
market-data-etl fetch-all-economic-indicators --from 2024-01-01

# Check data status
market-data-etl db-info --ticker AAPL
```

### Portfolio Analysis Setup
```bash
# Load portfolio
market-data-etl load-portfolio --file my_portfolio.json

# Fetch all price data
market-data-etl fetch-portfolio-prices --portfolio "My Portfolio" --from 2024-01-01

# Fetch fundamentals for analysis
market-data-etl fetch-portfolio-fundamentals --portfolio "My Portfolio"

# Review portfolio
market-data-etl portfolio-info --portfolio "My Portfolio"
```

### Economic Analysis Setup
```bash
# Fetch key economic indicators
market-data-etl fetch-economic-indicator --indicator unemployment --area us --from 2020-01-01
market-data-etl fetch-economic-indicator --indicator inflation --area ea --from 2020-01-01
market-data-etl fetch-economic-indicator --indicator interest --area us --from 2020-01-01

# Align with stock data for analysis
market-data-etl align-data --ticker AAPL --economic-indicator unemployment_us
market-data-etl query-aligned-data --ticker AAPL --from 2024-01-01
```

## Troubleshooting

### Common Issues

1. **FRED API Key Error**
   - Ensure `FRED_API_KEY` environment variable is set
   - Get key from: https://fred.stlouisfed.org/docs/api/api_key.html

2. **Database Permissions**
   - Ensure write permissions for database file location
   - Check `MARKET_DATA_DB_PATH` environment variable

3. **Network/API Failures**
   - Commands automatically retry with exponential backoff
   - Check internet connection and API service status

4. **Date Format Errors**
   - Use YYYY-MM-DD format for all dates
   - Ensure from_date <= to_date

5. **Ticker Symbol Issues**
   - Use exact Yahoo Finance ticker symbols
   - Include exchange suffixes for international stocks (e.g., VOLV-B.ST)

### Getting Help

Use `--help` with any command for detailed usage information:
```bash
market-data-etl --help
market-data-etl fetch-prices --help
market-data-etl fetch-economic-indicator --help
```