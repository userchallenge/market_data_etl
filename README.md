# Market Data ETL

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Version](https://img.shields.io/badge/version-1.0.0-green.svg)](https://github.com/your-username/market-data-etl)

A professional Python package for extracting, transforming, and loading (ETL) market data from Yahoo Finance into a local SQLite database. Built with enterprise-grade features including robust error handling, intelligent retry logic, and comprehensive data validation.

## 🚀 Features

### Core Functionality
- **Historical Price Data**: Extract daily OHLC (Open, High, Low, Close), Adjusted Close, and Volume data
- **Comprehensive Fundamental Data**: Complete financial statements (annual & quarterly), company information, analyst recommendations, institutional holdings, insider transactions, and SEC filings - everything needed for thorough fundamental analysis
- **Smart Gap Detection**: Intelligently identifies and fetches only missing data to avoid redundant API calls
- **Multi-Market Support**: Works with US stocks, international markets, and complex ticker formats (e.g., `ERIC-B.ST`)

### Enterprise Features
- **Robust Error Handling**: No silent failures - all errors reported with clear, actionable messages
- **Exponential Backoff Retry**: Handles Yahoo Finance rate limiting with configurable retry strategies
- **Configuration Management**: Environment variable support and flexible configuration options
- **Professional Logging**: Structured logging with multiple levels and optional file output
- **Database Integrity**: ACID-compliant SQLite storage with proper foreign key relationships

### Developer Experience
- **Clean Architecture**: Well-organized package structure with separate modules for data, database, CLI, and utilities
- **Type Hints**: Full type annotation for better IDE support and code maintainability
- **Comprehensive CLI**: Intuitive command-line interface with helpful error messages and examples
- **Easy Installation**: Standard setuptools package with entry points and dependency management

## 📦 Installation

### Quick Install
```bash
pip install market-data-etl
```

### Development Install
```bash
git clone https://github.com/your-username/market-data-etl.git
cd market-data-etl
pip install -e .
```

### With Development Dependencies
```bash
pip install -e .[dev]
```

## 🏃 Quick Start

### Command Line Usage

After installation, use the `market-data-etl` command or the shorter `mdetl` alias:

```bash
# Fetch price data for Apple stock
market-data-etl fetch-prices --ticker AAPL --from 2024-01-01 --to 2024-01-31

# Fetch fundamental data for Microsoft
market-data-etl fetch-fundamentals --ticker MSFT

# Check what data exists for a Swedish stock
market-data-etl db-info --ticker VOLV-B.ST

# Clear database for development/testing
market-data-etl clear-database --ticker AAPL

# Enable verbose logging
market-data-etl fetch-prices --ticker GOOGL --from 2024-01-01 --verbose
```

### Python API Usage

```python
from market_data_etl import PriceFetcher, DatabaseManager
from datetime import date

# Initialize components
fetcher = PriceFetcher()
db = DatabaseManager()

# Fetch and store price data
price_data = fetcher.fetch_price_data("AAPL", date(2024, 1, 1), date(2024, 1, 31))
inserted_count = db.store_price_data("AAPL", price_data)

print(f"Inserted {inserted_count} price records")

# Get database information
info = db.get_ticker_info("AAPL")
print(f"Price records: {info['price_data']['count']}")
print(f"Date range: {info['price_data']['date_range']}")
```

## 🛠 CLI Commands

### `fetch-prices`
Fetch historical price data for a ticker with intelligent gap detection.

```bash
market-data-etl fetch-prices --ticker TICKER --from YYYY-MM-DD [--to YYYY-MM-DD]
```

**Features:**
- Only fetches missing dates (gap detection)
- Supports both US and international tickers
- Handles weekends and holidays automatically
- Clear progress reporting

**Example Output:**
```
Fetching prices for AAPL from 2024-01-01 to 2024-01-31...
Found 15 days already in database, fetching 7 missing days...
Inserted 7 new price records for AAPL.
Operation completed successfully.
```

### `fetch-fundamentals`
Fetch comprehensive fundamental data including financials and key metrics.

```bash
market-data-etl fetch-fundamentals --ticker TICKER
```

**Features:**
- Fetches all available fundamental modules
- Stores both structured and raw JSON data
- Handles different reporting periods (annual, quarterly)
- Updates existing data when available

**Example Output:**
```
Fetching fundamentals for AAPL...
Modules fetched: financial_data, key_stats, income_statement, balance_sheet
Inserted/Updated 12 fundamental records for AAPL.
Operation completed successfully.
```

### `db-info`
Display comprehensive information about stored data for a ticker.

```bash
market-data-etl db-info --ticker TICKER
```

**Example Output:**
```
Database information for AAPL:
----------------------------------------
Ticker created: 2024-01-15 10:30:45

Price data:
  Records: 252
  Date range: 2024-01-01 to 2024-12-31

Fundamental modules (8):
  - balance_sheet
  - financial_data
  - income_statement
  - key_stats
```

### `clear-database`
Clear database data for development and testing purposes.

```bash
# Clear data for specific ticker
market-data-etl clear-database --ticker TICKER

# Clear all data from database
market-data-etl clear-database --all

# Skip confirmation prompt (useful for scripts)
market-data-etl clear-database --all --confirm
```

**Features:**
- Safety confirmation prompts before deletion
- Selective ticker clearing for targeted testing
- Complete database reset for fresh starts
- Confirmation bypass for automated scripts
- Comprehensive data removal (prices, financials, company info)

**Example Output:**
```
⚠️  WARNING: This will permanently delete all data for ticker AAPL!
Are you sure you want to continue? (yes/no): yes
Clearing all data for ticker AAPL...
✅ Successfully cleared all data for AAPL
```

## ⚙️ Configuration

### Environment Variables
Configure the system using environment variables:

```bash
# Database settings
export MARKET_DATA_DB_PATH="./my_market_data.db"
export MARKET_DATA_DB_ECHO="false"

# Logging settings  
export MARKET_DATA_LOG_LEVEL="INFO"
export MARKET_DATA_LOG_FILE="./market_data.log"

# API retry settings
export MARKET_DATA_MAX_RETRIES="5"
export MARKET_DATA_INITIAL_BACKOFF="1.0"
export MARKET_DATA_BACKOFF_MULTIPLIER="2.0"
```

### Python Configuration
```python
from market_data_etl.config import config

# Access current configuration
print(f"Database path: {config.database.path}")
print(f"Max retries: {config.retry.max_retries}")
print(f"Log level: {config.log_level}")
```

## 🗄 Database Schema

### SQLite + SQLAlchemy Design
**Why SQLite?** Perfect for single-user, local market data collection:
- **Zero Configuration**: No database server setup required
- **Portable**: Single file database that's easy to backup and move
- **ACID Compliant**: Ensures data integrity during concurrent operations
- **Minimal Operations**: Automatic schema creation and management

### Tables

#### `tickers`
```sql
id          INTEGER PRIMARY KEY
symbol      VARCHAR(20) UNIQUE NOT NULL
created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
```

#### `prices`
```sql
id          INTEGER PRIMARY KEY
ticker_id   INTEGER FOREIGN KEY -> tickers.id
date        DATE NOT NULL
open        FLOAT
high        FLOAT
low         FLOAT
close       FLOAT
adj_close   FLOAT
volume      INTEGER
created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
```

#### `fundamentals`
```sql
id             INTEGER PRIMARY KEY
ticker_id      INTEGER FOREIGN KEY -> tickers.id
module_name    VARCHAR(100) NOT NULL
report_date    DATE
period_type    VARCHAR(20)
revenue        FLOAT
net_income     FLOAT
total_assets   FLOAT
total_debt     FLOAT
market_cap     FLOAT
data_snapshot  JSON
created_at     DATETIME DEFAULT CURRENT_TIMESTAMP
updated_at     DATETIME DEFAULT CURRENT_TIMESTAMP
```

## 🔧 Advanced Usage

### Batch Processing Multiple Tickers
```python
from market_data_etl import PriceFetcher, DatabaseManager
from datetime import date

tickers = ["AAPL", "MSFT", "GOOGL", "TSLA"]
fetcher = PriceFetcher()
db = DatabaseManager()

for ticker in tickers:
    try:
        print(f"Processing {ticker}...")
        price_data = fetcher.fetch_price_data(
            ticker, 
            date(2024, 1, 1), 
            date(2024, 12, 31)
        )
        count = db.store_price_data(ticker, price_data)
        print(f"  Stored {count} records for {ticker}")
    except Exception as e:
        print(f"  Error processing {ticker}: {e}")
```

### Custom Database Path
```python
from market_data_etl import DatabaseManager

# Use custom database location
db = DatabaseManager("/path/to/my/market_data.db")
```

### Working with International Markets
```bash
# Swedish stocks
market-data-etl fetch-prices --ticker VOLV-B.ST --from 2024-01-01

# German stocks  
market-data-etl fetch-prices --ticker SAP.DE --from 2024-01-01

# Japanese stocks
market-data-etl fetch-prices --ticker 7203.T --from 2024-01-01
```

## 🚨 Error Handling

The system provides comprehensive error handling with clear, actionable messages:

### Rate Limiting
```
ERROR: Failed to fetch prices for AAPL from Yahoo Finance.
HTTP 429: Too Many Requests.
Endpoint: https://query1.finance.yahoo.com/v8/finance/chart/AAPL
Tried 5 times with exponential backoff. Please retry later.
```

### Invalid Ticker
```
ERROR: Failed to fetch prices for INVALID from Yahoo Finance.
Ticker INVALID not found or has no data
```

### Date Validation
```
ERROR: Invalid date format: 2024-13-01. Expected YYYY-MM-DD format.
```

## 🏗 Package Structure

```
market-data-etl/
├── README.md                           # This file
├── setup.py                            # Package installation
├── requirements.txt                    # Dependencies
├── market_data_etl/                    # Main package
│   ├── __init__.py                     # Package initialization
│   ├── config.py                       # Configuration management
│   ├── data/                           # Data fetching and models
│   │   ├── __init__.py
│   │   ├── fetchers.py                 # Yahoo Finance API clients
│   │   └── models.py                   # SQLAlchemy database models
│   ├── database/                       # Database operations
│   │   ├── __init__.py
│   │   └── manager.py                  # Database management
│   ├── cli/                            # Command-line interface
│   │   ├── __init__.py
│   │   ├── commands.py                 # CLI command implementations
│   │   └── main.py                     # CLI entry point
│   └── utils/                          # Utilities
│       ├── __init__.py
│       ├── exceptions.py               # Custom exceptions
│       └── logging.py                  # Logging configuration
├── tests/                              # Test suite
│   ├── __init__.py
│   ├── test_fetchers.py
│   ├── test_database.py
│   └── test_cli.py
├── examples/                           # Usage examples
│   ├── basic_usage.py
│   └── advanced_usage.py
└── docs/                               # Documentation
    └── API.md
```

## 🧪 Testing

### Run Tests
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=market_data_etl

# Run specific test file
pytest tests/test_fetchers.py
```

### Example Test
```python
from market_data_etl import PriceFetcher
from datetime import date

def test_price_fetcher():
    fetcher = PriceFetcher()
    data = fetcher.fetch_price_data("AAPL", date(2024, 1, 1), date(2024, 1, 5))
    
    assert not data.empty
    assert 'date' in data.columns
    assert 'close' in data.columns
    assert len(data) > 0
```

## 📊 Data Sources

### Yahoo Finance APIs
- **yfinance**: Both historical price data AND comprehensive fundamental data (refactored for better reliability)

### Supported Data Types

#### Price Data
- Open, High, Low, Close prices
- Adjusted Close (dividend and split adjusted)
- Trading Volume
- Daily frequency with smart gap detection

#### Fundamental Data (Perfect for Financial Analysis)
- **Financial Statements**: Income Statements, Balance Sheets, Cash Flow Statements (both annual & quarterly)
- **Company Information**: Business description, key statistics, market cap, financial metrics
- **Market Data**: Analyst recommendations, price targets, earnings estimates
- **Ownership Information**: Institutional holders, mutual fund holdings with ownership percentages
- **Insider Activity**: Insider purchases, sales, and transaction details
- **Corporate Actions**: SEC filings, financial calendar, earnings dates
- **Valuation Metrics**: P/E ratios, financial ratios, and key performance indicators

All data is properly structured and stored in SQLite for efficient querying and analysis.

## ⚠️ Rate Limiting & Best Practices

### Yahoo Finance Rate Limits
- The system handles rate limiting automatically with exponential backoff
- Default: 5 retries with delays of 1s, 2s, 4s, 8s, 16s
- Only retries on transient errors (HTTP 429, 500-599)

### Best Practices
1. **Batch Processing**: Process multiple tickers in sequence rather than parallel
2. **Gap Detection**: Always rely on the built-in gap detection to avoid redundant calls
3. **Error Handling**: Always check return codes and handle exceptions appropriately
4. **Logging**: Use verbose mode during development and testing

## 🆕 Recent Major Improvements

### Enhanced Fundamental Data Fetching (v1.0.0)
- **✅ Fixed DataFrame Errors**: Resolved "truth value of DataFrame is ambiguous" errors
- **🚀 Upgraded to yfinance 0.2.61**: Better API reliability and more comprehensive data
- **💾 Improved Data Storage**: Proper JSON serialization of Timestamps and complex data
- **📊 More Data Types**: Now fetches 12-15 types of fundamental data including:
  - Annual & Quarterly Financial Statements
  - Institutional & Mutual Fund Holdings  
  - Insider Trading Activity
  - SEC Filings & Financial Calendar
  - Analyst Recommendations

### What This Means for You
- **No More Errors**: Swedish and international tickers now work perfectly
- **Richer Analysis**: Complete fundamental data for thorough financial analysis
- **Better Reliability**: Robust error handling and data validation

## 🔄 Updates & Maintenance

This README is continuously updated to reflect the current state of the package. Key areas that are kept current:

- **API Changes**: Yahoo Finance API updates and compatibility
- **Feature Additions**: New functionality and capabilities
- **Configuration Options**: New environment variables and settings
- **Error Handling**: Updated error messages and troubleshooting
- **Examples**: Current, working code examples
- **Dependencies**: Package version requirements

## 🤝 Contributing

### Development Setup
```bash
git clone https://github.com/your-username/market-data-etl.git
cd market-data-etl
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows
pip install -e .[dev]
```

### Code Standards
- **Black**: Code formatting
- **flake8**: Linting
- **mypy**: Type checking
- **pytest**: Testing

```bash
# Format code
black market_data_etl/

# Lint code  
flake8 market_data_etl/

# Type check
mypy market_data_etl/

# Run tests
pytest
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Yahoo Finance** for providing free market data APIs
- **yfinance** and **yahooquery** maintainers for excellent Python interfaces
- **SQLAlchemy** team for the robust ORM framework
- **Claude Code** for development assistance

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/your-username/market-data-etl/issues)
- **Documentation**: [API Documentation](docs/API.md)
- **Examples**: [Examples Directory](examples/)

---

**Market Data ETL** - Professional market data extraction made simple.

*Generated with [Claude Code](https://claude.ai/code)*