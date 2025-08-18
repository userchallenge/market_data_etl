# Market Data ETL

[![Python 3.12.9+](https://img.shields.io/badge/python-3.12.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Python package for extracting market data from Yahoo Finance and economic data from Eurostat/ECB/FRED into SQLite database.

## Installation

**Requirements**: Python 3.12.9 or higher

```bash
git clone https://github.com/your-username/market-data-etl.git
cd market-data-etl

# Create virtual environment with Python 3.12.9
python3.12 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install --upgrade pip
pip install -e .
```

## Data Types

- **Price Data**: OHLC, volume, adjusted close
- **Fundamental Data**: Financial statements, company info, analyst data
- **Economic Data**: Indicators from Eurostat, ECB, FRED
- **Portfolio Data**: Holdings and transactions
- **Data Alignment**: Combine price and economic data for analysis

## CLI Commands

**Price Data**: `fetch-prices`, `load-price-csv`, `generate-price-csv-template`
**Financial Data**: `fetch-financial-statements`, `financial-summary`, `fetch-fundamentals`
**Economic Data**: `fetch-economic`, `economic-info`
**Portfolio Management**: `load-portfolio`, `load-transactions`, `fetch-portfolio-prices`, `fetch-portfolio-fundamentals`, `portfolio-info`
**Data Alignment**: `align-data`, `alignment-info`, `alignment-pairs`
**Database & Utility**: `db-info`, `clear-database`, `update-instrument-types`

```bash
# Examples
market-data-etl fetch-prices --ticker AAPL --from 2024-01-01
market-data-etl fetch-economic --source fred --indicator UNRATE --from 2024-01-01 --to 2024-12-31  # Uses FRED_API_KEY env var
market-data-etl fetch-economic --source eurostat --indicator prc_hicp_mmor --from 2024-01-01
market-data-etl economic-info --indicator unemployment_monthly_rate_us

# Data Alignment Examples
market-data-etl align-data --ticker AAPL --economic-indicator inflation_monthly_us
market-data-etl align-data --ticker MSFT --economic-indicator unemployment_monthly_rate_us --from 2024-01-01 --method forward_fill
market-data-etl alignment-info  # Show system information
market-data-etl alignment-pairs --limit 10  # Show available data combinations

market-data-etl --help  # Show all commands
```

## Python API

```python
from market_data_etl import PriceFetcher, DatabaseManager
from market_data_etl.etl.load import EconomicETLOrchestrator
from market_data_etl.data.data_alignment import DataAlignment
from datetime import date

# Market data
fetcher = PriceFetcher()
db = DatabaseManager()
price_data = fetcher.fetch_price_data("AAPL", date(2024, 1, 1), date(2024, 1, 31))
db.store_price_data("AAPL", price_data)

# Economic data
economic_etl = EconomicETLOrchestrator(db)
results = economic_etl.run_eurostat_etl("prc_hicp_mmor", "2020-01-01")
df = db.get_economic_data("inflation_monthly_euro")

# Data alignment
alignment = DataAlignment(db)
aligned_data = alignment.align_price_economic_data(
    ticker="AAPL",
    economic_indicator="inflation_monthly_us",
    method="last_of_period"
)
```

## Common Economic Indicators

| Source | Indicator | ID | Description |
|--------|-----------|----|-----------| 
| Eurostat | EU Inflation | `prc_hicp_midx` | HICP inflation index |
| Eurostat | EU Unemployment | `une_rt_m` | Unemployment rate |
| ECB | Interest Rate | `FM.B.U2.EUR.4F.KR.MRR_FR.LEV` | Main refinancing rate |
| FRED | US Unemployment | `UNRATE` | US unemployment rate |
| FRED | US Inflation | `CPIAUCSL` | Consumer Price Index |
| FRED | Fed Rate | `DFF` | Federal funds rate |

## Configuration

Environment variables:
```bash
export MARKET_DATA_DB_PATH="./market_data.db"
export MARKET_DATA_LOG_LEVEL="INFO"
export MARKET_DATA_MAX_RETRIES="5"
export FRED_API_KEY="your_fred_api_key_here"
```

## Database

Single SQLite file (`market_data.db`) containing all data types. Schema defined in `market_data_etl/data/models.py`.

## Architecture

- `data/` - Database models and API fetchers
- `etl/` - Extract, transform, load pipeline
- `database/` - Database management
- `cli/` - Command line interface
- `utils/` - Configuration, logging, exceptions

See `CLAUDE.md` for implementation guidelines.

## License

MIT License - see [LICENSE](LICENSE) file for details.