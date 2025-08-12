# Market Data ETL

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Python package for extracting market data from Yahoo Finance and economic data from Eurostat/ECB/FRED into SQLite database.

## Installation

```bash
git clone https://github.com/your-username/market-data-etl.git
cd market-data-etl
pip install -e .
```

## Data Types

- **Price Data**: OHLC, volume, adjusted close
- **Fundamental Data**: Financial statements, company info, analyst data
- **Economic Data**: Indicators from Eurostat, ECB, FRED
- **Portfolio Data**: Holdings and transactions

## CLI Usage

### Price Data
```bash
market-data-etl fetch-prices --ticker AAPL --from 2024-01-01 --to 2024-01-31
market-data-etl fetch-fundamentals --ticker MSFT
market-data-etl db-info --ticker VOLV-B.ST
```

### Economic Data
```bash
market-data-etl fetch-economic --source eurostat --indicator prc_hicp_midx --from 2024-01-01
market-data-etl fetch-economic --source fred --indicator UNRATE --from 2024-01-01 --to 2024-12-31 --api-key YOUR_KEY
market-data-etl economic-info --indicator prc_hicp_midx
```

### Portfolio Data
```bash
market-data-etl load-portfolio --file ./portfolios/my_portfolio.json
market-data-etl fetch-portfolio-prices --portfolio "My Portfolio" --from 2024-01-01
```

### Database Management
```bash
market-data-etl clear-database --ticker AAPL
market-data-etl clear-database --all --confirm
```

## Python API

```python
from market_data_etl import PriceFetcher, DatabaseManager
from market_data_etl.etl.load import EconomicETLOrchestrator
from datetime import date

# Market data
fetcher = PriceFetcher()
db = DatabaseManager()
price_data = fetcher.fetch_price_data("AAPL", date(2024, 1, 1), date(2024, 1, 31))
db.store_price_data("AAPL", price_data)

# Economic data
economic_etl = EconomicETLOrchestrator(db)
results = economic_etl.run_eurostat_etl("prc_hicp_midx", "2020-01-01")
df = db.get_economic_data("prc_hicp_midx")
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