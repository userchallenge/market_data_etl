Is there a date limitation for fetch-financial-statements? It only seems to get 4-5 year and the latest 4 quarters?

We did create a forward fill functionality for economic indicators that I want to remove. I want the latest data to in line with what's fetched from the database. I no longer need the dates to be extended from last date from API to todays date. Please analyse where this is present for all economic indicators and revert back to how data is treated for e.g. prices.

----------------
ffill ska bort

lägg till "hämta allt" för en ticker

I want a functionality update with a fetch-all-command that fetches all latest price data (for all instruments), economic indicators and financial statements based on the latest date for each in the database. The purpose of this command is to easily keep the data updated. 
I also want an updated fetch-prices function for a ticker to also fetch financial data everytime for that ticker. Please think about this and create a plan that reuses as much functionality as possible and works in a modular way.

Replace all print-statements with logging according to industry best practice
Remove all warnings for this
tests/integration/test_cli_commands.py: 57 warnings
tests/integration/test_etl_pipeline.py: 37 warnings
tests/unit/test_database_schema.py: 8431 warnings
  /Users/cw/Python/market_data_etl/venv/lib/python3.12/site-packages/sqlalchemy/sql/schema.py:3624: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
    return util.wrap_callable(lambda ctx: fn(), fn)  # type: ignore
----------------


