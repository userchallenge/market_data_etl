Fetch prices for stock x
market-data-etl fetch-prices --ticker ESSITY-B.ST --from 2020-01-01
market-data-etl fetch-fundamentals --ticker ESSITY-B.ST
market-data-etl db-info --ticker ESSITY-B.ST
tot rev 2024: 145.546.000.000 SEK (rätt)

Fetch prices for fund x
market-data-etl fetch-prices --ticker 0P00005U1J.ST --from 2020-01-01
market-data-etl fetch-fundamentals --ticker MSFT
market-data-etl db-info --ticker VOLV-B.ST

Testa att hämta aktieindex
market-data-etl fetch-prices --ticker AAPL --from 2024-01-01 --to 2024-01-31
market-data-etl fetch-fundamentals --ticker MSFT
market-data-etl db-info --ticker VOLV-B.ST

** Kolla att alla priser och datum stämmer **

Fetch fundamentals för 5 senaste år för företag
Dubbelkolla valuta och siffror samt om kpi:er beräknats eller hämtats
Hämta ekonomidata enl min egen kod och säkra att siffrorna är rätt

Åtgärder:
------------------------------
(1) I want a fallback for when data is not available for download from yfinance and suggest that you create a CSV-format that I can fill in with the required data (e.g. OMXS30). My suggestion is that you add functionality in line with the rest of the architecture for CSV-upload, I expect there to be happening later on.


(2) Failing to delete the trial for index OMXS30 that exists as a company, but with only one row of price data

(venv) macbookpro% market-data-etl clear-database --ticker ^OMXS30
/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/urllib3/__init__.py:35: NotOpenSSLWarning: urllib3 v2 only supports OpenSSL 1.1.1+, currently the 'ssl' module is compiled with 'LibreSSL 2.8.3'. See: https://github.com/urllib3/urllib3/issues/3020
  warnings.warn(
2025-08-12 19:36:26 - market_data_etl.market_data_etl.database.manager - INFO - Unified database manager initialized with market_data.db
⚠️  WARNING: This will permanently delete all data for ticker ^OMXS30!
Are you sure you want to continue? (yes/no): y
Clearing all data for ticker ^OMXS30...
2025-08-12 19:36:29 - market_data_etl.market_data_etl.database.manager - ERROR - Error clearing data for ticker ^OMXS30: (sqlite3.IntegrityError) NOT NULL constraint failed: portfolio_holdings.company_id
[SQL: UPDATE portfolio_holdings SET company_id=? WHERE portfolio_holdings.id = ?]
[parameters: (None, 3)]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
Traceback (most recent call last):
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/engine/base.py", line 1961, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/engine/default.py", line 944, in do_execute
    cursor.execute(statement, parameters)
sqlite3.IntegrityError: NOT NULL constraint failed: portfolio_holdings.company_id

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/Users/cw/Python/market_data_etl/market_data_etl/database/manager.py", line 1009, in clear_ticker_data
    session.commit()
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/orm/session.py", line 2032, in commit
    trans.commit(_to_root=True)
  File "<string>", line 2, in commit
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/orm/state_changes.py", line 137, in _go
    ret_value = fn(self, *arg, **kw)
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/orm/session.py", line 1313, in commit
    self._prepare_impl()
  File "<string>", line 2, in _prepare_impl
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/orm/state_changes.py", line 137, in _go
    ret_value = fn(self, *arg, **kw)
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/orm/session.py", line 1288, in _prepare_impl
    self.session.flush()
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/orm/session.py", line 4345, in flush
    self._flush(objects)
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/orm/session.py", line 4481, in _flush
    transaction.rollback(_capture_exception=True)
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/util/langhelpers.py", line 224, in __exit__
    raise exc_value.with_traceback(exc_tb)
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/orm/session.py", line 4441, in _flush
    flush_context.execute()
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/orm/unitofwork.py", line 466, in execute
    rec.execute(self)
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/orm/unitofwork.py", line 642, in execute
    util.preloaded.orm_persistence.save_obj(
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/orm/persistence.py", line 85, in save_obj
    _emit_update_statements(
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/orm/persistence.py", line 912, in _emit_update_statements
    c = connection.execute(
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/engine/base.py", line 1413, in execute
    return meth(
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/sql/elements.py", line 526, in _execute_on_connection
    return connection._execute_clauseelement(
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/engine/base.py", line 1635, in _execute_clauseelement
    ret = self._execute_context(
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/engine/base.py", line 1840, in _execute_context
    return self._exec_single_context(
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/engine/base.py", line 1980, in _exec_single_context
    self._handle_dbapi_exception(
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/engine/base.py", line 2349, in _handle_dbapi_exception
    raise sqlalchemy_exception.with_traceback(exc_info[2]) from e
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/engine/base.py", line 1961, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/engine/default.py", line 944, in do_execute
    cursor.execute(statement, parameters)
sqlalchemy.exc.IntegrityError: (sqlite3.IntegrityError) NOT NULL constraint failed: portfolio_holdings.company_id
[SQL: UPDATE portfolio_holdings SET company_id=? WHERE portfolio_holdings.id = ?]
[parameters: (None, 3)]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
2025-08-12 19:36:29 - market_data_etl.market_data_etl.cli.commands - ERROR - Unexpected error in clear_database_command: Failed to clear data for ticker ^OMXS30
Traceback (most recent call last):
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/engine/base.py", line 1961, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/engine/default.py", line 944, in do_execute
    cursor.execute(statement, parameters)
sqlite3.IntegrityError: NOT NULL constraint failed: portfolio_holdings.company_id

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/Users/cw/Python/market_data_etl/market_data_etl/database/manager.py", line 1009, in clear_ticker_data
    session.commit()
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/orm/session.py", line 2032, in commit
    trans.commit(_to_root=True)
  File "<string>", line 2, in commit
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/orm/state_changes.py", line 137, in _go
    ret_value = fn(self, *arg, **kw)
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/orm/session.py", line 1313, in commit
    self._prepare_impl()
  File "<string>", line 2, in _prepare_impl
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/orm/state_changes.py", line 137, in _go
    ret_value = fn(self, *arg, **kw)
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/orm/session.py", line 1288, in _prepare_impl
    self.session.flush()
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/orm/session.py", line 4345, in flush
    self._flush(objects)
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/orm/session.py", line 4481, in _flush
    transaction.rollback(_capture_exception=True)
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/util/langhelpers.py", line 224, in __exit__
    raise exc_value.with_traceback(exc_tb)
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/orm/session.py", line 4441, in _flush
    flush_context.execute()
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/orm/unitofwork.py", line 466, in execute
    rec.execute(self)
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/orm/unitofwork.py", line 642, in execute
    util.preloaded.orm_persistence.save_obj(
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/orm/persistence.py", line 85, in save_obj
    _emit_update_statements(
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/orm/persistence.py", line 912, in _emit_update_statements
    c = connection.execute(
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/engine/base.py", line 1413, in execute
    return meth(
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/sql/elements.py", line 526, in _execute_on_connection
    return connection._execute_clauseelement(
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/engine/base.py", line 1635, in _execute_clauseelement
    ret = self._execute_context(
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/engine/base.py", line 1840, in _execute_context
    return self._exec_single_context(
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/engine/base.py", line 1980, in _exec_single_context
    self._handle_dbapi_exception(
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/engine/base.py", line 2349, in _handle_dbapi_exception
    raise sqlalchemy_exception.with_traceback(exc_info[2]) from e
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/engine/base.py", line 1961, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/sqlalchemy/engine/default.py", line 944, in do_execute
    cursor.execute(statement, parameters)
sqlalchemy.exc.IntegrityError: (sqlite3.IntegrityError) NOT NULL constraint failed: portfolio_holdings.company_id
[SQL: UPDATE portfolio_holdings SET company_id=? WHERE portfolio_holdings.id = ?]
[parameters: (None, 3)]
(Background on this error at: https://sqlalche.me/e/20/gkpj)

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/Users/cw/Python/market_data_etl/market_data_etl/cli/commands.py", line 463, in clear_database_command
    result = db.clear_ticker_data(ticker)
  File "/Users/cw/Python/market_data_etl/market_data_etl/database/manager.py", line 1015, in clear_ticker_data
    raise DatabaseError(f"Failed to clear data for ticker {ticker}") from e
market_data_etl.utils.exceptions.DatabaseError: Failed to clear data for ticker ^OMXS30
ERROR: Unexpected error: Failed to clear data for ticker ^OMXS30


(3) Fetching fundamental data for Minesto fails:
(venv) macbookpro% market-data-etl fetch-financial-statements --ticker MINEST.ST
/Users/cw/Python/market_data_etl/venv/lib/python3.9/site-packages/urllib3/__init__.py:35: NotOpenSSLWarning: urllib3 v2 only supports OpenSSL 1.1.1+, currently the 'ssl' module is compiled with 'LibreSSL 2.8.3'. See: https://github.com/urllib3/urllib3/issues/3020
  warnings.warn(
Running financial ETL pipeline for MINEST.ST...
Including both annual and quarterly data for comprehensive analysis.
2025-08-12 19:43:30 - market_data_etl.market_data_etl.database.manager - INFO - Unified database manager initialized with market_data.db
2025-08-12 19:43:30 - market_data_etl.market_data_etl.etl.load - INFO - Starting financial ETL pipeline for MINEST.ST
2025-08-12 19:43:30 - market_data_etl.market_data_etl.etl.load - INFO - Extract phase: extracting raw data for MINEST.ST
2025-08-12 19:43:30 - market_data_etl.market_data_etl.data.fetchers - INFO - Extracting raw financial data for MINEST.ST
2025-08-12 19:43:32 - market_data_etl.market_data_etl.data.fetchers - INFO - Successfully extracted 7 data sources for MINEST.ST: income_stmt, quarterly_income_stmt, balance_sheet, quarterly_balance_sheet, cash_flow, quarterly_cash_flow, company_info
2025-08-12 19:43:32 - market_data_etl.market_data_etl.etl.load - INFO - Transform phase: transforming data for MINEST.ST
2025-08-12 19:43:32 - market_data_etl.market_data_etl.etl.transform - INFO - Transforming financial data for MINEST.ST
2025-08-12 19:43:32 - market_data_etl.market_data_etl.data.financial_standardizer - INFO - Standardized income_statement: 5 periods, average 13.6 metrics per period
2025-08-12 19:43:32 - market_data_etl.market_data_etl.data.financial_standardizer - INFO - Standardized income_statement: 5 periods, average 10.6 metrics per period
2025-08-12 19:43:32 - market_data_etl.market_data_etl.data.financial_standardizer - INFO - Standardized balance_sheet: 5 periods, average 17.4 metrics per period
2025-08-12 19:43:32 - market_data_etl.market_data_etl.data.financial_standardizer - INFO - Standardized balance_sheet: 6 periods, average 11.0 metrics per period
2025-08-12 19:43:32 - market_data_etl.market_data_etl.data.financial_standardizer - INFO - Standardized cash_flow: 5 periods, average 8.6 metrics per period
2025-08-12 19:43:32 - market_data_etl.market_data_etl.data.financial_standardizer - INFO - Standardized cash_flow: 6 periods, average 6.3 metrics per period
2025-08-12 19:43:32 - market_data_etl.market_data_etl.etl.load - ERROR - Financial ETL pipeline failed for MINEST.ST: float division by zero
2025-08-12 19:43:32 - market_data_etl.market_data_etl.cli.commands - ERROR - Unexpected error in fetch_financial_statements_command: float division by zero
Traceback (most recent call last):
  File "/Users/cw/Python/market_data_etl/market_data_etl/cli/commands.py", line 260, in fetch_financial_statements_command
    etl_results = etl.run_financial_etl(ticker)
  File "/Users/cw/Python/market_data_etl/market_data_etl/etl/load.py", line 217, in run_financial_etl
    raise e
  File "/Users/cw/Python/market_data_etl/market_data_etl/etl/load.py", line 189, in run_financial_etl
    transformed_data = self.transformer.transform_financial_data(raw_data)
  File "/Users/cw/Python/market_data_etl/market_data_etl/etl/transform.py", line 64, in transform_financial_data
    derived_metrics = self._calculate_derived_metrics(transformed_data['statements'])
  File "/Users/cw/Python/market_data_etl/market_data_etl/etl/transform.py", line 297, in _calculate_derived_metrics
    annual_derived = self.standardizer.calculate_derived_metrics(
  File "/Users/cw/Python/market_data_etl/market_data_etl/data/financial_standardizer.py", line 668, in calculate_derived_metrics
    period_metrics.update(self._calculate_efficiency_ratios(income, balance))
  File "/Users/cw/Python/market_data_etl/market_data_etl/data/financial_standardizer.py", line 784, in _calculate_efficiency_ratios
    ratios['days_sales_outstanding'] = 365 / ratios['receivables_turnover']
ZeroDivisionError: float division by zero
ERROR: Unexpected error: float division by zero