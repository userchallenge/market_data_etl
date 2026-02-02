-- =============================================================================
-- MARKET DATA ETL DATABASE SCHEMA
-- =============================================================================
-- Database: market_data.db (SQLite)
-- Purpose: Financial market data, economic indicators, and portfolio management
-- =============================================================================

-- =============================================================================
-- ENUMERATIONS
-- =============================================================================

-- Instrument Types
-- VALUES: 'stock', 'fund', 'etf', 'index', 'commodity', 'currency', 'cryptocurrency', 'unknown'

-- Transaction Types  
-- VALUES: 'buy', 'sell', 'dividend', 'split', 'spinoff', 'merger'

-- Data Frequencies
-- VALUES: 'daily', 'monthly', 'quarterly', 'yearly'

-- Threshold Categories
-- VALUES: 'bad', 'normal', 'good'

-- =============================================================================
-- CORE FINANCIAL DATA TABLES
-- =============================================================================

-- Financial instruments (stocks, funds, ETFs, indices, etc.)
CREATE TABLE instruments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_symbol VARCHAR(20) UNIQUE NOT NULL,
    isin VARCHAR(12) UNIQUE,  -- International Securities Identification Number
    instrument_name VARCHAR(200),
    instrument_type VARCHAR(20) NOT NULL DEFAULT 'stock',  -- ENUM: InstrumentType
    sector VARCHAR(100),
    industry VARCHAR(100),
    country VARCHAR(100),
    currency VARCHAR(10) NOT NULL,  -- ISO 4217 currency code (USD, EUR, SEK, etc.)
    market_cap FLOAT,  -- In base currency
    employees INTEGER,
    founded_year INTEGER,
    fund_type VARCHAR(50),  -- For funds/ETFs: equity, bond, mixed, etc.
    
    -- Index-specific fields
    index_methodology VARCHAR(200),  -- "Market cap weighted", "Price weighted", etc.
    constituent_count INTEGER,      -- Number of stocks in index
    base_date DATE,                 -- When index started  
    base_value FLOAT,               -- Starting value (e.g., 100)
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Historical price data (OHLC + Volume)
CREATE TABLE prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_id INTEGER NOT NULL,
    date DATE NOT NULL,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    adj_close FLOAT,
    volume INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (instrument_id) REFERENCES instruments(id)
);

-- Income statement data (P&L)
CREATE TABLE income_statements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_id INTEGER NOT NULL,
    period_end_date DATE NOT NULL,
    period_type VARCHAR(20) NOT NULL,  -- 'annual', 'quarterly'
    fiscal_year INTEGER NOT NULL,
    fiscal_quarter INTEGER,  -- 1-4 for quarterly, NULL for annual
    currency VARCHAR(10) NOT NULL,  -- Should match instrument currency
    
    -- Revenue and Sales
    total_revenue FLOAT,  -- Top line revenue
    cost_of_revenue FLOAT,  -- Cost of goods sold
    gross_profit FLOAT,  -- Revenue - COGS
    
    -- Operating Expenses
    research_development FLOAT,
    sales_marketing FLOAT,
    general_administrative FLOAT,
    total_operating_expenses FLOAT,
    
    -- Operating Results
    operating_income FLOAT,  -- EBIT
    interest_expense FLOAT,
    interest_income FLOAT,
    other_income_expense FLOAT,
    
    -- Pre-tax and Taxes
    income_before_tax FLOAT,  -- EBT
    tax_provision FLOAT,
    
    -- Net Income
    net_income FLOAT,  -- Bottom line
    net_income_common FLOAT,  -- Available to common shareholders
    
    -- Per Share Data
    basic_eps FLOAT,  -- Earnings per share
    diluted_eps FLOAT,  -- Diluted EPS
    weighted_average_shares FLOAT,  -- Share count
    weighted_average_shares_diluted FLOAT,  -- Diluted share count
    
    -- Additional Metrics
    ebitda FLOAT,  -- Calculated: Operating Income + Depreciation + Amortization
    depreciation_amortization FLOAT,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (instrument_id) REFERENCES instruments(id)
);

-- Balance sheet data (Assets, Liabilities, Equity)
CREATE TABLE balance_sheets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_id INTEGER NOT NULL,
    period_end_date DATE NOT NULL,
    period_type VARCHAR(20) NOT NULL,  -- 'annual', 'quarterly'
    fiscal_year INTEGER NOT NULL,
    fiscal_quarter INTEGER,  -- 1-4 for quarterly, NULL for annual
    currency VARCHAR(10) NOT NULL,
    
    -- Current Assets
    cash_and_equivalents FLOAT,
    short_term_investments FLOAT,
    accounts_receivable FLOAT,
    inventory FLOAT,
    prepaid_expenses FLOAT,
    other_current_assets FLOAT,
    total_current_assets FLOAT,
    
    -- Non-Current Assets
    property_plant_equipment FLOAT,  -- Net PPE
    goodwill FLOAT,
    intangible_assets FLOAT,
    long_term_investments FLOAT,
    other_non_current_assets FLOAT,
    total_non_current_assets FLOAT,
    
    -- Total Assets
    total_assets FLOAT,
    
    -- Current Liabilities
    accounts_payable FLOAT,
    short_term_debt FLOAT,
    accrued_expenses FLOAT,
    deferred_revenue_current FLOAT,
    other_current_liabilities FLOAT,
    total_current_liabilities FLOAT,
    
    -- Non-Current Liabilities
    long_term_debt FLOAT,
    deferred_revenue_non_current FLOAT,
    deferred_tax_liabilities FLOAT,
    other_non_current_liabilities FLOAT,
    total_non_current_liabilities FLOAT,
    
    -- Total Liabilities
    total_liabilities FLOAT,
    
    -- Shareholders' Equity
    common_stock FLOAT,
    retained_earnings FLOAT,
    accumulated_other_income FLOAT,
    treasury_stock FLOAT,
    total_shareholders_equity FLOAT,
    
    -- Calculated Fields
    total_debt FLOAT,  -- Short-term + Long-term debt
    net_debt FLOAT,  -- Total debt - Cash
    working_capital FLOAT,  -- Current assets - Current liabilities
    book_value_per_share FLOAT,  -- Total equity / Shares outstanding
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (instrument_id) REFERENCES instruments(id)
);

-- Cash flow statement data
CREATE TABLE cash_flows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_id INTEGER NOT NULL,
    period_end_date DATE NOT NULL,
    period_type VARCHAR(20) NOT NULL,  -- 'annual', 'quarterly'
    fiscal_year INTEGER NOT NULL,
    fiscal_quarter INTEGER,  -- 1-4 for quarterly, NULL for annual
    currency VARCHAR(10) NOT NULL,
    
    -- Operating Activities
    net_income FLOAT,  -- Starting point
    depreciation_amortization FLOAT,
    stock_compensation FLOAT,
    deferred_tax FLOAT,
    
    -- Changes in Working Capital
    change_accounts_receivable FLOAT,
    change_inventory FLOAT,
    change_accounts_payable FLOAT,
    change_other_working_capital FLOAT,
    
    operating_cash_flow FLOAT,  -- Total from operating activities
    
    -- Investing Activities
    capital_expenditures FLOAT,  -- Usually negative
    acquisitions FLOAT,
    purchases_investments FLOAT,
    sales_maturities_investments FLOAT,
    other_investing_activities FLOAT,
    investing_cash_flow FLOAT,  -- Total from investing activities
    
    -- Financing Activities
    debt_issuance FLOAT,
    debt_repayment FLOAT,
    common_stock_issuance FLOAT,
    common_stock_repurchase FLOAT,  -- Share buybacks
    dividends_paid FLOAT,
    other_financing_activities FLOAT,
    financing_cash_flow FLOAT,  -- Total from financing activities
    
    -- Net Change and Ending Cash
    net_change_cash FLOAT,  -- Sum of all three activities
    cash_beginning FLOAT,
    cash_ending FLOAT,
    
    -- Key Calculated Metrics
    free_cash_flow FLOAT,  -- Operating CF - CapEx
    fcf_per_share FLOAT,  -- FCF / Shares outstanding
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (instrument_id) REFERENCES instruments(id)
);

-- Pre-calculated financial ratios and metrics
CREATE TABLE financial_ratios (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_id INTEGER NOT NULL,
    period_end_date DATE NOT NULL,
    period_type VARCHAR(20) NOT NULL,
    fiscal_year INTEGER NOT NULL,
    fiscal_quarter INTEGER,
    
    -- Profitability Ratios
    gross_profit_margin FLOAT,  -- Gross Profit / Revenue
    operating_profit_margin FLOAT,  -- Operating Income / Revenue
    net_profit_margin FLOAT,  -- Net Income / Revenue
    return_on_assets FLOAT,  -- Net Income / Total Assets
    return_on_equity FLOAT,  -- Net Income / Shareholders Equity
    return_on_invested_capital FLOAT,  -- ROIC
    
    -- Liquidity Ratios
    current_ratio FLOAT,  -- Current Assets / Current Liabilities
    quick_ratio FLOAT,  -- (Current Assets - Inventory) / Current Liabilities
    cash_ratio FLOAT,  -- Cash / Current Liabilities
    
    -- Leverage Ratios
    debt_to_equity FLOAT,  -- Total Debt / Total Equity
    debt_to_assets FLOAT,  -- Total Debt / Total Assets
    interest_coverage FLOAT,  -- Operating Income / Interest Expense
    debt_service_coverage FLOAT,  -- Operating CF / Debt Service
    
    -- Efficiency Ratios
    asset_turnover FLOAT,  -- Revenue / Total Assets
    inventory_turnover FLOAT,  -- COGS / Inventory
    receivables_turnover FLOAT,  -- Revenue / Accounts Receivable
    days_sales_outstanding FLOAT,  -- 365 / Receivables Turnover
    
    -- Valuation Ratios (requires market data)
    price_to_earnings FLOAT,  -- Market Cap / Net Income
    price_to_book FLOAT,  -- Market Cap / Book Value
    price_to_sales FLOAT,  -- Market Cap / Revenue
    enterprise_value FLOAT,  -- Market Cap + Debt - Cash
    ev_to_revenue FLOAT,  -- EV / Revenue
    ev_to_ebitda FLOAT,  -- EV / EBITDA
    
    -- Growth Rates (Year-over-Year)
    revenue_growth_yoy FLOAT,
    net_income_growth_yoy FLOAT,
    eps_growth_yoy FLOAT,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (instrument_id) REFERENCES instruments(id)
);

-- =============================================================================
-- PORTFOLIO MANAGEMENT TABLES
-- =============================================================================

-- Portfolio definitions and metadata
CREATE TABLE portfolios (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) UNIQUE NOT NULL,
    description TEXT,
    currency VARCHAR(10) NOT NULL,  -- Base currency for portfolio
    created_date DATE NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Individual holdings within portfolios
CREATE TABLE portfolio_holdings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id INTEGER NOT NULL,
    instrument_id INTEGER NOT NULL,
    sector VARCHAR(100),  -- Can override instrument sector for portfolio-specific categorization
    fund_type VARCHAR(50),  -- For funds: equity, bond, mixed, etc.
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (portfolio_id) REFERENCES portfolios(id),
    FOREIGN KEY (instrument_id) REFERENCES instruments(id)
);

-- Individual transactions for portfolio tracking
CREATE TABLE transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id INTEGER,  -- Can be NULL for unassigned transactions
    instrument_id INTEGER NOT NULL,
    transaction_date DATE NOT NULL,
    transaction_type VARCHAR(20) NOT NULL,  -- ENUM: TransactionType
    
    -- Transaction Details
    quantity FLOAT NOT NULL,  -- Number of shares/units
    price_per_unit FLOAT NOT NULL,  -- Price per share/unit
    currency VARCHAR(10) NOT NULL,  -- Transaction currency
    fees FLOAT DEFAULT 0.0,  -- Brokerage fees and commissions
    
    -- Additional Information
    broker VARCHAR(100),  -- Broker name
    notes TEXT,  -- Additional notes
    
    -- Calculated Fields
    total_amount FLOAT,  -- quantity * price_per_unit +/- fees
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (portfolio_id) REFERENCES portfolios(id),
    FOREIGN KEY (instrument_id) REFERENCES instruments(id)
);

-- =============================================================================
-- ECONOMIC DATA TABLES
-- =============================================================================

-- Economic indicator definitions and metadata
CREATE TABLE economic_indicators (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) UNIQUE NOT NULL,  -- standardized name
    source VARCHAR(50) NOT NULL,  -- eurostat, ecb, fred
    source_identifier VARCHAR(100) NOT NULL,  -- original API identifier
    description VARCHAR(200) NOT NULL,  -- human-readable description
    unit VARCHAR(50),
    frequency VARCHAR(20) NOT NULL DEFAULT 'monthly',  -- ENUM: Frequency
    country_code VARCHAR(3),  -- ISO 3166-1 alpha-2 country code
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Time series data points for economic indicators
CREATE TABLE economic_indicator_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    indicator_id INTEGER NOT NULL,
    date DATE NOT NULL,
    value FLOAT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (indicator_id) REFERENCES economic_indicators(id)
);

-- Threshold definitions for economic indicator analysis
CREATE TABLE thresholds (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    indicator_id INTEGER NOT NULL,
    category VARCHAR(20) NOT NULL,  -- ENUM: ThresholdCategory ('bad', 'normal', 'good')
    min_value FLOAT,
    max_value FLOAT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (indicator_id) REFERENCES economic_indicators(id)
);

-- =============================================================================
-- DATA ALIGNMENT TABLE
-- =============================================================================

-- Trading-day aligned data combining price and economic indicators
CREATE TABLE aligned_daily_data (
    date DATE NOT NULL,
    instrument_id INTEGER NOT NULL,
    
    -- Price data (from actual trading day)
    open_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    close_price FLOAT,
    adjusted_close FLOAT,
    volume INTEGER,
    
    -- US Economic indicators (forward-filled from release dates)
    inflation_monthly_us FLOAT,           -- Monthly inflation rate %
    inflation_index_monthly_us FLOAT,     -- CPI index value
    unemployment_monthly_rate_us FLOAT,   -- Unemployment rate %
    interest_rate_monthly_us FLOAT,       -- Fed funds rate %
    
    -- European Economic indicators (forward-filled from release dates)
    inflation_monthly_euro FLOAT,         -- HICP monthly rate %
    unemployment_rate_monthly_euro FLOAT, -- Euro unemployment %
    interest_rate_change_day_euro FLOAT,  -- ECB main rate %
    interest_rate_monthly_euro FLOAT,     -- ECB monthly rate %
    
    -- Metadata
    trading_calendar VARCHAR(10) NOT NULL,  -- Exchange calendar used (US, STO, LSE, etc.)
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (date, instrument_id),
    FOREIGN KEY (instrument_id) REFERENCES instruments(id)
);

-- =============================================================================
-- INDEXES FOR PERFORMANCE OPTIMIZATION
-- =============================================================================

-- Instruments table indexes
CREATE INDEX ix_instruments_ticker_symbol ON instruments(ticker_symbol);
CREATE INDEX ix_instruments_isin ON instruments(isin);

-- Prices table indexes  
CREATE INDEX ix_prices_date ON prices(date);
CREATE INDEX ix_prices_instrument_date ON prices(instrument_id, date);

-- Income statements indexes
CREATE INDEX ix_income_instrument_period ON income_statements(instrument_id, period_end_date, period_type);
CREATE INDEX ix_income_fiscal ON income_statements(instrument_id, fiscal_year, fiscal_quarter);

-- Balance sheets indexes
CREATE INDEX ix_balance_instrument_period ON balance_sheets(instrument_id, period_end_date, period_type);
CREATE INDEX ix_balance_fiscal ON balance_sheets(instrument_id, fiscal_year, fiscal_quarter);

-- Cash flows indexes
CREATE INDEX ix_cashflow_instrument_period ON cash_flows(instrument_id, period_end_date, period_type);
CREATE INDEX ix_cashflow_fiscal ON cash_flows(instrument_id, fiscal_year, fiscal_quarter);

-- Financial ratios indexes
CREATE INDEX ix_ratios_instrument_period ON financial_ratios(instrument_id, period_end_date, period_type);

-- Portfolio management indexes
CREATE INDEX ix_portfolios_name ON portfolios(name);
CREATE UNIQUE INDEX ix_holdings_portfolio_company ON portfolio_holdings(portfolio_id, instrument_id);
CREATE INDEX ix_transactions_portfolio_date ON transactions(portfolio_id, transaction_date);
CREATE INDEX ix_transactions_instrument_date ON transactions(instrument_id, transaction_date);
CREATE INDEX ix_transactions_type_date ON transactions(transaction_type, transaction_date);
CREATE INDEX ix_transactions_date ON transactions(transaction_date);

-- Economic data indexes
CREATE INDEX ix_economic_indicators_name ON economic_indicators(name);
CREATE UNIQUE INDEX ix_indicator_data_indicator_date ON economic_indicator_data(indicator_id, date);
CREATE INDEX ix_indicator_data_date ON economic_indicator_data(date);
CREATE INDEX ix_thresholds_indicator_category ON thresholds(indicator_id, category);

-- Aligned data indexes
CREATE INDEX ix_aligned_date ON aligned_daily_data(date);
CREATE INDEX ix_aligned_instrument_date ON aligned_daily_data(instrument_id, date);
CREATE INDEX ix_aligned_calendar ON aligned_daily_data(trading_calendar);
CREATE INDEX ix_aligned_date_calendar ON aligned_daily_data(date, trading_calendar);

-- =============================================================================
-- FOREIGN KEY RELATIONSHIPS SUMMARY
-- =============================================================================

-- Core Financial Data:
-- prices.instrument_id → instruments.id
-- income_statements.instrument_id → instruments.id  
-- balance_sheets.instrument_id → instruments.id
-- cash_flows.instrument_id → instruments.id
-- financial_ratios.instrument_id → instruments.id

-- Portfolio Management:
-- portfolio_holdings.portfolio_id → portfolios.id
-- portfolio_holdings.instrument_id → instruments.id
-- transactions.portfolio_id → portfolios.id (nullable)
-- transactions.instrument_id → instruments.id

-- Economic Data:
-- economic_indicator_data.indicator_id → economic_indicators.id
-- thresholds.indicator_id → economic_indicators.id

-- Data Alignment:
-- aligned_daily_data.instrument_id → instruments.id

-- =============================================================================
-- DATABASE NOTES
-- =============================================================================
-- 1. All monetary values are stored in the instrument's base currency
-- 2. Dates are stored in YYYY-MM-DD format
-- 3. All timestamps are in UTC
-- 4. The aligned_daily_data table uses composite primary key (date, instrument_id)
-- 5. Economic indicators are forward-filled to trading days for alignment
-- 6. Portfolio transactions support both assigned and unassigned transactions
-- 7. Instrument types support various financial instruments beyond just stocks
-- =============================================================================