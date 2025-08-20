"""
Unified database models for market data and financial analysis.

This module defines the database schema for storing:
- Company information with currency support
- Historical price data (OHLC + Volume)
- Structured financial statements (Income, Balance Sheet, Cash Flow)
- Calculated financial ratios and metrics

Designed for comprehensive financial analysis of global companies.
"""

from sqlalchemy import Column, Integer, String, Float, Date, DateTime, ForeignKey, Index, Text, Enum
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime, timezone
import enum

Base = declarative_base()


class InstrumentType(enum.Enum):
    """Enumeration for different types of financial instruments."""
    STOCK = "stock"
    FUND = "fund" 
    ETF = "etf"
    INDEX = "index"
    COMMODITY = "commodity"
    CURRENCY = "currency"
    CRYPTOCURRENCY = "cryptocurrency"
    UNKNOWN = "unknown"


class TransactionType(enum.Enum):
    """Enumeration for different transaction types."""
    BUY = "buy"
    SELL = "sell"
    DIVIDEND = "dividend"
    SPLIT = "split"
    SPINOFF = "spinoff"
    MERGER = "merger"


class Instrument(Base):
    """
    Financial instrument information with currency and basic details.
    
    Stores essential information for stocks, funds, ETFs, indices, commodities,
    and other financial instruments needed for analysis including the base currency
    for all financial statements.
    """
    __tablename__ = 'instruments'
    
    id = Column(Integer, primary_key=True)
    ticker_symbol = Column(String(20), unique=True, nullable=False, index=True)
    isin = Column(String(12), unique=True, nullable=True, index=True)  # International Securities Identification Number
    instrument_name = Column(String(200))
    instrument_type = Column(Enum(InstrumentType), nullable=False, default=InstrumentType.STOCK)
    sector = Column(String(100))
    industry = Column(String(100))
    country = Column(String(100))
    currency = Column(String(10), nullable=False)  # ISO 4217 currency code (USD, EUR, SEK, etc.)
    market_cap = Column(Float)  # In base currency
    employees = Column(Integer)
    founded_year = Column(Integer)
    fund_type = Column(String(50))  # For funds/ETFs: equity, bond, mixed, etc.
    
    # Index-specific fields
    index_methodology = Column(String(200))  # "Market cap weighted", "Price weighted", etc.
    constituent_count = Column(Integer)      # Number of stocks in index
    base_date = Column(Date)                 # When index started
    base_value = Column(Float)               # Starting value (e.g., 100)
    
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    
    # Relationships
    prices = relationship("Price", back_populates="instrument")
    income_statements = relationship("IncomeStatement", back_populates="instrument")
    balance_sheets = relationship("BalanceSheet", back_populates="instrument")
    cash_flows = relationship("CashFlow", back_populates="instrument")
    financial_ratios = relationship("FinancialRatio", back_populates="instrument")
    portfolio_holdings = relationship("PortfolioHolding", back_populates="instrument")
    transactions = relationship("Transaction", back_populates="instrument")


# Backward compatibility alias
Company = Instrument


class Price(Base):
    """
    Daily OHLC price data with volume.
    
    Contains Open, High, Low, Close, Adjusted Close, and Volume data
    for each trading day. Linked to instruments rather than tickers.
    """
    __tablename__ = 'prices'
    
    id = Column(Integer, primary_key=True)
    instrument_id = Column(Integer, ForeignKey('instruments.id'), nullable=False)
    date = Column(Date, nullable=False, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    adj_close = Column(Float)
    volume = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    
    # Relationships
    instrument = relationship("Instrument", back_populates="prices")
    
    # Indexes for efficient querying
    __table_args__ = (
        Index('ix_prices_instrument_date', 'instrument_id', 'date'),
        {'sqlite_autoincrement': True}
    )


class IncomeStatement(Base):
    """
    Standardized income statement data with currency information.
    
    Stores profit & loss data in a structured format optimized for
    financial analysis and cross-company comparisons.
    """
    __tablename__ = 'income_statements'
    
    id = Column(Integer, primary_key=True)
    instrument_id = Column(Integer, ForeignKey('instruments.id'), nullable=False)
    period_end_date = Column(Date, nullable=False)
    period_type = Column(String(20), nullable=False)  # 'annual', 'quarterly'
    fiscal_year = Column(Integer, nullable=False)
    fiscal_quarter = Column(Integer)  # 1-4 for quarterly, NULL for annual
    currency = Column(String(10), nullable=False)  # Should match company currency
    
    # Revenue and Sales
    total_revenue = Column(Float)  # Top line revenue
    cost_of_revenue = Column(Float)  # Cost of goods sold
    gross_profit = Column(Float)  # Revenue - COGS
    
    # Operating Expenses
    research_development = Column(Float)
    sales_marketing = Column(Float)
    general_administrative = Column(Float)
    total_operating_expenses = Column(Float)
    
    # Operating Results
    operating_income = Column(Float)  # EBIT
    interest_expense = Column(Float)
    interest_income = Column(Float)
    other_income_expense = Column(Float)
    
    # Pre-tax and Taxes
    income_before_tax = Column(Float)  # EBT
    tax_provision = Column(Float)
    
    # Net Income
    net_income = Column(Float)  # Bottom line
    net_income_common = Column(Float)  # Available to common shareholders
    
    # Per Share Data
    basic_eps = Column(Float)  # Earnings per share
    diluted_eps = Column(Float)  # Diluted EPS
    weighted_average_shares = Column(Float)  # Share count
    weighted_average_shares_diluted = Column(Float)  # Diluted share count
    
    # Additional Metrics
    ebitda = Column(Float)  # Calculated: Operating Income + Depreciation + Amortization
    depreciation_amortization = Column(Float)
    
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    
    # Relationships
    instrument = relationship("Instrument", back_populates="income_statements")
    
    # Indexes for efficient querying
    __table_args__ = (
        Index('ix_income_instrument_period', 'instrument_id', 'period_end_date', 'period_type'),
        Index('ix_income_fiscal', 'instrument_id', 'fiscal_year', 'fiscal_quarter'),
        {'sqlite_autoincrement': True}
    )


class BalanceSheet(Base):
    """
    Standardized balance sheet data with currency information.
    
    Stores financial position data in a structured format optimized
    for liquidity, leverage, and efficiency analysis.
    """
    __tablename__ = 'balance_sheets'
    
    id = Column(Integer, primary_key=True)
    instrument_id = Column(Integer, ForeignKey('instruments.id'), nullable=False)
    period_end_date = Column(Date, nullable=False)
    period_type = Column(String(20), nullable=False)  # 'annual', 'quarterly'
    fiscal_year = Column(Integer, nullable=False)
    fiscal_quarter = Column(Integer)  # 1-4 for quarterly, NULL for annual
    currency = Column(String(10), nullable=False)
    
    # Current Assets
    cash_and_equivalents = Column(Float)
    short_term_investments = Column(Float)
    accounts_receivable = Column(Float)
    inventory = Column(Float)
    prepaid_expenses = Column(Float)
    other_current_assets = Column(Float)
    total_current_assets = Column(Float)
    
    # Non-Current Assets
    property_plant_equipment = Column(Float)  # Net PPE
    goodwill = Column(Float)
    intangible_assets = Column(Float)
    long_term_investments = Column(Float)
    other_non_current_assets = Column(Float)
    total_non_current_assets = Column(Float)
    
    # Total Assets
    total_assets = Column(Float)
    
    # Current Liabilities
    accounts_payable = Column(Float)
    short_term_debt = Column(Float)
    accrued_expenses = Column(Float)
    deferred_revenue_current = Column(Float)
    other_current_liabilities = Column(Float)
    total_current_liabilities = Column(Float)
    
    # Non-Current Liabilities
    long_term_debt = Column(Float)
    deferred_revenue_non_current = Column(Float)
    deferred_tax_liabilities = Column(Float)
    other_non_current_liabilities = Column(Float)
    total_non_current_liabilities = Column(Float)
    
    # Total Liabilities
    total_liabilities = Column(Float)
    
    # Shareholders' Equity
    common_stock = Column(Float)
    retained_earnings = Column(Float)
    accumulated_other_income = Column(Float)
    treasury_stock = Column(Float)
    total_shareholders_equity = Column(Float)
    
    # Calculated Fields
    total_debt = Column(Float)  # Short-term + Long-term debt
    net_debt = Column(Float)  # Total debt - Cash
    working_capital = Column(Float)  # Current assets - Current liabilities
    book_value_per_share = Column(Float)  # Total equity / Shares outstanding
    
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    
    # Relationships
    instrument = relationship("Instrument", back_populates="balance_sheets")
    
    # Indexes
    __table_args__ = (
        Index('ix_balance_instrument_period', 'instrument_id', 'period_end_date', 'period_type'),
        Index('ix_balance_fiscal', 'instrument_id', 'fiscal_year', 'fiscal_quarter'),
        {'sqlite_autoincrement': True}
    )


class CashFlow(Base):
    """
    Standardized cash flow statement data with currency information.
    
    Stores cash flow activities in a structured format optimized
    for liquidity and cash generation analysis.
    """
    __tablename__ = 'cash_flows'
    
    id = Column(Integer, primary_key=True)
    instrument_id = Column(Integer, ForeignKey('instruments.id'), nullable=False)
    period_end_date = Column(Date, nullable=False)
    period_type = Column(String(20), nullable=False)  # 'annual', 'quarterly'
    fiscal_year = Column(Integer, nullable=False)
    fiscal_quarter = Column(Integer)  # 1-4 for quarterly, NULL for annual
    currency = Column(String(10), nullable=False)
    
    # Operating Activities
    net_income = Column(Float)  # Starting point
    depreciation_amortization = Column(Float)
    stock_compensation = Column(Float)
    deferred_tax = Column(Float)
    
    # Changes in Working Capital
    change_accounts_receivable = Column(Float)
    change_inventory = Column(Float)
    change_accounts_payable = Column(Float)
    change_other_working_capital = Column(Float)
    
    operating_cash_flow = Column(Float)  # Total from operating activities
    
    # Investing Activities
    capital_expenditures = Column(Float)  # Usually negative
    acquisitions = Column(Float)
    purchases_investments = Column(Float)
    sales_maturities_investments = Column(Float)
    other_investing_activities = Column(Float)
    investing_cash_flow = Column(Float)  # Total from investing activities
    
    # Financing Activities
    debt_issuance = Column(Float)
    debt_repayment = Column(Float)
    common_stock_issuance = Column(Float)
    common_stock_repurchase = Column(Float)  # Share buybacks
    dividends_paid = Column(Float)
    other_financing_activities = Column(Float)
    financing_cash_flow = Column(Float)  # Total from financing activities
    
    # Net Change and Ending Cash
    net_change_cash = Column(Float)  # Sum of all three activities
    cash_beginning = Column(Float)
    cash_ending = Column(Float)
    
    # Key Calculated Metrics
    free_cash_flow = Column(Float)  # Operating CF - CapEx
    fcf_per_share = Column(Float)  # FCF / Shares outstanding
    
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    
    # Relationships
    instrument = relationship("Instrument", back_populates="cash_flows")
    
    # Indexes
    __table_args__ = (
        Index('ix_cashflow_instrument_period', 'instrument_id', 'period_end_date', 'period_type'),
        Index('ix_cashflow_fiscal', 'instrument_id', 'fiscal_year', 'fiscal_quarter'),
        {'sqlite_autoincrement': True}
    )


class FinancialRatio(Base):
    """
    Pre-calculated financial ratios and metrics for analysis.
    
    Stores commonly used financial ratios calculated from the
    structured financial statement data.
    """
    __tablename__ = 'financial_ratios'
    
    id = Column(Integer, primary_key=True)
    instrument_id = Column(Integer, ForeignKey('instruments.id'), nullable=False)
    period_end_date = Column(Date, nullable=False)
    period_type = Column(String(20), nullable=False)
    fiscal_year = Column(Integer, nullable=False)
    fiscal_quarter = Column(Integer)
    
    # Profitability Ratios
    gross_profit_margin = Column(Float)  # Gross Profit / Revenue
    operating_profit_margin = Column(Float)  # Operating Income / Revenue
    net_profit_margin = Column(Float)  # Net Income / Revenue
    return_on_assets = Column(Float)  # Net Income / Total Assets
    return_on_equity = Column(Float)  # Net Income / Shareholders Equity
    return_on_invested_capital = Column(Float)  # ROIC
    
    # Liquidity Ratios
    current_ratio = Column(Float)  # Current Assets / Current Liabilities
    quick_ratio = Column(Float)  # (Current Assets - Inventory) / Current Liabilities
    cash_ratio = Column(Float)  # Cash / Current Liabilities
    
    # Leverage Ratios
    debt_to_equity = Column(Float)  # Total Debt / Total Equity
    debt_to_assets = Column(Float)  # Total Debt / Total Assets
    interest_coverage = Column(Float)  # Operating Income / Interest Expense
    debt_service_coverage = Column(Float)  # Operating CF / Debt Service
    
    # Efficiency Ratios
    asset_turnover = Column(Float)  # Revenue / Total Assets
    inventory_turnover = Column(Float)  # COGS / Inventory
    receivables_turnover = Column(Float)  # Revenue / Accounts Receivable
    days_sales_outstanding = Column(Float)  # 365 / Receivables Turnover
    
    # Valuation Ratios (requires market data)
    price_to_earnings = Column(Float)  # Market Cap / Net Income
    price_to_book = Column(Float)  # Market Cap / Book Value
    price_to_sales = Column(Float)  # Market Cap / Revenue
    enterprise_value = Column(Float)  # Market Cap + Debt - Cash
    ev_to_revenue = Column(Float)  # EV / Revenue
    ev_to_ebitda = Column(Float)  # EV / EBITDA
    
    # Growth Rates (Year-over-Year)
    revenue_growth_yoy = Column(Float)
    net_income_growth_yoy = Column(Float)
    eps_growth_yoy = Column(Float)
    
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    
    # Relationships
    instrument = relationship("Instrument", back_populates="financial_ratios")
    
    # Indexes
    __table_args__ = (
        Index('ix_ratios_instrument_period', 'instrument_id', 'period_end_date', 'period_type'),
        {'sqlite_autoincrement': True}
    )


class Portfolio(Base):
    """
    Portfolio definition containing metadata and settings.
    
    Stores portfolio information loaded from configuration files
    including name, description, and creation metadata.
    """
    __tablename__ = 'portfolios'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False, index=True)
    description = Column(Text)
    currency = Column(String(10), nullable=False)  # Base currency for portfolio
    created_date = Column(Date, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    
    # Relationships
    holdings = relationship("PortfolioHolding", back_populates="portfolio")
    transactions = relationship("Transaction", back_populates="portfolio")


class PortfolioHolding(Base):
    """
    Individual holdings within a portfolio.
    
    Links companies/instruments to portfolios with additional metadata
    from the portfolio configuration file.
    """
    __tablename__ = 'portfolio_holdings'
    
    id = Column(Integer, primary_key=True)
    portfolio_id = Column(Integer, ForeignKey('portfolios.id'), nullable=False)
    instrument_id = Column(Integer, ForeignKey('instruments.id'), nullable=False)
    sector = Column(String(100))  # Can override company sector for portfolio-specific categorization
    fund_type = Column(String(50))  # For funds: equity, bond, mixed, etc.
    notes = Column(Text)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    
    # Relationships
    portfolio = relationship("Portfolio", back_populates="holdings")
    instrument = relationship("Instrument", back_populates="portfolio_holdings")
    
    # Indexes
    __table_args__ = (
        Index('ix_holdings_portfolio_company', 'portfolio_id', 'instrument_id', unique=True),
        {'sqlite_autoincrement': True}
    )


class Transaction(Base):
    """
    Individual transactions for portfolio tracking.
    
    Stores buy, sell, dividend, and other transactions with full details
    for portfolio performance calculation and tax reporting.
    """
    __tablename__ = 'transactions'
    
    id = Column(Integer, primary_key=True)
    portfolio_id = Column(Integer, ForeignKey('portfolios.id'), nullable=True)  # Can be NULL for unassigned transactions
    instrument_id = Column(Integer, ForeignKey('instruments.id'), nullable=False)
    transaction_date = Column(Date, nullable=False, index=True)
    transaction_type = Column(Enum(TransactionType), nullable=False, index=True)
    
    # Transaction Details
    quantity = Column(Float, nullable=False)  # Number of shares/units
    price_per_unit = Column(Float, nullable=False)  # Price per share/unit
    currency = Column(String(10), nullable=False)  # Transaction currency
    fees = Column(Float, default=0.0)  # Brokerage fees and commissions
    
    # Additional Information
    broker = Column(String(100))  # Broker name
    notes = Column(Text)  # Additional notes
    
    # Calculated Fields
    total_amount = Column(Float)  # quantity * price_per_unit + fees (for buys) or - fees (for sells)
    
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    
    # Relationships
    portfolio = relationship("Portfolio", back_populates="transactions")
    instrument = relationship("Instrument", back_populates="transactions")
    
    # Indexes
    __table_args__ = (
        Index('ix_transactions_portfolio_date', 'portfolio_id', 'transaction_date'),
        Index('ix_transactions_instrument_date', 'instrument_id', 'transaction_date'),
        Index('ix_transactions_type_date', 'transaction_type', 'transaction_date'),
        {'sqlite_autoincrement': True}
    )


# =============================================================================
# ECONOMIC DATA MODELS
# =============================================================================

class Frequency(enum.Enum):
    """Enumeration for different data frequencies."""
    DAILY = "daily"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    YEARLY = "yearly"


class ThresholdCategory(enum.Enum):
    """Enumeration for threshold categories."""
    bad = "bad"
    normal = "normal"
    good = "good"


class EconomicIndicator(Base):
    """
    Economic indicator definition with metadata.
    
    Stores information about economic indicators from various sources
    like Eurostat, ECB, FRED etc. with their metadata.
    """
    __tablename__ = 'economic_indicators'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False, index=True)  # standardized name
    source = Column(String(50), nullable=False)  # eurostat, ecb, fred
    source_identifier = Column(String(100), nullable=False)  # original API identifier
    description = Column(String(200), nullable=False)  # human-readable description
    unit = Column(String(50))
    frequency = Column(Enum(Frequency), nullable=False, default=Frequency.MONTHLY)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    
    # Relationships
    data_points = relationship("EconomicIndicatorData", back_populates="indicator")
    thresholds = relationship("Threshold", back_populates="indicator")


class EconomicIndicatorData(Base):
    """
    Time series data points for economic indicators.
    
    Stores the actual data values for economic indicators with dates.
    """
    __tablename__ = 'economic_indicator_data'
    
    id = Column(Integer, primary_key=True)
    indicator_id = Column(Integer, ForeignKey('economic_indicators.id'), nullable=False)
    date = Column(Date, nullable=False, index=True)
    value = Column(Float, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    
    # Relationships
    indicator = relationship("EconomicIndicator", back_populates="data_points")
    
    # Indexes for efficient querying
    __table_args__ = (
        Index('ix_indicator_data_indicator_date', 'indicator_id', 'date', unique=True),
        {'sqlite_autoincrement': True}
    )


class Threshold(Base):
    """
    Threshold definitions for economic indicator analysis.
    
    Defines good/normal/bad ranges for economic indicators to enable
    automated analysis and alerts.
    """
    __tablename__ = 'thresholds'
    
    id = Column(Integer, primary_key=True)
    indicator_id = Column(Integer, ForeignKey('economic_indicators.id'), nullable=False)
    category = Column(Enum(ThresholdCategory), nullable=False)
    min_value = Column(Float)
    max_value = Column(Float)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    
    # Relationships
    indicator = relationship("EconomicIndicator", back_populates="thresholds")
    
    # Indexes
    __table_args__ = (
        Index('ix_thresholds_indicator_category', 'indicator_id', 'category'),
        {'sqlite_autoincrement': True}
    )


class AlignedDailyData(Base):
    """
    Trading-day aligned data combining price and economic indicators.
    
    This table stores daily data aligned to trading calendars, with price data
    from trading days and forward-filled economic indicators. Provides a unified
    view for analysis across different data frequencies.
    
    Design principle: Each row represents one trading day for one instrument,
    with the most recent economic indicator values forward-filled.
    """
    __tablename__ = 'aligned_daily_data'
    
    # Composite primary key: trading date + instrument
    date = Column(Date, primary_key=True, nullable=False)
    instrument_id = Column(Integer, ForeignKey('instruments.id'), primary_key=True, nullable=False)
    
    # Price data (from actual trading day)
    open_price = Column(Float(precision=4))
    high_price = Column(Float(precision=4))
    low_price = Column(Float(precision=4))
    close_price = Column(Float(precision=4))
    adjusted_close = Column(Float(precision=4))
    volume = Column(Integer)
    
    # US Economic indicators (forward-filled from release dates)
    inflation_monthly_us = Column(Float(precision=4))           # Monthly inflation rate %
    inflation_index_monthly_us = Column(Float(precision=4))     # CPI index value
    unemployment_monthly_rate_us = Column(Float(precision=4))   # Unemployment rate %
    interest_rate_monthly_us = Column(Float(precision=4))       # Fed funds rate %
    
    # European Economic indicators (forward-filled from release dates)
    inflation_monthly_euro = Column(Float(precision=4))         # HICP monthly rate %
    unemployment_rate_monthly_euro = Column(Float(precision=4)) # Euro unemployment %
    interest_rate_change_day_euro = Column(Float(precision=4))  # ECB main rate %
    interest_rate_monthly_euro = Column(Float(precision=4))     # ECB monthly rate %
    
    # Metadata
    trading_calendar = Column(String(10), nullable=False)  # Exchange calendar used (US, STO, LSE, etc.)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    
    # Relationships
    instrument = relationship("Instrument")
    
    # Indexes for performance
    __table_args__ = (
        Index('ix_aligned_date', 'date'),
        Index('ix_aligned_instrument_date', 'instrument_id', 'date'),
        Index('ix_aligned_calendar', 'trading_calendar'),
        Index('ix_aligned_date_calendar', 'date', 'trading_calendar'),
        {'sqlite_autoincrement': True}
    )