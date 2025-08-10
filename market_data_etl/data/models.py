"""
Unified database models for market data and financial analysis.

This module defines the database schema for storing:
- Company information with currency support
- Historical price data (OHLC + Volume)
- Structured financial statements (Income, Balance Sheet, Cash Flow)
- Calculated financial ratios and metrics

Designed for comprehensive financial analysis of global companies.
"""

from sqlalchemy import Column, Integer, String, Float, Date, DateTime, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()


class Company(Base):
    """
    Company information with currency and basic details.
    
    Stores essential company information needed for financial analysis
    including the base currency for all financial statements.
    """
    __tablename__ = 'companies'
    
    id = Column(Integer, primary_key=True)
    ticker_symbol = Column(String(20), unique=True, nullable=False, index=True)
    company_name = Column(String(200))
    sector = Column(String(100))
    industry = Column(String(100))
    country = Column(String(100))
    currency = Column(String(10), nullable=False)  # ISO 4217 currency code (USD, EUR, SEK, etc.)
    market_cap = Column(Float)  # In base currency
    employees = Column(Integer)
    founded_year = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    prices = relationship("Price", back_populates="company")
    income_statements = relationship("IncomeStatement", back_populates="company")
    balance_sheets = relationship("BalanceSheet", back_populates="company")
    cash_flows = relationship("CashFlow", back_populates="company")
    financial_ratios = relationship("FinancialRatio", back_populates="company")


class Price(Base):
    """
    Daily OHLC price data with volume.
    
    Contains Open, High, Low, Close, Adjusted Close, and Volume data
    for each trading day. Linked to companies rather than tickers.
    """
    __tablename__ = 'prices'
    
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
    date = Column(Date, nullable=False, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    adj_close = Column(Float)
    volume = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    company = relationship("Company", back_populates="prices")
    
    # Indexes for efficient querying
    __table_args__ = (
        Index('ix_prices_company_date', 'company_id', 'date'),
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
    company_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
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
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    company = relationship("Company", back_populates="income_statements")
    
    # Indexes for efficient querying
    __table_args__ = (
        Index('ix_income_company_period', 'company_id', 'period_end_date', 'period_type'),
        Index('ix_income_fiscal', 'company_id', 'fiscal_year', 'fiscal_quarter'),
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
    company_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
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
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    company = relationship("Company", back_populates="balance_sheets")
    
    # Indexes
    __table_args__ = (
        Index('ix_balance_company_period', 'company_id', 'period_end_date', 'period_type'),
        Index('ix_balance_fiscal', 'company_id', 'fiscal_year', 'fiscal_quarter'),
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
    company_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
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
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    company = relationship("Company", back_populates="cash_flows")
    
    # Indexes
    __table_args__ = (
        Index('ix_cashflow_company_period', 'company_id', 'period_end_date', 'period_type'),
        Index('ix_cashflow_fiscal', 'company_id', 'fiscal_year', 'fiscal_quarter'),
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
    company_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
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
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    company = relationship("Company", back_populates="financial_ratios")
    
    # Indexes
    __table_args__ = (
        Index('ix_ratios_company_period', 'company_id', 'period_end_date', 'period_type'),
        {'sqlite_autoincrement': True}
    )