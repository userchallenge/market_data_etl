"""
Unified database manager for market data and financial analysis.

This module provides database operations for storing and retrieving:
- Instrument information with currency support
- Historical price data (OHLC + Volume)  
- Structured financial statements (Income, Balance Sheet, Cash Flow)
- Calculated financial ratios and metrics

Designed for comprehensive financial analysis of global companies.
"""

from typing import Dict, Any, List, Tuple, Optional, Union
from datetime import date, datetime, timezone
from sqlalchemy import create_engine, and_, desc
from sqlalchemy.orm import sessionmaker, Session
import pandas as pd

from ..config import config
from ..utils.logging import get_logger
from ..utils.exceptions import DatabaseError
from ..utils.validation import validate_ticker, sanitize_sql_input, validate_currency_code
from ..data.models import (
    Base, Instrument, Price, IncomeStatement, BalanceSheet, CashFlow, FinancialRatio,
    Portfolio, PortfolioHolding, Transaction, InstrumentType, TransactionType,
    EconomicIndicator, EconomicIndicatorData, Threshold, Frequency, ThresholdCategory,
    AlignedDailyData
)


# Constants
DEFAULT_SUMMARY_YEARS = 5


class DatabaseManager:
    """
    Unified database manager for market data and financial analysis.
    
    Handles storage and retrieval of instrument information, price data,
    and structured financial statements with proper relationships and currency handling.
    """
    
    def __init__(self, db_path: Optional[str] = None) -> None:
        self.logger = get_logger(__name__)
        
        # Use provided path or default from config
        self.db_path = db_path or config.database.path
        self.engine = create_engine(
            f"sqlite:///{self.db_path}",
            echo=config.database.echo,
            connect_args={"check_same_thread": False}
        )
        
        # Enable foreign key constraints for SQLite
        from sqlalchemy import event
        @event.listens_for(self.engine, "connect")
        def enable_foreign_keys(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()
        
        # Create tables
        Base.metadata.create_all(self.engine)
        
        # Create session factory
        self.Session = sessionmaker(bind=self.engine)
        
        self.logger.info(f"Unified database manager initialized with {self.db_path}")
    
    def get_session(self) -> Session:
        """
        Get a new database session.
        
        Returns:
            SQLAlchemy session object
        """
        return self.Session()
    
    # =============================================================================
    # INSTRUMENT MANAGEMENT
    # =============================================================================
    
    def get_or_create_instrument(
        self,
        ticker: str,
        currency: str = 'USD',
        instrument_info: Optional[Dict[str, Any]] = None,
        isin: Optional[str] = None,
        instrument_type: InstrumentType = InstrumentType.STOCK
    ) -> Instrument:
        """
        Get existing instrument record or create new one.
        
        Args:
            ticker: Instrument ticker symbol
            currency: Currency code
            instrument_info: Optional instrument information dictionary
            isin: Optional ISIN code
            instrument_type: Type of instrument (stock, fund, etf, index, etc.)
            
        Returns:
            Instrument database record
        """
        # Validate and sanitize inputs
        ticker = validate_ticker(ticker)
        currency = validate_currency_code(currency)
        
        with self.get_session() as session:
            # Try to find existing instrument by ticker or ISIN
            instrument = session.query(Instrument).filter(
                (Instrument.ticker_symbol == ticker) | 
                (Instrument.isin == isin) if isin else (Instrument.ticker_symbol == ticker)
            ).first()
            
            if instrument:
                # Update instrument information if provided
                if instrument_info:
                    instrument.instrument_name = instrument_info.get('instrument_name') or instrument.instrument_name
                    instrument.isin = isin or instrument.isin
                    instrument.instrument_type = instrument_type
                    instrument.sector = instrument_info.get('sector') or instrument.sector
                    instrument.industry = instrument_info.get('industry') or instrument.industry
                    instrument.country = instrument_info.get('country') or instrument.country
                    instrument.currency = currency
                    instrument.market_cap = instrument_info.get('market_cap') or instrument.market_cap
                    instrument.employees = instrument_info.get('employees') or instrument.employees
                    instrument.updated_at = datetime.now(timezone.utc)
                    session.commit()
                
                self.logger.debug(f"Updated existing instrument record for {ticker}")
            else:
                # Create new instrument record
                info = instrument_info or {}
                instrument = Instrument(
                    ticker_symbol=ticker,
                    isin=isin,
                    instrument_type=instrument_type,
                    instrument_name=info.get('instrument_name', ''),
                    sector=info.get('sector', ''),
                    industry=info.get('industry', ''),
                    country=info.get('country', ''),
                    currency=currency,
                    market_cap=info.get('market_cap'),
                    employees=info.get('employees'),
                    founded_year=info.get('founded_year'),
                    fund_type=info.get('fund_type')
                )
                session.add(instrument)
                session.commit()
                session.refresh(instrument)
                
                self.logger.debug(f"Created new instrument record for {ticker}")
            
            return instrument
    
    # Backward compatibility alias
    def get_or_create_company(self, *args, **kwargs):
        """Deprecated: Use get_or_create_instrument instead."""
        return self.get_or_create_instrument(*args, **kwargs)
    
    # =============================================================================
    # PRICE DATA OPERATIONS
    # =============================================================================
    
    def get_existing_price_dates(self, ticker: str) -> List[date]:
        """
        Get all dates that already have price data for a ticker.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            List of dates with existing price data
        """
        with self.get_session() as session:
            instrument = session.query(Instrument).filter(
                Instrument.ticker_symbol == ticker
            ).first()
            
            if not instrument:
                return []
            
            dates = session.query(Price.date).filter(
                Price.instrument_id == instrument.id
            ).all()
            return [d[0] for d in dates]
    
    def get_price_date_range(self, ticker: str) -> Optional[Tuple[date, date]]:
        """
        Get the date range of existing price data for a ticker.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Tuple of (min_date, max_date) or None if no data exists
        """
        with self.get_session() as session:
            instrument = session.query(Instrument).filter(
                Instrument.ticker_symbol == ticker
            ).first()
            
            if not instrument:
                return None
            
            from sqlalchemy import func
            result = session.query(
                func.min(Price.date), 
                func.max(Price.date)
            ).filter(Price.instrument_id == instrument.id).first()
            
            if result[0] is None:
                return None
            
            return result
    
    def store_price_data(
        self, 
        ticker: str, 
        price_data: pd.DataFrame, 
        instrument_type: Optional[InstrumentType] = None,
        instrument_info: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Store price data in the database.
        
        Args:
            ticker: Ticker symbol
            price_data: DataFrame with price data
            instrument_type: Optional instrument type (auto-detected)
            instrument_info: Optional instrument information from Yahoo Finance
            
        Returns:
            Number of records inserted
        """
        if price_data.empty:
            return 0
        
        try:
            # Use detected instrument type if provided, otherwise default to STOCK
            final_instrument_type = instrument_type or InstrumentType.STOCK
            
            # Transform raw Yahoo Finance info to expected format
            transformed_info = None
            if instrument_info:
                transformed_info = {
                    'instrument_name': instrument_info.get('longName', ''),
                    'sector': instrument_info.get('sector', ''),
                    'industry': instrument_info.get('industry', ''),
                    'country': instrument_info.get('country', ''),
                    'market_cap': instrument_info.get('marketCap'),
                    'employees': instrument_info.get('fullTimeEmployees')
                }
            
            # Extract currency from Yahoo Finance info
            currency = 'USD'  # Default
            if instrument_info:
                yf_currency = instrument_info.get('currency') or instrument_info.get('financialCurrency')
                if yf_currency and len(yf_currency) == 3:
                    currency = yf_currency.upper()
            
            instrument = self.get_or_create_instrument(
                ticker, 
                currency=currency,
                instrument_info=transformed_info,
                instrument_type=final_instrument_type
            )
            
            with self.get_session() as session:
                # Refresh instrument in this session
                instrument = session.merge(instrument)
                
                inserted_count = 0
                
                for _, row in price_data.iterrows():
                    try:
                        # Check if record already exists
                        existing = session.query(Price).filter(
                            and_(
                                Price.instrument_id == instrument.id,
                                Price.date == row['date']
                            )
                        ).first()
                        
                        if existing:
                            continue
                        
                        price_record = Price(
                            instrument_id=instrument.id,
                            date=row['date'],
                            open=row.get('open'),
                            high=row.get('high'),
                            low=row.get('low'),
                            close=row.get('close'),
                            adj_close=row.get('adj_close'),
                            volume=int(row.get('volume', 0)) if pd.notna(row.get('volume')) else None
                        )
                        
                        session.add(price_record)
                        inserted_count += 1
                        
                    except Exception as e:
                        self.logger.error(f"Error inserting price data for {ticker} on {row['date']}: {e}")
                        continue
                
                session.commit()
                self.logger.info(f"Inserted {inserted_count} price records for {ticker}")
                return inserted_count
                
        except Exception as e:
            self.logger.error(f"Failed to store price data for {ticker}: {e}", exc_info=True)
            raise DatabaseError(f"Price data storage failed: {e}") from e
    
    # =============================================================================
    # FINANCIAL DATA OPERATIONS
    # =============================================================================
    
    def store_financial_data(
        self,
        ticker: str,
        financial_data: Dict[str, Any]
    ) -> Dict[str, int]:
        """
        Store comprehensive financial statement data.
        
        Args:
            ticker: Stock ticker symbol
            financial_data: Standardized financial data from FinancialStatementFetcher
            
        Returns:
            Dictionary with counts of stored records by statement type
            
        Raises:
            DatabaseError: If storage fails
        """
        try:
            with self.get_session() as session:
                # Get or create instrument record
                instrument = self._get_or_create_instrument_in_session(
                    session, 
                    ticker, 
                    financial_data.get('currency', 'USD'),
                    financial_data.get('instrument_info', {})
                )
                
                storage_counts = {
                    'income_statements': 0,
                    'balance_sheets': 0,
                    'cash_flows': 0,
                    'financial_ratios': 0
                }
                
                statements = financial_data.get('statements', {})
                
                # Store income statements
                if 'income_stmt' in statements:
                    count = self._store_income_statements(
                        session, 
                        instrument.id, 
                        statements['income_stmt']
                    )
                    storage_counts['income_statements'] = count
                
                # Store balance sheets
                if 'balance_sheet' in statements:
                    count = self._store_balance_sheets(
                        session,
                        instrument.id,
                        statements['balance_sheet']
                    )
                    storage_counts['balance_sheets'] = count
                
                # Store cash flow statements
                if 'cash_flow' in statements:
                    count = self._store_cash_flows(
                        session,
                        instrument.id,
                        statements['cash_flow']
                    )
                    storage_counts['cash_flows'] = count
                
                # Store derived financial ratios
                derived_metrics = financial_data.get('derived_metrics', {})
                if derived_metrics:
                    count = self._store_financial_ratios(
                        session,
                        instrument.id,
                        derived_metrics
                    )
                    storage_counts['financial_ratios'] = count
                
                session.commit()
                
                total_records = sum(storage_counts.values())
                self.logger.info(
                    f"Stored {total_records} financial records for {ticker}: "
                    f"{storage_counts}"
                )
                
                return storage_counts
                
        except Exception as e:
            self.logger.error(f"Failed to store financial data for {ticker}: {e}", exc_info=True)
            raise DatabaseError(f"Database storage failed: {e}") from e
    
    def _get_or_create_instrument_in_session(
        self,
        session: Session,
        ticker: str,
        currency: str,
        instrument_info: Dict[str, Any]
    ) -> Instrument:
        """Get existing instrument record or create new one within a session."""
        # Try to find existing instrument
        instrument = session.query(Instrument).filter(
            Instrument.ticker_symbol == ticker
        ).first()
        
        if instrument:
            # Update instrument information
            instrument.instrument_name = instrument_info.get('instrument_name') or instrument.instrument_name
            instrument.sector = instrument_info.get('sector') or instrument.sector
            instrument.industry = instrument_info.get('industry') or instrument.industry
            instrument.country = instrument_info.get('country') or instrument.country
            instrument.currency = currency
            instrument.market_cap = instrument_info.get('market_cap') or instrument.market_cap
            instrument.employees = instrument_info.get('employees') or instrument.employees
            instrument.updated_at = datetime.now(timezone.utc)
            
            self.logger.debug(f"Updated existing instrument record for {ticker}")
        else:
            # Create new instrument record
            instrument = Instrument(
                ticker_symbol=ticker,
                instrument_name=instrument_info.get('instrument_name', ''),
                sector=instrument_info.get('sector', ''),
                industry=instrument_info.get('industry', ''),
                country=instrument_info.get('country', ''),
                currency=currency,
                market_cap=instrument_info.get('market_cap'),
                employees=instrument_info.get('employees'),
                founded_year=instrument_info.get('founded_year')
            )
            session.add(instrument)
            session.flush()  # Get the ID
            
            self.logger.debug(f"Created new instrument record for {ticker}")
        
        return instrument
    
    def _store_income_statements(
        self,
        session: Session,
        instrument_id: int,
        income_data: Dict[str, Any]
    ) -> int:
        """Store income statement records."""
        count = 0
        
        # Store annual data
        annual_data = income_data.get('annual', {})
        for period_date, period_info in annual_data.items():
            count += self._store_single_income_statement(
                session, instrument_id, period_date, period_info, 'annual'
            )
        
        # Store quarterly data
        quarterly_data = income_data.get('quarterly', {})
        for period_date, period_info in quarterly_data.items():
            count += self._store_single_income_statement(
                session, instrument_id, period_date, period_info, 'quarterly'
            )
        
        return count
    
    def _store_single_income_statement(
        self,
        session: Session,
        instrument_id: int,
        period_date: str,
        period_info: Dict[str, Any],
        period_type: str
    ) -> int:
        """Store a single income statement record."""
        try:
            period_end_date = datetime.strptime(period_date, '%Y-%m-%d').date()
            metrics = period_info.get('metrics', {})
            
            # Check if record already exists
            existing = session.query(IncomeStatement).filter(
                and_(
                    IncomeStatement.instrument_id == instrument_id,
                    IncomeStatement.period_end_date == period_end_date,
                    IncomeStatement.period_type == period_type
                )
            ).first()
            
            if existing:
                income_stmt = existing
            else:
                income_stmt = IncomeStatement(
                    instrument_id=instrument_id,
                    period_end_date=period_end_date,
                    period_type=period_type
                )
                session.add(income_stmt)
            
            # Set standard fields
            income_stmt.fiscal_year = period_info.get('fiscal_year', period_end_date.year)
            income_stmt.fiscal_quarter = period_info.get('fiscal_quarter')
            income_stmt.currency = period_info.get('currency', 'USD')
            
            # Set financial metrics
            income_stmt.total_revenue = metrics.get('total_revenue')
            income_stmt.cost_of_revenue = metrics.get('cost_of_revenue')
            income_stmt.gross_profit = metrics.get('gross_profit')
            income_stmt.research_development = metrics.get('research_development')
            income_stmt.sales_marketing = metrics.get('sales_marketing')
            income_stmt.general_administrative = metrics.get('general_administrative')
            income_stmt.total_operating_expenses = metrics.get('total_operating_expenses')
            income_stmt.operating_income = metrics.get('operating_income')
            income_stmt.interest_expense = metrics.get('interest_expense')
            income_stmt.interest_income = metrics.get('interest_income')
            income_stmt.other_income_expense = metrics.get('other_income_expense')
            income_stmt.income_before_tax = metrics.get('income_before_tax')
            income_stmt.tax_provision = metrics.get('tax_provision')
            income_stmt.net_income = metrics.get('net_income')
            income_stmt.net_income_common = metrics.get('net_income_common')
            income_stmt.basic_eps = metrics.get('basic_eps')
            income_stmt.diluted_eps = metrics.get('diluted_eps')
            income_stmt.weighted_average_shares = metrics.get('weighted_average_shares')
            income_stmt.weighted_average_shares_diluted = metrics.get('weighted_average_shares_diluted')
            income_stmt.ebitda = metrics.get('ebitda')
            income_stmt.depreciation_amortization = metrics.get('depreciation_amortization')
            
            return 1
            
        except Exception as e:
            self.logger.warning(f"Failed to store income statement for {period_date}: {e}")
            return 0
    
    def _store_balance_sheets(
        self,
        session: Session,
        instrument_id: int,
        balance_data: Dict[str, Any]
    ) -> int:
        """Store balance sheet records."""
        count = 0
        
        # Store annual data
        annual_data = balance_data.get('annual', {})
        for period_date, period_info in annual_data.items():
            count += self._store_single_balance_sheet(
                session, instrument_id, period_date, period_info, 'annual'
            )
        
        # Store quarterly data
        quarterly_data = balance_data.get('quarterly', {})
        for period_date, period_info in quarterly_data.items():
            count += self._store_single_balance_sheet(
                session, instrument_id, period_date, period_info, 'quarterly'
            )
        
        return count
    
    def _store_single_balance_sheet(
        self,
        session: Session,
        instrument_id: int,
        period_date: str,
        period_info: Dict[str, Any],
        period_type: str
    ) -> int:
        """Store a single balance sheet record."""
        try:
            period_end_date = datetime.strptime(period_date, '%Y-%m-%d').date()
            metrics = period_info.get('metrics', {})
            
            # Check if record already exists
            existing = session.query(BalanceSheet).filter(
                and_(
                    BalanceSheet.instrument_id == instrument_id,
                    BalanceSheet.period_end_date == period_end_date,
                    BalanceSheet.period_type == period_type
                )
            ).first()
            
            if existing:
                balance_sheet = existing
            else:
                balance_sheet = BalanceSheet(
                    instrument_id=instrument_id,
                    period_end_date=period_end_date,
                    period_type=period_type
                )
                session.add(balance_sheet)
            
            # Set standard fields
            balance_sheet.fiscal_year = period_info.get('fiscal_year', period_end_date.year)
            balance_sheet.fiscal_quarter = period_info.get('fiscal_quarter')
            balance_sheet.currency = period_info.get('currency', 'USD')
            
            # Set balance sheet fields using helper method
            self._populate_balance_sheet_fields(balance_sheet, metrics)
            
            # Calculate derived fields
            self._calculate_balance_sheet_derived_fields(balance_sheet)
            
            return 1
            
        except Exception as e:
            self.logger.warning(f"Failed to store balance sheet for {period_date}: {e}")
            return 0
    
    def _populate_balance_sheet_fields(self, balance_sheet: BalanceSheet, metrics: Dict[str, Any]) -> None:
        """Populate balance sheet fields from metrics dictionary."""
        # Current assets
        balance_sheet.cash_and_equivalents = metrics.get('cash_and_equivalents')
        balance_sheet.short_term_investments = metrics.get('short_term_investments')
        balance_sheet.accounts_receivable = metrics.get('accounts_receivable')
        balance_sheet.inventory = metrics.get('inventory')
        balance_sheet.prepaid_expenses = metrics.get('prepaid_expenses')
        balance_sheet.other_current_assets = metrics.get('other_current_assets')
        balance_sheet.total_current_assets = metrics.get('total_current_assets')
        
        # Non-current assets
        balance_sheet.property_plant_equipment = metrics.get('property_plant_equipment')
        balance_sheet.goodwill = metrics.get('goodwill')
        balance_sheet.intangible_assets = metrics.get('intangible_assets')
        balance_sheet.long_term_investments = metrics.get('long_term_investments')
        balance_sheet.other_non_current_assets = metrics.get('other_non_current_assets')
        balance_sheet.total_non_current_assets = metrics.get('total_non_current_assets')
        balance_sheet.total_assets = metrics.get('total_assets')
        
        # Current liabilities
        balance_sheet.accounts_payable = metrics.get('accounts_payable')
        balance_sheet.short_term_debt = metrics.get('short_term_debt')
        balance_sheet.accrued_expenses = metrics.get('accrued_expenses')
        balance_sheet.deferred_revenue_current = metrics.get('deferred_revenue_current')
        balance_sheet.other_current_liabilities = metrics.get('other_current_liabilities')
        balance_sheet.total_current_liabilities = metrics.get('total_current_liabilities')
        
        # Non-current liabilities
        balance_sheet.long_term_debt = metrics.get('long_term_debt')
        balance_sheet.deferred_revenue_non_current = metrics.get('deferred_revenue_non_current')
        balance_sheet.deferred_tax_liabilities = metrics.get('deferred_tax_liabilities')
        balance_sheet.other_non_current_liabilities = metrics.get('other_non_current_liabilities')
        balance_sheet.total_non_current_liabilities = metrics.get('total_non_current_liabilities')
        balance_sheet.total_liabilities = metrics.get('total_liabilities')
        
        # Shareholders' equity
        balance_sheet.common_stock = metrics.get('common_stock')
        balance_sheet.retained_earnings = metrics.get('retained_earnings')
        balance_sheet.accumulated_other_income = metrics.get('accumulated_other_income')
        balance_sheet.treasury_stock = metrics.get('treasury_stock')
        balance_sheet.total_shareholders_equity = metrics.get('total_shareholders_equity')
    
    def _calculate_balance_sheet_derived_fields(self, balance_sheet: BalanceSheet) -> None:
        """Calculate derived fields for balance sheet."""
        # Calculate total debt
        short_debt = balance_sheet.short_term_debt or 0
        long_debt = balance_sheet.long_term_debt or 0
        if short_debt or long_debt:
            balance_sheet.total_debt = short_debt + long_debt
        
        # Calculate net debt
        if balance_sheet.total_debt and balance_sheet.cash_and_equivalents:
            balance_sheet.net_debt = balance_sheet.total_debt - balance_sheet.cash_and_equivalents
        
        # Calculate working capital
        if balance_sheet.total_current_assets and balance_sheet.total_current_liabilities:
            balance_sheet.working_capital = balance_sheet.total_current_assets - balance_sheet.total_current_liabilities
    
    def _store_cash_flows(
        self,
        session: Session,
        instrument_id: int,
        cashflow_data: Dict[str, Any]
    ) -> int:
        """Store cash flow statement records."""
        count = 0
        
        # Store annual data
        annual_data = cashflow_data.get('annual', {})
        for period_date, period_info in annual_data.items():
            count += self._store_single_cash_flow(
                session, instrument_id, period_date, period_info, 'annual'
            )
        
        # Store quarterly data
        quarterly_data = cashflow_data.get('quarterly', {})
        for period_date, period_info in quarterly_data.items():
            count += self._store_single_cash_flow(
                session, instrument_id, period_date, period_info, 'quarterly'
            )
        
        return count
    
    def _store_single_cash_flow(
        self,
        session: Session,
        instrument_id: int,
        period_date: str,
        period_info: Dict[str, Any],
        period_type: str
    ) -> int:
        """Store a single cash flow statement record."""
        try:
            period_end_date = datetime.strptime(period_date, '%Y-%m-%d').date()
            metrics = period_info.get('metrics', {})
            
            # Check if record already exists
            existing = session.query(CashFlow).filter(
                and_(
                    CashFlow.instrument_id == instrument_id,
                    CashFlow.period_end_date == period_end_date,
                    CashFlow.period_type == period_type
                )
            ).first()
            
            if existing:
                cash_flow = existing
            else:
                cash_flow = CashFlow(
                    instrument_id=instrument_id,
                    period_end_date=period_end_date,
                    period_type=period_type
                )
                session.add(cash_flow)
            
            # Set standard fields
            cash_flow.fiscal_year = period_info.get('fiscal_year', period_end_date.year)
            cash_flow.fiscal_quarter = period_info.get('fiscal_quarter')
            cash_flow.currency = period_info.get('currency', 'USD')
            
            # Set cash flow metrics
            cash_flow.net_income = metrics.get('net_income')
            cash_flow.depreciation_amortization = metrics.get('depreciation_amortization')
            cash_flow.stock_compensation = metrics.get('stock_compensation')
            cash_flow.deferred_tax = metrics.get('deferred_tax')
            cash_flow.change_accounts_receivable = metrics.get('change_accounts_receivable')
            cash_flow.change_inventory = metrics.get('change_inventory')
            cash_flow.change_accounts_payable = metrics.get('change_accounts_payable')
            cash_flow.change_other_working_capital = metrics.get('change_other_working_capital')
            cash_flow.operating_cash_flow = metrics.get('operating_cash_flow')
            cash_flow.capital_expenditures = metrics.get('capital_expenditures')
            cash_flow.acquisitions = metrics.get('acquisitions')
            cash_flow.purchases_investments = metrics.get('purchases_investments')
            cash_flow.sales_maturities_investments = metrics.get('sales_maturities_investments')
            cash_flow.other_investing_activities = metrics.get('other_investing_activities')
            cash_flow.investing_cash_flow = metrics.get('investing_cash_flow')
            cash_flow.debt_issuance = metrics.get('debt_issuance')
            cash_flow.debt_repayment = metrics.get('debt_repayment')
            cash_flow.common_stock_issuance = metrics.get('common_stock_issuance')
            cash_flow.common_stock_repurchase = metrics.get('common_stock_repurchase')
            cash_flow.dividends_paid = metrics.get('dividends_paid')
            cash_flow.other_financing_activities = metrics.get('other_financing_activities')
            cash_flow.financing_cash_flow = metrics.get('financing_cash_flow')
            cash_flow.net_change_cash = metrics.get('net_change_cash')
            cash_flow.cash_beginning = metrics.get('cash_beginning')
            cash_flow.cash_ending = metrics.get('cash_ending')
            cash_flow.free_cash_flow = metrics.get('free_cash_flow')
            cash_flow.fcf_per_share = metrics.get('fcf_per_share')
            
            return 1
            
        except Exception as e:
            self.logger.warning(f"Failed to store cash flow for {period_date}: {e}")
            return 0
    
    def _store_financial_ratios(
        self,
        session: Session,
        instrument_id: int,
        derived_metrics: Dict[str, Any]
    ) -> int:
        """Store calculated financial ratios."""
        count = 0
        
        # Store annual ratios
        annual_data = derived_metrics.get('annual', {})
        for period_date, metrics in annual_data.items():
            count += self._store_single_financial_ratio(
                session, instrument_id, period_date, metrics, 'annual'
            )
        
        # Store quarterly ratios
        quarterly_data = derived_metrics.get('quarterly', {})
        for period_date, metrics in quarterly_data.items():
            count += self._store_single_financial_ratio(
                session, instrument_id, period_date, metrics, 'quarterly'
            )
        
        return count
    
    def _store_single_financial_ratio(
        self,
        session: Session,
        instrument_id: int,
        period_date: str,
        metrics: Dict[str, float],
        period_type: str
    ) -> int:
        """Store a single financial ratio record."""
        try:
            period_end_date = datetime.strptime(period_date, '%Y-%m-%d').date()
            
            # Check if record already exists
            existing = session.query(FinancialRatio).filter(
                and_(
                    FinancialRatio.instrument_id == instrument_id,
                    FinancialRatio.period_end_date == period_end_date,
                    FinancialRatio.period_type == period_type
                )
            ).first()
            
            if existing:
                ratio = existing
            else:
                ratio = FinancialRatio(
                    instrument_id=instrument_id,
                    period_end_date=period_end_date,
                    period_type=period_type
                )
                session.add(ratio)
            
            # Set standard fields
            ratio.fiscal_year = period_end_date.year
            ratio.fiscal_quarter = None if period_type == 'annual' else 1  # Simplified
            
            # Set calculated ratios
            ratio.gross_profit_margin = metrics.get('gross_profit_margin')
            ratio.operating_profit_margin = metrics.get('operating_profit_margin')
            ratio.net_profit_margin = metrics.get('net_profit_margin')
            ratio.return_on_assets = metrics.get('return_on_assets')
            ratio.return_on_equity = metrics.get('return_on_equity')
            ratio.current_ratio = metrics.get('current_ratio')
            ratio.quick_ratio = metrics.get('quick_ratio')
            ratio.cash_ratio = metrics.get('cash_ratio')
            ratio.debt_to_equity = metrics.get('debt_to_equity')
            ratio.debt_to_assets = metrics.get('debt_to_assets')
            ratio.interest_coverage = metrics.get('interest_coverage')
            ratio.asset_turnover = metrics.get('asset_turnover')
            ratio.inventory_turnover = metrics.get('inventory_turnover')
            ratio.receivables_turnover = metrics.get('receivables_turnover')
            ratio.days_sales_outstanding = metrics.get('days_sales_outstanding')
            
            return 1
            
        except Exception as e:
            self.logger.warning(f"Failed to store financial ratios for {period_date}: {e}")
            return 0
    
    # =============================================================================
    # INFORMATION AND REPORTING
    # =============================================================================
    
    def get_instrument_financial_summary(
        self, 
        ticker: str,
        years: int = DEFAULT_SUMMARY_YEARS
    ) -> Dict[str, Any]:
        """
        Get comprehensive financial summary for a instrument.
        
        Args:
            ticker: Stock ticker symbol
            years: Number of recent years to include
            
        Returns:
            Dictionary with financial summary data
        """
        try:
            with self.get_session() as session:
                instrument = session.query(Instrument).filter(
                    Instrument.ticker_symbol == ticker
                ).first()
                
                if not instrument:
                    return {}
                
                cutoff_date = date(date.today().year - years + 1, 1, 1)
                
                # Get recent data counts
                income_count = session.query(IncomeStatement).filter(
                    and_(
                        IncomeStatement.instrument_id == instrument.id,
                        IncomeStatement.period_end_date >= cutoff_date
                    )
                ).count()
                
                balance_count = session.query(BalanceSheet).filter(
                    and_(
                        BalanceSheet.instrument_id == instrument.id,
                        BalanceSheet.period_end_date >= cutoff_date
                    )
                ).count()
                
                cashflow_count = session.query(CashFlow).filter(
                    and_(
                        CashFlow.instrument_id == instrument.id,
                        CashFlow.period_end_date >= cutoff_date
                    )
                ).count()
                
                ratios_count = session.query(FinancialRatio).filter(
                    and_(
                        FinancialRatio.instrument_id == instrument.id,
                        FinancialRatio.period_end_date >= cutoff_date
                    )
                ).count()
                
                # Get latest data
                latest_income = session.query(IncomeStatement).filter(
                    IncomeStatement.instrument_id == instrument.id
                ).order_by(desc(IncomeStatement.period_end_date)).first()
                
                latest_balance = session.query(BalanceSheet).filter(
                    BalanceSheet.instrument_id == instrument.id
                ).order_by(desc(BalanceSheet.period_end_date)).first()
                
                latest_cashflow = session.query(CashFlow).filter(
                    CashFlow.instrument_id == instrument.id
                ).order_by(desc(CashFlow.period_end_date)).first()
                
                latest_ratios = session.query(FinancialRatio).filter(
                    FinancialRatio.instrument_id == instrument.id
                ).order_by(desc(FinancialRatio.period_end_date)).first()
                
                return {
                    'instrument': {
                        'ticker': instrument.ticker_symbol,
                        'name': instrument.instrument_name,
                        'sector': instrument.sector,
                        'industry': instrument.industry,
                        'country': instrument.country,
                        'currency': instrument.currency,
                        'market_cap': instrument.market_cap,
                        'employees': instrument.employees
                    },
                    'data_summary': {
                        'income_statements': income_count,
                        'balance_sheets': balance_count,
                        'cash_flows': cashflow_count,
                        'financial_ratios': ratios_count,
                        'date_range': {
                            'from': cutoff_date.isoformat(),
                            'to': date.today().isoformat()
                        }
                    },
                    'latest_data': {
                        'income_statement': latest_income,
                        'balance_sheet': latest_balance,
                        'cash_flow': latest_cashflow,
                        'financial_ratios': latest_ratios
                    }
                }
                
        except Exception as e:
            self.logger.error(f"Failed to get financial summary for {ticker}: {e}")
            raise DatabaseError(f"Failed to retrieve financial summary: {e}")
    
    def get_instrument_info(self, ticker: str) -> Dict[str, Any]:
        """
        Get information about stored data for an instrument.
        
        Args:
            ticker: Instrument ticker symbol
            
        Returns:
            Dictionary with instrument information and stored data summary
        """
        with self.get_session() as session:
            instrument = session.query(Instrument).filter(
                Instrument.ticker_symbol == ticker
            ).first()
            
            if not instrument:
                return {
                    'ticker': ticker,
                    'exists': False,
                    'price_data': {},
                    'financial_statements': {}
                }
            
            # Get price data info
            price_range = self.get_price_date_range(ticker)
            price_count = session.query(Price).filter(Price.instrument_id == instrument.id).count()
            
            # Get financial statement counts
            income_count = session.query(IncomeStatement).filter(IncomeStatement.instrument_id == instrument.id).count()
            balance_count = session.query(BalanceSheet).filter(BalanceSheet.instrument_id == instrument.id).count()
            cashflow_count = session.query(CashFlow).filter(CashFlow.instrument_id == instrument.id).count()
            ratios_count = session.query(FinancialRatio).filter(FinancialRatio.instrument_id == instrument.id).count()
            
            return {
                'ticker': ticker,
                'exists': True,
                'instrument_id': instrument.id,
                'instrument': {
                    'name': instrument.instrument_name,
                    'sector': instrument.sector,
                    'industry': instrument.industry,
                    'country': instrument.country,
                    'currency': instrument.currency,
                    'market_cap': instrument.market_cap,
                    'employees': instrument.employees,
                    'created_at': instrument.created_at
                },
                'price_data': {
                    'count': price_count,
                    'date_range': price_range
                },
                'financial_statements': {
                    'income_statements': income_count,
                    'balance_sheets': balance_count,
                    'cash_flows': cashflow_count,
                    'financial_ratios': ratios_count
                }
            }
    
    # Backward compatibility alias
    def get_ticker_info(self, ticker: str) -> Dict[str, Any]:
        """Deprecated: Use get_instrument_info instead."""
        return self.get_instrument_info(ticker)
    
    # =============================================================================
    # DATABASE CLEARING METHODS (for development/testing)
    # =============================================================================
    
    def clear_ticker_data(self, ticker: str) -> bool:
        """
        Clear all data for a specific ticker (for development/testing).
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            True if data was cleared, False if no data found
        """
        try:
            ticker = validate_ticker(ticker)
            
            with self.get_session() as session:
                # Find the instrument
                instrument = session.query(Instrument).filter(
                    Instrument.ticker_symbol == ticker
                ).first()
                
                if not instrument:
                    return False
                
                # Delete portfolio-related data first (due to foreign key constraints)
                session.query(Transaction).filter(
                    Transaction.portfolio_id.in_(
                        session.query(Portfolio.id).join(PortfolioHolding).filter(
                            PortfolioHolding.instrument_id == instrument.id
                        )
                    )
                ).delete(synchronize_session=False)
                
                session.query(PortfolioHolding).filter(PortfolioHolding.instrument_id == instrument.id).delete()
                
                # Delete all other related data
                session.query(Price).filter(Price.instrument_id == instrument.id).delete()
                session.query(IncomeStatement).filter(IncomeStatement.instrument_id == instrument.id).delete()
                session.query(BalanceSheet).filter(BalanceSheet.instrument_id == instrument.id).delete()
                session.query(CashFlow).filter(CashFlow.instrument_id == instrument.id).delete()
                session.query(FinancialRatio).filter(FinancialRatio.instrument_id == instrument.id).delete()
                
                # Delete the instrument record
                session.delete(instrument)
                
                session.commit()
                self.logger.info(f"Cleared all data for ticker {ticker}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error clearing data for ticker {ticker}: {e}", exc_info=True)
            raise DatabaseError(f"Failed to clear data for ticker {ticker}") from e
    
    def clear_all_data(self) -> bool:
        """
        Clear all data from the database (for development/testing).
        
        Returns:
            True if data was cleared successfully
        """
        try:
            with self.get_session() as session:
                # Delete all data from all tables in proper order (respecting foreign key constraints)
                # Delete child tables first (those with FKs to other tables)
                session.query(AlignedDailyData).delete()  # FKs to instrument + economic_indicator
                session.query(Transaction).delete()       # FK to instrument
                session.query(PortfolioHolding).delete()  # FKs to portfolio + instrument
                session.query(Price).delete()             # FK to instrument
                session.query(IncomeStatement).delete()   # FK to instrument
                session.query(BalanceSheet).delete()      # FK to instrument
                session.query(CashFlow).delete()          # FK to instrument
                session.query(FinancialRatio).delete()    # FK to instrument
                session.query(EconomicIndicatorData).delete()  # FK to economic_indicator
                session.query(Threshold).delete()         # FK to economic_indicator
                
                # Delete parent tables last (those referenced by FKs)
                session.query(Portfolio).delete()         # Referenced by portfolio_holdings
                session.query(EconomicIndicator).delete() # Referenced by economic_indicator_data, thresholds, aligned_daily_data
                session.query(Instrument).delete()        # Referenced by prices, statements, holdings, transactions, aligned_daily_data
                
                session.commit()
                self.logger.info("Cleared all data from database")
                return True
                
        except Exception as e:
            self.logger.error(f"Error clearing all database data: {e}", exc_info=True)
            raise DatabaseError("Failed to clear all database data") from e
    
    # =============================================================================
    # PORTFOLIO MANAGEMENT METHODS
    # =============================================================================
    
    def load_portfolio_from_config(self, portfolio_config: Dict[str, Any]) -> Portfolio:
        """
        Load or update portfolio from configuration dictionary.
        
        Args:
            portfolio_config: Portfolio configuration dictionary
            
        Returns:
            Portfolio database record
        """
        try:
            from datetime import datetime
            
            with self.get_session() as session:
                # Try to find existing portfolio
                portfolio = session.query(Portfolio).filter(
                    Portfolio.name == portfolio_config['name']
                ).first()
                
                if portfolio:
                    # Update existing portfolio
                    portfolio.description = portfolio_config.get('description', portfolio.description)
                    portfolio.currency = portfolio_config.get('currency', portfolio.currency)
                    portfolio.updated_at = datetime.now(timezone.utc)
                    self.logger.debug(f"Updated existing portfolio: {portfolio.name}")
                else:
                    # Create new portfolio with defaults for missing fields
                    if 'created_date' in portfolio_config:
                        created_date = datetime.strptime(
                            portfolio_config['created_date'], 
                            '%Y-%m-%d'
                        ).date()
                    else:
                        created_date = datetime.now().date()
                    
                    portfolio = Portfolio(
                        name=portfolio_config['name'],
                        description=portfolio_config.get('description', ''),
                        currency=portfolio_config.get('currency', 'USD'),  # Default currency
                        created_date=created_date
                    )
                    session.add(portfolio)
                    self.logger.debug(f"Created new portfolio: {portfolio.name}")
                
                # Process holdings - now expects list of tickers
                self._update_portfolio_holdings(session, portfolio, portfolio_config.get('holdings', []))
                
                session.commit()
                session.refresh(portfolio)
                return portfolio
                
        except Exception as e:
            self.logger.error(f"Error loading portfolio config: {e}", exc_info=True)
            raise DatabaseError(f"Failed to load portfolio from config") from e
    
    def _update_portfolio_holdings(self, session: Session, portfolio: Portfolio, holdings_list: List[str]):
        """Update portfolio holdings from ticker list."""
        # Clear existing holdings
        session.query(PortfolioHolding).filter(
            PortfolioHolding.portfolio_id == portfolio.id
        ).delete()
        
        # Add new holdings from ticker list
        for ticker in holdings_list:
            # Find or create minimal instrument record (will be populated later during price fetching)
            instrument = session.query(Instrument).filter(
                Instrument.ticker_symbol == ticker
            ).first()
            
            if not instrument:
                # Create minimal instrument record - details populated during price fetching
                instrument = Instrument(
                    ticker_symbol=ticker,
                    instrument_type=InstrumentType.STOCK,  # Default, will be updated during price fetch
                    instrument_name='',  # Populated during price fetch
                    currency='USD'  # Default, will be updated during price fetch
                )
                session.add(instrument)
                session.flush()  # Get the ID but don't commit yet
            
            # Create portfolio holding (no quantities - those come from transactions)
            holding = PortfolioHolding(
                portfolio_id=portfolio.id,
                instrument_id=instrument.id,
                notes=f"Added from portfolio config"
            )
            session.add(holding)
    
    def load_transactions_from_csv(self, csv_data: List[Dict[str, Any]], portfolio_name: Optional[str] = None) -> int:
        """
        Load transactions from CSV data.
        
        Args:
            csv_data: List of transaction dictionaries from CSV
            portfolio_name: Optional portfolio name to associate transactions with
            
        Returns:
            Number of transactions loaded
        """
        try:
            loaded_count = 0
            
            with self.get_session() as session:
                portfolio = None
                if portfolio_name:
                    portfolio = session.query(Portfolio).filter(
                        Portfolio.name == portfolio_name
                    ).first()
                    if not portfolio:
                        raise DatabaseError(f"Portfolio '{portfolio_name}' not found")
                
                for row in csv_data:
                    # Parse transaction data
                    ticker = row['ticker']
                    isin = row.get('isin')
                    transaction_type = TransactionType(row['transaction_type'].lower())
                    
                    # Find or create instrument
                    instrument = session.query(Instrument).filter(
                        (Instrument.ticker_symbol == ticker) | 
                        (Instrument.isin == isin) if isin else (Instrument.ticker_symbol == ticker)
                    ).first()
                    
                    if not instrument:
                        # Create basic instrument record for transaction
                        instrument = Instrument(
                            ticker_symbol=ticker,
                            isin=isin,
                            instrument_type=InstrumentType.STOCK,  # Default
                            instrument_name=f"Instrument for {ticker}",
                            currency=row['currency']
                        )
                        session.add(instrument)
                        session.flush()  # Get the ID
                    
                    # Calculate total amount
                    quantity = float(row['quantity'])
                    price_per_unit = float(row['price_per_unit'])
                    fees = float(row.get('fees', 0))
                    
                    if transaction_type in [TransactionType.BUY]:
                        total_amount = (quantity * price_per_unit) + fees
                    elif transaction_type in [TransactionType.SELL]:
                        total_amount = (quantity * price_per_unit) - fees
                    else:  # DIVIDEND and others
                        total_amount = quantity * price_per_unit
                    
                    # Create transaction
                    transaction = Transaction(
                        portfolio_id=portfolio.id if portfolio else None,
                        instrument_id=instrument.id,
                        transaction_date=datetime.strptime(row['date'], '%Y-%m-%d').date(),
                        transaction_type=transaction_type,
                        quantity=quantity,
                        price_per_unit=price_per_unit,
                        currency=row['currency'],
                        fees=fees,
                        broker=row.get('broker', ''),
                        notes=row.get('notes', ''),
                        total_amount=total_amount
                    )
                    session.add(transaction)
                    loaded_count += 1
                
                session.commit()
                self.logger.info(f"Loaded {loaded_count} transactions from CSV")
                return loaded_count
                
        except Exception as e:
            self.logger.error(f"Error loading transactions from CSV: {e}", exc_info=True)
            raise DatabaseError(f"Failed to load transactions from CSV") from e
    
    def get_portfolio_summary(self, portfolio_name: str) -> Dict[str, Any]:
        """
        Get portfolio summary with holdings and transaction counts.
        
        Args:
            portfolio_name: Portfolio name
            
        Returns:
            Dictionary with portfolio summary information
        """
        try:
            with self.get_session() as session:
                portfolio = session.query(Portfolio).filter(
                    Portfolio.name == portfolio_name
                ).first()
                
                if not portfolio:
                    return {'exists': False}
                
                # Count holdings
                holdings_count = session.query(PortfolioHolding).filter(
                    PortfolioHolding.portfolio_id == portfolio.id
                ).count()
                
                # Count transactions
                transactions_count = session.query(Transaction).filter(
                    Transaction.portfolio_id == portfolio.id
                ).count()
                
                # Get holdings breakdown by instrument type
                from sqlalchemy import func
                holdings_breakdown = session.query(
                    Instrument.instrument_type,
                    func.count().label('count')
                ).join(PortfolioHolding).filter(
                    PortfolioHolding.portfolio_id == portfolio.id
                ).group_by(Instrument.instrument_type).all()
                
                return {
                    'exists': True,
                    'portfolio': {
                        'name': portfolio.name,
                        'description': portfolio.description,
                        'currency': portfolio.currency,
                        'created_date': portfolio.created_date,
                        'created_at': portfolio.created_at
                    },
                    'holdings': {
                        'total_count': holdings_count,
                        'breakdown': {breakdown[0].value: breakdown[1] for breakdown in holdings_breakdown}
                    },
                    'transactions': {
                        'count': transactions_count
                    }
                }
                
        except Exception as e:
            self.logger.error(f"Error getting portfolio summary: {e}", exc_info=True)
            raise DatabaseError(f"Failed to get portfolio summary") from e
    
    # =============================================================================
    # ECONOMIC DATA OPERATIONS
    # =============================================================================
    
    def store_economic_data(self, economic_data: Dict[str, Any], auto_extend_to_today: bool = False) -> Dict[str, int]:
        """
        Store economic indicator data in the database.
        
        Args:
            economic_data: Transformed economic data dictionary
            auto_extend_to_today: Whether to forward-fill data to today's date when no end date specified
            
        Returns:
            Dictionary with counts of stored records
            
        Raises:
            DatabaseError: If storage fails
        """
        try:
            with self.get_session() as session:
                name = economic_data.get('name')
                source = economic_data.get('source')
                source_identifier = economic_data.get('source_identifier')
                
                # Get or create economic indicator
                indicator = self._get_or_create_economic_indicator(session, economic_data)
                
                # Store data points (API data takes precedence over forward-filled)
                data_points = economic_data.get('data_points', [])
                stored_points = self._store_economic_data_points(session, indicator.id, data_points)
                
                # Forward-fill to today's date if auto_extend_to_today is True
                forward_filled_points = 0
                if auto_extend_to_today and data_points:
                    forward_filled_points = self._forward_fill_to_today(session, indicator.id, data_points)
                    stored_points += forward_filled_points
                
                session.commit()
                
                storage_counts = {
                    'indicators': 1,
                    'data_points': stored_points
                }
                
                self.logger.info(
                    f"Stored economic data for {name} ({source}/{source_identifier}): "
                    f"{storage_counts['data_points']} data points"
                )
                
                return storage_counts
                
        except Exception as e:
            self.logger.error(f"Failed to store economic data: {e}", exc_info=True)
            raise DatabaseError(f"Economic data storage failed: {e}") from e
    
    def _get_or_create_economic_indicator(
        self,
        session: Session,
        economic_data: Dict[str, Any]
    ) -> EconomicIndicator:
        """Get existing economic indicator or create new one with duplicate prevention."""
        name = economic_data.get('name')  # standardized name
        source = economic_data.get('source')
        source_identifier = economic_data.get('source_identifier')
        
        if not name:
            raise ValueError("Economic indicator 'name' (standardized identifier) is required")
        
        # Check if indicator already exists by name (standardized identifier)
        indicator = session.query(EconomicIndicator).filter(
            EconomicIndicator.name == name
        ).first()
        
        if indicator:
            # Update existing indicator
            indicator.source = source or indicator.source
            indicator.source_identifier = source_identifier or indicator.source_identifier
            indicator.description = economic_data.get('description', indicator.description)
            indicator.unit = economic_data.get('unit', indicator.unit)
            indicator.frequency = self._parse_frequency(economic_data.get('frequency', 'monthly'))
            indicator.updated_at = datetime.now(timezone.utc)
            
            self.logger.debug(f"Updated existing economic indicator: {name}")
            return indicator
        
        # Create new indicator
        indicator = EconomicIndicator(
            name=name,
            source=source or '',
            source_identifier=source_identifier or '',
            description=economic_data.get('description', ''),
            unit=economic_data.get('unit', ''),
            frequency=self._parse_frequency(economic_data.get('frequency', 'monthly'))
        )
        session.add(indicator)
        session.flush()  # Get the ID
        
        self.logger.debug(f"Created new economic indicator: {name} ({source}/{source_identifier})")
        
        return indicator
    
    def _store_economic_data_points(
        self,
        session: Session,
        indicator_db_id: int,
        data_points: List[Dict[str, Any]]
    ) -> int:
        """Store economic data points using same pattern as save_data.py."""
        stored_count = 0
        
        # Get existing dates to avoid duplicates (same pattern as economic_data package)
        existing = set(
            session.query(EconomicIndicatorData.date)
            .filter(EconomicIndicatorData.indicator_id == indicator_db_id)
            .all()
        )
        existing_dates = {d[0] for d in existing}
        
        new_records = []
        for point in data_points:
            try:
                date_str = point.get('date')
                value = point.get('value')
                
                if not date_str or value is None:
                    continue
                
                # Parse date
                point_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                
                # Only add if not already exists
                if point_date not in existing_dates:
                    data_point = EconomicIndicatorData(
                        indicator_id=indicator_db_id,
                        date=point_date,
                        value=float(value)
                    )
                    new_records.append(data_point)
                    stored_count += 1
                    
            except Exception as e:
                self.logger.warning(f"Failed to store data point {point}: {e}")
                continue
        
        if new_records:
            session.add_all(new_records)
            self.logger.info(f"Inserted {len(new_records)} new records for indicator ID {indicator_db_id}")
        
        return stored_count
    
    def _forward_fill_to_today(
        self,
        session: Session,
        indicator_db_id: int,
        api_data_points: List[Dict[str, Any]]
    ) -> int:
        """
        Forward-fill economic indicator data from latest API date to today's date.
        
        API data takes precedence over forward-filled data. This method creates
        forward-filled records only for dates that don't have real API data.
        
        Args:
            session: Database session
            indicator_db_id: Database ID of the economic indicator
            api_data_points: List of API data points just stored
            
        Returns:
            Number of forward-filled records created
        """
        if not api_data_points:
            return 0
        
        try:
            from dateutil.rrule import rrule, MONTHLY
            from dateutil.relativedelta import relativedelta
            
            # Find the latest date and value from API data
            latest_point = max(api_data_points, key=lambda x: x['date'])
            latest_date = latest_point['date']
            latest_value = latest_point['value']
            
            # Convert to proper date objects if needed
            if isinstance(latest_date, str):
                latest_date = datetime.strptime(latest_date, '%Y-%m-%d').date()
            elif hasattr(latest_date, 'date'):
                latest_date = latest_date.date()
            
            today = date.today()
            
            # Only forward-fill if latest_date is before today
            if latest_date >= today:
                return 0
            
            self.logger.info(f"Forward-filling indicator {indicator_db_id} from {latest_date} to {today} with value {latest_value}")
            
            # Generate monthly dates from next month after latest_date through today
            start_fill_date = latest_date + relativedelta(months=1)
            start_fill_date = start_fill_date.replace(day=1)  # First of month
            
            # Create forward-filled records for each month
            fill_dates = []
            current_date = start_fill_date
            while current_date <= today:
                fill_dates.append(current_date)
                current_date += relativedelta(months=1)
            
            if not fill_dates:
                return 0
            
            # Check which dates already exist (to avoid overwriting real API data)
            existing_dates = set(
                row[0] for row in session.query(EconomicIndicatorData.date)
                .filter(EconomicIndicatorData.indicator_id == indicator_db_id)
                .filter(EconomicIndicatorData.date.in_(fill_dates))
                .all()
            )
            
            # Create forward-filled records only for dates that don't exist
            new_forward_filled = []
            for fill_date in fill_dates:
                if fill_date not in existing_dates:
                    record = EconomicIndicatorData(
                        indicator_id=indicator_db_id,
                        date=fill_date,
                        value=latest_value,
                        created_at=datetime.now(timezone.utc)
                    )
                    new_forward_filled.append(record)
            
            # Insert forward-filled records
            if new_forward_filled:
                session.add_all(new_forward_filled)
                self.logger.info(f"Forward-filled {len(new_forward_filled)} records for indicator {indicator_db_id}")
                return len(new_forward_filled)
            else:
                self.logger.debug(f"No forward-fill needed - all dates already have data")
                return 0
                
        except Exception as e:
            self.logger.error(f"Failed to forward-fill indicator {indicator_db_id}: {e}")
            return 0
    
    def _parse_frequency(self, frequency_str: str) -> Frequency:
        """Parse frequency string to enum."""
        frequency_map = {
            'daily': Frequency.DAILY,
            'monthly': Frequency.MONTHLY,
            'quarterly': Frequency.QUARTERLY,
            'yearly': Frequency.YEARLY,
            'annual': Frequency.YEARLY
        }
        return frequency_map.get(frequency_str.lower(), Frequency.MONTHLY)
    
    def get_economic_indicator_info(self, indicator_name: str) -> Dict[str, Any]:
        """
        Get information about stored data for an economic indicator.
        
        Args:
            indicator_name: Economic indicator name (standardized identifier)
            
        Returns:
            Dictionary with indicator information
        """
        with self.get_session() as session:
            indicator = session.query(EconomicIndicator).filter(
                EconomicIndicator.name == indicator_name
            ).first()
            
            if not indicator:
                return {
                    'indicator_name': indicator_name,
                    'exists': False,
                    'data_points': {}
                }
            
            # Get data point info
            data_count = session.query(EconomicIndicatorData).filter(
                EconomicIndicatorData.indicator_id == indicator.id
            ).count()
            
            # Get date range
            from sqlalchemy import func
            date_range = session.query(
                func.min(EconomicIndicatorData.date),
                func.max(EconomicIndicatorData.date)
            ).filter(EconomicIndicatorData.indicator_id == indicator.id).first()
            
            return {
                'indicator_name': indicator_name,
                'exists': True,
                'indicator': {
                    'name': indicator.name,
                    'source': indicator.source,
                    'source_identifier': indicator.source_identifier,
                    'description': indicator.description,
                    'unit': indicator.unit,
                    'frequency': indicator.frequency.value,
                    'created_at': indicator.created_at,
                    'updated_at': indicator.updated_at
                },
                'data_points': {
                    'count': data_count,
                    'date_range': date_range if date_range[0] else None
                }
            }
    
    def get_economic_data(
        self,
        indicator_name: str,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None
    ) -> pd.DataFrame:
        """
        Retrieve economic data as DataFrame.
        
        Args:
            indicator_name: Economic indicator name (standardized identifier)
            from_date: Optional start date filter
            to_date: Optional end date filter
            
        Returns:
            DataFrame with date and value columns
        """
        with self.get_session() as session:
            indicator = session.query(EconomicIndicator).filter(
                EconomicIndicator.name == indicator_name
            ).first()
            
            if not indicator:
                return pd.DataFrame(columns=['date', 'value'])
            
            query = session.query(
                EconomicIndicatorData.date,
                EconomicIndicatorData.value
            ).filter(EconomicIndicatorData.indicator_id == indicator.id)
            
            if from_date:
                query = query.filter(EconomicIndicatorData.date >= from_date)
            if to_date:
                query = query.filter(EconomicIndicatorData.date <= to_date)
            
            query = query.order_by(EconomicIndicatorData.date)
            
            results = query.all()
            
            if not results:
                return pd.DataFrame(columns=['date', 'value'])
            
            df = pd.DataFrame(results, columns=['date', 'value'])
            return df

    def get_all_economic_indicators(self) -> List[Dict[str, Any]]:
        """
        Get all economic indicators in the database.
        
        Returns:
            List of economic indicator dictionaries
        """
        try:
            with self.get_session() as session:
                indicators = session.query(EconomicIndicator).all()
                
                result = []
                for indicator in indicators:
                    result.append({
                        'id': indicator.id,
                        'name': indicator.name,
                        'source': indicator.source,
                        'source_identifier': indicator.source_identifier,
                        'description': indicator.description,
                        'unit': indicator.unit,
                        'frequency': indicator.frequency.value,
                        'created_at': indicator.created_at,
                        'updated_at': indicator.updated_at
                    })
                
                return result
                
        except Exception as e:
            self.logger.error(f"Failed to get all economic indicators: {e}")
            raise DatabaseError(f"Failed to get all economic indicators: {e}") from e

    def get_all_instruments_info(self) -> List[Dict[str, Any]]:
        """
        Get all instruments in the database with basic info.
        
        Returns:
            List of instrument info dictionaries
        """
        try:
            with self.get_session() as session:
                instruments = session.query(Instrument).all()
                
                result = []
                for instrument in instruments:
                    result.append({
                        'instrument_id': instrument.id,
                        'ticker_symbol': instrument.ticker_symbol,
                        'instrument_name': instrument.instrument_name,
                        'instrument_type': instrument.instrument_type.value,
                        'sector': instrument.sector,
                        'industry': instrument.industry,
                        'country': instrument.country,
                        'currency': instrument.currency
                    })
                
                return result
                
        except Exception as e:
            self.logger.error(f"Failed to get all instruments info: {e}")
            raise DatabaseError(f"Failed to get all instruments info: {e}") from e

    def get_price_data_count(self, ticker: str) -> int:
        """
        Get count of price data points for a ticker.
        
        Args:
            ticker: Ticker symbol
            
        Returns:
            Number of price data points
        """
        ticker = validate_ticker(ticker)
        
        try:
            with self.get_session() as session:
                instrument = session.query(Instrument).filter(
                    Instrument.ticker_symbol == ticker
                ).first()
                
                if not instrument:
                    return 0
                
                count = session.query(Price).filter(
                    Price.instrument_id == instrument.id
                ).count()
                
                return count
                
        except Exception as e:
            self.logger.error(f"Failed to get price data count for {ticker}: {e}")
            return 0

    def get_price_data(
        self, 
        ticker: str, 
        start_date: Optional[date] = None, 
        end_date: Optional[date] = None
    ) -> pd.DataFrame:
        """
        Get price data for a ticker as DataFrame.
        
        Args:
            ticker: Ticker symbol
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            DataFrame with price data (date index, OHLC columns)
        """
        ticker = validate_ticker(ticker)
        
        try:
            with self.get_session() as session:
                instrument = session.query(Instrument).filter(
                    Instrument.ticker_symbol == ticker
                ).first()
                
                if not instrument:
                    return pd.DataFrame()
                
                query = session.query(Price).filter(
                    Price.instrument_id == instrument.id
                )
                
                if start_date:
                    query = query.filter(Price.date >= start_date)
                if end_date:
                    query = query.filter(Price.date <= end_date)
                
                query = query.order_by(Price.date)
                results = query.all()
                
                if not results:
                    return pd.DataFrame()
                
                # Convert to DataFrame
                data = []
                for price in results:
                    data.append({
                        'date': price.date,
                        'open': price.open,
                        'high': price.high,
                        'low': price.low,
                        'close': price.close,
                        'adjusted_close': price.adj_close,
                        'volume': price.volume
                    })
                
                df = pd.DataFrame(data)
                df.set_index('date', inplace=True)
                
                return df
                
        except Exception as e:
            self.logger.error(f"Failed to get price data for {ticker}: {e}")
            return pd.DataFrame()
    
    def store_thresholds(
        self,
        indicator_name: str,
        thresholds: List[Dict[str, Any]]
    ) -> int:
        """
        Store threshold definitions for an economic indicator.
        
        Args:
            indicator_name: Economic indicator name (standardized identifier)
            thresholds: List of threshold dictionaries
            
        Returns:
            Number of thresholds stored
        """
        try:
            with self.get_session() as session:
                indicator = session.query(EconomicIndicator).filter(
                    EconomicIndicator.name == indicator_name
                ).first()
                
                if not indicator:
                    raise DatabaseError(f"Economic indicator {indicator_name} not found")
                
                # Clear existing thresholds
                session.query(Threshold).filter(
                    Threshold.indicator_id == indicator.id
                ).delete()
                
                stored_count = 0
                for threshold_data in thresholds:
                    try:
                        category_str = threshold_data.get('category', '').lower()
                        category = ThresholdCategory(category_str)
                        
                        threshold = Threshold(
                            indicator_id=indicator.id,
                            category=category,
                            min_value=threshold_data.get('min_value'),
                            max_value=threshold_data.get('max_value')
                        )
                        session.add(threshold)
                        stored_count += 1
                        
                    except Exception as e:
                        self.logger.warning(f"Failed to store threshold {threshold_data}: {e}")
                        continue
                
                session.commit()
                self.logger.info(f"Stored {stored_count} thresholds for {indicator_name}")
                return stored_count
                
        except Exception as e:
            self.logger.error(f"Failed to store thresholds for {indicator_name}: {e}")
            raise DatabaseError(f"Threshold storage failed: {e}") from e

    # =============================================================================
    # DATA ALIGNMENT METHODS
    # =============================================================================

    def get_aligned_price_economic_data(
        self, 
        instrument_ticker: str, 
        economic_indicator_name: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        alignment_method: str = "last_of_period"
    ) -> List[Dict[str, Any]]:
        """
        Get aligned price and economic data for analysis.
        
        Args:
            instrument_ticker: Ticker symbol for price data
            economic_indicator_name: Name of economic indicator
            start_date: Start date for data (optional)
            end_date: End date for data (optional)
            alignment_method: Method for alignment (last_of_period, first_of_period, etc.)
            
        Returns:
            List of aligned data points
        """
        try:
            with self.get_session() as session:
                # Get instrument
                instrument = session.query(Instrument).filter(
                    Instrument.ticker_symbol == instrument_ticker
                ).first()
                
                if not instrument:
                    raise DatabaseError(f"Instrument {instrument_ticker} not found")
                
                # Get economic indicator
                indicator = session.query(EconomicIndicator).filter(
                    EconomicIndicator.name == economic_indicator_name
                ).first()
                
                if not indicator:
                    raise DatabaseError(f"Economic indicator {economic_indicator_name} not found")
                
                # Build date filters
                price_filters = [Price.instrument_id == instrument.id]
                economic_filters = [EconomicIndicatorData.indicator_id == indicator.id]
                
                if start_date:
                    price_filters.append(Price.date >= start_date)
                    economic_filters.append(EconomicIndicatorData.date >= start_date)
                
                if end_date:
                    price_filters.append(Price.date <= end_date)
                    economic_filters.append(EconomicIndicatorData.date <= end_date)
                
                # Get price data
                price_data = session.query(Price).filter(and_(*price_filters)).order_by(Price.date).all()
                
                # Get economic data
                economic_data = session.query(EconomicIndicatorData).filter(
                    and_(*economic_filters)
                ).order_by(EconomicIndicatorData.date).all()
                
                if not price_data:
                    self.logger.warning(f"No price data found for {instrument_ticker}")
                    return []
                
                if not economic_data:
                    self.logger.warning(f"No economic data found for {economic_indicator_name}")
                    return []
                
                # Convert to list format for alignment
                price_list = [
                    {
                        'date': p.date.strftime('%Y-%m-%d'),
                        'open': p.open,
                        'high': p.high,
                        'low': p.low,
                        'close': p.close,
                        'adj_close': p.adj_close,
                        'volume': p.volume
                    }
                    for p in price_data
                ]
                
                economic_list = [
                    {
                        'date': e.date.strftime('%Y-%m-%d'),
                        'value': e.value,
                        'indicator_name': economic_indicator_name,
                        'source': indicator.source,
                        'unit': indicator.unit
                    }
                    for e in economic_data
                ]
                
                # Use data alignment module
                from ..data.data_alignment import DataAligner, AlignmentMethod
                aligner = DataAligner()
                
                # Map string method to enum
                method_mapping = {
                    'last_of_period': AlignmentMethod.LAST_OF_PERIOD,
                    'first_of_period': AlignmentMethod.FIRST_OF_PERIOD,
                    'forward_fill': AlignmentMethod.FORWARD_FILL,
                    'nearest': AlignmentMethod.NEAREST
                }
                
                alignment_enum = method_mapping.get(alignment_method, AlignmentMethod.LAST_OF_PERIOD)
                
                aligned_data = aligner.align_daily_to_monthly(
                    daily_data=price_list,
                    monthly_data=economic_list,
                    alignment_method=alignment_enum
                )
                
                # Add metadata
                for point in aligned_data:
                    point['instrument_ticker'] = instrument_ticker
                    point['instrument_name'] = instrument.instrument_name
                    point['economic_indicator'] = economic_indicator_name
                    point['alignment_method'] = alignment_method
                
                self.logger.info(f"Retrieved {len(aligned_data)} aligned data points for {instrument_ticker} vs {economic_indicator_name}")
                return aligned_data
                
        except Exception as e:
            self.logger.error(f"Failed to get aligned data: {e}")
            raise DatabaseError(f"Data alignment failed: {e}") from e

    def get_multiple_aligned_data(
        self,
        instrument_tickers: List[str],
        economic_indicator_names: List[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        alignment_method: str = "last_of_period"
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get aligned data for multiple instruments and economic indicators.
        
        Args:
            instrument_tickers: List of ticker symbols
            economic_indicator_names: List of economic indicator names
            start_date: Start date for data (optional)
            end_date: End date for data (optional)
            alignment_method: Method for alignment
            
        Returns:
            Dictionary with aligned data for each instrument-indicator combination
        """
        results = {}
        
        for ticker in instrument_tickers:
            for indicator_name in economic_indicator_names:
                key = f"{ticker}_{indicator_name}"
                try:
                    aligned_data = self.get_aligned_price_economic_data(
                        instrument_ticker=ticker,
                        economic_indicator_name=indicator_name,
                        start_date=start_date,
                        end_date=end_date,
                        alignment_method=alignment_method
                    )
                    results[key] = aligned_data
                    
                except Exception as e:
                    self.logger.warning(f"Failed to align {ticker} with {indicator_name}: {e}")
                    results[key] = []
        
        return results

    def get_available_alignment_pairs(self) -> List[Dict[str, Any]]:
        """
        Get available instrument-economic indicator pairs for alignment.
        
        Returns:
            List of available combinations with metadata
        """
        try:
            with self.get_session() as session:
                # Get instruments with price data
                instruments_with_prices = session.query(Instrument).join(Price).distinct().all()
                
                # Get economic indicators with data
                indicators_with_data = session.query(EconomicIndicator).join(EconomicIndicatorData).distinct().all()
                
                pairs = []
                for instrument in instruments_with_prices:
                    for indicator in indicators_with_data:
                        pairs.append({
                            'instrument_ticker': instrument.ticker_symbol,
                            'instrument_name': instrument.instrument_name,
                            'instrument_type': instrument.instrument_type.value if instrument.instrument_type else 'unknown',
                            'economic_indicator': indicator.name,
                            'indicator_description': indicator.description,
                            'indicator_source': indicator.source,
                            'indicator_frequency': indicator.frequency.value if indicator.frequency else 'unknown',
                            'indicator_unit': indicator.unit
                        })
                
                self.logger.info(f"Found {len(pairs)} available alignment pairs")
                return pairs
                
        except Exception as e:
            self.logger.error(f"Failed to get alignment pairs: {e}")
            raise DatabaseError(f"Failed to get alignment pairs: {e}") from e

    def get_alignment_data_summary(self) -> Dict[str, Any]:
        """
        Get summary of data available for alignment.
        
        Returns:
            Summary statistics for alignment capabilities
        """
        try:
            with self.get_session() as session:
                # Count instruments with price data
                instruments_with_prices = session.query(Instrument.id).join(Price).distinct().count()
                
                # Count economic indicators with data
                indicators_with_data = session.query(EconomicIndicator.id).join(EconomicIndicatorData).distinct().count()
                
                # Get date ranges
                price_date_range = session.query(
                    Price.date.label('min_date'),
                    Price.date.label('max_date')
                ).order_by(Price.date.asc()).first(), session.query(
                    Price.date.label('min_date'),
                    Price.date.label('max_date')
                ).order_by(Price.date.desc()).first()
                
                economic_date_range = session.query(
                    EconomicIndicatorData.date.label('min_date'),
                    EconomicIndicatorData.date.label('max_date')
                ).order_by(EconomicIndicatorData.date.asc()).first(), session.query(
                    EconomicIndicatorData.date.label('min_date'),
                    EconomicIndicatorData.date.label('max_date')
                ).order_by(EconomicIndicatorData.date.desc()).first()
                
                # Calculate potential alignment pairs
                total_pairs = instruments_with_prices * indicators_with_data
                
                summary = {
                    'instruments_with_price_data': instruments_with_prices,
                    'economic_indicators_with_data': indicators_with_data,
                    'potential_alignment_pairs': total_pairs,
                    'price_data_date_range': {
                        'start': price_date_range[0].min_date.strftime('%Y-%m-%d') if price_date_range[0] else None,
                        'end': price_date_range[1].max_date.strftime('%Y-%m-%d') if price_date_range[1] else None
                    },
                    'economic_data_date_range': {
                        'start': economic_date_range[0].min_date.strftime('%Y-%m-%d') if economic_date_range[0] else None,
                        'end': economic_date_range[1].max_date.strftime('%Y-%m-%d') if economic_date_range[1] else None
                    }
                }
                
                return summary
                
        except Exception as e:
            self.logger.error(f"Failed to get alignment summary: {e}")
            raise DatabaseError(f"Failed to get alignment summary: {e}") from e

    # ============================================================================
    # Aligned Daily Data Methods
    # ============================================================================

    def store_aligned_daily_data(
        self, 
        aligned_records: List[Dict[str, Any]], 
        clear_existing: bool = False
    ) -> int:
        """
        Store aligned daily data records in the database.
        
        Args:
            aligned_records: List of aligned data records from ForwardFillTransformer
            clear_existing: Whether to clear existing data before inserting
            
        Returns:
            Number of records stored
        """
        if not aligned_records:
            self.logger.warning("No aligned records to store")
            return 0
        
        try:
            with self.get_session() as session:
                stored_count = 0
                
                # Clear existing data if requested
                if clear_existing:
                    # Get unique instrument IDs and date range from records
                    instrument_ids = list(set(record['instrument_id'] for record in aligned_records))
                    dates = [record['date'] for record in aligned_records]
                    min_date, max_date = min(dates), max(dates)
                    
                    deleted_count = session.query(AlignedDailyData).filter(
                        AlignedDailyData.instrument_id.in_(instrument_ids),
                        AlignedDailyData.date.between(min_date, max_date)
                    ).delete(synchronize_session=False)
                    
                    if deleted_count > 0:
                        self.logger.info(f"Cleared {deleted_count} existing aligned records")
                
                # Insert new records in batches
                batch_size = 1000
                for i in range(0, len(aligned_records), batch_size):
                    batch = aligned_records[i:i + batch_size]
                    
                    # Convert to AlignedDailyData objects
                    aligned_objects = []
                    for record in batch:
                        aligned_obj = AlignedDailyData(**record)
                        aligned_objects.append(aligned_obj)
                    
                    # Use bulk insert for performance
                    session.bulk_save_objects(aligned_objects)
                    stored_count += len(batch)
                    
                    if i % (batch_size * 5) == 0:  # Log every 5000 records
                        self.logger.debug(f"Stored {stored_count}/{len(aligned_records)} aligned records")
                
                session.commit()
                self.logger.info(f"Successfully stored {stored_count} aligned daily records")
                return stored_count
                
        except Exception as e:
            self.logger.error(f"Failed to store aligned daily data: {e}")
            raise DatabaseError(f"Failed to store aligned daily data: {e}") from e

    def get_aligned_daily_data(
        self,
        ticker: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        indicators: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Retrieve aligned daily data for analysis.
        
        Args:
            ticker: Ticker symbol
            start_date: Start date for data (optional)
            end_date: End date for data (optional)
            indicators: List of specific indicators to include (optional)
            
        Returns:
            DataFrame with aligned daily data
        """
        ticker = validate_ticker(ticker)
        
        try:
            with self.get_session() as session:
                # Get instrument
                instrument = session.query(Instrument).filter(
                    Instrument.ticker_symbol == ticker
                ).first()
                
                if not instrument:
                    raise DatabaseError(f"Instrument not found for ticker {ticker}")
                
                # Build query
                query = session.query(AlignedDailyData).filter(
                    AlignedDailyData.instrument_id == instrument.id
                )
                
                if start_date:
                    query = query.filter(AlignedDailyData.date >= start_date)
                if end_date:
                    query = query.filter(AlignedDailyData.date <= end_date)
                
                query = query.order_by(AlignedDailyData.date)
                
                # Execute query
                results = query.all()
                
                if not results:
                    self.logger.warning(f"No aligned data found for {ticker}")
                    return pd.DataFrame()
                
                # Convert to DataFrame
                data = []
                for result in results:
                    record = {
                        'date': result.date,
                        'open': result.open_price,
                        'high': result.high_price,
                        'low': result.low_price,
                        'close': result.close_price,
                        'adjusted_close': result.adjusted_close,
                        'volume': result.volume,
                        'trading_calendar': result.trading_calendar
                    }
                    
                    # Add economic indicators
                    economic_fields = {
                        'inflation_monthly_us': result.inflation_monthly_us,
                        'inflation_index_monthly_us': result.inflation_index_monthly_us,
                        'unemployment_monthly_rate_us': result.unemployment_monthly_rate_us,
                        'interest_rate_monthly_us': result.interest_rate_monthly_us,
                        'inflation_monthly_euro': result.inflation_monthly_euro,
                        'unemployment_rate_monthly_euro': result.unemployment_rate_monthly_euro,
                        'interest_rate_change_day_euro': result.interest_rate_change_day_euro,
                        'interest_rate_monthly_euro': result.interest_rate_monthly_euro
                    }
                    
                    # Only include indicators if specified or include all
                    if indicators:
                        for indicator in indicators:
                            if indicator in economic_fields:
                                record[indicator] = economic_fields[indicator]
                    else:
                        record.update(economic_fields)
                    
                    data.append(record)
                
                df = pd.DataFrame(data)
                df.set_index('date', inplace=True)
                
                self.logger.info(f"Retrieved {len(df)} aligned records for {ticker}")
                return df
                
        except Exception as e:
            self.logger.error(f"Failed to get aligned data for {ticker}: {e}")
            raise DatabaseError(f"Failed to get aligned data for {ticker}: {e}") from e

    def get_aligned_data_coverage(self, ticker: Optional[str] = None) -> Dict[str, Any]:
        """
        Get coverage statistics for aligned daily data.
        
        Args:
            ticker: Optional ticker to filter by
            
        Returns:
            Coverage statistics
        """
        try:
            with self.get_session() as session:
                query = session.query(AlignedDailyData)
                
                if ticker:
                    ticker = validate_ticker(ticker)
                    instrument = session.query(Instrument).filter(
                        Instrument.ticker_symbol == ticker
                    ).first()
                    if not instrument:
                        raise DatabaseError(f"Instrument not found for ticker {ticker}")
                    query = query.filter(AlignedDailyData.instrument_id == instrument.id)
                
                # Get basic counts
                total_records = query.count()
                if total_records == 0:
                    return {'total_records': 0}
                
                # Get date range
                min_date = query.order_by(AlignedDailyData.date).first().date
                max_date = query.order_by(AlignedDailyData.date.desc()).first().date
                
                # Get coverage by field
                coverage = {
                    'total_records': total_records,
                    'date_range': {'start': min_date, 'end': max_date},
                    'field_coverage': {}
                }
                
                # Check coverage for each field
                price_fields = ['open_price', 'close_price', 'volume']
                economic_fields = [
                    'inflation_monthly_us', 'unemployment_monthly_rate_us',
                    'interest_rate_monthly_us', 'inflation_monthly_euro',
                    'unemployment_rate_monthly_euro', 'interest_rate_change_day_euro'
                ]
                
                for field in price_fields + economic_fields:
                    non_null_count = query.filter(
                        getattr(AlignedDailyData, field).isnot(None)
                    ).count()
                    coverage['field_coverage'][field] = {
                        'records_with_data': non_null_count,
                        'coverage_percentage': (non_null_count / total_records) * 100
                    }
                
                return coverage
                
        except Exception as e:
            self.logger.error(f"Failed to get aligned data coverage: {e}")
            raise DatabaseError(f"Failed to get aligned data coverage: {e}") from e

    def clear_aligned_daily_data(
        self, 
        ticker: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> int:
        """
        Clear aligned daily data from database.
        
        Args:
            ticker: Optional ticker to filter by
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            Number of records deleted
        """
        try:
            with self.get_session() as session:
                query = session.query(AlignedDailyData)
                
                if ticker:
                    ticker = validate_ticker(ticker)
                    instrument = session.query(Instrument).filter(
                        Instrument.ticker_symbol == ticker
                    ).first()
                    if not instrument:
                        raise DatabaseError(f"Instrument not found for ticker {ticker}")
                    query = query.filter(AlignedDailyData.instrument_id == instrument.id)
                
                if start_date:
                    query = query.filter(AlignedDailyData.date >= start_date)
                if end_date:
                    query = query.filter(AlignedDailyData.date <= end_date)
                
                deleted_count = query.delete(synchronize_session=False)
                session.commit()
                
                self.logger.info(f"Cleared {deleted_count} aligned daily records")
                return deleted_count
                
        except Exception as e:
            self.logger.error(f"Failed to clear aligned daily data: {e}")
            raise DatabaseError(f"Failed to clear aligned daily data: {e}") from e