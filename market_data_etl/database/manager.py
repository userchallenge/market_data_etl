"""
Unified database manager for market data and financial analysis.

This module provides database operations for storing and retrieving:
- Company information with currency support
- Historical price data (OHLC + Volume)  
- Structured financial statements (Income, Balance Sheet, Cash Flow)
- Calculated financial ratios and metrics

Designed for comprehensive financial analysis of global companies.
"""

from typing import Dict, Any, List, Tuple, Optional
from datetime import date, datetime
from sqlalchemy import create_engine, and_, desc
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import IntegrityError
import pandas as pd

from ..config import config
from ..utils.logging import get_logger
from ..utils.exceptions import DatabaseError
from ..data.models import Base, Company, Price, IncomeStatement, BalanceSheet, CashFlow, FinancialRatio


class DatabaseManager:
    """
    Unified database manager for market data and financial analysis.
    
    Handles storage and retrieval of company information, price data,
    and structured financial statements with proper relationships and currency handling.
    """
    
    def __init__(self, db_path: Optional[str] = None):
        self.logger = get_logger(__name__)
        
        # Use provided path or default from config
        self.db_path = db_path or config.database.path
        self.engine = create_engine(
            f"sqlite:///{self.db_path}",
            echo=config.database.echo
        )
        
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
    # COMPANY MANAGEMENT
    # =============================================================================
    
    def get_or_create_company(
        self,
        ticker: str,
        currency: str = 'USD',
        company_info: Optional[Dict[str, Any]] = None
    ) -> Company:
        """
        Get existing company record or create new one.
        
        Args:
            ticker: Stock ticker symbol
            currency: Currency code
            company_info: Optional company information dictionary
            
        Returns:
            Company database record
        """
        with self.get_session() as session:
            # Try to find existing company
            company = session.query(Company).filter(
                Company.ticker_symbol == ticker
            ).first()
            
            if company:
                # Update company information if provided
                if company_info:
                    company.company_name = company_info.get('company_name') or company.company_name
                    company.sector = company_info.get('sector') or company.sector
                    company.industry = company_info.get('industry') or company.industry
                    company.country = company_info.get('country') or company.country
                    company.currency = currency
                    company.market_cap = company_info.get('market_cap') or company.market_cap
                    company.employees = company_info.get('employees') or company.employees
                    company.updated_at = datetime.utcnow()
                    session.commit()
                
                self.logger.debug(f"Updated existing company record for {ticker}")
            else:
                # Create new company record
                info = company_info or {}
                company = Company(
                    ticker_symbol=ticker,
                    company_name=info.get('company_name', ''),
                    sector=info.get('sector', ''),
                    industry=info.get('industry', ''),
                    country=info.get('country', ''),
                    currency=currency,
                    market_cap=info.get('market_cap'),
                    employees=info.get('employees'),
                    founded_year=info.get('founded_year')
                )
                session.add(company)
                session.commit()
                session.refresh(company)
                
                self.logger.debug(f"Created new company record for {ticker}")
            
            return company
    
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
            company = session.query(Company).filter(
                Company.ticker_symbol == ticker
            ).first()
            
            if not company:
                return []
            
            dates = session.query(Price.date).filter(
                Price.company_id == company.id
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
            company = session.query(Company).filter(
                Company.ticker_symbol == ticker
            ).first()
            
            if not company:
                return None
            
            from sqlalchemy import func
            result = session.query(
                func.min(Price.date), 
                func.max(Price.date)
            ).filter(Price.company_id == company.id).first()
            
            if result[0] is None:
                return None
            
            return result
    
    def store_price_data(self, ticker: str, price_data: pd.DataFrame) -> int:
        """
        Store price data in the database.
        
        Args:
            ticker: Stock ticker symbol
            price_data: DataFrame with price data
            
        Returns:
            Number of records inserted
        """
        if price_data.empty:
            return 0
        
        try:
            company = self.get_or_create_company(ticker)
            
            with self.get_session() as session:
                # Refresh company in this session
                company = session.merge(company)
                
                inserted_count = 0
                
                for _, row in price_data.iterrows():
                    try:
                        # Check if record already exists
                        existing = session.query(Price).filter(
                            and_(
                                Price.company_id == company.id,
                                Price.date == row['date']
                            )
                        ).first()
                        
                        if existing:
                            continue
                        
                        price_record = Price(
                            company_id=company.id,
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
            self.logger.error(f"Failed to store price data for {ticker}: {e}")
            raise DatabaseError(f"Price data storage failed: {e}")
    
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
                # Get or create company record
                company = self._get_or_create_company_in_session(
                    session, 
                    ticker, 
                    financial_data.get('currency', 'USD'),
                    financial_data.get('company_info', {})
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
                        company.id, 
                        statements['income_stmt']
                    )
                    storage_counts['income_statements'] = count
                
                # Store balance sheets
                if 'balance_sheet' in statements:
                    count = self._store_balance_sheets(
                        session,
                        company.id,
                        statements['balance_sheet']
                    )
                    storage_counts['balance_sheets'] = count
                
                # Store cash flow statements
                if 'cash_flow' in statements:
                    count = self._store_cash_flows(
                        session,
                        company.id,
                        statements['cash_flow']
                    )
                    storage_counts['cash_flows'] = count
                
                # Store derived financial ratios
                derived_metrics = financial_data.get('derived_metrics', {})
                if derived_metrics:
                    count = self._store_financial_ratios(
                        session,
                        company.id,
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
            self.logger.error(f"Failed to store financial data for {ticker}: {e}")
            raise DatabaseError(f"Database storage failed: {e}")
    
    def _get_or_create_company_in_session(
        self,
        session: Session,
        ticker: str,
        currency: str,
        company_info: Dict[str, Any]
    ) -> Company:
        """Get existing company record or create new one within a session."""
        # Try to find existing company
        company = session.query(Company).filter(
            Company.ticker_symbol == ticker
        ).first()
        
        if company:
            # Update company information
            company.company_name = company_info.get('company_name') or company.company_name
            company.sector = company_info.get('sector') or company.sector
            company.industry = company_info.get('industry') or company.industry
            company.country = company_info.get('country') or company.country
            company.currency = currency
            company.market_cap = company_info.get('market_cap') or company.market_cap
            company.employees = company_info.get('employees') or company.employees
            company.updated_at = datetime.utcnow()
            
            self.logger.debug(f"Updated existing company record for {ticker}")
        else:
            # Create new company record
            company = Company(
                ticker_symbol=ticker,
                company_name=company_info.get('company_name', ''),
                sector=company_info.get('sector', ''),
                industry=company_info.get('industry', ''),
                country=company_info.get('country', ''),
                currency=currency,
                market_cap=company_info.get('market_cap'),
                employees=company_info.get('employees'),
                founded_year=company_info.get('founded_year')
            )
            session.add(company)
            session.flush()  # Get the ID
            
            self.logger.debug(f"Created new company record for {ticker}")
        
        return company
    
    def _store_income_statements(
        self,
        session: Session,
        company_id: int,
        income_data: Dict[str, Any]
    ) -> int:
        """Store income statement records."""
        count = 0
        
        # Store annual data
        annual_data = income_data.get('annual', {})
        for period_date, period_info in annual_data.items():
            count += self._store_single_income_statement(
                session, company_id, period_date, period_info, 'annual'
            )
        
        # Store quarterly data
        quarterly_data = income_data.get('quarterly', {})
        for period_date, period_info in quarterly_data.items():
            count += self._store_single_income_statement(
                session, company_id, period_date, period_info, 'quarterly'
            )
        
        return count
    
    def _store_single_income_statement(
        self,
        session: Session,
        company_id: int,
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
                    IncomeStatement.company_id == company_id,
                    IncomeStatement.period_end_date == period_end_date,
                    IncomeStatement.period_type == period_type
                )
            ).first()
            
            if existing:
                income_stmt = existing
            else:
                income_stmt = IncomeStatement(
                    company_id=company_id,
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
        company_id: int,
        balance_data: Dict[str, Any]
    ) -> int:
        """Store balance sheet records."""
        count = 0
        
        # Store annual data
        annual_data = balance_data.get('annual', {})
        for period_date, period_info in annual_data.items():
            count += self._store_single_balance_sheet(
                session, company_id, period_date, period_info, 'annual'
            )
        
        # Store quarterly data
        quarterly_data = balance_data.get('quarterly', {})
        for period_date, period_info in quarterly_data.items():
            count += self._store_single_balance_sheet(
                session, company_id, period_date, period_info, 'quarterly'
            )
        
        return count
    
    def _store_single_balance_sheet(
        self,
        session: Session,
        company_id: int,
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
                    BalanceSheet.company_id == company_id,
                    BalanceSheet.period_end_date == period_end_date,
                    BalanceSheet.period_type == period_type
                )
            ).first()
            
            if existing:
                balance_sheet = existing
            else:
                balance_sheet = BalanceSheet(
                    company_id=company_id,
                    period_end_date=period_end_date,
                    period_type=period_type
                )
                session.add(balance_sheet)
            
            # Set standard fields
            balance_sheet.fiscal_year = period_info.get('fiscal_year', period_end_date.year)
            balance_sheet.fiscal_quarter = period_info.get('fiscal_quarter')
            balance_sheet.currency = period_info.get('currency', 'USD')
            
            # Set balance sheet metrics (abbreviated for space)
            balance_sheet.cash_and_equivalents = metrics.get('cash_and_equivalents')
            balance_sheet.accounts_receivable = metrics.get('accounts_receivable')
            balance_sheet.inventory = metrics.get('inventory')
            balance_sheet.total_current_assets = metrics.get('total_current_assets')
            balance_sheet.total_assets = metrics.get('total_assets')
            balance_sheet.accounts_payable = metrics.get('accounts_payable')
            balance_sheet.total_current_liabilities = metrics.get('total_current_liabilities')
            balance_sheet.long_term_debt = metrics.get('long_term_debt')
            balance_sheet.total_liabilities = metrics.get('total_liabilities')
            balance_sheet.total_shareholders_equity = metrics.get('total_shareholders_equity')
            
            # Set all other balance sheet fields
            balance_sheet.short_term_investments = metrics.get('short_term_investments')
            balance_sheet.prepaid_expenses = metrics.get('prepaid_expenses')
            balance_sheet.other_current_assets = metrics.get('other_current_assets')
            balance_sheet.property_plant_equipment = metrics.get('property_plant_equipment')
            balance_sheet.goodwill = metrics.get('goodwill')
            balance_sheet.intangible_assets = metrics.get('intangible_assets')
            balance_sheet.long_term_investments = metrics.get('long_term_investments')
            balance_sheet.other_non_current_assets = metrics.get('other_non_current_assets')
            balance_sheet.total_non_current_assets = metrics.get('total_non_current_assets')
            balance_sheet.short_term_debt = metrics.get('short_term_debt')
            balance_sheet.accrued_expenses = metrics.get('accrued_expenses')
            balance_sheet.deferred_revenue_current = metrics.get('deferred_revenue_current')
            balance_sheet.other_current_liabilities = metrics.get('other_current_liabilities')
            balance_sheet.deferred_revenue_non_current = metrics.get('deferred_revenue_non_current')
            balance_sheet.deferred_tax_liabilities = metrics.get('deferred_tax_liabilities')
            balance_sheet.other_non_current_liabilities = metrics.get('other_non_current_liabilities')
            balance_sheet.total_non_current_liabilities = metrics.get('total_non_current_liabilities')
            balance_sheet.common_stock = metrics.get('common_stock')
            balance_sheet.retained_earnings = metrics.get('retained_earnings')
            balance_sheet.accumulated_other_income = metrics.get('accumulated_other_income')
            balance_sheet.treasury_stock = metrics.get('treasury_stock')
            
            # Calculate derived fields
            if balance_sheet.short_term_debt and balance_sheet.long_term_debt:
                balance_sheet.total_debt = balance_sheet.short_term_debt + balance_sheet.long_term_debt
            elif balance_sheet.short_term_debt:
                balance_sheet.total_debt = balance_sheet.short_term_debt
            elif balance_sheet.long_term_debt:
                balance_sheet.total_debt = balance_sheet.long_term_debt
            
            if balance_sheet.total_debt and balance_sheet.cash_and_equivalents:
                balance_sheet.net_debt = balance_sheet.total_debt - balance_sheet.cash_and_equivalents
            
            if balance_sheet.total_current_assets and balance_sheet.total_current_liabilities:
                balance_sheet.working_capital = balance_sheet.total_current_assets - balance_sheet.total_current_liabilities
            
            return 1
            
        except Exception as e:
            self.logger.warning(f"Failed to store balance sheet for {period_date}: {e}")
            return 0
    
    def _store_cash_flows(
        self,
        session: Session,
        company_id: int,
        cashflow_data: Dict[str, Any]
    ) -> int:
        """Store cash flow statement records."""
        count = 0
        
        # Store annual data
        annual_data = cashflow_data.get('annual', {})
        for period_date, period_info in annual_data.items():
            count += self._store_single_cash_flow(
                session, company_id, period_date, period_info, 'annual'
            )
        
        # Store quarterly data
        quarterly_data = cashflow_data.get('quarterly', {})
        for period_date, period_info in quarterly_data.items():
            count += self._store_single_cash_flow(
                session, company_id, period_date, period_info, 'quarterly'
            )
        
        return count
    
    def _store_single_cash_flow(
        self,
        session: Session,
        company_id: int,
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
                    CashFlow.company_id == company_id,
                    CashFlow.period_end_date == period_end_date,
                    CashFlow.period_type == period_type
                )
            ).first()
            
            if existing:
                cash_flow = existing
            else:
                cash_flow = CashFlow(
                    company_id=company_id,
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
        company_id: int,
        derived_metrics: Dict[str, Any]
    ) -> int:
        """Store calculated financial ratios."""
        count = 0
        
        # Store annual ratios
        annual_data = derived_metrics.get('annual', {})
        for period_date, metrics in annual_data.items():
            count += self._store_single_financial_ratio(
                session, company_id, period_date, metrics, 'annual'
            )
        
        # Store quarterly ratios
        quarterly_data = derived_metrics.get('quarterly', {})
        for period_date, metrics in quarterly_data.items():
            count += self._store_single_financial_ratio(
                session, company_id, period_date, metrics, 'quarterly'
            )
        
        return count
    
    def _store_single_financial_ratio(
        self,
        session: Session,
        company_id: int,
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
                    FinancialRatio.company_id == company_id,
                    FinancialRatio.period_end_date == period_end_date,
                    FinancialRatio.period_type == period_type
                )
            ).first()
            
            if existing:
                ratio = existing
            else:
                ratio = FinancialRatio(
                    company_id=company_id,
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
    
    def get_company_financial_summary(
        self, 
        ticker: str,
        years: int = 5
    ) -> Dict[str, Any]:
        """
        Get comprehensive financial summary for a company.
        
        Args:
            ticker: Stock ticker symbol
            years: Number of recent years to include
            
        Returns:
            Dictionary with financial summary data
        """
        try:
            with self.get_session() as session:
                company = session.query(Company).filter(
                    Company.ticker_symbol == ticker
                ).first()
                
                if not company:
                    return {}
                
                cutoff_date = date(date.today().year - years + 1, 1, 1)
                
                # Get recent data counts
                income_count = session.query(IncomeStatement).filter(
                    and_(
                        IncomeStatement.company_id == company.id,
                        IncomeStatement.period_end_date >= cutoff_date
                    )
                ).count()
                
                balance_count = session.query(BalanceSheet).filter(
                    and_(
                        BalanceSheet.company_id == company.id,
                        BalanceSheet.period_end_date >= cutoff_date
                    )
                ).count()
                
                cashflow_count = session.query(CashFlow).filter(
                    and_(
                        CashFlow.company_id == company.id,
                        CashFlow.period_end_date >= cutoff_date
                    )
                ).count()
                
                ratios_count = session.query(FinancialRatio).filter(
                    and_(
                        FinancialRatio.company_id == company.id,
                        FinancialRatio.period_end_date >= cutoff_date
                    )
                ).count()
                
                # Get latest data
                latest_income = session.query(IncomeStatement).filter(
                    IncomeStatement.company_id == company.id
                ).order_by(desc(IncomeStatement.period_end_date)).first()
                
                latest_balance = session.query(BalanceSheet).filter(
                    BalanceSheet.company_id == company.id
                ).order_by(desc(BalanceSheet.period_end_date)).first()
                
                latest_cashflow = session.query(CashFlow).filter(
                    CashFlow.company_id == company.id
                ).order_by(desc(CashFlow.period_end_date)).first()
                
                latest_ratios = session.query(FinancialRatio).filter(
                    FinancialRatio.company_id == company.id
                ).order_by(desc(FinancialRatio.period_end_date)).first()
                
                return {
                    'company': {
                        'ticker': company.ticker_symbol,
                        'name': company.company_name,
                        'sector': company.sector,
                        'industry': company.industry,
                        'country': company.country,
                        'currency': company.currency,
                        'market_cap': company.market_cap,
                        'employees': company.employees
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
    
    def get_ticker_info(self, ticker: str) -> Dict[str, Any]:
        """
        Get information about stored data for a company.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Dictionary with company information
        """
        with self.get_session() as session:
            company = session.query(Company).filter(
                Company.ticker_symbol == ticker
            ).first()
            
            if not company:
                return {
                    'ticker': ticker,
                    'exists': False,
                    'price_data': {},
                    'financial_statements': {}
                }
            
            # Get price data info
            price_range = self.get_price_date_range(ticker)
            price_count = session.query(Price).filter(Price.company_id == company.id).count()
            
            # Get financial statement counts
            income_count = session.query(IncomeStatement).filter(IncomeStatement.company_id == company.id).count()
            balance_count = session.query(BalanceSheet).filter(BalanceSheet.company_id == company.id).count()
            cashflow_count = session.query(CashFlow).filter(CashFlow.company_id == company.id).count()
            ratios_count = session.query(FinancialRatio).filter(FinancialRatio.company_id == company.id).count()
            
            return {
                'ticker': ticker,
                'exists': True,
                'company': {
                    'name': company.company_name,
                    'sector': company.sector,
                    'industry': company.industry,
                    'country': company.country,
                    'currency': company.currency,
                    'market_cap': company.market_cap,
                    'employees': company.employees,
                    'created_at': company.created_at
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