"""
Financial data standardization system for consistent financial analysis.

This module provides standardization and mapping capabilities for financial
statement data to ensure consistent metric names and values across different
companies and data sources.
"""

from typing import Dict, Any, Optional, List, Union
import re
from decimal import Decimal
import pandas as pd
from datetime import date as date_type, datetime

from ..utils.logging import get_logger


class FinancialStandardizer:
    """
    Standardizes financial statement data for consistent analysis.
    
    Handles variations in financial metric names, units, and formats
    to create clean, comparable financial data.
    """
    
    # Income Statement standardization mapping
    INCOME_STATEMENT_MAPPING = {
        # Revenue variations
        'total_revenue': [
            'Total Revenue', 'Revenue', 'Net Sales', 'Net Revenue', 
            'Sales', 'Total Net Sales', 'Operating Revenue', 'Revenues',
            'Total Sales', 'Net Revenues'
        ],
        'cost_of_revenue': [
            'Cost Of Revenue', 'Cost of Goods Sold', 'COGS', 'Cost of Sales',
            'Cost Of Goods Sold', 'Direct Costs', 'Cost of Revenue'
        ],
        'gross_profit': [
            'Gross Profit', 'Gross Income', 'Gross Margin'
        ],
        
        # Operating expenses
        'research_development': [
            'Research And Development', 'R&D', 'Research Development',
            'Research & Development', 'Research and Development Expenses'
        ],
        'sales_marketing': [
            'Sales General Administrative', 'SG&A', 'Sales & Marketing',
            'Sales and Marketing', 'Selling and Marketing'
        ],
        'general_administrative': [
            'General Administrative', 'Administrative Expenses',
            'General & Administrative'
        ],
        'total_operating_expenses': [
            'Total Operating Expenses', 'Operating Expenses', 'Total OpEx'
        ],
        
        # Operating results
        'operating_income': [
            'Operating Income', 'Operating Profit', 'EBIT',
            'Earnings Before Interest and Taxes', 'Operating Earnings'
        ],
        'interest_expense': [
            'Interest Expense', 'Interest Expenses', 'Net Interest Expense'
        ],
        'interest_income': [
            'Interest Income', 'Interest Revenue', 'Investment Income'
        ],
        'other_income_expense': [
            'Other Income Expense Net', 'Other Income', 'Non Operating Income',
            'Other Operating Income'
        ],
        
        # Pre-tax and tax
        'income_before_tax': [
            'Pretax Income', 'Income Before Tax', 'EBT',
            'Earnings Before Tax', 'Pre-Tax Income'
        ],
        'tax_provision': [
            'Tax Provision', 'Income Tax Expense', 'Provision For Income Taxes',
            'Tax Expense'
        ],
        
        # Net income
        'net_income': [
            'Net Income', 'Net Earnings', 'Profit', 'Net Profit',
            'Net Income Attributable To Shareholders', 'Bottom Line'
        ],
        'net_income_common': [
            'Net Income Common Stockholders', 'Net Income Available to Common',
            'Net Income Attributable to Common Shareholders'
        ],
        
        # Per share data
        'basic_eps': [
            'Basic EPS', 'Basic Earnings Per Share', 'EPS Basic'
        ],
        'diluted_eps': [
            'Diluted EPS', 'Diluted Earnings Per Share', 'EPS Diluted'
        ],
        'weighted_average_shares': [
            'Basic Average Shares', 'Weighted Average Shares Outstanding',
            'Average Shares Outstanding Basic'
        ],
        'weighted_average_shares_diluted': [
            'Diluted Average Shares', 'Weighted Average Shares Outstanding Diluted'
        ],
        
        # Additional metrics
        'ebitda': [
            'EBITDA', 'Earnings Before Interest Taxes Depreciation Amortization'
        ],
        'depreciation_amortization': [
            'Depreciation And Amortization', 'Depreciation Amortization',
            'Depreciation & Amortization'
        ]
    }
    
    # Balance Sheet standardization mapping
    BALANCE_SHEET_MAPPING = {
        # Current assets
        'cash_and_equivalents': [
            'Cash And Cash Equivalents', 'Cash', 'Cash and Equivalents',
            'Cash & Cash Equivalents', 'Cash and Short Term Investments'
        ],
        'short_term_investments': [
            'Short Term Investments', 'Marketable Securities',
            'Short-term Investments'
        ],
        'accounts_receivable': [
            'Accounts Receivable', 'Receivables', 'Trade Receivables',
            'Accounts Receivable Net'
        ],
        'inventory': [
            'Inventory', 'Inventories', 'Total Inventory'
        ],
        'prepaid_expenses': [
            'Prepaid Expenses', 'Prepaid Assets', 'Prepaid'
        ],
        'other_current_assets': [
            'Other Current Assets', 'Other Short Term Assets'
        ],
        'total_current_assets': [
            'Total Current Assets', 'Current Assets'
        ],
        
        # Non-current assets
        'property_plant_equipment': [
            'Property Plant Equipment Net', 'Net PPE', 'PP&E',
            'Property Plant And Equipment', 'Fixed Assets'
        ],
        'goodwill': [
            'Goodwill', 'Goodwill And Other Intangible Assets'
        ],
        'intangible_assets': [
            'Other Intangible Assets', 'Intangible Assets', 'Intangibles'
        ],
        'long_term_investments': [
            'Long Term Investments', 'Investments', 'Long-term Investments'
        ],
        'other_non_current_assets': [
            'Other Non Current Assets', 'Other Long Term Assets'
        ],
        'total_non_current_assets': [
            'Total Non Current Assets', 'Non Current Assets'
        ],
        'total_assets': [
            'Total Assets', 'Total Asset'
        ],
        
        # Current liabilities
        'accounts_payable': [
            'Accounts Payable', 'Trade Payables', 'Payables'
        ],
        'short_term_debt': [
            'Current Debt', 'Short Term Debt', 'Current Portion Long Term Debt',
            'Short-term Debt'
        ],
        'accrued_expenses': [
            'Accrued Expenses', 'Accrued Liabilities', 'Accruals'
        ],
        'deferred_revenue_current': [
            'Deferred Revenue', 'Unearned Revenue', 'Deferred Income'
        ],
        'other_current_liabilities': [
            'Other Current Liabilities', 'Other Short Term Liabilities'
        ],
        'total_current_liabilities': [
            'Total Current Liabilities', 'Current Liabilities'
        ],
        
        # Non-current liabilities
        'long_term_debt': [
            'Long Term Debt', 'Total Debt', 'Long-term Debt'
        ],
        'deferred_revenue_non_current': [
            'Deferred Revenue Non Current', 'Long Term Deferred Revenue'
        ],
        'deferred_tax_liabilities': [
            'Deferred Tax Liabilities', 'Deferred Income Tax'
        ],
        'other_non_current_liabilities': [
            'Other Non Current Liabilities', 'Other Long Term Liabilities'
        ],
        'total_non_current_liabilities': [
            'Total Non Current Liabilities', 'Non Current Liabilities'
        ],
        'total_liabilities': [
            'Total Liabilities', 'Total Liab'
        ],
        
        # Shareholders' equity
        'common_stock': [
            'Common Stock', 'Ordinary Shares', 'Share Capital'
        ],
        'retained_earnings': [
            'Retained Earnings', 'Accumulated Earnings'
        ],
        'accumulated_other_income': [
            'Accumulated Other Comprehensive Income', 'Other Comprehensive Income',
            'AOCI'
        ],
        'treasury_stock': [
            'Treasury Stock', 'Treasury Shares'
        ],
        'total_shareholders_equity': [
            'Total Stockholder Equity', 'Shareholders Equity', 'Total Equity',
            'Stockholders Equity'
        ]
    }
    
    # Cash Flow Statement standardization mapping
    CASH_FLOW_MAPPING = {
        # Operating activities
        'net_income': [
            'Net Income', 'Net Earnings', 'Net Income Starting Line'
        ],
        'depreciation_amortization': [
            'Depreciation And Amortization', 'Depreciation Amortization'
        ],
        'stock_compensation': [
            'Stock Based Compensation', 'Share Based Compensation',
            'Employee Stock Options'
        ],
        'deferred_tax': [
            'Deferred Income Tax', 'Deferred Tax'
        ],
        'change_accounts_receivable': [
            'Changes In Accounts Receivable', 'Change In Accounts Receivable'
        ],
        'change_inventory': [
            'Changes In Inventory', 'Change In Inventory'
        ],
        'change_accounts_payable': [
            'Changes In Accounts Payable', 'Change In Accounts Payable'
        ],
        'change_other_working_capital': [
            'Changes In Other Working Capital', 'Change In Working Capital'
        ],
        'operating_cash_flow': [
            'Operating Cash Flow', 'Cash From Operating Activities',
            'Net Cash From Operating Activities'
        ],
        
        # Investing activities
        'capital_expenditures': [
            'Capital Expenditures', 'CapEx', 'Capital Expenditure'
        ],
        'acquisitions': [
            'Acquisitions Net', 'Business Acquisitions'
        ],
        'purchases_investments': [
            'Purchase Of Investments', 'Investments'
        ],
        'sales_maturities_investments': [
            'Sale Of Investments', 'Maturities Of Investments'
        ],
        'other_investing_activities': [
            'Other Investing Activites', 'Other Investing Activities'
        ],
        'investing_cash_flow': [
            'Investing Cash Flow', 'Cash From Investing Activities',
            'Net Cash From Investing Activities'
        ],
        
        # Financing activities
        'debt_issuance': [
            'Long Term Debt Issuance', 'Issuance Of Debt'
        ],
        'debt_repayment': [
            'Long Term Debt Payments', 'Repayment Of Debt'
        ],
        'common_stock_issuance': [
            'Common Stock Issuance', 'Issuance Of Stock'
        ],
        'common_stock_repurchase': [
            'Common Stock Repurchase', 'Repurchase Of Stock', 'Share Buybacks'
        ],
        'dividends_paid': [
            'Cash Dividends Paid', 'Dividends Paid'
        ],
        'other_financing_activities': [
            'Other Financing Activites', 'Other Financing Activities'
        ],
        'financing_cash_flow': [
            'Financing Cash Flow', 'Cash From Financing Activities',
            'Net Cash From Financing Activities'
        ],
        
        # Net change and ending cash
        'net_change_cash': [
            'Net Change In Cash', 'Change In Cash And Cash Equivalents'
        ],
        'cash_beginning': [
            'Beginning Cash Position', 'Cash At Beginning Of Period'
        ],
        'cash_ending': [
            'End Cash Position', 'Cash At End Of Period'
        ],
        'free_cash_flow': [
            'Free Cash Flow', 'FCF'
        ]
    }
    
    def __init__(self):
        self.logger = get_logger(__name__)
    
    def standardize_income_statement(
        self, 
        raw_data: Dict[str, Any], 
        currency: str = 'USD'
    ) -> Dict[str, Any]:
        """
        Standardize income statement data to consistent format.
        
        Args:
            raw_data: Raw income statement data from yfinance
            currency: Currency code for the financial data
            
        Returns:
            Standardized income statement dictionary
        """
        return self._standardize_financial_data(
            raw_data, 
            self.INCOME_STATEMENT_MAPPING,
            'income_statement',
            currency
        )
    
    def standardize_balance_sheet(
        self, 
        raw_data: Dict[str, Any], 
        currency: str = 'USD'
    ) -> Dict[str, Any]:
        """
        Standardize balance sheet data to consistent format.
        
        Args:
            raw_data: Raw balance sheet data from yfinance
            currency: Currency code for the financial data
            
        Returns:
            Standardized balance sheet dictionary
        """
        return self._standardize_financial_data(
            raw_data, 
            self.BALANCE_SHEET_MAPPING,
            'balance_sheet',
            currency
        )
    
    def standardize_cash_flow(
        self, 
        raw_data: Dict[str, Any], 
        currency: str = 'USD'
    ) -> Dict[str, Any]:
        """
        Standardize cash flow statement data to consistent format.
        
        Args:
            raw_data: Raw cash flow data from yfinance
            currency: Currency code for the financial data
            
        Returns:
            Standardized cash flow dictionary
        """
        return self._standardize_financial_data(
            raw_data, 
            self.CASH_FLOW_MAPPING,
            'cash_flow',
            currency
        )
    
    def _standardize_financial_data(
        self, 
        raw_data: Dict[str, Any],
        mapping: Dict[str, List[str]],
        statement_type: str,
        currency: str
    ) -> Dict[str, Any]:
        """
        Core standardization logic for financial statement data.
        
        Args:
            raw_data: Raw financial data
            mapping: Standardization mapping dictionary
            statement_type: Type of financial statement
            currency: Currency code
            
        Returns:
            Standardized financial data organized by period
        """
        if not raw_data:
            return {}
        
        self.logger.debug(f"Standardizing {statement_type} data with {len(raw_data)} periods")
        
        standardized_periods = {}
        
        # Process each time period in the raw data
        for period_key, period_data in raw_data.items():
            if not isinstance(period_data, dict):
                continue
                
            # Convert period key to standard date format
            standardized_date = self._standardize_date(period_key)
            if not standardized_date:
                continue
            
            standardized_metrics = {}
            
            # Map each metric to standardized name
            for std_name, variations in mapping.items():
                value = self._find_metric_value(period_data, variations)
                if value is not None:
                    # Standardize the value (handle units, formats, etc.)
                    standardized_value = self._standardize_value(value)
                    standardized_metrics[std_name] = standardized_value
            
            if standardized_metrics:
                standardized_periods[standardized_date] = {
                    'currency': currency,
                    'metrics': standardized_metrics,
                    'period_type': self._detect_period_type(period_key),
                    'fiscal_year': self._extract_fiscal_year(standardized_date),
                    'fiscal_quarter': self._extract_fiscal_quarter(period_key)
                }
        
        self.logger.info(
            f"Standardized {statement_type}: {len(standardized_periods)} periods, "
            f"average {sum(len(p['metrics']) for p in standardized_periods.values()) / max(len(standardized_periods), 1):.1f} metrics per period"
        )
        
        return standardized_periods
    
    def _find_metric_value(
        self, 
        period_data: Dict[str, Any], 
        variations: List[str]
    ) -> Optional[Union[int, float]]:
        """
        Find a metric value using various name variations.
        
        Args:
            period_data: Data for a specific period
            variations: List of possible metric names
            
        Returns:
            Metric value if found, None otherwise
        """
        for variation in variations:
            # Try exact match first
            if variation in period_data:
                return period_data[variation]
            
            # Try case-insensitive match
            for key in period_data.keys():
                if isinstance(key, str) and key.lower() == variation.lower():
                    return period_data[key]
            
            # Try partial match (contains)
            for key in period_data.keys():
                if isinstance(key, str) and variation.lower() in key.lower():
                    return period_data[key]
        
        return None
    
    def _standardize_value(self, value: Any) -> Optional[float]:
        """
        Standardize a financial metric value.
        
        Args:
            value: Raw metric value
            
        Returns:
            Standardized numeric value or None
        """
        if value is None or pd.isna(value):
            return None
        
        # Handle string values
        if isinstance(value, str):
            value = value.replace(',', '').replace('$', '').strip()
            if value == '' or value.lower() in ('n/a', 'na', '-'):
                return None
            try:
                value = float(value)
            except (ValueError, TypeError):
                return None
        
        # Convert to float
        try:
            numeric_value = float(value)
            # Filter out unrealistic values
            if abs(numeric_value) > 1e15:  # Sanity check for extremely large values
                return None
            return numeric_value
        except (ValueError, TypeError, OverflowError):
            return None
    
    def _standardize_date(self, date_key: str) -> Optional[str]:
        """
        Standardize date format to YYYY-MM-DD.
        
        Args:
            date_key: Raw date key from financial data
            
        Returns:
            Standardized date string or None
        """
        if not isinstance(date_key, str):
            return None
        
        # Try various date formats
        date_patterns = [
            r'(\d{4})-(\d{2})-(\d{2})',  # YYYY-MM-DD
            r'(\d{4})-(\d{1,2})-(\d{1,2})',  # YYYY-M-D
            r'(\d{2})/(\d{2})/(\d{4})',  # MM/DD/YYYY
            r'(\d{1,2})/(\d{1,2})/(\d{4})',  # M/D/YYYY
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, date_key)
            if match:
                groups = match.groups()
                if len(groups) == 3:
                    if len(groups[0]) == 4:  # YYYY-MM-DD format
                        year, month, day = groups
                    else:  # MM/DD/YYYY format
                        month, day, year = groups
                    
                    try:
                        # Validate and format date
                        date_obj = date_type(int(year), int(month), int(day))
                        return date_obj.strftime('%Y-%m-%d')
                    except (ValueError, TypeError):
                        continue
        
        return None
    
    def _detect_period_type(self, period_key: str) -> str:
        """
        Detect if the period is annual or quarterly.
        
        Args:
            period_key: Raw period key
            
        Returns:
            'annual' or 'quarterly'
        """
        # Simple heuristic: if month is 12, likely annual; otherwise quarterly
        if isinstance(period_key, str) and '-12-' in period_key:
            return 'annual'
        return 'quarterly'
    
    def _extract_fiscal_year(self, standardized_date: str) -> int:
        """
        Extract fiscal year from standardized date.
        
        Args:
            standardized_date: Date in YYYY-MM-DD format
            
        Returns:
            Fiscal year as integer
        """
        try:
            return int(standardized_date.split('-')[0])
        except (ValueError, IndexError):
            return datetime.now().year
    
    def _extract_fiscal_quarter(self, period_key: str) -> Optional[int]:
        """
        Extract fiscal quarter from period key.
        
        Args:
            period_key: Raw period key
            
        Returns:
            Quarter (1-4) or None for annual periods
        """
        if not isinstance(period_key, str):
            return None
        
        # Try to extract month and determine quarter
        month_match = re.search(r'-(\d{1,2})-', period_key)
        if month_match:
            try:
                month = int(month_match.group(1))
                if 1 <= month <= 3:
                    return 1
                elif 4 <= month <= 6:
                    return 2
                elif 7 <= month <= 9:
                    return 3
                elif 10 <= month <= 12:
                    return 4
            except ValueError:
                pass
        
        # If it's December, it's likely annual data
        if '-12-' in period_key:
            return None
        
        # Default to quarterly if we can't determine
        return 1
    
    def calculate_derived_metrics(
        self, 
        income_data: Dict[str, Any],
        balance_data: Dict[str, Any],
        cashflow_data: Dict[str, Any]
    ) -> Dict[str, Dict[str, float]]:
        """
        Calculate derived financial metrics from standardized data.
        
        Args:
            income_data: Standardized income statement data
            balance_data: Standardized balance sheet data
            cashflow_data: Standardized cash flow data
            
        Returns:
            Dictionary of calculated metrics by period
        """
        derived_metrics = {}
        
        # Get common periods across all statements
        all_periods = set()
        if income_data:
            all_periods.update(income_data.keys())
        if balance_data:
            all_periods.update(balance_data.keys())
        if cashflow_data:
            all_periods.update(cashflow_data.keys())
        
        for period in all_periods:
            period_metrics = {}
            
            # Get data for this period
            income = income_data.get(period, {}).get('metrics', {})
            balance = balance_data.get(period, {}).get('metrics', {})
            cashflow = cashflow_data.get(period, {}).get('metrics', {})
            
            # Calculate financial ratios
            period_metrics.update(self._calculate_profitability_ratios(income, balance))
            period_metrics.update(self._calculate_liquidity_ratios(balance))
            period_metrics.update(self._calculate_leverage_ratios(income, balance))
            period_metrics.update(self._calculate_efficiency_ratios(income, balance))
            period_metrics.update(self._calculate_cash_metrics(cashflow, income))
            
            if period_metrics:
                derived_metrics[period] = period_metrics
        
        return derived_metrics
    
    def _calculate_profitability_ratios(
        self, 
        income: Dict[str, float], 
        balance: Dict[str, float]
    ) -> Dict[str, float]:
        """Calculate profitability ratios."""
        ratios = {}
        
        total_revenue = income.get('total_revenue')
        gross_profit = income.get('gross_profit')
        operating_income = income.get('operating_income')
        net_income = income.get('net_income')
        total_assets = balance.get('total_assets')
        shareholders_equity = balance.get('total_shareholders_equity')
        
        if total_revenue and total_revenue != 0:
            if gross_profit is not None:
                ratios['gross_profit_margin'] = gross_profit / total_revenue
            if operating_income is not None:
                ratios['operating_profit_margin'] = operating_income / total_revenue
            if net_income is not None:
                ratios['net_profit_margin'] = net_income / total_revenue
        
        if total_assets and total_assets != 0 and net_income is not None:
            ratios['return_on_assets'] = net_income / total_assets
        
        if shareholders_equity and shareholders_equity != 0 and net_income is not None:
            ratios['return_on_equity'] = net_income / shareholders_equity
        
        return ratios
    
    def _calculate_liquidity_ratios(self, balance: Dict[str, float]) -> Dict[str, float]:
        """Calculate liquidity ratios."""
        ratios = {}
        
        current_assets = balance.get('total_current_assets')
        current_liabilities = balance.get('total_current_liabilities')
        cash = balance.get('cash_and_equivalents')
        inventory = balance.get('inventory')
        
        if current_liabilities and current_liabilities != 0:
            if current_assets is not None:
                ratios['current_ratio'] = current_assets / current_liabilities
            
            # Quick ratio (current assets - inventory) / current liabilities
            if current_assets is not None:
                quick_assets = current_assets - (inventory or 0)
                ratios['quick_ratio'] = quick_assets / current_liabilities
            
            if cash is not None:
                ratios['cash_ratio'] = cash / current_liabilities
        
        return ratios
    
    def _calculate_leverage_ratios(
        self, 
        income: Dict[str, float], 
        balance: Dict[str, float]
    ) -> Dict[str, float]:
        """Calculate leverage ratios."""
        ratios = {}
        
        total_debt = balance.get('total_debt')
        if total_debt is None:
            # Calculate from short-term and long-term debt
            short_debt = balance.get('short_term_debt') or 0
            long_debt = balance.get('long_term_debt') or 0
            total_debt = short_debt + long_debt
        
        total_assets = balance.get('total_assets')
        shareholders_equity = balance.get('total_shareholders_equity')
        operating_income = income.get('operating_income')
        interest_expense = income.get('interest_expense')
        
        if total_debt and total_debt != 0:
            if shareholders_equity and shareholders_equity != 0:
                ratios['debt_to_equity'] = total_debt / shareholders_equity
            
            if total_assets and total_assets != 0:
                ratios['debt_to_assets'] = total_debt / total_assets
        
        if interest_expense and interest_expense != 0 and operating_income is not None:
            ratios['interest_coverage'] = operating_income / interest_expense
        
        return ratios
    
    def _calculate_efficiency_ratios(
        self, 
        income: Dict[str, float], 
        balance: Dict[str, float]
    ) -> Dict[str, float]:
        """Calculate efficiency ratios."""
        ratios = {}
        
        total_revenue = income.get('total_revenue')
        cost_of_revenue = income.get('cost_of_revenue')
        total_assets = balance.get('total_assets')
        inventory = balance.get('inventory')
        accounts_receivable = balance.get('accounts_receivable')
        
        if total_assets and total_assets != 0 and total_revenue is not None and total_revenue != 0:
            ratios['asset_turnover'] = total_revenue / total_assets
        
        if inventory and inventory != 0 and cost_of_revenue is not None and cost_of_revenue != 0:
            ratios['inventory_turnover'] = cost_of_revenue / inventory
        
        if accounts_receivable and accounts_receivable != 0 and total_revenue is not None and total_revenue != 0:
            receivables_turnover = total_revenue / accounts_receivable
            if receivables_turnover != 0:
                ratios['receivables_turnover'] = receivables_turnover
                ratios['days_sales_outstanding'] = 365 / receivables_turnover
        
        return ratios
    
    def _calculate_cash_metrics(
        self, 
        cashflow: Dict[str, float], 
        income: Dict[str, float]
    ) -> Dict[str, float]:
        """Calculate cash flow metrics."""
        ratios = {}
        
        operating_cash_flow = cashflow.get('operating_cash_flow')
        capital_expenditures = cashflow.get('capital_expenditures')
        free_cash_flow = cashflow.get('free_cash_flow')
        
        # Calculate free cash flow if not provided
        if free_cash_flow is None and operating_cash_flow is not None and capital_expenditures is not None:
            free_cash_flow = operating_cash_flow + capital_expenditures  # CapEx is usually negative
            ratios['free_cash_flow'] = free_cash_flow
        
        return ratios