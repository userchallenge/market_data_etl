"""
Financial Overview Generator

This module provides functionality to create high-level financial overviews
combining fundamental and price data for all instruments in the database.

This is a utility for getting overview data, not core ETL functionality.
"""

from typing import Optional, Dict, Any
import pandas as pd
import numpy as np
from datetime import datetime

from ..database.manager import DatabaseManager
from ..utils.logging import get_logger


class FinancialOverviewGenerator:
    """
    Generator for comprehensive financial overview DataFrames.
    
    Creates overview DataFrames combining:
    - Basic instrument information
    - Latest fundamental data (revenue, EBITDA, FCF)
    - Latest price data
    - Historical year-end data
    - Median growth rates for all metrics
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.logger = get_logger(__name__)
        self.db_manager = db_manager or DatabaseManager()
    
    def create_financial_overview(self) -> pd.DataFrame:
        """
        Create comprehensive financial overview DataFrame.
        
        Returns:
            DataFrame with financial and price overview for all instruments
        """
        try:
            self.logger.info("Starting financial overview generation")
            
            # Step 1: Get financial data
            financial_data = self._query_financial_data()
            if financial_data.empty:
                self.logger.warning("No financial data found")
                return pd.DataFrame()
            
            # Step 2: Process financial data
            financial_overview = self._process_financial_data(financial_data)
            financial_growth = self._calculate_financial_growth_rates(financial_data)
            financial_ratios = self._calculate_financial_ratios(financial_data)
            
            # Merge financial data
            financial_complete = financial_overview.merge(
                financial_growth, on='ticker_symbol', how='left'
            ).merge(
                financial_ratios, on='ticker_symbol', how='left', suffixes=('', '_ratio')
            )
            
            # Step 3: Get price data
            price_data = self._query_price_data()
            if not price_data.empty:
                price_overview = self._process_price_data(price_data)
                price_growth = self._calculate_price_growth_rates(price_data)
                valuation_ratios = self._calculate_valuation_ratios(financial_data, price_data)
                
                # Merge price data
                price_complete = price_overview.merge(
                    price_growth, on='ticker_symbol', how='left'
                )
                
                # Merge valuation ratios
                if not valuation_ratios.empty:
                    price_complete = price_complete.merge(
                        valuation_ratios, on='ticker_symbol', how='left', suffixes=('', '_val')
                    )
                
                # Rename fundamental date column for clarity
                financial_complete = financial_complete.rename(
                    columns={'latest_date': 'latest_fundamental_date'}
                )
                
                # Merge financial and price data
                enhanced_overview = financial_complete.merge(
                    price_complete, on='ticker_symbol', how='left'
                )
            else:
                self.logger.warning("No price data found")
                enhanced_overview = financial_complete
            
            # Step 4: Reorder columns and add percentage columns
            final_overview = self._format_final_overview(enhanced_overview)
            
            self.logger.info(
                f"Generated overview for {len(final_overview)} instruments with "
                f"{len(final_overview.columns)} columns"
            )
            
            return final_overview
            
        except Exception as e:
            self.logger.error(f"Error generating financial overview: {e}")
            raise
    
    def _query_to_df(self, sql_query: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame"""
        with self.db_manager.get_session() as session:
            return pd.read_sql(sql_query, session.connection())
    
    def _classify_company(self, ticker_symbol: str, instrument_name: str) -> str:
        """Classify company into financial industry categories"""
        ticker = ticker_symbol.upper() if ticker_symbol else ""
        name = instrument_name.lower() if instrument_name else ""
        
        # Banks
        bank_tickers = ['NDA-SE.ST', 'SEB-C.ST', 'SHB-A.ST']
        if ticker in bank_tickers or 'bank' in name:
            return 'Bank'
        
        # Investment companies
        investment_tickers = ['INVE-B.ST', 'LATO-B.ST']
        if ticker in investment_tickers or ('investment' in name and 'ab' in name):
            return 'Investment Company'
        
        # Crypto/Tech companies
        crypto_tickers = ['COIN', 'HUT', 'RIOT']
        if ticker in crypto_tickers:
            return 'Crypto/Tech'
        
        # Default to traditional
        return 'Traditional'
    
    def _query_financial_data(self) -> pd.DataFrame:
        """Query financial data from database"""
        financial_kpis_query = """
        WITH financial_data AS (
            SELECT 
                i.id as instrument_id,
                i.ticker_symbol,
                i.instrument_name,
                i.instrument_type,
                i.currency,
                
                -- Income statement data
                inc.period_end_date,
                inc.period_type,
                inc.total_revenue,
                inc.operating_income,
                inc.ebitda,
                inc.net_income,
                inc.diluted_eps,
                inc.weighted_average_shares_diluted,
                inc.interest_income,
                inc.interest_expense,
                inc.total_operating_expenses,
                
                -- Balance sheet data for ratios
                bs.total_assets,
                bs.total_shareholders_equity,
                
                -- Cash flow data (using correct column names)
                cf.operating_cash_flow,
                cf.capital_expenditures,
                (cf.operating_cash_flow - COALESCE(cf.capital_expenditures, 0)) as free_cash_flow,
                
                -- Date components for grouping
                strftime('%Y', inc.period_end_date) as year,
                strftime('%m', inc.period_end_date) as month
                
            FROM instruments i
            LEFT JOIN income_statements inc ON i.id = inc.instrument_id
            LEFT JOIN balance_sheets bs ON i.id = bs.instrument_id
                AND inc.period_end_date = bs.period_end_date 
                AND inc.period_type = bs.period_type
            LEFT JOIN cash_flows cf ON i.id = cf.instrument_id 
                AND inc.period_end_date = cf.period_end_date 
                AND inc.period_type = cf.period_type
            WHERE inc.period_type IN ('annual', 'ttm')
                AND inc.period_end_date IS NOT NULL
        )

        SELECT * FROM financial_data
        ORDER BY ticker_symbol, year DESC, period_end_date DESC
        """
        
        return self._query_to_df(financial_kpis_query)
    
    def _query_price_data(self) -> pd.DataFrame:
        """Query price data from database"""
        price_data_query = """
        WITH price_data AS (
            SELECT 
                i.id as instrument_id,
                i.ticker_symbol,
                i.instrument_name,
                i.instrument_type,
                i.currency,
                
                -- Price data (using correct column names)
                p.date as price_date,
                p.close as close_price,
                p.adj_close as adjusted_close_price,
                
                -- Date components for grouping
                strftime('%Y', p.date) as year,
                strftime('%m', p.date) as month,
                strftime('%d', p.date) as day
                
            FROM instruments i
            INNER JOIN prices p ON i.id = p.instrument_id
            WHERE p.close IS NOT NULL
        )

        SELECT * FROM price_data
        ORDER BY ticker_symbol, year DESC, price_date DESC
        """
        
        return self._query_to_df(price_data_query)
    
    def _process_financial_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process financial data to get year-end and latest values"""
        df['period_end_date'] = pd.to_datetime(df['period_end_date'])
        df['year'] = df['period_end_date'].dt.year
        current_year = datetime.now().year
        
        results = []
        
        for ticker in df['ticker_symbol'].unique():
            if pd.isna(ticker):
                continue
                
            ticker_data = df[df['ticker_symbol'] == ticker].copy()
            basic_info = ticker_data.iloc[0]
            
            # Classify company type based on ticker and name
            company_category = self._classify_company(basic_info['ticker_symbol'], basic_info['instrument_name'])
            
            ticker_result = {
                'id': basic_info['instrument_id'],
                'instrument_name': basic_info['instrument_name'],
                'ticker_symbol': basic_info['ticker_symbol'],
                'instrument_type': basic_info['instrument_type'],
                'currency': basic_info['currency'],
                'company_category': company_category,
            }
            
            # Get year-end data
            years_available = sorted(ticker_data['year'].unique())
            
            for year in years_available:
                if year == current_year:
                    continue
                    
                year_data = ticker_data[ticker_data['year'] == year]
                
                # Prefer annual data, but use TTM if annual not available
                annual_data = year_data[year_data['period_type'] == 'annual']
                if not annual_data.empty:
                    year_record = annual_data.iloc[-1]
                else:
                    ttm_data = year_data[year_data['period_type'] == 'ttm']
                    if not ttm_data.empty:
                        year_record = ttm_data.iloc[-1]
                    else:
                        continue
                
                ticker_result[f'revenue_{year}'] = year_record['total_revenue']
                ticker_result[f'ebitda_{year}'] = year_record['ebitda']
                ticker_result[f'fcf_{year}'] = year_record['free_cash_flow']
                ticker_result[f'net_income_{year}'] = year_record['net_income']
                ticker_result[f'diluted_eps_{year}'] = year_record['diluted_eps']
                ticker_result[f'shares_diluted_{year}'] = year_record['weighted_average_shares_diluted']
            
            # Get latest data for current year
            current_year_data = ticker_data[ticker_data['year'] == current_year]
            if not current_year_data.empty:
                latest_record = current_year_data.iloc[0]
                ticker_result['latest_date'] = latest_record['period_end_date'].strftime('%Y-%m-%d')
                ticker_result['latest_revenue'] = latest_record['total_revenue']
                ticker_result['latest_ebitda'] = latest_record['ebitda']
                ticker_result['latest_fcf'] = latest_record['free_cash_flow']
                ticker_result['latest_net_income'] = latest_record['net_income']
                ticker_result['latest_diluted_eps'] = latest_record['diluted_eps']
                ticker_result['latest_shares_diluted'] = latest_record['weighted_average_shares_diluted']
            else:
                # If no current year data, use latest available
                if not ticker_data.empty:
                    latest_record = ticker_data.iloc[0]
                    ticker_result['latest_date'] = latest_record['period_end_date'].strftime('%Y-%m-%d')
                    ticker_result['latest_revenue'] = latest_record['total_revenue']
                    ticker_result['latest_ebitda'] = latest_record['ebitda']
                    ticker_result['latest_fcf'] = latest_record['free_cash_flow']
                    ticker_result['latest_net_income'] = latest_record['net_income']
                    ticker_result['latest_diluted_eps'] = latest_record['diluted_eps']
                    ticker_result['latest_shares_diluted'] = latest_record['weighted_average_shares_diluted']
            
            # Get latest TTM data (separate from annual data)
            ttm_data = ticker_data[ticker_data['period_type'] == 'ttm']
            if not ttm_data.empty:
                latest_ttm_record = ttm_data.iloc[0]  # Most recent TTM record
                ticker_result['latest_ttm_date'] = latest_ttm_record['period_end_date'].strftime('%Y-%m-%d')
                ticker_result['latest_ttm_revenue'] = latest_ttm_record['total_revenue']
                ticker_result['latest_ttm_ebitda'] = latest_ttm_record['ebitda']
                ticker_result['latest_ttm_fcf'] = latest_ttm_record['free_cash_flow']
                ticker_result['latest_ttm_net_income'] = latest_ttm_record['net_income']
                ticker_result['latest_ttm_diluted_eps'] = latest_ttm_record['diluted_eps']
                ticker_result['latest_ttm_shares_diluted'] = latest_ttm_record['weighted_average_shares_diluted']
            
            results.append(ticker_result)
        
        return pd.DataFrame(results)
    
    def _calculate_financial_growth_rates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate median growth rates for financial metrics"""
        growth_results = []
        
        for ticker in df['ticker_symbol'].unique():
            if pd.isna(ticker):
                continue
                
            ticker_data = df[df['ticker_symbol'] == ticker].copy()
            
            if len(ticker_data) < 2:
                continue
            
            ticker_data = ticker_data.sort_values('period_end_date')
            ticker_data['period_end_date'] = pd.to_datetime(ticker_data['period_end_date'])
            
            revenue_growth = []
            ebitda_growth = []
            fcf_growth = []
            
            for i in range(1, len(ticker_data)):
                current = ticker_data.iloc[i]
                previous = ticker_data.iloc[i - 1]
                
                # Revenue growth
                if (pd.notna(current['total_revenue']) and pd.notna(previous['total_revenue']) 
                    and previous['total_revenue'] != 0):
                    growth = (current['total_revenue'] - previous['total_revenue']) / abs(previous['total_revenue'])
                    revenue_growth.append(growth)
                
                # EBITDA growth
                if (pd.notna(current['ebitda']) and pd.notna(previous['ebitda']) 
                    and previous['ebitda'] != 0):
                    growth = (current['ebitda'] - previous['ebitda']) / abs(previous['ebitda'])
                    ebitda_growth.append(growth)
                
                # FCF growth
                if (pd.notna(current['free_cash_flow']) and pd.notna(previous['free_cash_flow']) 
                    and previous['free_cash_flow'] != 0):
                    growth = (current['free_cash_flow'] - previous['free_cash_flow']) / abs(previous['free_cash_flow'])
                    fcf_growth.append(growth)
            
            median_revenue_growth = np.median(revenue_growth) if revenue_growth else np.nan
            median_ebitda_growth = np.median(ebitda_growth) if ebitda_growth else np.nan
            median_fcf_growth = np.median(fcf_growth) if fcf_growth else np.nan
            
            growth_results.append({
                'ticker_symbol': ticker,
                'median_revenue_growth': median_revenue_growth,
                'median_ebitda_growth': median_ebitda_growth,
                'median_fcf_growth': median_fcf_growth,
                'revenue_growth_periods': len(revenue_growth),
                'ebitda_growth_periods': len(ebitda_growth),
                'fcf_growth_periods': len(fcf_growth)
            })
        
        return pd.DataFrame(growth_results)
    
    def _calculate_financial_ratios(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate financial ratios for latest period and historical data"""
        ratio_results = []
        
        for ticker in df['ticker_symbol'].unique():
            if pd.isna(ticker):
                continue
                
            ticker_data = df[df['ticker_symbol'] == ticker].copy()
            basic_info = ticker_data.iloc[0]
            
            # Get company category for appropriate ratios
            company_category = self._classify_company(basic_info['ticker_symbol'], basic_info['instrument_name'])
            
            ticker_result = {'ticker_symbol': ticker}
            
            # Process each period's data for ratios
            for _, row in ticker_data.iterrows():
                period_ratios = self._calculate_period_ratios(row, company_category)
                
                # Add latest ratios (most recent data)
                if ticker_result.get('latest_date') is None or row['period_end_date'] > pd.to_datetime(ticker_result.get('latest_date', '1900-01-01')):
                    ticker_result['latest_date'] = row['period_end_date']
                    for ratio_name, ratio_value in period_ratios.items():
                        ticker_result[f'latest_{ratio_name}'] = ratio_value
                
                # Add historical ratios by year
                year = pd.to_datetime(row['period_end_date']).year
                current_year = datetime.now().year
                
                if year != current_year and row['period_type'] == 'annual':
                    for ratio_name, ratio_value in period_ratios.items():
                        ticker_result[f'{ratio_name}_{year}'] = ratio_value
            
            ratio_results.append(ticker_result)
        
        return pd.DataFrame(ratio_results)
    
    def _calculate_period_ratios(self, row: pd.Series, company_category: str) -> Dict[str, float]:
        """Calculate financial ratios for a single period"""
        ratios = {}
        
        # Common ratios for all financial companies
        if pd.notna(row['net_income']) and pd.notna(row['total_assets']) and row['total_assets'] != 0:
            ratios['roa'] = row['net_income'] / row['total_assets']
        
        if pd.notna(row['net_income']) and pd.notna(row['total_shareholders_equity']) and row['total_shareholders_equity'] != 0:
            ratios['roe'] = row['net_income'] / row['total_shareholders_equity']
        
        # Bank-specific ratios
        if company_category == 'Bank':
            # Net Interest Margin = (Interest Income - Interest Expense) / Total Assets
            if (pd.notna(row['interest_income']) and pd.notna(row['interest_expense']) 
                and pd.notna(row['total_assets']) and row['total_assets'] != 0):
                net_interest = row['interest_income'] - row['interest_expense']
                ratios['nim'] = net_interest / row['total_assets']
            
            # Capital Ratio = Total Shareholders Equity / Total Assets
            if pd.notna(row['total_shareholders_equity']) and pd.notna(row['total_assets']) and row['total_assets'] != 0:
                ratios['capital_ratio'] = row['total_shareholders_equity'] / row['total_assets']
        
        # Investment Company-specific ratios
        elif company_category == 'Investment Company':
            # Investment Return Rate = Total Revenue / Total Assets
            if pd.notna(row['total_revenue']) and pd.notna(row['total_assets']) and row['total_assets'] != 0:
                ratios['investment_return'] = row['total_revenue'] / row['total_assets']
            
            # Net Margin = Net Income / Total Revenue
            if pd.notna(row['net_income']) and pd.notna(row['total_revenue']) and row['total_revenue'] != 0:
                ratios['net_margin'] = row['net_income'] / row['total_revenue']
        
        return ratios
    
    def _calculate_valuation_ratios(self, financial_data: pd.DataFrame, price_data: pd.DataFrame) -> pd.DataFrame:
        """Calculate P/E and P/S ratios for latest period and historical data"""
        if financial_data.empty or price_data.empty:
            return pd.DataFrame()
            
        ratio_results = []
        
        for ticker in financial_data['ticker_symbol'].unique():
            if pd.isna(ticker):
                continue
                
            ticker_financial = financial_data[financial_data['ticker_symbol'] == ticker].copy()
            ticker_price = price_data[price_data['ticker_symbol'] == ticker].copy()
            
            if ticker_financial.empty or ticker_price.empty:
                continue
                
            ticker_result = {'ticker_symbol': ticker}
            
            # Process each period's data for valuation ratios
            for _, fin_row in ticker_financial.iterrows():
                fin_year = fin_row['year']  # Already a string from the query
                
                # Find corresponding price data for this year
                price_year_data = ticker_price[ticker_price['year'] == fin_year]
                if price_year_data.empty:
                    continue
                    
                # Use year-end price for the financial period
                year_end_price = price_year_data.iloc[-1]['close_price'] if not price_year_data.empty else None
                
                if pd.notna(year_end_price):
                    period_ratios = self._calculate_period_valuation_ratios(fin_row, year_end_price)
                    
                    # Add latest ratios (most recent data)
                    if ticker_result.get('latest_date') is None or fin_row['period_end_date'] > pd.to_datetime(ticker_result.get('latest_date', '1900-01-01')):
                        ticker_result['latest_date'] = fin_row['period_end_date']
                        for ratio_name, ratio_value in period_ratios.items():
                            ticker_result[f'latest_{ratio_name}'] = ratio_value
                    
                    # Add historical ratios by year
                    current_year = str(datetime.now().year)
                    
                    if fin_year != current_year and fin_row['period_type'] == 'annual':
                        for ratio_name, ratio_value in period_ratios.items():
                            ticker_result[f'{ratio_name}_{fin_year}'] = ratio_value
            
            if len(ticker_result) > 1:  # Only add if we have actual ratio data
                ratio_results.append(ticker_result)
        
        return pd.DataFrame(ratio_results)
    
    def _calculate_period_valuation_ratios(self, fin_row: pd.Series, price: float) -> Dict[str, float]:
        """Calculate P/E and P/S ratios for a single period"""
        ratios = {}
        
        # P/E Ratio = Price / EPS
        if pd.notna(fin_row['diluted_eps']) and fin_row['diluted_eps'] != 0:
            ratios['pe_ratio'] = price / fin_row['diluted_eps']
        
        # P/S Ratio = Market Cap / Revenue = (Price * Shares) / Revenue
        if (pd.notna(fin_row['total_revenue']) and fin_row['total_revenue'] != 0 
            and pd.notna(fin_row['weighted_average_shares_diluted'])):
            market_cap = price * fin_row['weighted_average_shares_diluted']
            ratios['ps_ratio'] = market_cap / fin_row['total_revenue']
        
        return ratios
    
    def _process_price_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process price data to get year-end and latest values"""
        if df.empty:
            return pd.DataFrame()
        
        df['price_date'] = pd.to_datetime(df['price_date'])
        df['year'] = df['price_date'].dt.year
        current_year = datetime.now().year
        
        results = []
        
        for ticker in df['ticker_symbol'].unique():
            if pd.isna(ticker):
                continue
                
            ticker_data = df[df['ticker_symbol'] == ticker].copy()
            ticker_data = ticker_data.sort_values('price_date')
            
            basic_info = ticker_data.iloc[0]
            ticker_result = {'ticker_symbol': basic_info['ticker_symbol']}
            
            # Get year-end prices
            years_available = sorted(ticker_data['year'].unique())
            
            for year in years_available:
                if year == current_year:
                    continue
                    
                year_data = ticker_data[ticker_data['year'] == year]
                if not year_data.empty:
                    year_end_record = year_data.iloc[-1]
                    ticker_result[f'price_{year}'] = year_end_record['close_price']
            
            # Get latest price data
            current_year_data = ticker_data[ticker_data['year'] == current_year]
            if not current_year_data.empty:
                latest_record = current_year_data.iloc[-1]
                ticker_result['latest_price_date'] = latest_record['price_date'].strftime('%Y-%m-%d')
                ticker_result['latest_price'] = latest_record['close_price']
            else:
                if not ticker_data.empty:
                    latest_record = ticker_data.iloc[-1]
                    ticker_result['latest_price_date'] = latest_record['price_date'].strftime('%Y-%m-%d')
                    ticker_result['latest_price'] = latest_record['close_price']
            
            results.append(ticker_result)
        
        return pd.DataFrame(results)
    
    def _calculate_price_growth_rates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate median price growth rates"""
        if df.empty:
            return pd.DataFrame()
        
        growth_results = []
        
        for ticker in df['ticker_symbol'].unique():
            if pd.isna(ticker):
                continue
                
            ticker_data = df[df['ticker_symbol'] == ticker].copy()
            
            if len(ticker_data) < 2:
                continue
            
            ticker_data = ticker_data.sort_values('price_date')
            ticker_data['price_date'] = pd.to_datetime(ticker_data['price_date'])
            
            price_growth = []
            
            # Group by year and get year-end price for each year
            yearly_prices = ticker_data.groupby('year')['close_price'].last().reset_index()
            yearly_prices = yearly_prices.sort_values('year')
            
            for i in range(1, len(yearly_prices)):
                current_price = yearly_prices.iloc[i]['close_price']
                previous_price = yearly_prices.iloc[i-1]['close_price']
                
                if pd.notna(current_price) and pd.notna(previous_price) and previous_price > 0:
                    growth = (current_price - previous_price) / previous_price
                    price_growth.append(growth)
            
            median_price_growth = np.median(price_growth) if price_growth else np.nan
            
            growth_results.append({
                'ticker_symbol': ticker,
                'median_price_growth': median_price_growth,
                'price_growth_periods': len(price_growth)
            })
        
        return pd.DataFrame(growth_results)
    
    def _format_final_overview(self, df: pd.DataFrame) -> pd.DataFrame:
        """Format and reorder columns in final overview"""
        # Define logical column groupings
        base_cols = ['id', 'instrument_name', 'ticker_symbol', 'instrument_type', 'currency', 'company_category']
        
        # Handle both renamed and original latest_date column
        if 'latest_fundamental_date' in df.columns:
            latest_fundamental_cols = ['latest_fundamental_date', 'latest_revenue', 'latest_ebitda', 'latest_fcf', 
                                     'latest_net_income', 'latest_diluted_eps', 'latest_shares_diluted']
        else:
            latest_fundamental_cols = ['latest_date', 'latest_revenue', 'latest_ebitda', 'latest_fcf',
                                     'latest_net_income', 'latest_diluted_eps', 'latest_shares_diluted']
            
        # Add TTM columns if they exist
        latest_ttm_cols = ['latest_ttm_date', 'latest_ttm_revenue', 'latest_ttm_ebitda', 'latest_ttm_fcf',
                          'latest_ttm_net_income', 'latest_ttm_diluted_eps', 'latest_ttm_shares_diluted']
        latest_ttm_cols = [col for col in latest_ttm_cols if col in df.columns]
            
        latest_price_cols = ['latest_price_date', 'latest_price'] if 'latest_price_date' in df.columns else []
        
        # Financial ratios columns (excluding valuation ratios)
        latest_ratio_cols = [col for col in df.columns if col.startswith('latest_') and col not in latest_fundamental_cols + latest_ttm_cols + latest_price_cols and not any(val in col for val in ['pe_ratio', 'ps_ratio'])]
        
        # Valuation ratios columns
        latest_valuation_cols = [col for col in df.columns if col.startswith('latest_') and any(val in col for val in ['pe_ratio', 'ps_ratio'])]
        
        financial_growth_cols = ['median_revenue_growth', 'median_ebitda_growth', 'median_fcf_growth']
        price_growth_cols = ['median_price_growth'] if 'median_price_growth' in df.columns else []
        
        # Get historical year columns
        all_cols = df.columns.tolist()
        financial_year_cols = [col for col in all_cols 
                              if col.startswith(('revenue_', 'ebitda_', 'fcf_', 'net_income_', 'diluted_eps_', 'shares_diluted_')) and col.split('_')[-1].isdigit()]
        price_year_cols = [col for col in all_cols 
                          if col.startswith('price_') and col.split('_')[-1].isdigit()]
        ratio_year_cols = [col for col in all_cols 
                          if col.split('_')[-1].isdigit() and any(col.startswith(ratio) for ratio in ['roa_', 'roe_', 'nim_', 'capital_ratio_', 'investment_return_', 'net_margin_'])]
        valuation_year_cols = [col for col in all_cols 
                              if col.split('_')[-1].isdigit() and any(col.startswith(val) for val in ['pe_ratio_', 'ps_ratio_'])]
        
        # Sort year columns
        financial_year_cols = sorted(financial_year_cols)
        price_year_cols = sorted(price_year_cols)
        ratio_year_cols = sorted(ratio_year_cols)
        valuation_year_cols = sorted(valuation_year_cols)
        
        # Final column order
        final_column_order = (base_cols + latest_fundamental_cols + latest_ttm_cols + 
                             latest_price_cols + latest_ratio_cols + latest_valuation_cols +
                             financial_growth_cols + price_growth_cols + 
                             financial_year_cols + price_year_cols + ratio_year_cols + valuation_year_cols)
        
        # Select only columns that exist and reorder
        available_cols = [col for col in final_column_order if col in df.columns]
        final_df = df[available_cols].copy()
        
        # Add percentage columns for growth rates
        if 'median_revenue_growth' in final_df.columns:
            final_df['median_revenue_growth_pct'] = final_df['median_revenue_growth'] * 100
        if 'median_ebitda_growth' in final_df.columns:
            final_df['median_ebitda_growth_pct'] = final_df['median_ebitda_growth'] * 100
        if 'median_fcf_growth' in final_df.columns:
            final_df['median_fcf_growth_pct'] = final_df['median_fcf_growth'] * 100
        if 'median_price_growth' in final_df.columns:
            final_df['median_price_growth_pct'] = final_df['median_price_growth'] * 100
        
        return final_df


def create_financial_overview(db_manager: Optional[DatabaseManager] = None) -> pd.DataFrame:
    """
    Create comprehensive financial overview DataFrame.
    
    Convenience function that creates a FinancialOverviewGenerator instance
    and returns the overview DataFrame.
    
    Args:
        db_manager: Optional DatabaseManager instance
        
    Returns:
        DataFrame with comprehensive financial and price overview
    """
    generator = FinancialOverviewGenerator(db_manager)
    return generator.create_financial_overview()