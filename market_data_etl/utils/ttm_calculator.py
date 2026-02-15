"""
TTM (Trailing Twelve Months) Calculator

This module calculates TTM financial metrics using quarterly and annual data.
Follows the requirement to use quarterly data when available, fallback to annual data.
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
from sqlalchemy import text

from ..database.manager import DatabaseManager
from ..utils.logging import get_logger
from ..utils.exceptions import ValidationError


class TTMCalculator:
    """
    Calculator for TTM (Trailing Twelve Months) financial metrics.
    
    Uses quarterly data when available (minimum 4 quarters required),
    falls back to annual data as specified in requirements.
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.logger = get_logger(__name__)
        self.db_manager = db_manager or DatabaseManager()
    
    def calculate_ttm_metrics(self, ticker: str, as_of_date: date) -> Optional[Dict[str, Any]]:
        """
        Calculate TTM metrics for a ticker as of a specific date.
        
        Args:
            ticker: Stock ticker symbol
            as_of_date: Calculate TTM metrics as of this date
            
        Returns:
            Dictionary with TTM metrics or None if insufficient data
            {
                'ttm_revenue': float,
                'ttm_net_income': float,
                'ttm_eps': float,
                'shares_diluted': float,
                'ttm_as_of_date': date,
                'source_periods': List[Dict],  # Source quarters/annual used
                'calculation_method': str  # 'quarterly' or 'annual'
            }
        """
        try:
            # First try quarterly approach
            quarterly_ttm = self._calculate_quarterly_ttm(ticker, as_of_date)
            if quarterly_ttm:
                return quarterly_ttm
            
            # Fallback to annual approach
            annual_ttm = self._calculate_annual_ttm(ticker, as_of_date)
            if annual_ttm:
                return annual_ttm
            
            # No sufficient data
            self.logger.debug(f"No sufficient data for TTM calculation: {ticker} as of {as_of_date}")
            return None
            
        except Exception as e:
            self.logger.error(f"Error calculating TTM for {ticker} as of {as_of_date}: {e}")
            return None
    
    def get_historical_ttm_timeline(self, ticker: str) -> List[Dict[str, Any]]:
        """
        Get chronological timeline of TTM metric changes for a ticker.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            List of TTM calculations sorted by date, each with same structure as calculate_ttm_metrics
        """
        try:
            timeline = []
            
            # Get all quarter-end dates where we might be able to calculate TTM
            potential_dates = self._get_potential_ttm_dates(ticker)
            
            for ttm_date in potential_dates:
                ttm_metrics = self.calculate_ttm_metrics(ticker, ttm_date)
                if ttm_metrics:
                    timeline.append(ttm_metrics)
            
            # Sort chronologically
            timeline.sort(key=lambda x: x['ttm_as_of_date'])
            
            self.logger.info(f"Generated TTM timeline for {ticker}: {len(timeline)} periods")
            return timeline
            
        except Exception as e:
            self.logger.error(f"Error generating TTM timeline for {ticker}: {e}")
            return []
    
    def _calculate_quarterly_ttm(self, ticker: str, as_of_date: date) -> Optional[Dict[str, Any]]:
        """Calculate TTM using last 4 quarters of data."""
        # Get last 4 quarters ending on or before as_of_date
        quarters = self._get_last_n_quarters(ticker, as_of_date, 4)
        
        if len(quarters) < 4:
            return None
            
        # Check data completeness
        if not all(q['total_revenue'] is not None and q['net_income'] is not None for q in quarters):
            return None
        
        # Sum revenue and net income
        ttm_revenue = sum(q['total_revenue'] for q in quarters)
        ttm_net_income = sum(q['net_income'] for q in quarters)
        
        # Use most recent shares_diluted
        shares_diluted = None
        for q in reversed(quarters):  # Most recent first
            if q['weighted_average_shares_diluted'] is not None:
                shares_diluted = q['weighted_average_shares_diluted']
                break
        
        if shares_diluted is None or shares_diluted <= 0:
            return None
            
        # Calculate TTM EPS
        ttm_eps = ttm_net_income / shares_diluted
        
        return {
            'ttm_revenue': ttm_revenue,
            'ttm_net_income': ttm_net_income,
            'ttm_eps': ttm_eps,
            'shares_diluted': shares_diluted,
            'ttm_as_of_date': quarters[-1]['period_end_date'],  # Latest quarter end date
            'source_periods': quarters,
            'calculation_method': 'quarterly'
        }
    
    def _calculate_annual_ttm(self, ticker: str, as_of_date: date) -> Optional[Dict[str, Any]]:
        """Calculate TTM using latest annual data."""
        # Get most recent annual data ending on or before as_of_date
        annual_data = self._get_latest_annual_data(ticker, as_of_date)
        
        if not annual_data:
            return None
            
        # Check data completeness
        required_fields = ['total_revenue', 'net_income', 'weighted_average_shares_diluted']
        if not all(annual_data.get(field) is not None for field in required_fields):
            return None
            
        if annual_data['weighted_average_shares_diluted'] <= 0:
            return None
            
        # Use annual data as TTM
        ttm_revenue = annual_data['total_revenue']
        ttm_net_income = annual_data['net_income']
        shares_diluted = annual_data['weighted_average_shares_diluted']
        ttm_eps = ttm_net_income / shares_diluted
        
        return {
            'ttm_revenue': ttm_revenue,
            'ttm_net_income': ttm_net_income,
            'ttm_eps': ttm_eps,
            'shares_diluted': shares_diluted,
            'ttm_as_of_date': annual_data['period_end_date'],
            'source_periods': [annual_data],
            'calculation_method': 'annual'
        }
    
    def _get_last_n_quarters(self, ticker: str, as_of_date: date, n: int) -> List[Dict[str, Any]]:
        """Get last N quarters ending on or before as_of_date."""
        with self.db_manager.get_session() as session:
            query = text("""
            SELECT 
                inc.period_end_date,
                inc.fiscal_year,
                inc.fiscal_quarter,
                inc.total_revenue,
                inc.net_income,
                inc.weighted_average_shares_diluted
            FROM instruments ins
            JOIN income_statements inc ON ins.id = inc.instrument_id
            WHERE ins.ticker_symbol = :ticker
                AND inc.period_type = 'quarterly'
                AND inc.period_end_date <= :as_of_date
            ORDER BY inc.period_end_date DESC
            LIMIT :limit_n
            """)
            
            result = session.execute(query, {
                'ticker': ticker, 
                'as_of_date': as_of_date, 
                'limit_n': n
            }).fetchall()
            
            # Convert to list of dictionaries
            quarters = []
            for row in result:
                quarters.append({
                    'period_end_date': row[0],
                    'fiscal_year': row[1],
                    'fiscal_quarter': row[2], 
                    'total_revenue': row[3],
                    'net_income': row[4],
                    'weighted_average_shares_diluted': row[5]
                })
            
            # Reverse to get chronological order
            return list(reversed(quarters))
    
    def _get_latest_annual_data(self, ticker: str, as_of_date: date) -> Optional[Dict[str, Any]]:
        """Get most recent annual data ending on or before as_of_date."""
        with self.db_manager.get_session() as session:
            query = text("""
            SELECT 
                inc.period_end_date,
                inc.fiscal_year,
                inc.total_revenue,
                inc.net_income,
                inc.weighted_average_shares_diluted
            FROM instruments ins
            JOIN income_statements inc ON ins.id = inc.instrument_id
            WHERE ins.ticker_symbol = :ticker
                AND inc.period_type = 'annual'
                AND inc.period_end_date <= :as_of_date
            ORDER BY inc.period_end_date DESC
            LIMIT 1
            """)
            
            result = session.execute(query, {
                'ticker': ticker, 
                'as_of_date': as_of_date
            }).fetchone()
            
            if result:
                return {
                    'period_end_date': result[0],
                    'fiscal_year': result[1],
                    'total_revenue': result[2],
                    'net_income': result[3],
                    'weighted_average_shares_diluted': result[4]
                }
            return None
    
    def _get_potential_ttm_dates(self, ticker: str) -> List[date]:
        """Get all potential dates where TTM might be calculable."""
        with self.db_manager.get_session() as session:
            # Get all quarter-end dates plus annual dates
            query = text("""
            SELECT DISTINCT inc.period_end_date
            FROM instruments ins
            JOIN income_statements inc ON ins.id = inc.instrument_id
            WHERE ins.ticker_symbol = :ticker
                AND (inc.period_type = 'quarterly' OR inc.period_type = 'annual')
            ORDER BY inc.period_end_date
            """)
            
            result = session.execute(query, {'ticker': ticker}).fetchall()
            return [row[0] for row in result]