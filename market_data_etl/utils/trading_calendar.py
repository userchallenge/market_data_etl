"""
Trading calendar utilities for handling market trading days.

This module provides functionality to work with trading calendars across
multiple international markets, using pandas-market-calendars as the backend.
"""

from typing import List, Dict, Optional
from datetime import date, datetime
import pandas as pd
import pandas_market_calendars as mcal

from .logging import get_logger


class TradingCalendar:
    """
    Provides trading calendar functionality for multiple international markets.
    
    This class handles trading days across different exchanges and provides
    unified access to market calendars for data alignment purposes.
    """
    
    # Market calendar mappings for different exchanges
    EXCHANGE_CALENDARS = {
        # US Markets
        'US': 'NYSE',
        'NYSE': 'NYSE', 
        'NASDAQ': 'NASDAQ',
        'NYSEARCA': 'NYSE',  # ETFs on NYSE
        
        # European Markets
        'STO': 'XSTO',  # Stockholm (Nasdaq Stockholm)
        'XSTO': 'XSTO',
        'LSE': 'LSE',   # London Stock Exchange
        'XLON': 'LSE',
        'FRA': 'XFRA',  # Frankfurt (XETRA)
        'XETRA': 'XFRA',
        'XFRA': 'XFRA',
        'EPA': 'XPAR',  # Euronext Paris
        'XPAR': 'XPAR',
        
        # Other Markets
        'TSE': 'JPX',   # Tokyo Stock Exchange
        'JPX': 'JPX',
        'TYO': 'JPX',
        'HKG': 'XHKG',  # Hong Kong
        'XHKG': 'XHKG',
        
        # Default fallback
        'DEFAULT': 'NYSE'
    }
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self._calendars = {}  # Cache for loaded calendars
        
    def get_trading_days(
        self, 
        start_date: date, 
        end_date: date, 
        exchange: str = 'US'
    ) -> List[date]:
        """
        Get list of trading days between start and end dates for specified exchange.
        
        Args:
            start_date: Start date for trading days
            end_date: End date for trading days (inclusive)
            exchange: Exchange identifier (US, STO, LSE, etc.)
            
        Returns:
            List of trading days as date objects
        """
        self.logger.debug(f"Getting trading days for {exchange} from {start_date} to {end_date}")
        
        try:
            calendar = self._get_calendar(exchange)
            
            # Get trading days as pandas DatetimeIndex
            trading_days = calendar.schedule(
                start_date=pd.Timestamp(start_date),
                end_date=pd.Timestamp(end_date)
            ).index
            
            # Convert to list of date objects
            trading_dates = [day.date() for day in trading_days]
            
            self.logger.debug(f"Found {len(trading_dates)} trading days for {exchange}")
            return trading_dates
            
        except Exception as e:
            self.logger.error(f"Failed to get trading days for {exchange}: {e}")
            # Fallback to US calendar
            if exchange != 'US':
                self.logger.warning(f"Falling back to US calendar for {exchange}")
                return self.get_trading_days(start_date, end_date, 'US')
            raise
    
    def is_trading_day(self, target_date: date, exchange: str = 'US') -> bool:
        """
        Check if a specific date is a trading day for the given exchange.
        
        Args:
            target_date: Date to check
            exchange: Exchange identifier
            
        Returns:
            True if the date is a trading day
        """
        try:
            calendar = self._get_calendar(exchange)
            return calendar.valid_days(
                start_date=pd.Timestamp(target_date),
                end_date=pd.Timestamp(target_date)
            ).empty == False
            
        except Exception as e:
            self.logger.error(f"Failed to check trading day for {exchange}: {e}")
            # Fallback to simple weekday check
            return target_date.weekday() < 5  # Monday=0, Sunday=6
    
    def next_trading_day(self, target_date: date, exchange: str = 'US') -> date:
        """
        Get the next trading day after the given date.
        
        Args:
            target_date: Starting date
            exchange: Exchange identifier
            
        Returns:
            Next trading day as date object
        """
        try:
            calendar = self._get_calendar(exchange)
            
            # Get next valid trading day
            next_day = calendar.next_valid_session(pd.Timestamp(target_date))
            return next_day.date()
            
        except Exception as e:
            self.logger.error(f"Failed to get next trading day for {exchange}: {e}")
            # Simple fallback - add days until weekday
            current = target_date
            while True:
                current = date.fromordinal(current.toordinal() + 1)
                if current.weekday() < 5:  # Weekday
                    return current
    
    def previous_trading_day(self, target_date: date, exchange: str = 'US') -> date:
        """
        Get the previous trading day before the given date.
        
        Args:
            target_date: Starting date
            exchange: Exchange identifier
            
        Returns:
            Previous trading day as date object
        """
        try:
            calendar = self._get_calendar(exchange)
            
            # Get previous valid trading day
            prev_day = calendar.previous_valid_session(pd.Timestamp(target_date))
            return prev_day.date()
            
        except Exception as e:
            self.logger.error(f"Failed to get previous trading day for {exchange}: {e}")
            # Simple fallback - subtract days until weekday
            current = target_date
            while True:
                current = date.fromordinal(current.toordinal() - 1)
                if current.weekday() < 5:  # Weekday
                    return current
    
    def get_unified_trading_days(
        self, 
        start_date: date, 
        end_date: date, 
        exchanges: List[str] = None
    ) -> List[date]:
        """
        Get unified trading calendar that works across multiple exchanges.
        
        This method returns the intersection of trading days across specified exchanges,
        ensuring that returned dates are valid trading days for all exchanges.
        
        Args:
            start_date: Start date for trading days
            end_date: End date for trading days
            exchanges: List of exchanges to consider. If None, uses US calendar.
            
        Returns:
            List of unified trading days
        """
        if not exchanges:
            exchanges = ['US']
        
        self.logger.info(f"Creating unified trading calendar for exchanges: {exchanges}")
        
        try:
            # Get trading days for each exchange
            all_trading_days = []
            for exchange in exchanges:
                trading_days = self.get_trading_days(start_date, end_date, exchange)
                all_trading_days.append(set(trading_days))
            
            # Find intersection of all trading days
            if len(all_trading_days) == 1:
                unified_days = list(all_trading_days[0])
            else:
                unified_days = list(set.intersection(*all_trading_days))
            
            # Sort the unified days
            unified_days.sort()
            
            self.logger.info(f"Created unified calendar with {len(unified_days)} trading days")
            return unified_days
            
        except Exception as e:
            self.logger.error(f"Failed to create unified trading calendar: {e}")
            # Fallback to US calendar
            return self.get_trading_days(start_date, end_date, 'US')
    
    def detect_exchange_from_ticker(self, ticker: str) -> str:
        """
        Detect the likely exchange for a ticker symbol.
        
        Args:
            ticker: Ticker symbol (e.g., 'AAPL', 'ESSITY-B.ST', '^OMXS30')
            
        Returns:
            Exchange identifier
        """
        ticker = ticker.upper()
        
        # Handle suffixes that indicate exchanges
        if '.ST' in ticker:
            return 'STO'  # Stockholm
        elif '.L' in ticker or '.LON' in ticker:
            return 'LSE'  # London
        elif '.F' in ticker or '.DE' in ticker:
            return 'FRA'  # Frankfurt
        elif '.PA' in ticker:
            return 'EPA'  # Paris
        elif '.T' in ticker or '.TYO' in ticker:
            return 'TSE'  # Tokyo
        elif '.HK' in ticker:
            return 'HKG'  # Hong Kong
        
        # Handle index symbols
        if ticker.startswith('^'):
            if 'OMXS' in ticker:
                return 'STO'  # Swedish indices
            elif 'FTSE' in ticker or 'UKX' in ticker:
                return 'LSE'  # UK indices
            elif 'DAX' in ticker:
                return 'FRA'  # German indices
            elif 'N225' in ticker or 'TOPIX' in ticker:
                return 'TSE'  # Japanese indices
            else:
                return 'US'   # US indices by default
        
        # Default to US for plain symbols
        return 'US'
    
    def _get_calendar(self, exchange: str):
        """Get or create market calendar for the specified exchange."""
        calendar_name = self.EXCHANGE_CALENDARS.get(exchange, self.EXCHANGE_CALENDARS['DEFAULT'])
        
        if calendar_name not in self._calendars:
            try:
                self._calendars[calendar_name] = mcal.get_calendar(calendar_name)
                self.logger.debug(f"Loaded calendar for {calendar_name}")
            except Exception as e:
                self.logger.error(f"Failed to load calendar {calendar_name}: {e}")
                # Try fallback to NYSE
                if calendar_name != 'NYSE':
                    self._calendars[calendar_name] = mcal.get_calendar('NYSE')
                else:
                    raise
        
        return self._calendars[calendar_name]
    
    def get_supported_exchanges(self) -> Dict[str, str]:
        """
        Get dictionary of supported exchanges and their calendar names.
        
        Returns:
            Dictionary mapping exchange codes to calendar names
        """
        return self.EXCHANGE_CALENDARS.copy()


# Global instance for easy access
trading_calendar = TradingCalendar()