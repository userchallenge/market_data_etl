"""
Forward-fill transformer for aligning sparse economic data to trading calendars.

This module implements the forward-fill logic required to transform sparse
economic indicators (monthly releases, event-based changes) into daily
trading-calendar aligned data for unified analysis.
"""

from typing import Dict, List, Any, Optional
from datetime import date, datetime
import pandas as pd

from ..utils.logging import get_logger
from ..utils.trading_calendar import trading_calendar


class ForwardFillTransformer:
    """
    Transforms sparse economic data into trading-day aligned data using forward-fill.
    
    This class implements the core logic for:
    - Taking sparse economic data (monthly indicators, rate changes)
    - Forward-filling values from release dates to next release
    - Aligning to trading calendar (excluding weekends/holidays)
    """
    
    def __init__(self):
        self.logger = get_logger(__name__)
    
    def forward_fill_economic_data(
        self,
        economic_data: Dict[str, List[Dict[str, Any]]],
        trading_days: List[date],
        exchange: str = 'US'
    ) -> pd.DataFrame:
        """
        Forward-fill economic data to align with trading calendar.
        
        Args:
            economic_data: Dictionary mapping indicator names to data points
                          Format: {'indicator_name': [{'date': date, 'value': float}, ...]}
            trading_days: List of trading days to align to
            exchange: Exchange calendar used
            
        Returns:
            DataFrame with trading days as index and economic indicators as columns
        """
        self.logger.info(f"Forward-filling {len(economic_data)} economic indicators to {len(trading_days)} trading days")
        
        if not trading_days:
            return pd.DataFrame()
        
        # Create base DataFrame with trading days
        result_df = pd.DataFrame(index=pd.to_datetime(trading_days))
        result_df.index.name = 'date'
        
        # Process each economic indicator
        for indicator_name, data_points in economic_data.items():
            if not data_points:
                self.logger.warning(f"No data points for indicator {indicator_name}")
                continue
            
            try:
                # Forward-fill this indicator
                filled_series = self._forward_fill_indicator(
                    data_points, 
                    trading_days, 
                    indicator_name
                )
                
                # Add to result DataFrame
                result_df[indicator_name] = filled_series
                
            except Exception as e:
                self.logger.error(f"Failed to forward-fill {indicator_name}: {e}")
                # Add NaN column to maintain structure
                result_df[indicator_name] = None
        
        self.logger.info(f"Forward-fill complete: {len(result_df.columns)} indicators aligned")
        return result_df
    
    def _forward_fill_indicator(
        self,
        data_points: List[Dict[str, Any]],
        trading_days: List[date],
        indicator_name: str
    ) -> pd.Series:
        """
        Forward-fill a single economic indicator to trading days.
        
        Args:
            data_points: List of data points for this indicator
            trading_days: List of trading days to align to
            indicator_name: Name of indicator (for logging)
            
        Returns:
            Series with trading days as index and forward-filled values
        """
        if not data_points:
            return pd.Series(index=pd.to_datetime(trading_days), dtype=float)
        
        # Convert data points to DataFrame and sort by date
        indicator_df = pd.DataFrame(data_points)
        indicator_df['date'] = pd.to_datetime(indicator_df['date'])
        indicator_df = indicator_df.sort_values('date')
        
        # Create result series
        trading_index = pd.to_datetime(trading_days)
        result_series = pd.Series(index=trading_index, dtype=float)
        
        # Forward-fill logic
        current_value = None
        current_date_idx = 0
        
        for trading_day in trading_index:
            # Check if we have a new value for this trading day or earlier
            while (current_date_idx < len(indicator_df) and 
                   indicator_df.iloc[current_date_idx]['date'] <= trading_day):
                current_value = indicator_df.iloc[current_date_idx]['value']
                current_date_idx += 1
            
            # Assign current value (or None if no data available yet)
            result_series[trading_day] = current_value
        
        # Count non-null values for logging
        non_null_count = result_series.count()
        self.logger.debug(f"Forward-filled {indicator_name}: {non_null_count}/{len(trading_days)} days with data")
        
        return result_series
    
    def align_price_with_economic_data(
        self,
        price_data: pd.DataFrame,
        economic_data: pd.DataFrame,
        ticker: str
    ) -> pd.DataFrame:
        """
        Combine price data with forward-filled economic data.
        
        Args:
            price_data: DataFrame with price data (date index, OHLC columns)
            economic_data: DataFrame with economic indicators (date index, indicator columns)
            ticker: Ticker symbol for logging
            
        Returns:
            DataFrame with combined price and economic data
        """
        self.logger.info(f"Aligning price data for {ticker} with economic indicators")
        
        if price_data.empty and economic_data.empty:
            return pd.DataFrame()
        
        # Ensure date indexes
        if hasattr(price_data.index, 'date'):
            price_data.index = price_data.index.date
        if hasattr(economic_data.index, 'date'):
            economic_data.index = economic_data.index.date
        
        # Perform outer join to combine all data
        if not price_data.empty and not economic_data.empty:
            # Both have data - join on trading days that exist in either
            aligned_df = price_data.join(economic_data, how='outer')
        elif not price_data.empty:
            # Only price data - use price trading days
            aligned_df = price_data.copy()
        else:
            # Only economic data - use all economic data dates
            aligned_df = economic_data.copy()
        
        # Sort by date
        aligned_df = aligned_df.sort_index()
        
        self.logger.info(
            f"Aligned data for {ticker}: {len(aligned_df)} rows, "
            f"{len([col for col in aligned_df.columns if 'price' in col.lower() or col in ['open', 'high', 'low', 'close', 'volume']])} price columns, "
            f"{len([col for col in aligned_df.columns if col not in ['open', 'high', 'low', 'close', 'volume', 'adjusted_close']])} economic columns"
        )
        
        return aligned_df
    
    def create_aligned_daily_records(
        self,
        ticker: str,
        aligned_data: pd.DataFrame,
        instrument_id: int,
        trading_calendar: str = 'US'
    ) -> List[Dict[str, Any]]:
        """
        Convert aligned DataFrame into database-ready records.
        
        Args:
            ticker: Ticker symbol
            aligned_data: DataFrame with aligned price and economic data
            instrument_id: Database instrument ID
            trading_calendar: Trading calendar used
            
        Returns:
            List of dictionaries ready for database insertion
        """
        self.logger.info(f"Creating aligned daily records for {ticker}: {len(aligned_data)} rows")
        
        records = []
        
        for trading_date, row in aligned_data.iterrows():
            # Convert pandas Timestamp to date if needed
            if hasattr(trading_date, 'date'):
                trading_date = trading_date.date()
            elif isinstance(trading_date, str):
                trading_date = datetime.strptime(trading_date, '%Y-%m-%d').date()
            
            # Build record dictionary
            record = {
                'date': trading_date,
                'instrument_id': instrument_id,
                'trading_calendar': trading_calendar,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            }
            
            # Add price data (if available)
            price_columns = {
                'open': 'open_price',
                'high': 'high_price', 
                'low': 'low_price',
                'close': 'close_price',
                'adj close': 'adjusted_close',
                'adjusted_close': 'adjusted_close',
                'volume': 'volume'
            }
            
            for df_col, db_col in price_columns.items():
                if df_col in row.index and pd.notna(row[df_col]):
                    record[db_col] = float(row[df_col])
            
            # Add economic indicators (if available)
            economic_columns = {
                'inflation_monthly_us': 'inflation_monthly_us',
                'inflation_index_monthly_us': 'inflation_index_monthly_us',
                'unemployment_monthly_rate_us': 'unemployment_monthly_rate_us',
                'interest_rate_monthly_us': 'interest_rate_monthly_us',
                'inflation_monthly_euro': 'inflation_monthly_euro',
                'unemployment_rate_monthly_euro': 'unemployment_rate_monthly_euro',
                'interest_rate_change_day_euro': 'interest_rate_change_day_euro',
                'interest_rate_monthly_euro': 'interest_rate_monthly_euro'
            }
            
            for df_col, db_col in economic_columns.items():
                if df_col in row.index and pd.notna(row[df_col]):
                    record[db_col] = float(row[df_col])
            
            records.append(record)
        
        self.logger.info(f"Created {len(records)} aligned daily records for {ticker}")
        return records
    
    def get_date_range_for_instrument(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
        exchange: Optional[str] = None
    ) -> List[date]:
        """
        Get appropriate trading calendar for an instrument.
        
        Args:
            ticker: Ticker symbol to detect exchange
            start_date: Start date for range
            end_date: End date for range
            exchange: Optional explicit exchange (overrides detection)
            
        Returns:
            List of trading days for the instrument's exchange
        """
        if not exchange:
            exchange = trading_calendar.detect_exchange_from_ticker(ticker)
        
        self.logger.debug(f"Using {exchange} trading calendar for {ticker}")
        return trading_calendar.get_trading_days(start_date, end_date, exchange)
    
    def validate_aligned_data(self, aligned_df: pd.DataFrame, ticker: str) -> Dict[str, Any]:
        """
        Validate aligned data and return quality metrics.
        
        Args:
            aligned_df: Aligned DataFrame to validate
            ticker: Ticker symbol for logging
            
        Returns:
            Dictionary with validation metrics
        """
        metrics = {
            'ticker': ticker,
            'total_rows': len(aligned_df),
            'date_range': {
                'start': aligned_df.index.min(),
                'end': aligned_df.index.max()
            },
            'price_data_coverage': 0,
            'economic_data_coverage': {},
            'gaps': []
        }
        
        if aligned_df.empty:
            return metrics
        
        # Check price data coverage
        price_cols = ['open', 'high', 'low', 'close', 'volume']
        price_coverage = sum(1 for col in price_cols if col in aligned_df.columns and aligned_df[col].count() > 0)
        metrics['price_data_coverage'] = price_coverage / len(price_cols)
        
        # Check economic data coverage
        economic_cols = [col for col in aligned_df.columns if col not in price_cols]
        for col in economic_cols:
            if col in aligned_df.columns:
                metrics['economic_data_coverage'][col] = {
                    'total_points': aligned_df[col].count(),
                    'coverage_ratio': aligned_df[col].count() / len(aligned_df)
                }
        
        # Check for significant gaps (more than 7 consecutive missing price days)
        if 'close' in aligned_df.columns:
            close_prices = aligned_df['close'].dropna()
            if len(close_prices) > 0:
                gaps = []
                gap_start = None
                
                for i, date_idx in enumerate(aligned_df.index):
                    has_price = pd.notna(aligned_df.loc[date_idx, 'close'])
                    
                    if not has_price and gap_start is None:
                        gap_start = i
                    elif has_price and gap_start is not None:
                        gap_length = i - gap_start
                        if gap_length > 7:
                            gaps.append({
                                'start': aligned_df.index[gap_start],
                                'end': aligned_df.index[i-1],
                                'length': gap_length
                            })
                        gap_start = None
                
                metrics['gaps'] = gaps
        
        return metrics


# Global instance for easy access
forward_fill_transformer = ForwardFillTransformer()