"""
Data alignment functions for handling different asset date frequencies.

This module provides functionality to align data from different sources that have
different date frequencies:
- Daily data: Stock prices, index values
- Monthly data: Economic indicators (inflation, unemployment, interest rates)
- Quarterly/Annual data: Financial statements

The alignment system enables meaningful analysis across different time scales.
"""

from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
from enum import Enum

from ..utils.logging import get_logger
from .models import Frequency


class AlignmentMethod(Enum):
    """Methods for aligning data with different frequencies."""
    FORWARD_FILL = "forward_fill"  # Use the most recent value
    BACKWARD_FILL = "backward_fill"  # Use the next available value
    LINEAR_INTERPOLATE = "linear_interpolate"  # Linear interpolation between points
    NEAREST = "nearest"  # Use the nearest available value
    FIRST_OF_PERIOD = "first_of_period"  # Use first day of the period
    LAST_OF_PERIOD = "last_of_period"  # Use last day of the period


class DataAligner:
    """
    Aligns data from different frequencies to enable cross-asset analysis.
    
    Handles the alignment of:
    - Daily stock prices with monthly economic indicators
    - Quarterly financial statements with daily prices
    - Different economic indicators with varying frequencies
    """
    
    def __init__(self):
        self.logger = get_logger(__name__)
    
    def align_daily_to_monthly(
        self, 
        daily_data: List[Dict[str, Any]], 
        monthly_data: List[Dict[str, Any]],
        alignment_method: AlignmentMethod = AlignmentMethod.LAST_OF_PERIOD
    ) -> List[Dict[str, Any]]:
        """
        Align daily data (e.g., stock prices) with monthly data (e.g., economic indicators).
        
        Args:
            daily_data: List of daily data points with 'date' and other fields
            monthly_data: List of monthly data points with 'date' and other fields
            alignment_method: Method to use for alignment
            
        Returns:
            List of aligned data points combining both datasets
        """
        self.logger.info(f"Aligning {len(daily_data)} daily points with {len(monthly_data)} monthly points")
        
        if not daily_data or not monthly_data:
            self.logger.warning("Empty data provided for alignment")
            return []
        
        # Convert to DataFrames for easier manipulation
        daily_df = pd.DataFrame(daily_data)
        monthly_df = pd.DataFrame(monthly_data)
        
        # Ensure date columns are datetime
        daily_df['date'] = pd.to_datetime(daily_df['date'])
        monthly_df['date'] = pd.to_datetime(monthly_df['date'])
        
        # Sort by date
        daily_df = daily_df.sort_values('date')
        monthly_df = monthly_df.sort_values('date')
        
        aligned_data = []
        
        if alignment_method == AlignmentMethod.LAST_OF_PERIOD:
            # For each monthly data point, find the last daily data point in that month
            aligned_data = self._align_last_of_period(daily_df, monthly_df)
        elif alignment_method == AlignmentMethod.FIRST_OF_PERIOD:
            # For each monthly data point, find the first daily data point in that month
            aligned_data = self._align_first_of_period(daily_df, monthly_df)
        elif alignment_method == AlignmentMethod.FORWARD_FILL:
            # Forward fill monthly data to daily frequency
            aligned_data = self._align_forward_fill(daily_df, monthly_df)
        elif alignment_method == AlignmentMethod.NEAREST:
            # Use nearest monthly data point for each daily point
            aligned_data = self._align_nearest(daily_df, monthly_df)
        else:
            raise ValueError(f"Alignment method {alignment_method} not implemented")
        
        self.logger.info(f"Alignment complete: {len(aligned_data)} aligned data points")
        return aligned_data
    
    def _align_last_of_period(self, daily_df: pd.DataFrame, monthly_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Align using last daily data point of each month."""
        aligned_data = []
        
        for _, monthly_row in monthly_df.iterrows():
            month_start = monthly_row['date'].replace(day=1)
            next_month = month_start + relativedelta(months=1)
            
            # Find daily data in this month
            month_daily = daily_df[
                (daily_df['date'] >= month_start) & 
                (daily_df['date'] < next_month)
            ]
            
            if not month_daily.empty:
                # Use the last daily data point of the month
                last_daily = month_daily.iloc[-1]
                
                # Combine data
                aligned_point = {
                    'date': last_daily['date'].strftime('%Y-%m-%d'),
                    'year_month': month_start.strftime('%Y-%m'),
                }
                
                # Add daily data fields (prefixed with 'daily_')
                for col in daily_df.columns:
                    if col != 'date':
                        aligned_point[f'daily_{col}'] = last_daily[col]
                
                # Add monthly data fields (prefixed with 'monthly_')
                for col in monthly_df.columns:
                    if col != 'date':
                        aligned_point[f'monthly_{col}'] = monthly_row[col]
                
                aligned_data.append(aligned_point)
        
        return aligned_data
    
    def _align_first_of_period(self, daily_df: pd.DataFrame, monthly_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Align using first daily data point of each month."""
        aligned_data = []
        
        for _, monthly_row in monthly_df.iterrows():
            month_start = monthly_row['date'].replace(day=1)
            next_month = month_start + relativedelta(months=1)
            
            # Find daily data in this month
            month_daily = daily_df[
                (daily_df['date'] >= month_start) & 
                (daily_df['date'] < next_month)
            ]
            
            if not month_daily.empty:
                # Use the first daily data point of the month
                first_daily = month_daily.iloc[0]
                
                # Combine data
                aligned_point = {
                    'date': first_daily['date'].strftime('%Y-%m-%d'),
                    'year_month': month_start.strftime('%Y-%m'),
                }
                
                # Add daily data fields
                for col in daily_df.columns:
                    if col != 'date':
                        aligned_point[f'daily_{col}'] = first_daily[col]
                
                # Add monthly data fields
                for col in monthly_df.columns:
                    if col != 'date':
                        aligned_point[f'monthly_{col}'] = monthly_row[col]
                
                aligned_data.append(aligned_point)
        
        return aligned_data
    
    def _align_forward_fill(self, daily_df: pd.DataFrame, monthly_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Forward fill monthly data to daily frequency."""
        aligned_data = []
        
        # Create a complete date range from daily data
        start_date = daily_df['date'].min()
        end_date = daily_df['date'].max()
        
        for _, daily_row in daily_df.iterrows():
            current_date = daily_row['date']
            
            # Find the most recent monthly data point
            available_monthly = monthly_df[monthly_df['date'] <= current_date]
            
            if not available_monthly.empty:
                latest_monthly = available_monthly.iloc[-1]
                
                # Combine data
                aligned_point = {
                    'date': current_date.strftime('%Y-%m-%d'),
                    'year_month': current_date.strftime('%Y-%m'),
                }
                
                # Add daily data fields
                for col in daily_df.columns:
                    if col != 'date':
                        aligned_point[f'daily_{col}'] = daily_row[col]
                
                # Add monthly data fields (forward filled)
                for col in monthly_df.columns:
                    if col != 'date':
                        aligned_point[f'monthly_{col}'] = latest_monthly[col]
                
                aligned_data.append(aligned_point)
        
        return aligned_data
    
    def _align_nearest(self, daily_df: pd.DataFrame, monthly_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Align using nearest monthly data point."""
        aligned_data = []
        
        for _, daily_row in daily_df.iterrows():
            current_date = daily_row['date']
            
            # Calculate distance to all monthly data points
            monthly_df['distance'] = abs((monthly_df['date'] - current_date).dt.days)
            nearest_monthly = monthly_df.loc[monthly_df['distance'].idxmin()]
            
            # Only align if the nearest point is within reasonable distance (e.g., 45 days)
            if nearest_monthly['distance'] <= 45:
                aligned_point = {
                    'date': current_date.strftime('%Y-%m-%d'),
                    'year_month': current_date.strftime('%Y-%m'),
                    'distance_days': int(nearest_monthly['distance'])
                }
                
                # Add daily data fields
                for col in daily_df.columns:
                    if col != 'date':
                        aligned_point[f'daily_{col}'] = daily_row[col]
                
                # Add monthly data fields
                for col in monthly_df.columns:
                    if col not in ['date', 'distance']:
                        aligned_point[f'monthly_{col}'] = nearest_monthly[col]
                
                aligned_data.append(aligned_point)
        
        # Clean up temporary column
        monthly_df.drop('distance', axis=1, inplace=True)
        
        return aligned_data
    
    def align_multiple_frequencies(
        self,
        data_sets: List[Tuple[List[Dict[str, Any]], Frequency, str]],
        target_frequency: Frequency = Frequency.MONTHLY,
        alignment_method: AlignmentMethod = AlignmentMethod.LAST_OF_PERIOD
    ) -> List[Dict[str, Any]]:
        """
        Align multiple datasets with different frequencies to a target frequency.
        
        Args:
            data_sets: List of tuples (data, frequency, prefix) for each dataset
            target_frequency: Target frequency for alignment
            alignment_method: Method to use for alignment
            
        Returns:
            List of aligned data points
        """
        self.logger.info(f"Aligning {len(data_sets)} datasets to {target_frequency.value} frequency")
        
        if not data_sets:
            return []
        
        # Find the dataset with the target frequency to use as base
        base_data = None
        other_data_sets = []
        
        for data, freq, prefix in data_sets:
            if freq == target_frequency:
                if base_data is None:
                    base_data = (data, freq, prefix)
                else:
                    # If multiple datasets have target frequency, treat as additional data
                    other_data_sets.append((data, freq, prefix))
            else:
                other_data_sets.append((data, freq, prefix))
        
        if base_data is None:
            # No dataset has target frequency, use the first dataset as base
            base_data = data_sets[0]
            other_data_sets = data_sets[1:]
        
        # Start with base data
        aligned_results = []
        base_df = pd.DataFrame(base_data[0])
        base_df['date'] = pd.to_datetime(base_df['date'])
        
        for _, row in base_df.iterrows():
            aligned_point = {
                'date': row['date'].strftime('%Y-%m-%d'),
                'year_month': row['date'].strftime('%Y-%m'),
            }
            
            # Add base data fields
            for col in base_df.columns:
                if col != 'date':
                    aligned_point[f'{base_data[2]}_{col}'] = row[col]
            
            aligned_results.append(aligned_point)
        
        # Align each additional dataset
        for other_data, other_freq, other_prefix in other_data_sets:
            if other_freq == Frequency.DAILY and target_frequency == Frequency.MONTHLY:
                # Align daily to monthly
                aligned_results = self._merge_aligned_data(
                    aligned_results,
                    self.align_daily_to_monthly(other_data, base_data[0], alignment_method),
                    other_prefix
                )
            elif other_freq == Frequency.MONTHLY and target_frequency == Frequency.MONTHLY:
                # Align monthly to monthly (direct merge by date)
                aligned_results = self._merge_monthly_data(aligned_results, other_data, other_prefix)
            else:
                self.logger.warning(f"Alignment from {other_freq.value} to {target_frequency.value} not yet implemented")
        
        return aligned_results
    
    def _merge_aligned_data(
        self, 
        base_aligned: List[Dict[str, Any]], 
        new_aligned: List[Dict[str, Any]], 
        prefix: str
    ) -> List[Dict[str, Any]]:
        """Merge newly aligned data into base aligned data."""
        # Convert to DataFrames for easier merging
        base_df = pd.DataFrame(base_aligned)
        new_df = pd.DataFrame(new_aligned)
        
        if base_df.empty or new_df.empty:
            return base_aligned
        
        # Merge on date
        merged_df = base_df.merge(new_df, on=['date', 'year_month'], how='left')
        
        return merged_df.to_dict('records')
    
    def _merge_monthly_data(
        self, 
        base_aligned: List[Dict[str, Any]], 
        monthly_data: List[Dict[str, Any]], 
        prefix: str
    ) -> List[Dict[str, Any]]:
        """Merge monthly data directly by matching year-month."""
        monthly_df = pd.DataFrame(monthly_data)
        monthly_df['date'] = pd.to_datetime(monthly_df['date'])
        monthly_df['year_month'] = monthly_df['date'].dt.strftime('%Y-%m')
        
        # Create lookup dictionary
        monthly_lookup = {}
        for _, row in monthly_df.iterrows():
            year_month = row['year_month']
            monthly_lookup[year_month] = {
                f'{prefix}_{col}': row[col] 
                for col in monthly_df.columns 
                if col not in ['date', 'year_month']
            }
        
        # Add monthly data to aligned results
        for aligned_point in base_aligned:
            year_month = aligned_point.get('year_month')
            if year_month in monthly_lookup:
                aligned_point.update(monthly_lookup[year_month])
        
        return base_aligned
    
    def calculate_date_coverage(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calculate date coverage statistics for a dataset.
        
        Args:
            data: List of data points with 'date' field
            
        Returns:
            Dictionary with coverage statistics
        """
        if not data:
            return {
                'total_points': 0,
                'date_range': None,
                'coverage_days': 0
            }
        
        dates = [datetime.strptime(point['date'], '%Y-%m-%d').date() for point in data]
        dates.sort()
        
        first_date = dates[0]
        last_date = dates[-1]
        total_days = (last_date - first_date).days + 1
        
        return {
            'total_points': len(data),
            'first_date': first_date.strftime('%Y-%m-%d'),
            'last_date': last_date.strftime('%Y-%m-%d'),
            'date_range_days': total_days,
            'coverage_percentage': (len(dates) / total_days) * 100 if total_days > 0 else 0,
            'missing_days': total_days - len(dates)
        }