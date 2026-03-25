import pandas as pd
import numpy as np
from typing import Tuple, Dict

class DataValidator:
    """
    The Sanity Gate for raw market data.
    Runs strict checks on OHLCV data to detect anomalies, missing bars, and impossible prices.
    """
    def __init__(self, max_pct_change: float = 0.03, timeframe_minutes: int = 15):
        self.max_pct_change = max_pct_change
        self.timeframe = pd.Timedelta(minutes=timeframe_minutes)

    def validate(self, df: pd.DataFrame, symbol: str) -> Tuple[pd.DataFrame, Dict]:
        """
        Runs the full suite of validations on a standard OHLCV DataFrame.
        Assumes the DataFrame index is a DatetimeIndex.
        
        Returns:
            Tuple containing the (Cleaned DataFrame, Health Report Dictionary)
        """
        report = {"symbol": symbol, "initial_rows": len(df), "issues_found": False}
        clean_df = df.copy()

        # 1. The NaN Check
        nan_counts = clean_df.isna().sum()
        if nan_counts.sum() > 0:
            report['issues_found'] = True
            report['nan_rows_dropped'] = int(nan_counts.sum())
            # Drop NaNs to prevent math errors downstream
            clean_df.dropna(inplace=True)
        else:
            report['nan_rows_dropped'] = 0

        # 2. The Zero/Negative Price Check
        # Prices cannot be 0 or negative. 
        price_cols = [col for col in ['open', 'high', 'low', 'close'] if col in clean_df.columns]
        invalid_prices = (clean_df[price_cols] <= 0).any(axis=1)
        
        if invalid_prices.sum() > 0:
            report['issues_found'] = True
            report['invalid_price_rows_dropped'] = int(invalid_prices.sum())
            clean_df = clean_df[~invalid_prices]
        else:
            report['invalid_price_rows_dropped'] = 0

        # 3. The Outlier Check (Fat Finger / Bad Ticks)
        if 'close' in clean_df.columns:
            pct_change = clean_df['close'].pct_change().abs()
            outliers = pct_change > self.max_pct_change
            if outliers.sum() > 0:
                report['issues_found'] = True
                report['outlier_spikes_detected'] = int(outliers.sum())
                # For simplicity, we drop these bad ticks. In a more complex system, 
                # you might interpolate them based on surrounding bars.
                clean_df = clean_df[~outliers]
            else:
                report['outlier_spikes_detected'] = 0

        # 4. The Continuity Check (Missing Time Intervals)
        # Ensure the index is sorted chronologically first
        clean_df.sort_index(inplace=True)
        time_diffs = clean_df.index.to_series().diff()
        
        # Any gap larger than our expected timeframe indicates missing bars
        # Note: This will naturally flag weekends, which is expected in FX. 
        # A true pipeline filters out weekends entirely before this check, 
        # but tracking large gaps is still critical metadata.
        gaps = time_diffs > self.timeframe
        report['time_gaps_detected'] = int(gaps.sum())
        
        if report['time_gaps_detected'] > 0:
            report['issues_found'] = True

        report['final_rows'] = len(clean_df)
        
        return clean_df, report