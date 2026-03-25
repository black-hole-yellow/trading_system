import pandas as pd
import pytz

class TimeManager:
    """
    The UTC Anchor. 
    Ensures all historical market data is normalized to UTC 0, completely 
    eliminating Daylight Saving Time (DST) drift across multi-decade backtests.
    """
    def __init__(self, target_tz: str = 'UTC'):
        self.target_tz = target_tz

    def normalize(self, df: pd.DataFrame, source_tz: str) -> pd.DataFrame:
        """
        Converts a DataFrame's naive DatetimeIndex from its local timezone into UTC.
        
        Args:
            df: DataFrame with a naive (no timezone) DatetimeIndex.
            source_tz: The timezone of the raw data (e.g., 'America/New_York' or 'US/Eastern').
            
        Returns:
            DataFrame with a timezone-aware UTC index.
        """
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("DataFrame index must be a DatetimeIndex to normalize time.")

        normalized_df = df.copy()

        # Step 1: Check if the data already has a timezone. 
        if normalized_df.index.tz is None:
            # If naive, we "localize" it. This tells Pandas: "Treat these raw 
            # strings as if they happened in this specific timezone."
            # ambiguous='NaT' handles the rare edge case where a DST "fall back" 
            # hour happens twice, dropping the confusing duplicate.
            try:
                normalized_df.index = normalized_df.index.tz_localize(
                    source_tz, ambiguous='NaT', nonexistent='NaT'
                )
            except Exception as e:
                raise ValueError(f"Failed to localize to {source_tz}: {e}")
        
        # Step 2: Convert to the absolute target timezone (UTC)
        normalized_df.index = normalized_df.index.tz_convert(self.target_tz)
        
        # Step 3: Drop any rows that became 'NaT' (Not a Time) during DST transitions
        original_len = len(normalized_df)
        normalized_df = normalized_df[normalized_df.index.notnull()]
        dropped_rows = original_len - len(normalized_df)
        
        if dropped_rows > 0:
            print(f"TimeManager: Dropped {dropped_rows} ambiguous/nonexistent rows during DST alignment.")

        return normalized_df