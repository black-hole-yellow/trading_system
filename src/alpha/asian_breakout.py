import pandas as pd
import numpy as np
from .base_strategy import BaseStrategy

class AsianSessionBreakout(BaseStrategy):
    """
    Goes long if price breaks above the Asian session high.
    Goes short if price breaks below the Asian session low.
    Flattens position at the end of the NY session.
    """
    
    def generate_signals(self, market_data: pd.DataFrame) -> pd.DataFrame:
        df = market_data.copy()
        
        # 1. Extract parameters defined in the Hypothesis Lab
        asian_start = self.parameters.get('asian_start_hour', 23)
        asian_end = self.parameters.get('asian_end_hour', 7)
        
        # 2. Identify the Asian Session
        if asian_start > asian_end: # Handles overnight (e.g., 23:00 to 07:00)
            is_asian = (df.index.hour >= asian_start) | (df.index.hour < asian_end)
        else:
            is_asian = (df.index.hour >= asian_start) & (df.index.hour < asian_end)
            
        # 3. Calculate Session High/Low per FX Trading Day
        # Shifting index by +2 hours makes the 22:00 UTC NY Close align with midnight.
        # This allows us to cleanly group by standard dates.
        trade_day = (df.index + pd.Timedelta(hours=2)).date
        
        # Calculate the max/min only during Asian hours, then broadcast to the whole day
        df['asian_high'] = df['high'].where(is_asian).groupby(trade_day).transform('max')
        df['asian_low'] = df['low'].where(is_asian).groupby(trade_day).transform('min')
        
        # 4. Generate Target Positions
        is_active_session = ~is_asian
        signals = pd.Series(0, index=df.index)
        
        # Long condition: 15m close breaks above Asian High
        long_cond = is_active_session & (df['close'] > df['asian_high'])
        # Short condition: 15m close breaks below Asian Low
        short_cond = is_active_session & (df['close'] < df['asian_low'])
        
        # Set target states
        signals.loc[long_cond] = 1
        signals.loc[short_cond] = -1
        
        # Output clean DataFrame aligned with Pillar 1 data
        return pd.DataFrame({'target_position': signals}, index=df.index)

# Example Config injected from your Lab:
config = {
     "universe": ["GBPUSD"],
     "parameters": {
         "asian_start_hour": 23,
         "asian_end_hour": 7
     }
 }