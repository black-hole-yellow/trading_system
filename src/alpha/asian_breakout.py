import pandas as pd
import numpy as np
from .base_strategy import BaseStrategy

class AsianSessionBreakout(BaseStrategy):
    """
    Production Asian Session Breakout.
    - Captures the High/Low of the defined Asian Session.
    - Enters on breakouts during the London/NY session.
    - Flattens positions at the end of the day.
    - Optional: Uses the MTF engine to align with the 4H trend.
    """
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        signals = pd.DataFrame(index=df.index)
        signals['target_position'] = 0.0

        # 1. Extract parameters from config
        asian_start = self.parameters.get('asian_start_hour', 23)
        asian_end = self.parameters.get('asian_end_hour', 7)
        exit_hour = self.parameters.get('exit_hour', 21) # EOD Flatten
        
        use_trend_filter = self.parameters.get('use_trend_filter', False)
        trend_tf = self.parameters.get('trend_timeframe', '4h')

        # 2. Identify the Asian Session 
        is_asian_session = (df.index.hour >= asian_start) | (df.index.hour < asian_end)

        # 3. Calculate Session High/Low robustly across midnight
        # We offset the "day" by the asian_end hour so the grouping logic doesn't break at midnight UTC
        offset_hours = 24 - asian_end
        trading_day = (df.index + pd.Timedelta(hours=offset_hours)).date
        
        asian_high = df['high'].where(is_asian_session).groupby(trading_day).transform('max')
        asian_low = df['low'].where(is_asian_session).groupby(trading_day).transform('min')
        
        # Forward-fill the levels so they remain active as lines to cross during London/NY
        signals['asian_high'] = asian_high.ffill()
        signals['asian_low'] = asian_low.ffill()

        # 4. Multi-Timeframe Trend Filter (The Anchor)
        if use_trend_filter:
            mtf_df = self.get_mtf_data(df, trend_tf)
            mtf_close = mtf_df[f'mtf_{trend_tf}_close']
            
            # Simple MTF filter: 15m price must be above 4H close to long, below to short
            trend_up = df['close'] > mtf_close
            trend_down = df['close'] < mtf_close
        else:
            trend_up = pd.Series(True, index=df.index)
            trend_down = pd.Series(True, index=df.index)

        # 5. Core Entry & Exit Logic
        # Breakout occurs outside Asian hours, breaking the session levels, in direction of HTF trend
        long_entry = (~is_asian_session) & (df['close'] > signals['asian_high']) & trend_up
        short_entry = (~is_asian_session) & (df['close'] < signals['asian_low']) & trend_down
        
        # Flatten before the rollover / spread widening at 21:00 UTC
        time_exit = df.index.hour == exit_hour

        # 6. Apply States to Target Position
        signals.loc[long_entry, 'target_position'] = 1.0
        signals.loc[short_entry, 'target_position'] = -1.0
        signals.loc[time_exit, 'target_position'] = 0.0

        # 7. State Persistence (Hold the trade until exit)
        # We temporarily replace 0 with NaN so we can forward-fill the active 1 or -1 states
        signals['target_position'] = signals['target_position'].replace(0.0, np.nan).ffill().fillna(0.0)
        
        # Hard constraint: Never hold positions inside the Asian Session range building phase
        signals.loc[is_asian_session, 'target_position'] = 0.0

        # Return strictly the normalized execution column
        return signals[['target_position']]