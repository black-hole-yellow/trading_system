import pandas as pd
from .base_strategy import BaseStrategy
from .features import add_monday_gap_reversion_context

class MondayReversionStrategy(BaseStrategy):
    """
    Production Implementation of Hypothesis H_037_MONDAY_REVERSION.
    Uses the BaseStrategy Exit Engine for ATR stops, Dynamic RR, and Time Stops.
    """
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        # 1. Roll up to 1H for feature calculation AND exit evaluation
        df_1h = df.resample('1h', closed='left', label='left').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'
        }).dropna()
        
        # 2. Apply Feature Library Logic
        df_1h = add_monday_gap_reversion_context(df_1h)
        
        # 3. Generate Raw Triggers on the 1H grid
        raw_signals_1h = pd.Series(0.0, index=df_1h.index)
        raw_signals_1h.loc[df_1h['Monday_Reversion_Long'] == 1] = 1.0
        raw_signals_1h.loc[df_1h['Monday_Reversion_Short'] == 1] = -1.0
        
        # 4. Pass the 1H data directly to the Universal Exit Engine!
        # This evaluates the Highs/Lows of the 1H candles, which is vastly faster.
        # (Note: We removed the *4 multiplier for max_hold_bars because we are back on 1H time)
        executed_1h = self.apply_exits(df_1h, raw_signals_1h)
        
        # 5. Map the finalized 1H target positions and SL/TP back down to the 15m execution grid
        signals_15m = pd.DataFrame(index=df.index)
        
        # Forward fill the state so the 15m engine knows exactly what to hold and where the stops are
        signals_15m['target_position'] = executed_1h['target_position'].reindex(df.index).ffill().fillna(0.0)
        signals_15m['sl_price'] = executed_1h['sl_price'].reindex(df.index).ffill()
        signals_15m['tp_price'] = executed_1h['tp_price'].reindex(df.index).ffill()
        
        # Safety cleanup: If target position is 0, ensure SL/TP are NaN so we don't accidentally exit future trades
        is_flat = signals_15m['target_position'] == 0.0
        signals_15m.loc[is_flat, 'sl_price'] = float('nan')
        signals_15m.loc[is_flat, 'tp_price'] = float('nan')
        
        return signals_15m[['target_position', 'sl_price', 'tp_price']]