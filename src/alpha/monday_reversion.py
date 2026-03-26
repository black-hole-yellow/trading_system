import pandas as pd
from .base_strategy import BaseStrategy
from .features import add_monday_gap_reversion_context

class MondayReversionStrategy(BaseStrategy):
    """
    Production Implementation of Hypothesis H_037_MONDAY_REVERSION.
    Uses the BaseStrategy Exit Engine for ATR stops, Dynamic RR, and Time Stops.
    """
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        # 1. Roll up to 1H ONLY for feature calculation
        df_1h = df.resample('1h', closed='left', label='left').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'
        }).dropna()
        
        # 2. Apply Feature Library Logic
        df_1h = add_monday_gap_reversion_context(df_1h)
        
        # 3. Generate Raw Triggers on the 1H grid
        raw_signals_1h = pd.Series(0.0, index=df_1h.index)
        raw_signals_1h.loc[df_1h['Monday_Reversion_Long'] == 1] = 1.0
        raw_signals_1h.loc[df_1h['Monday_Reversion_Short'] == 1] = -1.0
        
        # 4. THE FIX: Map to the 15m grid BEFORE running Exits.
        # This locks the SL and TP math to the exact 15m Entry Price.
        raw_signals_15m = raw_signals_1h.reindex(df.index).fillna(0.0)
        
        # 5. Pass the 15m data directly to the high-performance Exit Engine
        executed_15m = self.apply_exits(df, raw_signals_15m)
        
        # Return the finalized signals
        return executed_15m[['target_position', 'sl_price', 'tp_price']]