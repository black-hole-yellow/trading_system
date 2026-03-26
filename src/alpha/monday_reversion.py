import pandas as pd
from .base_strategy import BaseStrategy
from .features import add_monday_gap_reversion_context

class MondayReversionStrategy(BaseStrategy):
    """
    Production Implementation of Hypothesis H_037_MONDAY_REVERSION.
    Uses the BaseStrategy Exit Engine for ATR stops, Dynamic RR, and Time Stops.
    """
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df_1h = df.resample('1h', closed='left', label='left').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'
        }).dropna()
        
        df_1h = add_monday_gap_reversion_context(df_1h)
        
        raw_signals_1h = pd.Series(0.0, index=df_1h.index)
        raw_signals_1h.loc[df_1h['Monday_Reversion_Long'] == 1] = 1.0
        raw_signals_1h.loc[df_1h['Monday_Reversion_Short'] == 1] = -1.0
        
        # Map to 15m for entry precision
        raw_signals_15m = raw_signals_1h.reindex(df.index).fillna(0.0)
        
        # Pass 15m data to Exit Engine
        executed_15m = self.apply_exits(df, raw_signals_15m)
        
        return executed_15m[['target_position', 'sl_price', 'tp_price', 'exact_exit_price']]