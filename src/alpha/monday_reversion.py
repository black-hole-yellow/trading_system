import pandas as pd
from .base_strategy import BaseStrategy
from .features import add_monday_gap_reversion_context

class MondayReversionStrategy(BaseStrategy):
    """
    Production Implementation of Hypothesis H_037_MONDAY_REVERSION.
    Uses the BaseStrategy Exit Engine for ATR stops, Dynamic RR, and Time Stops.
    """
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        # 1. Roll up to 1H as required by the JSON
        df_1h = df.resample('1h', closed='left', label='left').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'
        }).dropna()
        
        # 2. Apply Feature Library Logic
        df_1h = add_monday_gap_reversion_context(df_1h)
        
        # 3. Generate Raw Triggers (+1 for Long, -1 for Short, 0 for Flat)
        raw_signals = pd.Series(0.0, index=df_1h.index)
        raw_signals.loc[df_1h['Monday_Reversion_Long'] == 1] = 1.0
        raw_signals.loc[df_1h['Monday_Reversion_Short'] == 1] = -1.0
        
        # 4. Pass to the Universal Exit Engine!
        # This automatically applies your max hold bars, ATR stops, and Thursday 3RR target.
        executed_1h = self.apply_exits(df_1h, raw_signals)
        
        # 5. Map the 1H target positions back down to the 15m execution grid
        signals_15m = pd.DataFrame(index=df.index)
        signals_15m['target_position'] = executed_1h['target_position'].reindex(df.index).ffill().fillna(0.0)
        
        return signals_15m[['target_position']]