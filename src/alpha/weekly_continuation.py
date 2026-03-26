import pandas as pd
from .base_strategy import BaseStrategy
from .features import add_1w_level_rejection_context, add_htf_trend_probability

class WeeklyContinuationStrategy(BaseStrategy):
    """
    Production Implementation of Hypothesis H_018_WEEKLY_LEVEL_CONTINUATION.
    Combines Weekly Boundary Sweeps, 1H Fractals, and 4H Trend Probabilities.
    """
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        # 1. Base Resolution for this strategy is 1H (per your JSON)
        df_1h = df.resample('1h', closed='left', label='left').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'
        }).dropna()
        
        # 2. Apply the heavy Multi-Timeframe Feature logic
        df_1h = add_1w_level_rejection_context(df_1h, max_dist_pips=20)
        df_1h = add_htf_trend_probability(df_1h, htf='4h', lookback=60)
        
        # 3. Apply the specific Entry Rules defined in your JSON
        raw_signals_1h = pd.Series(0.0, index=df_1h.index)
        
        # LONG CONDITION: First Rejection == 1 AND HTF Prob >= 55
        long_cond = (df_1h['First_1W_Rej_Long'] == 1) & (df_1h['HTF_Bullish_Prob'] >= 55)
        raw_signals_1h.loc[long_cond] = 1.0
        
        # SHORT CONDITION: First Rejection == 1 AND HTF Prob <= 45
        short_cond = (df_1h['First_1W_Rej_Short'] == 1) & (df_1h['HTF_Bullish_Prob'] <= 45)
        raw_signals_1h.loc[short_cond] = -1.0
        
        # 4. Map the 1H signal back to the precise 15m execution grid
        raw_signals_15m = raw_signals_1h.reindex(df.index).fillna(0.0)
        
        # 5. Hand off to the high-performance Execution Engine 
        # (This automatically applies your slippage-adjusted 2RR/3RR logic!)
        executed_15m = self.apply_exits(df, raw_signals_15m)
        
        return executed_15m[['target_position', 'sl_price', 'tp_price', 'exact_exit_price']]