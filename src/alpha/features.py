import pandas as pd

def add_monday_gap_reversion_context(df: pd.DataFrame) -> pd.DataFrame:
    """
    Hypothesis: H_037_MONDAY_REVERSION
    Calculates the condition for an institutional reset on Monday open 
    following an extreme (> 2 ATR) weekly trend.
    
    WARNING: This feature requires a 1-Hour (1H) DataFrame.
    """
    # 1. Calculate Volatility (14-day average of the Daily Range)
    daily_range = df['high'].rolling(24).max() - df['low'].rolling(24).min()
    atr_14d = daily_range.rolling(336).mean()
    
    # 2. Calculate the Weekly Delta (Close now vs. Open 120 hours / 5 days ago)
    weekly_delta = df['close'] - df['open'].shift(120)
    
    # 3. Identify Monday Open (00:00 UTC)
    is_monday_open = (df.index.dayofweek == 0) & (df.index.hour == 0)
    
    # 4. Generate the Deterministic Binary Features (1 or 0)
    df['Monday_Reversion_Short'] = (is_monday_open & (weekly_delta > (2 * atr_14d))).astype(int)
    df['Monday_Reversion_Long'] = (is_monday_open & (weekly_delta < -(2 * atr_14d))).astype(int)
    
    return df