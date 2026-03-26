import pandas as pd
import numpy as np
from scipy.stats import linregress
import pytz

# --- Helper Functions for Hypothesis 018 ---
def add_previous_boundaries(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates the Previous Weekly High (PWH) and Low (PWL)."""
    weekly = df.resample('W', closed='left', label='left').agg({'high': 'max', 'low': 'min'})
    weekly['PWH'] = weekly['high'].shift(1)
    weekly['PWL'] = weekly['low'].shift(1)
    df['PWH'] = weekly['PWH'].reindex(df.index).ffill()
    df['PWL'] = weekly['PWL'].reindex(df.index).ffill()
    return df

def add_williams_fractals(df: pd.DataFrame) -> pd.DataFrame:
    """Identifies standard 5-bar Williams Fractals."""
    df['Fractal_High'] = (df['high'] > df['high'].shift(1)) & (df['high'] > df['high'].shift(2)) & \
                         (df['high'] > df['high'].shift(-1)) & (df['high'] > df['high'].shift(-2))
    df['Fractal_Low'] = (df['low'] < df['low'].shift(1)) & (df['low'] < df['low'].shift(2)) & \
                        (df['low'] < df['low'].shift(-1)) & (df['low'] < df['low'].shift(-2))
    return df

def add_confirmed_fractals(df: pd.DataFrame) -> pd.DataFrame:
    """Shifts fractals by 2 bars to prevent forward-looking bias."""
    df['Confirmed_Fractal_High'] = df['Fractal_High'].shift(2).astype(int)
    df['Confirmed_Fractal_High_Price'] = df['high'].shift(2)
    df['Confirmed_Fractal_Low'] = df['Fractal_Low'].shift(2).astype(int)
    df['Confirmed_Fractal_Low_Price'] = df['low'].shift(2)
    return df

# --- Your Core Hypothesis Features ---
def add_1w_level_rejection_context(df: pd.DataFrame, max_dist_pips: int = 20) -> pd.DataFrame:
    pip_size = 0.0001
    tol = max_dist_pips * pip_size
    
    if 'PWL' not in df.columns: 
        df = add_previous_boundaries(df)
    if 'Confirmed_Fractal_Low' not in df.columns:
        if 'Fractal_Low' not in df.columns: 
            df = add_williams_fractals(df)
        df = add_confirmed_fractals(df)

    fractal_low = (df['Confirmed_Fractal_Low'].fillna(0) == 1)
    fractal_high = (df['Confirmed_Fractal_High'].fillna(0) == 1)
    
    tap_pwl = df['Confirmed_Fractal_Low_Price'] <= (df['PWL'] + tol)
    tap_pwh = df['Confirmed_Fractal_High_Price'] >= (df['PWH'] - tol)
              
    # Timezone handling for UA Hour
    kyiv_time = df.index.tz_localize('UTC').tz_convert('Europe/Kyiv') if df.index.tz is None else df.index.tz_convert('Europe/Kyiv')
    df['UA_Hour'] = kyiv_time.hour
    
    is_active = (df['UA_Hour'] >= 10) & (df['UA_Hour'] <= 21)
    
    rej_long = is_active & fractal_low & tap_pwl
    rej_short = is_active & fractal_high & tap_pwh
    
    df['Date'] = kyiv_time.date
    rej_count = (rej_long | rej_short).groupby(df['Date']).cumsum()
    
    df['First_1W_Rej_Long'] = (rej_long & (rej_count == 1)).astype(int)
    df['First_1W_Rej_Short'] = (rej_short & (rej_count == 1)).astype(int)
    df.drop(columns=['Date', 'UA_Hour'], inplace=True)
    return df

def add_htf_trend_probability(df: pd.DataFrame, htf: str = '4h', lookback: int = 60) -> pd.DataFrame:
    # 1. Resample to 4H
    htf_df = df.resample(htf, closed='left', label='left').agg({
        'open':'first', 'high':'max', 'low':'min', 'close':'last'
    }).dropna()

    slopes, r_squareds = [], []
    for i in range(len(htf_df)):
        if i < lookback:
            slopes.append(0)
            r_squareds.append(0)
            continue
        y = htf_df['close'].iloc[i-lookback:i].values
        x = np.arange(lookback)
        slope, intercept, r_value, p_value, std_err = linregress(x, y)
        slopes.append(slope)
        r_squareds.append(r_value**2)

    htf_df['Slope'] = slopes
    htf_df['R2'] = r_squareds
    htf_df['Stat_Score'] = np.where(htf_df['Slope'] > 0, 50 * htf_df['R2'], 0)

    # Calculate HTF Fractals
    n = 2
    htf_df['Fractal_Up'] = False
    htf_df['Fractal_Down'] = False
    
    for i in range(2*n, len(htf_df)):
        window_high = htf_df['high'].iloc[i - 2*n : i + 1]
        window_low = htf_df['low'].iloc[i - 2*n : i + 1]
        mid_idx = i - n
        
        if htf_df['high'].iloc[mid_idx] == window_high.max():
            htf_df.iat[mid_idx, htf_df.columns.get_loc('Fractal_Up')] = True
        if htf_df['low'].iloc[mid_idx] == window_low.min():
            htf_df.iat[mid_idx, htf_df.columns.get_loc('Fractal_Down')] = True

    struct_scores = []
    last_up_1, last_up_2, last_down_1, last_down_2 = None, None, None, None

    for i in range(len(htf_df)):
        check_idx = i - n
        if check_idx >= 0:
            if htf_df['Fractal_Up'].iloc[check_idx]:
                last_up_2, last_up_1 = last_up_1, htf_df['high'].iloc[check_idx]
            if htf_df['Fractal_Down'].iloc[check_idx]:
                last_down_2, last_down_1 = last_down_1, htf_df['low'].iloc[check_idx]

        score = 25 
        if last_up_1 and last_up_2 and last_down_1 and last_down_2:
            hh = last_up_1 > last_up_2
            hl = last_down_1 > last_down_2
            lh = last_up_1 < last_up_2
            ll = last_down_1 < last_down_2
            if hh and hl: score = 50 
            elif lh and ll: score = 0 
        struct_scores.append(score)

    htf_df['Struct_Score'] = struct_scores
    # Shift by 1 to prevent forward-looking bias, align to execution time
    htf_df['HTF_Bullish_Prob'] = (htf_df['Stat_Score'] + htf_df['Struct_Score']).shift(1)
    
    # Map back to the main DataFrame
    df = df.join(htf_df[['HTF_Bullish_Prob']])
    df['HTF_Bullish_Prob'] = df['HTF_Bullish_Prob'].ffill().fillna(50.0).round(1)
    return df