import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from typing import Dict, Any
import pytz

class BaseStrategy(ABC):
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.universe = config.get("universe", {}).get("instruments", [])
        self.parameters = config.get("parameters", {})
        self.execution = config.get("execution", {})

    @abstractmethod
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    def get_mtf_data(self, df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        # (Keep your existing MTF code here)
        mtf_df = df.resample(timeframe, closed='left', label='left').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'
        })
        mtf_df = mtf_df.shift(1)
        mtf_aligned = mtf_df.reindex(df.index).ffill()
        mtf_aligned.columns = [f"mtf_{timeframe}_{col}" for col in mtf_aligned.columns]
        return mtf_aligned

    def apply_exits(self, df: pd.DataFrame, raw_signals: pd.Series) -> pd.DataFrame:
        import numpy as np
        
        tr1 = df['high'] - df['low']
        tr2 = (df['high'] - df['close'].shift(1)).abs()
        tr3 = (df['low'] - df['close'].shift(1)).abs()
        true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = true_range.rolling(14).mean()

        atr_multiplier = self.execution.get('atr_sl_multiplier', 1.5)
        use_time_filter = self.execution.get('use_time_filter', False)

        import pytz
        if df.index.tz is None:
            kyiv_time = df.index.tz_localize('UTC').tz_convert('Europe/Kyiv')
        else:
            kyiv_time = df.index.tz_convert('Europe/Kyiv')

        signals_arr = raw_signals.values
        closes_arr = df['close'].values
        highs_arr = df['high'].values
        lows_arr = df['low'].values
        atr_arr = atr.values
        days_arr = df.index.dayofweek.values
        hours_arr = kyiv_time.hour.values
        
        n = len(df)
        target_positions = [0.0] * n
        sl_prices = [np.nan] * n
        tp_prices = [np.nan] * n

        current_position = 0.0
        sl_price = 0.0
        tp_price = 0.0

        for i in range(n):
            signal = signals_arr[i]
            current_close = closes_arr[i]
            current_high = highs_arr[i]
            current_low = lows_arr[i]
            current_atr = atr_arr[i]
            current_day = days_arr[i]
            kyiv_hour = hours_arr[i]

            # --- EXIT LOGIC ---
            if current_position > 0: # LONG
                if current_low <= sl_price or current_high >= tp_price:
                    current_position = 0.0
            elif current_position < 0: # SHORT
                if current_high >= sl_price or current_low <= tp_price:
                    current_position = 0.0

            # --- ENTRY LOGIC ---
            if current_position == 0 and signal != 0 and not np.isnan(current_atr):
                
                if use_time_filter and not (10 <= kyiv_hour < 18):
                    pass
                else:
                    current_position = float(signal)
                    
                    # Force absolute positive distances
                    sl_dist = abs(current_atr * atr_multiplier)
                    rr_ratio = 3.0 if current_day == 3 else 2.0
                    tp_dist = abs(sl_dist * rr_ratio)
                    
                    # Explicit mapping
                    if current_position > 0: # LONG
                        sl_price = current_close - sl_dist
                        tp_price = current_close + tp_dist
                    elif current_position < 0: # SHORT
                        sl_price = current_close + sl_dist
                        tp_price = current_close - tp_dist

            target_positions[i] = current_position
            sl_prices[i] = sl_price if current_position != 0 else np.nan
            tp_prices[i] = tp_price if current_position != 0 else np.nan

        result = pd.DataFrame(index=df.index)
        result['target_position'] = target_positions
        result['sl_price'] = sl_prices
        result['tp_price'] = tp_prices
        return result