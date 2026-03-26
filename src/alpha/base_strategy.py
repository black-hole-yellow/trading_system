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

    def apply_exits(self, df: pd.DataFrame, raw_signals: pd.Series) -> pd.DataFrame:
        # 1. Calculate Volatility
        tr1 = df['high'] - df['low']
        tr2 = (df['high'] - df['close'].shift(1)).abs()
        tr3 = (df['low'] - df['close'].shift(1)).abs()
        true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = true_range.rolling(14).mean()

        atr_multiplier = self.execution.get('atr_sl_multiplier', 1.5)
        # Enable time filter via config (10:00 to 18:00 Kyiv)
        use_time_filter = self.execution.get('use_time_filter', False)

        # 2. Timezone handling for Kyiv
        if df.index.tz is None:
            kyiv_time = df.index.tz_localize('UTC').tz_convert('Europe/Kyiv')
        else:
            kyiv_time = df.index.tz_convert('Europe/Kyiv')

        # 3. Use NumPy for performance
        signals_arr = raw_signals.values
        closes_arr = df['close'].values
        highs_arr = df['high'].values
        lows_arr = df['low'].values
        atr_arr = atr.values
        days_arr = df.index.dayofweek.values # 0=Mon, 3=Thu
        hours_arr = kyiv_time.hour.values
        
        n = len(df)
        target_positions = [0.0] * n
        sl_prices = [np.nan] * n
        tp_prices = [np.nan] * n

        exact_exit_prices = [np.nan] * n

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
            
            # Reset exact exit price for this bar
            exact_exit_price = np.nan

            # --- EXIT LOGIC ---
            if current_position > 0: # LONG
                if current_low <= sl_price:
                    current_position = 0.0
                    exact_exit_price = sl_price  # Hit Stop Loss exactly
                elif current_high >= tp_price:
                    current_position = 0.0
                    exact_exit_price = tp_price  # Hit Take Profit exactly
                    
            elif current_position < 0: # SHORT
                if current_high >= sl_price:
                    current_position = 0.0
                    exact_exit_price = sl_price
                elif current_low <= tp_price:
                    current_position = 0.0
                    exact_exit_price = tp_price

            # --- ENTRY LOGIC (Keep exactly as it was) ---
            if current_position == 0 and signal != 0 and not np.isnan(current_atr):
                
                if use_time_filter and not (10 <= kyiv_hour < 18):
                    pass
                else:
                    current_position = float(signal)
                    
                    # 1. Fetch the slippage penalty from config (Default 1.0 pip)
                    slippage_val = self.execution.get('slippage_pips', 1.0) / 10000.0
                    
                    # 2. Calculate the True Risk Distance based on ATR
                    sl_dist = abs(current_atr * atr_multiplier)
                    rr_ratio = 3.0 if current_day == 3 else 2.0
                    
                    # 3. SLIPPAGE-ADJUSTED ANCHORING
                    # We calculate the SL and TP from the price you will ACTUALLY get filled at
                    if current_position > 0: # LONG
                        simulated_filled_entry = current_close + slippage_val
                        sl_price = simulated_filled_entry - sl_dist
                        
                        # Add the exit slippage cost to the TP distance so you net exactly 2RR
                        tp_price = simulated_filled_entry + (sl_dist * rr_ratio) + slippage_val
                        
                    elif current_position < 0: # SHORT
                        simulated_filled_entry = current_close - slippage_val
                        sl_price = simulated_filled_entry + sl_dist
                        
                        # Add the exit slippage cost to the TP distance so you net exactly 2RR
                        tp_price = simulated_filled_entry - (sl_dist * rr_ratio) - slippage_val

            target_positions[i] = current_position
            sl_prices[i] = sl_price if current_position != 0 else np.nan
            tp_prices[i] = tp_price if current_position != 0 else np.nan
            
            # NEW: Save the exact fill price for the execution engine
            exact_exit_prices[i] = exact_exit_price

        result = pd.DataFrame(index=df.index)
        result['target_position'] = target_positions
        result['sl_price'] = sl_prices
        result['tp_price'] = tp_prices
        result['exact_exit_price'] = exact_exit_prices # NEW
        return result