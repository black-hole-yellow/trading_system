import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from typing import Dict, Any

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
        """
        The Universal Exit Engine.
        Applies ATR-based Stops, Dynamic RR Targets, and Time Stops.
        """
        # 1. Calculate Universal 14-Period ATR for volatility
        tr1 = df['high'] - df['low']
        tr2 = (df['high'] - df['close'].shift(1)).abs()
        tr3 = (df['low'] - df['close'].shift(1)).abs()
        true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = true_range.rolling(14).mean()

        # 2. Extract Execution Config
        max_hold_bars = self.execution.get('max_hold_bars', 5)
        atr_multiplier = self.execution.get('atr_sl_multiplier', 1.5)

        # 3. State Tracking Variables
        current_position = 0.0
        sl_price = 0.0
        tp_price = 0.0
        bars_held = 0
        target_positions = []

        # 4. The Execution Loop
        for i in range(len(df)):
            signal = raw_signals.iloc[i]
            current_close = df['close'].iloc[i]
            current_atr = atr.iloc[i]
            
            # Pandas dayofweek: Monday=0, Tuesday=1, Wednesday=2, Thursday=3
            current_day = df.index[i].dayofweek 

            # --- EXIT LOGIC ---
            if current_position != 0:
                bars_held += 1
                
                # Exit 1: Time Stop
                if bars_held >= max_hold_bars:
                    current_position = 0.0
                
                # Exit 2: Hard Stop Loss & Take Profit
                elif current_position == 1.0:
                    if current_close <= sl_price or current_close >= tp_price:
                        current_position = 0.0
                elif current_position == -1.0:
                    if current_close >= sl_price or current_close <= tp_price:
                        current_position = 0.0

            # --- ENTRY LOGIC ---
            if current_position == 0 and signal != 0 and pd.notna(current_atr):
                current_position = signal
                bars_held = 0
                
                # Calculate Stop Loss Distance
                sl_dist = current_atr * atr_multiplier
                
                # Calculate Take Profit Distance (Dynamic RR)
                # If Thursday (3), use 3RR. Otherwise use 2RR.
                rr_ratio = 3.0 if current_day == 3 else 2.0
                tp_dist = sl_dist * rr_ratio
                
                # Set specific price levels
                if current_position == 1.0:
                    sl_price = current_close - sl_dist
                    tp_price = current_close + tp_dist
                else:
                    sl_price = current_close + sl_dist
                    tp_price = current_close - tp_dist

            target_positions.append(current_position)

        # Return a DataFrame with the final positions
        result = pd.DataFrame(index=df.index)
        result['target_position'] = target_positions
        return result