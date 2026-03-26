import pandas as pd
from typing import Dict, Any

class ExecutionEngine:
    """
    The Hands (Pillar 4).
    Simulates a Prime Broker. Converts target portfolio weights into physical 
    orders, applying realistic slippage, spreads, and commissions.
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
        # Virtual Account State
        self.initial_capital = config.get("initial_capital", 10000.0)
        self.cash = self.initial_capital
        self.positions = {}  # Tracks physical units held (e.g., {"GBPUSD": 12500})
        
        # Transaction Cost Analysis (TCA) Parameters
        # $2.00 commission per 100,000 units is a standard pro FX rate (0.00002 per unit)
        self.commission_per_unit = config.get("commission_per_unit", 0.00002)
        # Slippage assumes a 1 pip spread on average
        self.slippage_pips = config.get("slippage_pips", 1.0)
        
        # Reporting logs
        self.trade_log = []
        self.equity_curve = []

    def get_equity(self, current_prices: Dict[str, float]) -> float:
        """
        Calculates total account equity: Cash + Unrealized PnL of open positions.
        """
        equity = self.cash
        for symbol, units in self.positions.items():
            price = current_prices.get(symbol, 0.0)
            # In FX, unit value is in the quote currency (USD for GBPUSD)
            equity += units * price
        return equity

    def process_weights(self, target_weights: Dict[str, float], current_prices: Dict[str, float], timestamp: pd.Timestamp, sl: float = None, tp: float = None):
        current_equity = self.get_equity(current_prices)
        
        for symbol, target_weight in target_weights.items():
            current_price = current_prices.get(symbol)
            if current_price is None or current_price <= 0: continue
            
            current_units = self.positions.get(symbol, 0)

            # THE FIX: If we are already holding a position for this symbol, 
            # do not do "micro-adjustments". Only calculate units on fresh entries.
            if target_weight != 0.0 and current_units == 0:
                target_exposure_usd = current_equity * target_weight
                units_to_trade = int(target_exposure_usd / current_price) 
                
                if units_to_trade != 0:
                    self._execute_trade(symbol, units_to_trade, current_price, timestamp, sl, tp)
            
        # Handle Exits (Flattening)
        for symbol in list(self.positions.keys()):
            # If the target is 0, completely flatten the existing units
            if symbol not in target_weights or target_weights[symbol] == 0.0:
                current_units = self.positions[symbol]
                if current_units != 0:
                    current_price = current_prices.get(symbol, 0.0)
                    self._execute_trade(symbol, -current_units, current_price, timestamp, None, None)

        self.equity_curve.append({"timestamp": timestamp, "equity": self.get_equity(current_prices)})

    def _execute_trade(self, symbol: str, units_to_trade: int, current_price: float, timestamp: pd.Timestamp, sl: float, tp: float):
        """
        The Simulated Broker. Fills the order, applies slippage, deducts commission.
        """
        # Convert pips to exact price value (assumes 4-decimal fiat pair like GBPUSD)
        slippage_value = self.slippage_pips / 10000.0
        
        # Apply Slippage: You always get a slightly worse price than the raw mid-price
        if units_to_trade > 0:  # Buying: Pay the Ask (Higher)
            fill_price = current_price + slippage_value
            action = "Buy"
        else:                   # Selling: Hit the Bid (Lower)
            fill_price = current_price - slippage_value
            action = "Sell"

        # Calculate exact transaction costs
        commission = abs(units_to_trade) * self.commission_per_unit
        
        # A positive trade (Buy) reduces cash, a negative trade (Sell) increases cash
        cash_impact = -(units_to_trade * fill_price) - commission

        # Update Account State
        self.cash += cash_impact
        self.positions[symbol] = self.positions.get(symbol, 0) + units_to_trade

        # Record the receipt
        self.trade_log.append({
            "timestamp": timestamp,
            "symbol": symbol,
            "action": action,
            "units": abs(units_to_trade),
            "fill_price": round(fill_price, 5),
            "commission": round(commission, 2),
            "sl_price": round(sl, 5) if pd.notna(sl) else None,
            "tp_price": round(tp, 5) if pd.notna(tp) else None
        })