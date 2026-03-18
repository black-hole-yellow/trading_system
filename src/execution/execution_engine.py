from typing import Dict
from .paper_broker import PaperBroker

class ExecutionEngine:
    """
    Bridges the Target Portfolio (Pillar 3) and the Exchange (Pillar 4).
    Calculates the required trade delta to synchronize states.
    """
    def __init__(self, broker: PaperBroker):
        self.broker = broker
        # Minimum notional trade size (e.g., $100) to prevent trading "dust"
        self.min_trade_notional = 100.0 

    def synchronize_portfolio(self, target_weights: Dict[str, float]):
        """
        Calculates the delta between Target State and Current State, 
        then fires execution orders to the broker.
        """
        equity = self.broker.get_total_equity()
        current_positions = self.broker.positions
        current_prices = self.broker.current_prices

        # 1. Handle target positions
        for symbol, target_weight in target_weights.items():
            price = current_prices.get(symbol)
            if not price:
                continue

            # What we WANT
            target_notional = equity * target_weight
            target_qty = target_notional / price

            # What we HAVE
            current_qty = current_positions.get(symbol, 0.0)

            # The DELTA (The actual order)
            qty_delta = target_qty - current_qty
            notional_delta = abs(qty_delta * price)

            # Only trade if the change is meaningful (filters out noise)
            if notional_delta >= self.min_trade_notional:
                self.broker.execute_order(symbol, qty_delta)

        # 2. Flatten positions that are no longer in the target_weights dictionary
        for symbol in list(current_positions.keys()):
            if symbol not in target_weights and current_positions[symbol] != 0:
                qty_delta = -current_positions[symbol] # Sell to 0
                self.broker.execute_order(symbol, qty_delta)