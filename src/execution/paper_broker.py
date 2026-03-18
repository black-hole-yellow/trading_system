from typing import Dict

class PaperBroker:
    """
    Simulates a live exchange environment. 
    Maintains ledger state and applies institutional slippage.
    """
    def __init__(self, initial_cash: float = 100000.0, slippage_bps: float = 1.0):
        self.cash = initial_cash
        self.positions: Dict[str, float] = {}       # e.g., {'GBPUSD': 50000.0}
        self.current_prices: Dict[str, float] = {}  # e.g., {'GBPUSD': 1.2500}
        self.slippage_bps = slippage_bps

    def update_market_data(self, prices: Dict[str, float]):
        """Injects the latest tick/bar prices so the broker knows execution values."""
        self.current_prices = prices

    def get_total_equity(self) -> float:
        """Calculates Cash + Unrealized PnL."""
        equity = self.cash
        for symbol, qty in self.positions.items():
            equity += qty * self.current_prices.get(symbol, 0.0)
        return equity

    def execute_order(self, symbol: str, qty_delta: float) -> dict:
        """Simulates an immediate market order fill with slippage."""
        if qty_delta == 0:
            return {"status": "ignored"}

        base_price = self.current_prices.get(symbol)
        if not base_price:
            raise ValueError(f"Broker has no price feed for {symbol}")

        # Calculate slippage: Buy higher, Sell lower
        direction = 1 if qty_delta > 0 else -1
        fill_price = base_price * (1 + (direction * (self.slippage_bps / 10000)))

        # Update Ledger
        trade_cost = qty_delta * fill_price
        self.cash -= trade_cost
        self.positions[symbol] = self.positions.get(symbol, 0.0) + qty_delta

        return {
            "status": "FILLED",
            "symbol": symbol,
            "fill_price": round(fill_price, 5),
            "qty": round(qty_delta, 2)
        }