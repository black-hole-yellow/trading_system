import pandas as pd
from typing import Dict, Any

class PortfolioManager:
    """
    Translates raw strategy signals into strictly risk-managed portfolio weights.
    Enforces Gross Leverage and Net USD Exposure limits.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.max_gross_leverage = config.get('max_gross_leverage', 3.0)
        self.max_net_usd_exposure = config.get('max_net_usd_exposure', 1.5)
        
        # Maps how a Long (+1) position in an asset affects USD exposure.
        # e.g., Long GBPUSD = Short USD (-1). Long USDJPY = Long USD (+1).
        self.usd_exposure_map = config.get('usd_exposure_map', {})

    def generate_target_weights(self, raw_signals: Dict[str, float]) -> Dict[str, float]:
        """
        Takes raw signals (+1, -1, 0) and outputs final capital allocation weights.
        
        Args:
            raw_signals: Dict of {symbol: signal} e.g., {'GBPUSD': 1.0, 'XAUUSD': 1.0}
            
        Returns:
            Dict of {symbol: final_weight} e.g., {'GBPUSD': 0.75, 'XAUUSD': 0.75}
        """
        weights = raw_signals.copy()
        
        # 1. Calculate Net USD Exposure
        current_net_usd = 0.0
        for symbol, signal in weights.items():
            usd_beta = self.usd_exposure_map.get(symbol, 0)
            current_net_usd += signal * usd_beta
            
        # 2. Apply Net Exposure Limit (The USD Trap Filter)
        if abs(current_net_usd) > self.max_net_usd_exposure:
            # Scale down all positions contributing to the excess USD exposure
            reduction_factor = self.max_net_usd_exposure / abs(current_net_usd)
            for symbol in weights:
                # Only reduce assets that have USD exposure
                if self.usd_exposure_map.get(symbol, 0) != 0:
                    weights[symbol] *= reduction_factor

        # 3. Calculate Gross Exposure
        gross_exposure = sum(abs(w) for w in weights.values())
        
        # 4. Apply Gross Exposure Limit (The Anchor)
        if gross_exposure > self.max_gross_leverage:
            leverage_reduction = self.max_gross_leverage / gross_exposure
            for symbol in weights:
                weights[symbol] *= leverage_reduction
                
        # Round to 4 decimal places for clean execution
        return {symbol: round(weight, 4) for symbol, weight in weights.items()}

# Example Config for the Portfolio Manager:
# config = {
#     "max_gross_leverage": 3.0,
#     "max_net_usd_exposure": 1.0,
#     "usd_exposure_map": {
#         "GBPUSD": -1,  # Base/Quote: Long = Short USD
#         "XAUUSD": -1,  # Base/Quote: Long = Short USD
#         "USDJPY": 1    # Base/Quote: Long = Long USD
#     }
# }