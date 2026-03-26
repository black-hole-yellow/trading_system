from typing import Dict, Any

class PortfolioManager:
    """
    The Risk Firewall (Pillar 3).
    Translates raw directional signals (+1, -1) into precise portfolio weights.
    Enforces Maximum Gross Leverage and Maximum Net Asset/Currency Exposures.
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
        # Risk Limits
        # How much total leverage can the portfolio use? (e.g., 3.0 = 300% of equity)
        self.max_gross_leverage = config.get("max_gross_leverage", 2.0)
        
        # How much exposure to one specific currency is allowed?
        self.max_net_usd_exposure = config.get("max_net_usd_exposure", 1.0)
        
        # Map to track how assets correlate to the USD
        # e.g., Long GBPUSD = Short USD (-1). Long USDCAD = Long USD (+1).
        self.usd_exposure_map = config.get("usd_exposure_map", {"GBPUSD": -1})
        
        # The default allocation weight per trade if no dynamic risk is provided
        self.base_weight_per_trade = config.get("base_weight_per_trade", 0.10) # 10% of equity per trade

    def generate_target_weights(self, signals: Dict[str, float]) -> Dict[str, float]:
        """
        Takes raw signals (e.g., {"GBPUSD": 1.0}) and applies risk constraints.
        Returns safe target weights (e.g., {"GBPUSD": 0.10}).
        """
        # 1. The Sizing Engine: Apply Base Weight
        target_weights = {}
        for symbol, signal in signals.items():
            # A signal of 1.0 becomes a weight of 0.10. A signal of 0.0 becomes 0.0.
            target_weights[symbol] = signal * self.base_weight_per_trade

        # 2. The Risk Gatekeeper: Enforce Constraints
        target_weights = self._enforce_usd_exposure_limit(target_weights)
        target_weights = self._enforce_gross_leverage_limit(target_weights)
        
        return target_weights

    def _enforce_gross_leverage_limit(self, weights: Dict[str, float]) -> Dict[str, float]:
        """
        Ensures the sum of all absolute weights does not exceed max_gross_leverage.
        If it does, it scales all positions down proportionally.
        """
        total_gross_exposure = sum(abs(w) for w in weights.values())
        
        if total_gross_exposure > self.max_gross_leverage:
            # Calculate the scale-down factor
            scale_factor = self.max_gross_leverage / total_gross_exposure
            
            # Apply the reduction to all assets equally
            scaled_weights = {sym: w * scale_factor for sym, w in weights.items()}
            
            print(f"RISK ALERT: Gross Leverage ({total_gross_exposure:.2f}) exceeded limit ({self.max_gross_leverage}). Scaling down trades.")
            return scaled_weights
            
        return weights

    def _enforce_usd_exposure_limit(self, weights: Dict[str, float]) -> Dict[str, float]:
        """
        The Currency Heat Limit. 
        Calculates total net USD exposure. If it breaches the limit, it rejects or reduces trades.
        """
        net_usd_exposure = 0.0
        
        for symbol, weight in weights.items():
            # If the symbol isn't in the map, assume 0 USD correlation
            usd_direction = self.usd_exposure_map.get(symbol, 0)
            net_usd_exposure += weight * usd_direction
            
        if abs(net_usd_exposure) > self.max_net_usd_exposure:
            print(f"RISK ALERT: Net USD Exposure ({abs(net_usd_exposure):.2f}) exceeded limit ({self.max_net_usd_exposure}).")
            
            # For strict institutional compliance, if a new signal breaches currency limits, 
            # the safest route is to aggressively scale down those specific correlated trades.
            scale_factor = self.max_net_usd_exposure / abs(net_usd_exposure)
            
            safe_weights = {}
            for symbol, weight in weights.items():
                if self.usd_exposure_map.get(symbol, 0) != 0:
                    safe_weights[symbol] = weight * scale_factor
                else:
                    safe_weights[symbol] = weight
                    
            return safe_weights
            
        return weights