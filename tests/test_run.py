from src.portfolio.portfolio_manager import PortfolioManager

def run_test():
    config = {
        "max_gross_leverage": 3.0,
        "max_net_usd_exposure": 1.5,
        "usd_exposure_map": {
            "GBPUSD": -1.0, 
            "XAUUSD": -1.0, 
            "EURUSD": -1.0,
            "USDJPY": 1.0
        }
    }
    
    pm = PortfolioManager(config)
    
    # Case 1: The USD Trap (Exceeds Net USD limit of 1.5)
    # 3 Longs against the USD = -3.0 Net USD Exposure. Should scale by 50%.
    trap_signals = {"GBPUSD": 1.0, "XAUUSD": 1.0, "EURUSD": 1.0}
    print("Test 1: USD Trap")
    print(f"Raw:       {trap_signals}")
    print(f"Allocated: {pm.generate_target_weights(trap_signals)}\n")

    # Case 2: Gross Leverage Breach (Exceeds 3.0 max gross)
    # Net USD is 0 (perfectly hedged), but gross exposure is 4.0. Should scale to 3.0.
    leverage_signals = {"GBPUSD": 1.0, "XAUUSD": 1.0, "USDJPY": 2.0}
    print("Test 2: Gross Leverage Limit")
    print(f"Raw:       {leverage_signals}")
    print(f"Allocated: {pm.generate_target_weights(leverage_signals)}")

if __name__ == "__main__":
    run_test()