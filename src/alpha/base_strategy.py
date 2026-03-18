from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Any

class BaseStrategy(ABC):
    """
    Abstract base class for all production alpha models.
    Enforces the handoff checklist from the Hypothesis Lab.
    """
    
    def __init__(self, config: Dict[str, Any]):
        # The config dictionary contains the exact parameters and universe 
        # handed over from the Lab, entirely decoupled from the logic.
        self.config = config
        self.universe = config.get('universe', [])
        self.parameters = config.get('parameters', {})

    @abstractmethod
    def generate_signals(self, market_data: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms standardized data from Pillar 1 into target positions.
        
        Args:
            market_data (pd.DataFrame): The clean, UTC-aligned Parquet data.
            
        Returns:
            pd.DataFrame: A time-series of target positions (e.g., 1 for long, 
                          -1 for short, 0 for flat). The index MUST match the 
                          input market_data index exactly.
        """
        pass