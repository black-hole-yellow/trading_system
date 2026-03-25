from pathlib import Path
import json
from alpha.monday_reversion import MondayReversionStrategy


# Find the project root
root = Path(__file__).resolve().parent
hypothesis_path = root / "config" / "hypotheses" / "monday_gap_reversion.json"

with open(hypothesis_path, 'r') as f:
    config = json.load(f)

# Initialize your strategy with the config
strategy = MondayReversionStrategy(config)