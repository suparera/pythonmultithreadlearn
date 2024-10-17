import pandas as pd
import numpy as np

from strategies.MovingAverageCrossover import MovingAverageCrossover
from strategies.MultiStockTradingSystem import MultiStockTradingSystem

# Usage example
trading_system = MultiStockTradingSystem()

# Add strategies for different stocks
trading_system.add_strategy('AAPL', MovingAverageCrossover(short_window=5, long_window=20))
trading_system.add_strategy('GOOGL', MovingAverageCrossover(short_window=3, long_window=10))

# Simulate incoming 1-minute data
def simulate_data():
    data = {}
    for symbol in ['AAPL', 'GOOGL']:
        df = pd.DataFrame({
            'timestamp': pd.date_range(start='2023-01-01', periods=1000, freq='1min'),
            'open': np.random.randn(1000).cumsum() + 100,
            'high': np.random.randn(1000).cumsum() + 101,
            'low': np.random.randn(1000).cumsum() + 99,
            'close': np.random.randn(1000).cumsum() + 100
        })
        data[symbol] = df
    return data

# Main trading loop
data = simulate_data()
for i in range(len(data['AAPL'])):
    current_data = {symbol: df.iloc[:i+1] for symbol, df in data.items()}
    trading_system.execute_trades(current_data)
