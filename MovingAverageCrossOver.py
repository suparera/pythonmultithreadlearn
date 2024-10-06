from TradingStrategy import TradingStrategy
import pandas as pd

class MovingAverageCrossOver(TradingStrategy):
    def __init__(self, short_window, long_window):
        self.short_window = short_window
        self.long_window = long_window

    def generate_signal(self, stock, amount):
        signals = pd.DataFrame(index=stock.index)
        signals['signal'] = 0.0

        # Create short simple moving average over the short window
        signals['short_mavg'] = stock['Close'].rolling(window=self.short_window, min_periods=1, center=False).mean()

        # Create long simple moving average over the long window
        signals['long_mavg'] = stock['Close'].rolling(window=self.long_window, min_periods=1, center=False).mean()

        # Create signals
        signals['signal'][self.short_window:] = np.where(signals['short_mavg'][self.short_window:] > signals['long_mavg'][self.short_window:], 1.0, 0.0)

        # Generate trading orders
        signals['positions'] = signals['signal'].diff()

        return signals

    def get_name(self):
        return "Moving Average Crossover"