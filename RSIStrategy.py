from TradingStrategy import TradingStrategy
import pandas as pd
import numpy as np

class RSIStrategy(TradingStrategy):
    def __init__(self, rsi_window, overbought, oversold):
        self.rsi_window = rsi_window
        self.overbought = overbought
        self.oversold = oversold

    def generate_signal(self, stock, amount):
        signals = pd.DataFrame(index=stock.index)
        signals['signal'] = 0.0

        # Calculate RSI
        delta = stock['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=self.rsi_window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=self.rsi_window).mean()
        RS = gain / loss
        RSI = 100 - (100 / (1 + RS))

        # Create signals
        signals['signal'] = 0
        signals['signal'][self.rsi_window:] = np.where(RSI[self.rsi_window:] > self.overbought, -1.0, 0.0)
        signals['signal'][self.rsi_window:] = np.where(RSI[self.rsi_window:] < self.oversold, 1.0, 0.0)

        # Generate trading orders
        signals['positions'] = signals['signal'].diff()

        return signals

    def get_name(self):
        return "RSI Strategy"