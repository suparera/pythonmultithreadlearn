from strategies.TradingStrategy import TradingStrategy


class MovingAverageCrossover(TradingStrategy):
    def __init__(self, short_window=5, long_window=20):
        self.short_window = short_window
        self.long_window = long_window

    def generate_signal(self, data):
        """
        Generate trading signal for a given stock.

        :param data: DpriataFrame with 'timestamp', 'open', 'high', 'low', 'close' columns
        :return: signal (-1 for sell, 0 for hold, 1 for buy)
        """
        if len(data) < self.long_window:
            return 0  # Not enough data to generate a signal

        # Calculate short and long moving averages
        short_ma = data['close'].rolling(window=self.short_window).mean()
        long_ma = data['close'].rolling(window=self.long_window).mean()

        # Generate signals
        if short_ma.iloc[-1] > long_ma.iloc[-1] and short_ma.iloc[-2] <= long_ma.iloc[-2]:
            return 1  # Buy signal (short MA crosses above long MA)
        elif short_ma.iloc[-1] < long_ma.iloc[-1] and short_ma.iloc[-2] >= long_ma.iloc[-2]:
            return -1  # Sell signal (short MA crosses below long MA)
        else:
            return 0  # Hold signal (no crossover)

