class MultiStockTradingSystem:
    def __init__(self):
        self.strategies = {}
        self.positions = {}

    def add_strategy(self, symbol, strategy):
        self.strategies[symbol] = strategy
        self.positions[symbol] = 0

    def execute_trades(self, data):
        """
        Execute trades for all stocks based on their respective strategies.

        :param data: Dict with stock symbols as keys and DataFrames as values
        """
        for symbol, stock_data in data.items():
            if symbol in self.strategies:
                signal = self.strategies[symbol].generate_signal(stock_data)
                self.process_signal(symbol, signal, stock_data)

    def process_signal(self, symbol, signal, data):
        current_price = data['close'].iloc[-1]

        if signal == 1 and self.positions[symbol] <= 0:
            # Buy logic
            self.positions[symbol] = 1
            print(f"Buy {symbol} at {current_price}")
        elif signal == -1 and self.positions[symbol] >= 0:
            # Sell logic
            self.positions[symbol] = -1
            print(f"Sell {symbol} at {current_price}")


