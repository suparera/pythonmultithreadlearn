from StrategyManager import StrategyManager


class TradingSystem:
    def __init__(self, initial_capital, risk_per_trade=0.02):
        self.strategy_manager = StrategyManager()
        self.capital = initial_capital
        self.position = 0
        self.risk_per_trade = risk_per_trade

    def execute_trades(self, data):
        signal = self.strategy_manager.generate_combined_signal(data)
        current_price = data['close'][-1]  # Assuming 'data' contains price information

        # Determine the trade action based on the signal
        if signal > 0.5 and self.position <= 0:
            self.open_long_position(current_price)
        elif signal < -0.5 and self.position >= 0:
            self.open_short_position(current_price)
        elif -0.5 <= signal <= 0.5 and self.position != 0:
            self.close_position(current_price)

    def open_long_position(self, price):
        position_size = self.calculate_position_size(price)
        cost = position_size * price
        if cost <= self.capital:
            self.position += position_size
            self.capital -= cost
            print(f"Opened long position: {position_size} units at {price}")

    def open_short_position(self, price):
        position_size = self.calculate_position_size(price)
        self.position -= position_size
        print(f"Opened short position: {position_size} units at {price}")

    def close_position(self, price):
        if self.position > 0:
            self.capital += self.position * price
            print(f"Closed long position: {self.position} units at {price}")
        elif self.position < 0:
            self.capital -= self.position * price
            print(f"Closed short position: {self.position} units at {price}")
        self.position = 0

    def calculate_position_size(self, price):
        risk_amount = self.capital * self.risk_per_trade
        # This is a simple position sizing method. You might want to use more sophisticated methods.
        return round(risk_amount / price, 2)