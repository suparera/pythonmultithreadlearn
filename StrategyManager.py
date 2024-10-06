class StrategyManager:
    def __init__(self):
        self.strategies = []

    def add_strategy(self, strategy):
        self.strategies.append(strategy)

    def generate_combined_signal(self, data):
        weighted_signals = [
            strategy.generate_signal(data) * weight
            for strategy, weight in self.strategies.items()
        ]
        return sum(weighted_signals) / sum(self.strategies.values())