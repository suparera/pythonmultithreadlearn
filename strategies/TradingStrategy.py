from abc import ABC, abstractmethod

class TradingStrategy(ABC):
    @abstractmethod
    def generate_signal(self, data):
        pass



class RSIStrategy(TradingStrategy):
    def generate_signal(self, data):
        # Implement RSI strategy logic
        pass