from abc import ABC, abstractmethod

class TradingStrategy(ABC):
    @abstractmethod
    def generate_signal(self, stock, amount):
        pass

    # name of the strategy
    @abstractmethod
    def get_name(self):
        pass