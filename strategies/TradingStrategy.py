from abc import ABC, abstractmethod


class TradingStrategy(ABC):
    @abstractmethod
    def generate_signal(self, data):
        pass
