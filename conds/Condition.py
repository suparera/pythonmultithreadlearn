from abc import ABC, abstractmethod

class Condition(ABC):
    @abstractmethod
    def check(self, data, cond_results=None):
        pass

