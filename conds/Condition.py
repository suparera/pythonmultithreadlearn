from abc import ABC, abstractmethod


class Condition(ABC):

    @abstractmethod
    def check(self, data, df_1m, cond_results=None):
        pass
