from conds.Condition import Condition

# use Condition same as StockManager
# later will add Strategy, that groups of Condition
class TickerTestCondition(Condition):

    def __init__(self, name, threshold=50):
        self.name = name
        self.threshold = threshold  # Define a threshold for detecting reversion

    def check(self, df, data=None, cond_results=None):
        is_pass = False
        if (len(df) > 1
                and df.iloc[-1]['totalId'] > self.threshold
                and df.iloc[-2]['totalId'] > self.threshold
        ):
            is_pass = True

        return is_pass