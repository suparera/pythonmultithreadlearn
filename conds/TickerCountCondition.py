from conds.Condition import Condition


class TickerCountCondition(Condition):

    def __init__(self, name, threshold=50):
        self.name = name
        self.threshold = threshold  # Define a threshold for detecting reversion

    def check(self, data, df, cond_results=None):
        is_pass = False
        if (len(df) > 1
                and df.iloc[-1]['totalId_ema5'] > self.threshold
                and df.iloc[-2]['totalId_ema5'] > self.threshold
        ):
            is_pass = True
            cond_results[self.name] = True

        return is_pass
