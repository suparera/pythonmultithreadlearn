from conds.Condition import Condition


class SellEMA5CrossEMA10DownCondition(Condition):

    def __init__(self, name, fast_ema_field='pip_ema5', slow_ema_field='pip_ema10'):
        self.name = name
        self.fast_ema_field = fast_ema_field
        self.slow_ema_field = slow_ema_field

    def check(self, data, df_1m, cond_results=None):
        is_pass = False
        if (df_1m.iloc[-1][self.fast_ema_field] < df_1m.iloc[-1][self.slow_ema_field]
                and df_1m.iloc[-1][self.fast_ema_field] < df_1m.iloc[-2][self.fast_ema_field]
                and df_1m.iloc[-1][self.slow_ema_field] < df_1m.iloc[-2][self.slow_ema_field]
        ):
            is_pass = True
            cond_results[self.name] = True

        return is_pass
