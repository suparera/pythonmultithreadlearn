from conds.Condition import Condition


class PrevDayHighPipSupportCondition(Condition):
    """
    This condition checks for a reversion pattern.
    A reversion pattern might be defined as a significant change in price direction.
    It will work when considered both with first condition: OpenPipJumpCondition
    """

    def __init__(self, name, prev_day_close_pip, threshold=2, ema_fast_diff_column='pip_ema5_diff',
                 ema_slow_diff_column='pip_ema10_diff'):
        """
        Initialize the condition.
        :param threshold: The pip threshold for detecting a reversion, that should not below previous close pip - Threshold
        """
        self.name = name
        self.prev_day_close_pip = prev_day_close_pip
        self.threshold = threshold  # Define a threshold for detecting reversion
        self.ema_fast_diff_column = ema_fast_diff_column
        self.ema_slow_diff_column = ema_slow_diff_column

    def check(self, data, df_1m, cond_results=None):
        """
        Check if the condition is met.
        :param data: A dictionary containing the relevant data for the condition.
        :param cond_results: A dictionary containing the results of other conditions.
        :return: True if the condition is met, False otherwise.
        condition:
        1. EMA5 > 0
        2. EMA5 > EMA5[-1]
        3. EMA5[-1] > 0
        4. lowest before reverse not less than prev_day_close_pip - threshold
        5. prerequisite: OpenPipJumpCondition is True
        """
        is_pass = False
        if (len(df_1m) > 1 and df_1m.iloc[-1][self.ema_fast_diff_column] > 0
                and df_1m.iloc[-1][self.ema_fast_diff_column] > df_1m.iloc[-2][self.ema_fast_diff_column])\
                and df_1m.iloc[-2][self.ema_fast_diff_column] > 0\
                and df_1m['pip_ema5'].min() >= self.prev_day_close_pip - self.threshold\
                and cond_results['OpenPipJumpCondition']:
            is_pass = True
            cond_results['PrevDayHighPipSupportCondition'] = True

        return is_pass
