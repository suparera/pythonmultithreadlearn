from conds.Condition import Condition


class OpenPipJumpCondition(Condition):
    '''
    OneTimeCondition: so has flag to check if condition has been checked or not
    TimeLimitCondition: so has time limit to check the condition, just 1 min after ther market open

    This condition checks if the pip_high and pip_low of the current row is higher than the previous row.
    required columns: pip_high, pip_low
    '''

    def __init__(self, name, prev_day_close_pip=None):
        self.name = name
        if prev_day_close_pip is not None:
            self.prev_day_close_pip = 0

    def check(self, data, df_1m, cond_results=None):
        return data.iloc[0]['pip_open'] - 2 >= self.prev_day_close_pip
