# conds/PrevDayHighPipSupportCondition.py
from conds.Condition import Condition


class PrevDayHighPipSupportCondition(Condition):
    """
    This condition checks for a reversion pattern.
    A reversion pattern might be defined as a significant change in price direction.
    It will work when considered both with first condition: OpenPipJumpCondition
    """

    def __init__(self, prev_day_close_pip, threshold=2):
        """
        Initialize the condition.
        :param threshold: The pip threshold for detecting a reversion, that should not below previous close pip - Threshold
        """
        self.threshold = threshold  # Define a threshold for detecting reversion

    def check(self, data, cond_results=None):
        """
        Check if the condition is met.
        :param data: A dictionary containing the relevant data for the condition.
        :param cond_results: A dictionary containing the results of other conditions.
        :return: True if the condition is met, False otherwise.
        """
        price = data['price']
        prev_price = data['prev_price']
        return abs(price - prev_price) / prev_price >= self.threshold
