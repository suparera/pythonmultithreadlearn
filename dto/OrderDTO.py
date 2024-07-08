from typing import TypedDict


# this will work like Strategy_01.py orders dataframe
class OrderDTO(TypedDict):
    symbol: str
    buy_price: float
    buy_time: str
    max_price: float
    buy_pip_no: float
    stop_loss_pip_no: float
    sell_price: float
    sell_time: str
    max_pip_no: float

    # function to print out the OrderDTO
    def __str__(self):
        return f"symbil:{self['symbol']}, buy_price: {self['buy_price']}, buy_time: {self['buy_time']}, max_price: {self['max_price']}, buy_pip_no: {self['buy_pip_no']}, stop_loss_pip_no: {self['stop_loss_pip_no']}, sell_price: {self['sell_price']}, sell_time: {self['sell_time']}, max_pip_no: {self['max_pip_no']}"
