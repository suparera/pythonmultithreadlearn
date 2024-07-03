# dto class for ohlc data
class OhlcDTO:
    def __init__(self, open, high, low, close, buy_volume, sell_volume, qty, pip_open, pip_high, pip_low, pip_close, buy_tick_qty, sell_tick_qty):
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.buy_volume = buy_volume
        self.sell_volume = sell_volume
        self.qty = qty
        self.pip_open = pip_open
        self.pip_high = pip_high
        self.pip_low = pip_low
        self.pip_close = pip_close
        self.buy_tick_qty = buy_tick_qty
        self.sell_tick_qty = sell_tick_qty
