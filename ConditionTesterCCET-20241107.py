import utils.order_utils as order_utils
import utils.stockeod_utils as stockeod


def test_condition(dateText, symbol):
    global order
    from utils import symbol_1m_man as s1man
    from conds.TickerTestCondition import TickerTestCondition

    # Load data for the given date and symbol
    df1m = s1man.get_by_date_symbol(dateText, symbol)

    ticker_test_condition = TickerTestCondition('ticker_test_condition', 50)

    # Iterate over tickers
    for index, row in df1m.iterrows():
        is_pass = ticker_test_condition.check(df1m.loc[:index])
        if is_pass:
            print(f"ConditionTesterCCET-20241107-18, {row['symbol']}, {str(row['time'])[-8:]}")
            order = order_utils.open_order(dateText, symbol, str(row['time'])[-8:], df1m, row)
            return order

##################################################
# Example usage
# dateList = ['2024-11-08']
dateList = ['2024-10-15', '2024-10-16', '2024-10-17', '2024-10-18']

for date_text in dateList:
    prev_date = stockeod.get_prev_date(date_text)
    prev_date_text = prev_date.strftime('%Y-%m-%d')

    symbols = stockeod.get_stock_list_in_date_text(prev_date_text)

    # print column headers
    print("Symbol, Time")
    for symbol in symbols:
        order = test_condition(date_text, symbol)
        # if order is not None:
        #     print("Order = ", order)

print("END")


