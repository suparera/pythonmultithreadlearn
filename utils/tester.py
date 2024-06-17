
import date_utils as du
import db_utils as dbu
import ticker_table_manager as ttm

# test db_utils, date_utils
stockeod = du.get_prev_date_from_stockeod('2024-06-12')
# test call du.get_prev_date_from_stockeod('2024-06-12') 1000 times, sleep 0.2s
for i in range(1000):
    stockeod = du.get_prev_date_from_stockeod('2024-06-12')
    print (f"stockeod: {stockeod}")
# print (f"stockeod: {stockeod}")

# table_name_from_string = ttm.get_table_name_from_string('2024-12-31')
# print (f"table_name_from_string: {table_name_from_string}")
# ttm.create_table_for_day(table_name_from_string)


