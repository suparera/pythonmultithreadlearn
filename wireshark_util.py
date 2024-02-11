from module_init import logger
from MarketEnum import MarketEnum

log = logger()


###
# from packet data, it can contain many stock datas, so we need to split it
def split_packet_data_to_stock_packets(packet_data):
    stock_packets = []
    current_index = 0
    while current_index < len(packet_data):
        # get length
        length = packet_data[current_index:current_index + 4]
        length = int(length, 16)
        log.debug(f"stock data length: {length}")

        # validate length of data, if invalid, mean captured data was truncated, we will skip
        if current_index + 4 + length * 2 > len(packet_data):
            break

        # get streaming data, it possible to be
        # start with 02: market data
        # start with 04: stock data
        # if start with 02: then first 4 chars is length of data. then it will start with 21
        # type_of_packet = packet_data[current_index:current_index + 2]
        # if type_of_packet == '02':
        #     # if it is market data, then skip
        #     current_index += 4 + length * 2
        #     continue
        streaming_data = packet_data[current_index + 4:current_index + 4 + length * 2]
        stock_packets.append(streaming_data)
        current_index += 4 + length * 2

    return stock_packets


def get_market(stock_packet):
    market_type = int(stock_packet[4:6], 16)
    return MarketEnum(market_type)


def get_symbol_start_and_end_positions(stock_packet):
    market_type = get_market(stock_packet)
    # calculate symbol start index
    symbol_len_index = 10 if market_type == MarketEnum.SET else 12
    symbol_index = symbol_len_index + 2

    symbol_byte_len = int(stock_packet[symbol_len_index:symbol_index], 16)
    end_symbol_pos = symbol_index + symbol_byte_len * 2
    return {'start': symbol_index, 'end': end_symbol_pos}


def get_symbol(stock_packet):
    position = get_symbol_start_and_end_positions(stock_packet)
    symbol = bytes.fromhex(stock_packet[position['start']: position['end']]).decode('utf-8')
    return symbol

def get_price(stock_packet):
    # get end position
    symbol_end_pos = get_symbol_start_and_end_positions(stock_packet)['end']
    price_size = get_price_size(stock_packet)

    # declare variable for dividend and divisor
    divisor = \
        100

    # if it is tfex, then get number of fraction point from position 10 to 12
    if MarketEnum.TFEX == get_market(stock_packet):
        fraction_point = int(stock_packet[10:12], 16)
        divisor = (10 ** fraction_point)

    price = int(stock_packet[symbol_end_pos: symbol_end_pos + price_size], 16) / divisor

    return price

# get side of ticket, at 9th char, len = 1
# if 0, then buy
# if 1, then sell
def get_side(stock_packet):
    side = int(stock_packet[9:10], 16)
    return 'B' if side == 0 else 'S'

# def get_volume_qty(stock_packet):
#     # get end position
#     symbol_end_pos = get_symbol_start_and_end_positions(stock_packet)['end']
#
#     get_price_size(stock_packet)
#
#     volume_qty = int(stock_packet[symbol_end_pos +
#     return volume_qty


def get_price_size(stock_packet):
    # get volume-price type, from position 3 to 4
    # 00: price 4 chars, volume 4 chars
    # 02: price 8 chars, volume 4 chars, for ex: GF10V23
    # 08: price 4 chars, volume 8 chars
    volume_price_type = int(stock_packet[2:4], 16)
    price_size = 4 if volume_price_type == 0 or volume_price_type == 8 else 8
    return price_size

def get_vol_size(stock_packet):
    # get volume-price type, from position 3 to 4
    # 00: price 4 chars, volume 4 chars
    # 02: price 8 chars, volume 4 chars, for ex: GF10V23
    # 08: price 4 chars, volume 8 chars
    volume_price_type = int(stock_packet[2:4], 16)
    vol_size = 4 if volume_price_type <= 4 else 8
    return vol_size

def get_volqty_pos(stock_packet):
    # get end position
    symbol_end_pos = get_symbol_start_and_end_positions(stock_packet)['end']
    price_size = get_price_size(stock_packet)
    # price_size for both price, diff, and orderNo
    return symbol_end_pos + (price_size*2) + 8

def get_volqty(stock_packet):
    volqty_pos = get_volqty_pos(stock_packet)
    volqty = int(stock_packet[volqty_pos:volqty_pos+2], 16)
    return volqty

def get_total_vol(stock_packet):
    volqty = get_volqty(stock_packet)
    vol_size = get_vol_size(stock_packet)
    vol_start_pos = get_volqty_pos(stock_packet) + 2
    # at each volqty, get volume from stcok packet
    total_vol = 0
    for i in range(volqty):
        vol_pos = vol_start_pos + ( i * vol_size )
        vol = int(stock_packet[ vol_pos:vol_pos+vol_size ], 16)
        total_vol += vol
    return total_vol


