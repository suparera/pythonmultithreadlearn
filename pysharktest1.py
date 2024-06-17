import os
from datetime import datetime

import mysql.connector
import pyshark
from module_init import logger
import wireshark_util as wsu
import stockeod_util

log = logger()

def process_streaming_packets(packet):
    ''' process streaming packet '''
    print("time: {}, pid: {}, packet: {}".format( datetime.now(), os.getpid(), packet))






# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    cap = pyshark.FileCapture(
        'C:\\Users\\supar\\capture20240126.pcapng'
        # '/Users/wireshark/20231206.pcapng'
        , display_filter='tcp.srcport == 3000')

    date_text = '2023-10-05'
    db = mysql.connector.connect(host="localhost", user="suparera", password='34erdfcv', database="settradedb")
    mycursor = db.cursor()

    prev_date = stockeod_util.getPrevDate(date_text, mycursor)

    # loop through packets, get time from packet
    for inpack in cap:
        if hasattr(inpack, 'data'):
            streaming_packets = wsu.split_packet_data_to_stock_packets(inpack.data.data)
            for packet in streaming_packets:
                process_streaming_packets(packet)  # process streaming packet
