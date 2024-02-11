import os

import pyshark
from module_init import logger
import wireshark_util as wsu

log = logger()

def process_streaming_packets(packet):
    ''' process streaming packet '''
    print("pid: {}, packet: {}".format(os.getpid(), packet))

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    cap = pyshark.FileCapture(
        '/Users/wireshark/20231206.pcapng', display_filter='tcp.srcport == 3000')

    # loop through packets, get time from packet
    for inpack in cap:
        if hasattr(inpack, 'data'):
            streaming_packets = wsu.split_packet_data_to_stock_packets(inpack.data.data)
            for packet in streaming_packets:
                process_streaming_packets(packet)  # process streaming packet
