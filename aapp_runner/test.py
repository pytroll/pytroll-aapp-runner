#!/usr/bin/env python

import sys
import os
import os.path
import datetime as dt
import time

from posttroll.publisher import NoisyPublisher
from posttroll.message import Message
from trollsift import parse

def send_message(topic, info):
    '''Send message with the given topic and info'''
    pub_ = NoisyPublisher("dummy_sender", 0, topic)
    pub = pub_.start()
    time.sleep(2)
    msg = Message(topic, 'file', info)
    print "Sending message: %s" % str(msg)
    pub.send(str(msg))
    pub_.stop()

def main():
    '''Main.'''

    topic = "/CHECK/LBANDANTENNA/HRPT/L0"
    #info_dicts = [{"format": "HRPT", "sensor": ["avhrr/3", "mhs", "amsu-b", "amsu-a", "hirs/3", "hirs/4"],
    #                "start_time": "2016-06-15T07:24:48", "orbit_number": "57047",
    #                 "uri": "/disk1/trollduction-test/data/clear_NO18_57047_2016-06-15T07:24:48.703_790-expanded-16bit",
    #                  "platform_name": "NOAA-18", "data_processing_level": "L0"},]


    info_dicts = [{"origin": "157.249.17.66:9093", "uid": "MHSx_HRP_00_M01_20160622071459Z_20160622072436Z_N_O_20160622071512Z", "antenna": "XL-BAND",
                   "process_time": "2016-06-22T07:15:12", "start_time": "2016-06-22T07:14:59", "orbit_number": 19515,
                   "uri": "file:///disk1/trollduction-test/data/MHSx_HRP_00_M01_20160622071459Z_20160622072436Z_N_O_20160622071512Z",
                   "platform_name": "M01", "end_time": "2016-06-22T07:24:36", "pass_key": "bc67690fb42f2d219fb7fbec84a6d008", "sensor": "mhs"},
                  {"origin": "157.249.17.66:9092", "uid": "AVHR_HRP_00_M01_20160622071505Z_20160622072446Z_N_O_20160622071509Z", "antenna": "XL-BAND",
                   "process_time": "2016-06-22T07:15:09", "start_time": "2016-06-22T07:15:05", "orbit_number": 19515,
                   "uri": "file:///disk1/trollduction-test/data/AVHR_HRP_00_M01_20160622071505Z_20160622072446Z_N_O_20160622071509Z",
                   "platform_name": "M01", "end_time": "2016-06-22T07:24:46", "pass_key": "bc67690fb42f2d219fb7fbec84a6d008", "sensor": "avhrr/3"},
                  {"origin": "157.249.17.66:9094", "uid": "AMSA_HRP_00_M01_20160622071459Z_20160622072428Z_N_O_20160622071522Z", "antenna": "XL-BAND",
                   "process_time": "2016-06-22T07:15:22", "start_time": "2016-06-22T07:14:59", "orbit_number": 19515,
                   "uri": "file:///disk1/trollduction-test/data/AMSA_HRP_00_M01_20160622071459Z_20160622072428Z_N_O_20160622071522Z",
                   "platform_name": "M01", "end_time": "2016-06-22T07:24:28", "pass_key": "bc67690fb42f2d219fb7fbec84a6d008", "sensor": "amsu-a"},
                  {"origin": "157.249.17.66:9095", "uid": "HIRS_HRP_00_M01_20160622071501Z_20160622072425Z_N_O_20160622071519Z", "antenna": "XL-BAND",
                   "process_time": "2016-06-22T07:15:19", "start_time": "2016-06-22T07:15:01", "orbit_number": 19515,
                   "uri": "file:///disk1/trollduction-test/data/HIRS_HRP_00_M01_20160622071501Z_20160622072425Z_N_O_20160622071519Z",
                   "platform_name": "M01", "end_time": "2016-06-22T07:24:25", "pass_key": "bc67690fb42f2d219fb7fbec84a6d008", "sensor": "hirs/4"},]

    info_dicts = [{"origin": "157.249.17.66:9092", "uid": "AVHR_HRP_00_M02_20160617095302Z_20160617095954Z_N_O_20160617095305Z",
                   "antenna": "XL-BAND", "process_time": "2016-06-17T09:53:05", "start_time": "2016-06-17T09:53:02",
                   #"orbit_number": 50132,
                   "uri": "file:///disk1/trollduction-test/data/AVHR_HRP_00_M02_20160617095302Z_20160617095954Z_N_O_20160617095305Z",
                   "platform_name": "Metop-A", "end_time": "2016-06-17T09:59:54",
                   "pass_key": "65913dde99d8537bb959453e23eb296f", "sensor": "avhrr/3"},
                   {"origin": "157.249.17.66:9093", "uid": "MHSx_HRP_00_M02_20160617095255Z_20160617095944Z_N_O_20160617095308Z",
                    "antenna": "XL-BAND", "process_time": "2016-06-17T09:53:08", "start_time": "2016-06-17T09:52:55",
                    #orbit_number": 50132,
                    "uri": "file:///disk1/trollduction-test/data/MHSx_HRP_00_M02_20160617095255Z_20160617095944Z_N_O_20160617095308Z",
                    "platform_name": "Metop-A", "end_time": "2016-06-17T09:59:44",
                    "pass_key": "65913dde99d8537bb959453e23eb296f", "sensor": "mhs"},
                  {"origin": "157.249.17.66:9095", "uid": "HIRS_HRP_00_M02_20160617095255Z_20160617095939Z_N_O_20160617095315Z",
                   "antenna": "XL-BAND", "process_time": "2016-06-17T09:53:15", "start_time": "2016-06-17T09:52:55",
                   #"orbit_number": 50132,
                   "uri": "file:///disk1/trollduction-test/data/HIRS_HRP_00_M02_20160617095255Z_20160617095939Z_N_O_20160617095315Z",
                   "platform_name": "Metop-A", "end_time": "2016-06-17T09:59:39",
                   "pass_key": "65913dde99d8537bb959453e23eb296f", "sensor": "hirs/4"},
                  {"origin": "157.249.17.66:9094", "uid": "AMSA_HRP_00_M02_20160617095255Z_20160617095936Z_N_O_20160617095316Z",
                   "antenna": "XL-BAND", "process_time": "2016-06-17T09:53:16", "start_time": "2016-06-17T09:52:55",
                   #"orbit_number": 50132,
                   "uri": "file:///disk1/trollduction-test/data/AMSA_HRP_00_M02_20160617095255Z_20160617095936Z_N_O_20160617095316Z",
                   "platform_name": "Metop-A", "end_time": "2016-06-17T09:59:36",
                   "pass_key": "65913dde99d8537bb959453e23eb296f", "sensor": "amsu-a"},
                  ]
    
    #info_dicts = [{"format": "HRPT", "sensor": ["avhrr/3", "mhs", "amsu-b", "amsu-a", "hirs/3", "hirs/4"],
    #                "start_time": "2016-06-16T02:43:43", "orbit_number": "37899",
    #                 "uri": "/disk1/trollduction-test/data/clear_NO19_37899_2016-06-16T02:43:43.827_951-expanded-16bit",
    #                  "platform_name": "NOAA-19", "data_processing_level": "L0"},]
    
    
    #info_dicts = [{"uid": "20160428063545_NOAA_18.hmf", "format": "HRPT", "sensor": ["avhrr/3", "mhs", "amsu-a", "hirs/4"],
    #                "start_time": "2016-04-28T06:35:45", "orbit_number": 56368,
    #                 "uri": "ssh://193.166.211.186/lustre/tmp/data/oper/avhrr/hrpt/level0/20160428063545_NOAA_18.hmf",
    #                  "platform_name": "NOAA-18", "end_time": "2016-04-28T06:50:33",
    #                  "type": "binary", "data_processing_level": "0"},]

    for info_dict in info_dicts:
        send_message(topic, info_dict)

if __name__ == "__main__":
    main()