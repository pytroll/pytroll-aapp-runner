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
    time.sleep(1)
    msg = Message(topic, 'file', info)
    print "Sending message: %s" % str(msg)
    pub.send(str(msg))
    pub_.stop()

def main():
    '''Main.'''

    topic = "/TEST/HRPT/0"
    #info_dicts = [{"format": "HRPT", "sensor": ["avhrr/3", "mhs", "amsu-b", "amsu-a", "hirs/3", "hirs/4"],
    #                "start_time": "2016-06-15T07:24:48", "orbit_number": "57047",
    #                 "uri": "/disk1/trollduction-test/data/clear_NO18_57047_2016-06-15T07:24:48.703_790-expanded-16bit",
    #                  "platform_name": "NOAA-18", "data_processing_level": "L0"},]
    info_dicts = [{"format": "HRPT", "sensor": ["avhrr/3", "mhs", "amsu-b", "amsu-a", "hirs/3", "hirs/4"],
                    "start_time": "2016-06-16T02:43:43", "orbit_number": "37899",
                     "uri": "/disk1/trollduction-test/data/clear_NO19_37899_2016-06-16T02:43:43.827_951-expanded-16bit",
                      "platform_name": "NOAA-19", "data_processing_level": "L0"},]
    #info_dicts = [{"uid": "20160428063545_NOAA_18.hmf", "format": "HRPT", "sensor": ["avhrr/3", "mhs", "amsu-a", "hirs/4"],
    #                "start_time": "2016-04-28T06:35:45", "orbit_number": 56368,
    #                 "uri": "ssh://193.166.211.186/lustre/tmp/data/oper/avhrr/hrpt/level0/20160428063545_NOAA_18.hmf",
    #                  "platform_name": "NOAA-18", "end_time": "2016-04-28T06:50:33",
    #                  "type": "binary", "data_processing_level": "0"},]

    for info_dict in info_dicts:
        send_message(topic, info_dict)

if __name__ == "__main__":
    main()