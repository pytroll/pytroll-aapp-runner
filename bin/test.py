#!/usr/bin/env python

import sys
import os
import os.path
import datetime as dt
import time

from posttroll.publisher import NoisyPublisher
from posttroll.message import Message
from trollsift import parse

def send_message(topic, info, message_type):
    '''Send message with the given topic and info'''
    pub_ = NoisyPublisher("dummy_sender", 0, topic)
    pub = pub_.start()
    time.sleep(2)
    msg = Message(topic, message_type, info)
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

    topic = "/EARS/VIIRS/SDR_compact"

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
    
    info_dicts = [{"proctime": "2016-08-09T09:41:22.282148", "end_decimal": 9, "orig_platform_name": "npp", "start_time": "2016-08-09T09:33:48", "orbit_number": 24786, "collection": [{"start_time": "2016-08-09T09:33:48", "end_time": "2016-08-09T09:35:12", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS/SVMC_npp_d20160809_t0933487_e0935129_b24786_c20160809094122282148_eum_ops.h5.bz2", "uid": "SVMC_npp_d20160809_t0933487_e0935129_b24786_c20160809094122282148_eum_ops.h5.bz2"}]}, {"start_time": "2016-08-09T09:35:14", "end_time": "2016-08-09T09:36:38", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS/SVMC_npp_d20160809_t0935141_e0936383_b24786_c20160809094124661132_eum_ops.h5.bz2", "uid": "SVMC_npp_d20160809_t0935141_e0936383_b24786_c20160809094124661132_eum_ops.h5.bz2"}]}, {"start_time": "2016-08-09T09:36:39", "end_time": "2016-08-09T09:38:03", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS/SVMC_npp_d20160809_t0936395_e0938038_b24786_c20160809094411806136_eum_ops.h5.bz2", "uid": "SVMC_npp_d20160809_t0936395_e0938038_b24786_c20160809094411806136_eum_ops.h5.bz2"}]}, {"start_time": "2016-08-09T09:38:04", "end_time": "2016-08-09T09:39:29", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS/SVMC_npp_d20160809_t0938048_e0939291_b24786_c20160809094630185129_eum_ops.h5.bz2", "uid": "SVMC_npp_d20160809_t0938048_e0939291_b24786_c20160809094630185129_eum_ops.h5.bz2"}]}, {"start_time": "2016-08-09T09:39:30", "end_time": "2016-08-09T09:40:54", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS/SVMC_npp_d20160809_t0939303_e0940545_b24786_c20160809094632646131_eum_ops.h5.bz2", "uid": "SVMC_npp_d20160809_t0939303_e0940545_b24786_c20160809094632646131_eum_ops.h5.bz2"}]}, {"start_time": "2016-08-09T09:40:55", "end_time": "2016-08-09T09:42:19", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS/SVMC_npp_d20160809_t0940557_e0942199_b24786_c20160809095004568135_eum_ops.h5.bz2", "uid": "SVMC_npp_d20160809_t0940557_e0942199_b24786_c20160809095004568135_eum_ops.h5.bz2"}]}, {"start_time": "2016-08-09T09:42:21", "end_time": "2016-08-09T09:43:45", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS/SVMC_npp_d20160809_t0942211_e0943453_b24786_c20160809095007078130_eum_ops.h5.bz2", "uid": "SVMC_npp_d20160809_t0942211_e0943453_b24786_c20160809095007078130_eum_ops.h5.bz2"}]}, {"start_time": "2016-08-09T09:43:46", "end_time": "2016-08-09T09:45:10", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS/SVMC_npp_d20160809_t0943465_e0945107_b24786_c20160809095244583173_eum_ops.h5.bz2", "uid": "SVMC_npp_d20160809_t0943465_e0945107_b24786_c20160809095244583173_eum_ops.h5.bz2"}]}, {"start_time": "2016-08-09T09:45:11", "end_time": "2016-08-09T09:46:36", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS/SVMC_npp_d20160809_t0945119_e0946361_b24786_c20160809095246946125_eum_ops.h5.bz2", "uid": "SVMC_npp_d20160809_t0945119_e0946361_b24786_c20160809095246946125_eum_ops.h5.bz2"}]}, {"start_time": "2016-08-09T09:46:37", "end_time": "2016-08-09T09:48:01", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS/SVMC_npp_d20160809_t0946373_e0948015_b24786_c20160809095553538118_eum_ops.h5.bz2", "uid": "SVMC_npp_d20160809_t0946373_e0948015_b24786_c20160809095553538118_eum_ops.h5.bz2"}]}, {"start_time": "2016-08-09T09:48:02", "end_time": "2016-08-09T09:49:26", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS/SVMC_npp_d20160809_t0948027_e0949269_b24786_c20160809095555697127_eum_ops.h5.bz2", "uid": "SVMC_npp_d20160809_t0948027_e0949269_b24786_c20160809095555697127_eum_ops.h5.bz2"}]}, {"start_time": "2016-08-09T09:49:28", "end_time": "2016-08-09T09:50:52", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS/SVMC_npp_d20160809_t0949281_e0950523_b24786_c20160809095857160179_eum_ops.h5.bz2", "uid": "SVMC_npp_d20160809_t0949281_e0950523_b24786_c20160809095857160179_eum_ops.h5.bz2"}]}], "platform_name": "Suomi-NPP", "end_time": "2016-08-09T09:50:52", "path": "", "sensor": ["viirs"], "collection_area_id": "eurol", "start_decimal": 7},]

    info_dicts = [{"stream": "eumetcast", "orig_platform_name": "M01", "start_time": "2016-08-12T13:00:00", "variant": "EARS", "collection": [{"uid": "AVHR_HRP_00_M01_20160812130000Z_20160812130100Z_N_O_20160812130322Z", "start_time": "2016-08-12T13:00:00", "end_time": "2016-08-12T13:01:00", "uri": "/data/MET/eumetcast-incomming/EARS-AVHRR-METOP-B-decompressed/AVHR_HRP_00_M01_20160812130000Z_20160812130100Z_N_O_20160812130322Z"}, {"uid": "AVHR_HRP_00_M01_20160812130100Z_20160812130200Z_N_O_20160812130323Z", "start_time": "2016-08-12T13:01:00", "end_time": "2016-08-12T13:02:00", "uri": "/data/MET/eumetcast-incomming/EARS-AVHRR-METOP-B-decompressed/AVHR_HRP_00_M01_20160812130100Z_20160812130200Z_N_O_20160812130323Z"}, {"uid": "AVHR_HRP_00_M01_20160812130200Z_20160812130300Z_N_O_20160812130323Z", "start_time": "2016-08-12T13:02:00", "end_time": "2016-08-12T13:03:00", "uri": "/data/MET/eumetcast-incomming/EARS-AVHRR-METOP-B-decompressed/AVHR_HRP_00_M01_20160812130200Z_20160812130300Z_N_O_20160812130323Z"}, {"uid": "AVHR_HRP_00_M01_20160812130300Z_20160812130400Z_N_O_20160812130652Z", "start_time": "2016-08-12T13:03:00", "end_time": "2016-08-12T13:04:00", "uri": "/data/MET/eumetcast-incomming/EARS-AVHRR-METOP-B-decompressed/AVHR_HRP_00_M01_20160812130300Z_20160812130400Z_N_O_20160812130652Z"}, {"uid": "AVHR_HRP_00_M01_20160812130400Z_20160812130500Z_N_O_20160812130653Z", "start_time": "2016-08-12T13:04:00", "end_time": "2016-08-12T13:05:00", "uri": "/data/MET/eumetcast-incomming/EARS-AVHRR-METOP-B-decompressed/AVHR_HRP_00_M01_20160812130400Z_20160812130500Z_N_O_20160812130653Z"}, {"uid": "AVHR_HRP_00_M01_20160812130500Z_20160812130600Z_N_O_20160812130653Z", "start_time": "2016-08-12T13:05:00", "end_time": "2016-08-12T13:06:00", "uri": "/data/MET/eumetcast-incomming/EARS-AVHRR-METOP-B-decompressed/AVHR_HRP_00_M01_20160812130500Z_20160812130600Z_N_O_20160812130653Z"}, {"uid": "AVHR_HRP_00_M01_20160812130600Z_20160812130700Z_N_O_20160812130952Z", "start_time": "2016-08-12T13:06:00", "end_time": "2016-08-12T13:07:00", "uri": "/data/MET/eumetcast-incomming/EARS-AVHRR-METOP-B-decompressed/AVHR_HRP_00_M01_20160812130600Z_20160812130700Z_N_O_20160812130952Z"}, {"uid": "AVHR_HRP_00_M01_20160812130700Z_20160812130800Z_N_O_20160812130953Z", "start_time": "2016-08-12T13:07:00", "end_time": "2016-08-12T13:08:00", "uri": "/data/MET/eumetcast-incomming/EARS-AVHRR-METOP-B-decompressed/AVHR_HRP_00_M01_20160812130700Z_20160812130800Z_N_O_20160812130953Z"}, {"uid": "AVHR_HRP_00_M01_20160812130800Z_20160812130900Z_N_O_20160812130953Z", "start_time": "2016-08-12T13:08:00", "end_time": "2016-08-12T13:09:00", "uri": "/data/MET/eumetcast-incomming/EARS-AVHRR-METOP-B-decompressed/AVHR_HRP_00_M01_20160812130800Z_20160812130900Z_N_O_20160812130953Z"}, {"uid": "AVHR_HRP_00_M01_20160812130900Z_20160812131000Z_N_O_20160812131122Z", "start_time": "2016-08-12T13:09:00", "end_time": "2016-08-12T13:10:00", "uri": "/data/MET/eumetcast-incomming/EARS-AVHRR-METOP-B-decompressed/AVHR_HRP_00_M01_20160812130900Z_20160812131000Z_N_O_20160812131122Z"}], "platform_name": "Metop-B", "end_time": "2016-08-12T13:10:00", "process_time": "2016-08-12T13:03:22", "sensor": "avhrr/3", "collection_area_id": "euron1"},]

    topic = "/EARS/METOP-AVHRR/L0"

    info_dicts =[{"end_decimal": 7, "stream": "eumetcast", "format": "SDR_compact", "orig_platform_name": "npp", "start_time": "2016-09-01T05:50:32", "variant": "EARS", "collection": [{"start_time": "2016-09-01T05:50:32", "end_time": "1900-01-01T05:51:56", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160901_t0550325_e0551567_b25110_c20160901055808624133_eum_ops.h5", "uid": "SVMC_npp_d20160901_t0550325_e0551567_b25110_c20160901055808624133_eum_ops.h5"}]}, {"start_time": "2016-09-01T05:51:57", "end_time": "1900-01-01T05:53:22", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160901_t0551579_e0553221_b25110_c20160901060110855130_eum_ops.h5", "uid": "SVMC_npp_d20160901_t0551579_e0553221_b25110_c20160901060110855130_eum_ops.h5"}]}, {"start_time": "2016-09-01T05:53:23", "end_time": "1900-01-01T05:54:47", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160901_t0553233_e0554475_b25110_c20160901060114182137_eum_ops.h5", "uid": "SVMC_npp_d20160901_t0553233_e0554475_b25110_c20160901060114182137_eum_ops.h5"}]}, {"start_time": "2016-09-01T05:54:48", "end_time": "1900-01-01T05:56:12", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160901_t0554487_e0556129_b25110_c20160901060416186113_eum_ops.h5", "uid": "SVMC_npp_d20160901_t0554487_e0556129_b25110_c20160901060416186113_eum_ops.h5"}]}, {"start_time": "2016-09-01T05:56:14", "end_time": "1900-01-01T05:57:38", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160901_t0556141_e0557383_b25110_c20160901060418451146_eum_ops.h5", "uid": "SVMC_npp_d20160901_t0556141_e0557383_b25110_c20160901060418451146_eum_ops.h5"}]}, {"start_time": "2016-09-01T05:57:39", "end_time": "1900-01-01T05:59:03", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-DNB-EARS-uncompressed/SVDNBC_npp_d20160901_t0557395_e0559037_b25110_c20160901060702000089_eum_ops.h5", "uid": "SVDNBC_npp_d20160901_t0557395_e0559037_b25110_c20160901060702000089_eum_ops.h5"}, {"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160901_t0557395_e0559037_b25110_c20160901060709972134_eum_ops.h5", "uid": "SVMC_npp_d20160901_t0557395_e0559037_b25110_c20160901060709972134_eum_ops.h5"}]}, {"start_time": "2016-09-01T05:59:04", "end_time": "1900-01-01T06:00:29", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-DNB-EARS-uncompressed/SVDNBC_npp_d20160901_t0559049_e0600291_b25110_c20160901060706000302_eum_ops.h5", "uid": "SVDNBC_npp_d20160901_t0559049_e0600291_b25110_c20160901060706000302_eum_ops.h5"}, {"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160901_t0559049_e0600291_b25110_c20160901060712348153_eum_ops.h5", "uid": "SVMC_npp_d20160901_t0559049_e0600291_b25110_c20160901060712348153_eum_ops.h5"}]}, {"start_time": "2016-09-01T06:00:30", "end_time": "1900-01-01T06:01:54", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-DNB-EARS-uncompressed/SVDNBC_npp_d20160901_t0600303_e0601545_b25110_c20160901061043000204_eum_ops.h5", "uid": "SVDNBC_npp_d20160901_t0600303_e0601545_b25110_c20160901061043000204_eum_ops.h5"}, {"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160901_t0600303_e0601545_b25110_c20160901061051514128_eum_ops.h5", "uid": "SVMC_npp_d20160901_t0600303_e0601545_b25110_c20160901061051514128_eum_ops.h5"}]}, {"start_time": "2016-09-01T06:01:55", "end_time": "1900-01-01T06:03:19", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-DNB-EARS-uncompressed/SVDNBC_npp_d20160901_t0601558_e0603199_b25110_c20160901061047000948_eum_ops.h5", "uid": "SVDNBC_npp_d20160901_t0601558_e0603199_b25110_c20160901061047000948_eum_ops.h5"}, {"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160901_t0601558_e0603199_b25110_c20160901061053869132_eum_ops.h5", "uid": "SVMC_npp_d20160901_t0601558_e0603199_b25110_c20160901061053869132_eum_ops.h5"}]}, {"start_time": "2016-09-01T06:03:21", "end_time": "1900-01-01T06:04:45", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-DNB-EARS-uncompressed/SVDNBC_npp_d20160901_t0603212_e0604453_b25110_c20160901061317000562_eum_ops.h5", "uid": "SVDNBC_npp_d20160901_t0603212_e0604453_b25110_c20160901061317000562_eum_ops.h5"}, {"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160901_t0603212_e0604453_b25110_c20160901061330420172_eum_ops.h5", "uid": "SVMC_npp_d20160901_t0603212_e0604453_b25110_c20160901061330420172_eum_ops.h5"}]}, {"start_time": "2016-09-01T06:04:46", "end_time": "1900-01-01T06:06:10", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-DNB-EARS-uncompressed/SVDNBC_npp_d20160901_t0604466_e0606107_b25110_c20160901061322000111_eum_ops.h5", "uid": "SVDNBC_npp_d20160901_t0604466_e0606107_b25110_c20160901061322000111_eum_ops.h5"}, {"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160901_t0604466_e0606107_b25110_c20160901061332808172_eum_ops.h5", "uid": "SVMC_npp_d20160901_t0604466_e0606107_b25110_c20160901061332808172_eum_ops.h5"}]}, {"start_time": "2016-09-01T06:06:12", "end_time": "1900-01-01T06:07:36", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-DNB-EARS-uncompressed/SVDNBC_npp_d20160901_t0606120_e0607364_b25110_c20160901061326000472_eum_ops.h5", "uid": "SVDNBC_npp_d20160901_t0606120_e0607364_b25110_c20160901061326000472_eum_ops.h5"}, {"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160901_t0606120_e0607364_b25110_c20160901061335227124_eum_ops.h5", "uid": "SVMC_npp_d20160901_t0606120_e0607364_b25110_c20160901061335227124_eum_ops.h5"}]}], "start_decimal": 5, "type": "HDF5", "segment": "M", "proctime": "20160901055808624133", "data_processing_level": "1B", "collection_area_id": "eurol", "orbit_number": 25110, "platform_name": "Suomi-NPP", "end_time": "1900-01-01T06:07:36", "sensor": "viirs"},]    

    topic = "/EARS/VIIRS/SDR_compact"

    info_dicts =[{"end_decimal": 8, "stream": "eumetcast", "format": "SDR_compact", "orig_platform_name": "npp", "start_time": "2016-09-22T05:52:53", "variant": "EARS", "collection": [{"start_time": "2016-09-22T05:52:53", "end_time": "1900-01-01T05:54:17", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160922_t0552536_e0554178_b25408_c20160922060047059124_eum_ops.h5", "uid": "SVMC_npp_d20160922_t0552536_e0554178_b25408_c20160922060047059124_eum_ops.h5"}]}, {"start_time": "2016-09-22T05:54:19", "end_time": "1900-01-01T05:55:43", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160922_t0554190_e0555432_b25408_c20160922060049182153_eum_ops.h5", "uid": "SVMC_npp_d20160922_t0554190_e0555432_b25408_c20160922060049182153_eum_ops.h5"}]}, {"start_time": "2016-09-22T05:55:44", "end_time": "1900-01-01T05:57:08", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160922_t0555444_e0557086_b25408_c20160922060442248139_eum_ops.h5", "uid": "SVMC_npp_d20160922_t0555444_e0557086_b25408_c20160922060442248139_eum_ops.h5"}]}, {"start_time": "2016-09-22T05:57:09", "end_time": "1900-01-01T05:58:34", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160922_t0557098_e0558340_b25408_c20160922060445370163_eum_ops.h5", "uid": "SVMC_npp_d20160922_t0557098_e0558340_b25408_c20160922060445370163_eum_ops.h5"}]}, {"start_time": "2016-09-22T05:58:35", "end_time": "1900-01-01T05:59:59", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160922_t0558352_e0559594_b25408_c20160922060726885128_eum_ops.h5", "uid": "SVMC_npp_d20160922_t0558352_e0559594_b25408_c20160922060726885128_eum_ops.h5"}]}, {"start_time": "2016-09-22T06:00:00", "end_time": "1900-01-01T06:01:24", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160922_t0600006_e0601248_b25408_c20160922060729316129_eum_ops.h5", "uid": "SVMC_npp_d20160922_t0600006_e0601248_b25408_c20160922060729316129_eum_ops.h5"}]}, {"start_time": "2016-09-22T06:01:26", "end_time": "1900-01-01T06:02:50", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-DNB-EARS-uncompressed/SVDNBC_npp_d20160922_t0601260_e0602502_b25408_c20160922061012000736_eum_ops.h5", "uid": "SVDNBC_npp_d20160922_t0601260_e0602502_b25408_c20160922061012000736_eum_ops.h5"}, {"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160922_t0601260_e0602502_b25408_c20160922061020033128_eum_ops.h5", "uid": "SVMC_npp_d20160922_t0601260_e0602502_b25408_c20160922061020033128_eum_ops.h5"}]}, {"start_time": "2016-09-22T06:02:51", "end_time": "1900-01-01T06:04:15", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-DNB-EARS-uncompressed/SVDNBC_npp_d20160922_t0602514_e0604156_b25408_c20160922061016000791_eum_ops.h5", "uid": "SVDNBC_npp_d20160922_t0602514_e0604156_b25408_c20160922061016000791_eum_ops.h5"}, {"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160922_t0602514_e0604156_b25408_c20160922061022193206_eum_ops.h5", "uid": "SVMC_npp_d20160922_t0602514_e0604156_b25408_c20160922061022193206_eum_ops.h5"}]}, {"start_time": "2016-09-22T06:04:16", "end_time": "1900-01-01T06:05:41", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-DNB-EARS-uncompressed/SVDNBC_npp_d20160922_t0604168_e0605410_b25408_c20160922061305000406_eum_ops.h5", "uid": "SVDNBC_npp_d20160922_t0604168_e0605410_b25408_c20160922061305000406_eum_ops.h5"}, {"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160922_t0604168_e0605410_b25408_c20160922061317928135_eum_ops.h5", "uid": "SVMC_npp_d20160922_t0604168_e0605410_b25408_c20160922061317928135_eum_ops.h5"}]}, {"start_time": "2016-09-22T06:05:42", "end_time": "1900-01-01T06:07:06", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-DNB-EARS-uncompressed/SVDNBC_npp_d20160922_t0605422_e0607064_b25408_c20160922061310000015_eum_ops.h5", "uid": "SVDNBC_npp_d20160922_t0605422_e0607064_b25408_c20160922061310000015_eum_ops.h5"}, {"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160922_t0605422_e0607064_b25408_c20160922061320274131_eum_ops.h5", "uid": "SVMC_npp_d20160922_t0605422_e0607064_b25408_c20160922061320274131_eum_ops.h5"}]}, {"start_time": "2016-09-22T06:07:07", "end_time": "1900-01-01T06:08:31", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-DNB-EARS-uncompressed/SVDNBC_npp_d20160922_t0607076_e0608318_b25408_c20160922061652000908_eum_ops.h5", "uid": "SVDNBC_npp_d20160922_t0607076_e0608318_b25408_c20160922061652000908_eum_ops.h5"}, {"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160922_t0607076_e0608318_b25408_c20160922061656858135_eum_ops.h5", "uid": "SVMC_npp_d20160922_t0607076_e0608318_b25408_c20160922061656858135_eum_ops.h5"}]}, {"start_time": "2016-09-22T06:08:33", "end_time": "1900-01-01T06:09:57", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-DNB-EARS-uncompressed/SVDNBC_npp_d20160922_t0608330_e0609572_b25408_c20160922061702000425_eum_ops.h5", "uid": "SVDNBC_npp_d20160922_t0608330_e0609572_b25408_c20160922061702000425_eum_ops.h5"}, {"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160922_t0608330_e0609572_b25408_c20160922061659273128_eum_ops.h5", "uid": "SVMC_npp_d20160922_t0608330_e0609572_b25408_c20160922061659273128_eum_ops.h5"}]}, {"start_time": "2016-09-22T06:09:58", "end_time": "1900-01-01T06:11:22", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-DNB-EARS-uncompressed/SVDNBC_npp_d20160922_t0609585_e0611226_b25408_c20160922061927000386_eum_ops.h5", "uid": "SVDNBC_npp_d20160922_t0609585_e0611226_b25408_c20160922061927000386_eum_ops.h5"}, {"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160922_t0609585_e0611226_b25408_c20160922061931356143_eum_ops.h5", "uid": "SVMC_npp_d20160922_t0609585_e0611226_b25408_c20160922061931356143_eum_ops.h5"}]}, {"start_time": "2016-09-22T06:11:23", "end_time": "1900-01-01T06:12:48", "dataset": [{"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-M-EARS-uncompressed/SVMC_npp_d20160922_t0611239_e0612480_b25408_c20160922061933751145_eum_ops.h5", "uid": "SVMC_npp_d20160922_t0611239_e0612480_b25408_c20160922061933751145_eum_ops.h5"}, {"uri": "/data/MET/eumetcast-incomming/NPP-VIIRS-DNB-EARS-uncompressed/SVDNBC_npp_d20160922_t0611239_e0612480_b25408_c20160922061939000100_eum_ops.h5", "uid": "SVDNBC_npp_d20160922_t0611239_e0612480_b25408_c20160922061939000100_eum_ops.h5"}]}], "start_decimal": 6, "type": "HDF5", "segment": "M", "proctime": "20160922060047059124", "data_processing_level": "1B", "collection_area_id": "eurol", "orbit_number": 25408, "platform_name": "Suomi-NPP", "end_time": "1900-01-01T06:12:48", "sensor": "viirs"},]

    info_dicts = [{"origin": "157.249.17.66:9092", "uid": "AVHR_HRP_00_M02_20160617095302Z_20160617095954Z_N_O_20160617095305Z",
                   "antenna": "XL-BAND", "process_time": "2016-06-17T09:53:05", "start_time": "2016-06-17T09:53:02",
                   "orbit_number": 50132,
                   "uri": "file://157.249.116.173/disk1/testdata/AVHR_HRP_00_M02_20160617095302Z_20160617095954Z_N_O_20160617095305Z",
                   "platform_name": "Metop-A", "end_time": "2016-06-17T09:59:54",
                   "pass_key": "65913dde99d8537bb959453e23eb296f", "sensor": "avhrr/3"},
                  ]
    
    topic = "/XLBANDANTENNA/METOP-AVHRR/L0"
    
    topic = "/XLBANDANTENNA/METOP/L0"
    
    info_dicts = [{"origin": "157.249.17.66:9103",
                   "sensor": ["amsu-a", "avhrr/3", "hirs/4", "mhs"],
                   "antenna": "XL-BAND", "processing_time": "2016-12-02T11:04:25", "process_time": "2016-12-02T11:04:25",
                   "start_time": "2016-12-02T11:04:03",
                   "orbit_number": 21833,
                   "dataset": [{"uri": "file:///disk1/testdata/AMSA_HRP_00_M01_20161202110403Z_20161202111652Z_N_O_20161202110425Z",
                                "uid": "AMSA_HRP_00_M01_20161202110403Z_20161202111652Z_N_O_20161202110425Z"},
                               {"uri": "file:///disk1/testdata/AVHR_HRP_00_M01_20161202110414Z_20161202111708Z_N_O_20161202110417Z",
                                "uid": "AVHR_HRP_00_M01_20161202110414Z_20161202111708Z_N_O_20161202110417Z"},
                               {"uri": "file:///disk1/testdata/HIRS_HRP_00_M01_20161202110410Z_20161202111652Z_N_O_20161202110430Z",
                                "uid": "HIRS_HRP_00_M01_20161202110410Z_20161202111652Z_N_O_20161202110430Z"},
                               {"uri": "file:///disk1/testdata/MHSx_HRP_00_M01_20161202110406Z_20161202111658Z_N_O_20161202110419Z",
                                "uid": "MHSx_HRP_00_M01_20161202110406Z_20161202111658Z_N_O_20161202110419Z"}],
                   "platform_name": "Metop-B",
                   "end_time": "2016-12-02T11:16:52", "orig_platform_name": "M01",
                   "pass_key": "e613c5becfde7b0d96a4829c184ea197"},
                  ]

    topic = "/XLBANDANTENNA/HRPT/L0"

    info_dicts = [{"format": "HRPT",
                   "sensor": ["avhrr/3", "mhs", "amsu-b", "amsu-a", "hirs/3", "hirs/4"],
                   "start_time": "2016-11-30T12:50:55",
                   "orbit_number": "40262",
                   "uri": "/disk2/testdata/clear_NO19_40262_2016-11-30T12:50:55.289_160-expanded-16bit",
                   "platform_name": "NOAA-19",
                   "data_processing_level": "L0"},]
    
    #info_dicts = [{"format": "HRPT", "sensor": ["avhrr/3", "mhs", "amsu-b", "amsu-a", "hirs/3", "hirs/4"],
    #                "start_time": "2016-06-16T02:43:43", "orbit_number": "37899",
    #                 "uri": "/disk1/trollduction-test/data/clear_NO19_37899_2016-06-16T02:43:43.827_951-expanded-16bit",
    #                  "platform_name": "NOAA-19", "data_processing_level": "L0"},]
    
    
    #info_dicts = [{"uid": "20160428063545_NOAA_18.hmf", "format": "HRPT", "sensor": ["avhrr/3", "mhs", "amsu-a", "hirs/4"],
    #                "start_time": "2016-04-28T06:35:45", "orbit_number": 56368,
    #                 "uri": "ssh://193.166.211.186/lustre/tmp/data/oper/avhrr/hrpt/level0/20160428063545_NOAA_18.hmf",
    #                  "platform_name": "NOAA-18", "end_time": "2016-04-28T06:50:33",
    #                  "type": "binary", "data_processing_level": "0"},]

    topic = "/BANDANTENNA/HRPT/L0"

    info_dicts = [{"uid": "clear_NO15_2470_2018-01-22T09:18:16.681_756-expanded-16bit", "format": "HRPT", "sensor": ["avhrr/3", "amsu-b", "amsu-a", "mhs", "hirs/3", "hirs/4"], "start_time": "2018-01-22T09:18:16", "orbit_number": "2470", "uri": "/data/pytroll/hrpt-16bit/clear_NO15_2470_2018-01-22T09:18:16.681_756-expanded-16bit", "platform_name": "NOAA-15", "data_processing_level": "L0"},]

    message_type = 'collection'
    message_type = 'file'
    #message_type = 'dataset'

    for info_dict in info_dicts:
        send_message(topic, info_dict, message_type)

if __name__ == "__main__":
    main()
