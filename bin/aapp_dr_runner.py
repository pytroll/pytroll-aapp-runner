#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014, 2015, 2016 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <adam.dybbroe@smhi.se>
#   Janne Kotro fmi.fi
#   Trygve Aspenes <trygveas@met.no>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""AAPP Level-1 processing on NOAA and Metop HRPT Direct Readout data. Listens
for pytroll messages from Nimbus (NOAA/Metop file dispatch) and triggers
processing on direct readout HRPT level 0 files (full swaths - no granules at
the moment)
"""
from ConfigParser import RawConfigParser
import os
import sys
import logging
from logging import handlers
from trollsift.parser import compose

from aapp_runner.read_aapp_config import read_config_file_options
from aapp_runner.tle_satpos_prepare import do_tleing
from aapp_runner.tle_satpos_prepare import do_tle_satpos
from aapp_runner.do_commutation import do_decommutation
from aapp_runner.exceptions import TleError, SatposError, DecommutationError

import socket
import netifaces
from aapp_runner.helper_functions import run_shell_command

import copy

LOG = logging.getLogger(__name__)


# ----------------------------
# Default settings for logging
# ----------------------------
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

# -------------------------------
# Default settings for satellites
# -------------------------------
TLE_SATNAME = {'NOAA-19': 'NOAA 19', 'NOAA-18': 'NOAA 18',
               'NOAA-15': 'NOAA 15',
               'Metop-A': 'METOP-A', 'Metop-B': 'METOP-B',
               'Metop-C': 'METOP-C'}

METOP_NAME = {'metop01': 'Metop-B', 'metop02': 'Metop-A'}
METOP_NAME_INV = {'metopb': 'metop01', 'metopa': 'metop02'}
SATELLITE_NAME = {'NOAA-19': 'noaa19', 'NOAA-18': 'noaa18',
                  'NOAA-15': 'noaa15', 'NOAA-14': 'noaa14',
                  'Metop-A': 'metop02', 'Metop-B': 'metop01',
                  'Metop-C': 'metop03'}

SENSOR_NAMES = ['amsu-a', 'amsu-b', 'mhs', 'avhrr/3', 'hirs/4']

SENSOR_NAME_CONVERTER = {
    'amsua': 'amsu-a',
    'amsub': 'amsu-b',
    'hirs': 'hirs/4',
    'mhs': 'mhs',
    'avhrr': 'avhrr/3',
    
    'amsu-a': 'amsua',
    'amsu-b': 'amsub',
    'hirs/4': 'hirs',
    'hirs/3': 'hirs',
    'mhs': 'mhs',
    'avhrr/3': 'avhrr'}

METOP_NUMBER = {'b': '01', 'a': '02'}

"""
These are the standard names used by the various AAPP decommutation scripts.
If you change these, you will also have to change the decommutation scripts.
"""

from urlparse import urlparse
import posttroll.subscriber
from posttroll.publisher import Publish
from posttroll.message import Message
from aapp_runner.helper_functions import overlapping_timeinterval

import tempfile
from glob import glob
# import os
import shutil
# import aapp_stat
import threading
from subprocess import Popen, PIPE
import shlex
# import subrocess
from datetime import timedelta, datetime
from time import time as _time


def get_local_ips():
    inet_addrs = [netifaces.ifaddresses(iface).get(netifaces.AF_INET)
                  for iface in netifaces.interfaces()]
    ips = []
    for addr in inet_addrs:
        if addr is not None:
            for add in addr:
                ips.append(add['addr'])
    return ips

def reset_job_registry(objdict, key, start_end_times):
    """Remove job key from registry"""

    LOG.debug("Register: " + str(objdict))
    starttime, endtime = start_end_times
    if key in objdict:
        if objdict[key] and len(objdict[key]) > 0:
            objdict[key].remove(start_end_times)
            LOG.debug("Release/reset job-key " + str(key) + " " +
                      str(starttime) + " " + str(endtime) +
                      " from job registry")
            LOG.debug("Register: " + str(objdict))
            return

    LOG.warning("Nothing to reset/release - " +
                "Register didn't contain any entry matching: " +
                str(key))
    return

class AappL1Config(object):
    """
    Container for the configuration for AAPP
    """

    def __init__(self, config, process_name):
        """
        Init the config
        """
        self.orig_config = copy.deepcopy(config)
        self.config = config
        self.process_name = process_name
        self.job_register = {}
        self.local_env = {}

    def __getitem__(self, key):
        return self.config[key]

    def __setitem__(self, key, value):
        self.config[key] = value

    def reset(self):
        """
        Clear/reset dynamic configuration
        """
        self.config = {}
        self.config = copy.deepcopy(self.orig_config)
        self.local_env = {}
        self.local_env = os.environ.copy()
        
    def add_process_config_paramenter(self, config_key, config_value):
        """
        Add a config parameter to the running config
        """
        self.config['aapp_processes'][self.process_name][config_key] = config_value
    
    def get_parameter(self, key):
        return self.config['aapp_processes'][self.process_name][key]
         

def cleanup_aapp_logfiles_archive(config):
    """
    Loop over the aapp log files directories and remove expired directories accordingly
    """

    try:
        directory_list = glob('%s/*' % config['aapp_processes'][config.process_name]['aapp_log_files_archive_dir'])
        dummy = [delete_old_dirs(s,config['aapp_processes'][config.process_name]['aapp_log_files_archive_length']) for s in directory_list if os.path.isdir(s)]
    except Exception as err:
        LOG.error("Failed with: {}".format(err))
        return False
    
    return True

def delete_old_dirs(dir_path, older_than_days):
    """
    Delete old directories
    """
    try:
        older_than = int(older_than_days) * 86400  # convert days to seconds
        time_now = _time()
        if (time_now - os.path.getmtime(dir_path)) > older_than:
            LOG.debug("Removing: {} and all its content.".format(dir_path))
            shutil.rmtree(dir_path)
    except ValueError as ve:
        LOG.error("Failed to handle value {} as int: {}".format(older_than_days,ve))
        LOG.error("Will NOT clean the directory: {}".format(dir_path))
        return False
    except Exception as err:
        LOG.error("Failed with {}".format(err))
        return False
    
    return True
    
def cleanup_aapp_workdir(config):
    """Clean up the AAPP working dir after processing"""

    try:
        filelist = glob('%s/*' % config['aapp_processes'][config.process_name]['working_dir'])
        dummy = [os.remove(s) for s in filelist if os.path.isfile(s)]
        #filelist = glob('%s/*' % self.working_dir)
        #LOG.info("Number of items left after cleaning working dir = " +
        #          str(len(filelist)))
        #LOG.debug("Files: " + str(filelist))
        shutil.rmtree(config['aapp_processes'][config.process_name]['working_dir'])
    except Exception as err:
        LOG.warning("Failed to cleanup working dir: {}".format(err))
        return False
        
    return True


def move_aapp_log_files(config):
    """ Move AAPP processing log files from AAPP working directory
    in to sub-directory.
    The directory path is defined in config file (aapp_log_files)
    """
    try:
        filelist = glob('%s/*.log' % config['aapp_processes'][config.process_name]['working_dir'])
        
        try:
            tmp_config = config.config.copy()
            tmp_config.update(tmp_config['aapp_processes'][config.process_name])

            _outdir = compose(tmp_config['aapp_outdir_format'],tmp_config)
            destination = os.path.join(tmp_config['aapp_log_files_archive_dir'], _outdir)
        except KeyError as err:
            LOG.error("Failed to compose log files dir: {}. Missing key:{}".format(config['aapp_processes'][config.process_name]['aapp_outdir_format'],err))
            return False
        except Exception as err:
            LOG.error("Failed to compose log files dir: {}. Error:{}".format(config['aapp_processes'][config.process_name]['aapp_outdir_format'],err))
            return False

        LOG.debug("move_aapp_log_files destination: " + destination)

        if not os.path.exists(destination):
            try:
                os.makedirs(destination)
            except OSError as err:
                LOG.error("Can't create directory: {} because: {}".format(destination, err))
                return False  # FIXME: Check!
            else:
                LOG.debug("Created new directory for AAPP log files:" + destination)

        for file_name in filelist:
            try:
                base_filename = os.path.basename(file_name)
                dst = os.path.join(destination, base_filename)
                shutil.move(file_name, dst)
            except Exception as err:
                LOG.warning("Failed to move log file: {} to: {}".format(file_name, dst))
            else:
                LOG.debug("Moved {} to {}".format(file_name,dst))

    except OSError as err:
        LOG.error("Moving AAPP log files to " + destination + " failed ", err)

    LOG.info("AAPP log files saved in to " + destination)

    return True

def check_scene_id(self, scene_id):
    # Check for keys representing the same scene (slightly different
    # start/end times):
    LOG.debug("Level-0files = " + str(self.level0files))
    time_thr = timedelta(seconds=30)#FIXME configure
    for key in self.level0files:
        pltrfn, startt, endt = key.split('_')
        if not self.platform_name == pltrfn:
            continue
        t1_ = datetime.strptime(startt, '%Y%m%d%H%M%S')
        t2_ = datetime.strptime(endt, '%Y%m%d%H%M%S')
        # Get the relative time overlap:
        sec_inside = (
            min(t2_, self.endtime) - max(t1_, self.starttime)).total_seconds()
        dsec = (t2_ - t1_).total_seconds()
        if dsec < 0.01:
            LOG.warning(
                "Something awkward with this scene: start_time = end_time!")
            break
        elif float(sec_inside / dsec) > 0.85:
            # It is the same scene!
            LOG.debug(
                "It is the same scene,"
                " though the file times may deviate a bit...")
            scene_id = key
            break

        elif float(sec_inside / dsec) > 0.01:
            LOG.warning("There was an overlap but probably not the " +
                        "same scene: Time interval = " +
                        "(%s, %s)",
                        t1_.strftime('%Y-%m-%d %H:%M:%S'),
                        t2_.strftime('%Y-%m-%d %H:%M:%S'))
    return scene_id

def sensors_to_process(self, msg, sensors):
    LOG.debug("Sensor = " + str(msg.data['sensor']))
    LOG.debug("type: " + str(type(msg.data['sensor'])))
    if isinstance(msg.data['sensor'], (str, unicode)):
        sensors.append(msg.data['sensor'])
    elif isinstance(msg.data['sensor'], (list, set, tuple)):
        sensors.extend(msg.data['sensor'])
    else:
        sensors = []
        LOG.warning('Failed interpreting sensor(s)!')

    LOG.info("Sensor(s): " + str(sensors))
    sensor_ok = False
    for sensor in sensors:
        if sensor in SENSOR_NAMES:
            sensor_ok = True
            break
    if not sensor_ok:
        LOG.info("No required sensors....")
        return False
        
    return True

def available_sensors(self, msg, sensors, scene_id):
    if scene_id not in self.level0files:
        LOG.debug("Reset level-0 files: scene_id = " + str(scene_id))
        self.level0files[scene_id] = []

    for sensor in sensors:
        item = (self.level0_filename, sensor)
        if item not in self.level0files[scene_id]:
            self.level0files[scene_id].append(item)
            LOG.debug("Appending item to list: " + str(item))
        else:
            LOG.debug("item already in list: " + str(item))

    if len(self.level0files[scene_id]) < 4 and msg.data.get("variant") != "EARS":
        LOG.info("Not enough sensor data available yet. " +
                 "Level-0 files = " +
                 str(self.level0files[scene_id]))
        return False
    else:
        LOG.info("Level 0 files ready: " + str(self.level0files[scene_id]))

    return True


def block_before_rerun(config, msg):
    """
    Add run to registry to block this from rerun if that is configured
    """

    if config['platform_name'] not in config.job_register.keys():
        config.job_register[config['platform_name']] = []

    config.job_register[config['platform_name']].append((config['starttime'], config['endtime']))
    LOG.debug("End: job register = " + str(config.job_register))

    try:
        # Block any future run on this scene for time_to_block_before_rerun
        # (e.g. 10) minutes from now:
        t__ = threading.Timer(config['aapp_processes'][config.process_name]['locktime_before_rerun'],
                              reset_job_registry, args=(config.job_register,
                                                        config['platform_name'],
                                                        (config['starttime'],
                                                         config['endtime'])))
        t__.start()

        LOG.debug("After timer call: job register = " + str(config.job_register))

        #LOG.info("Ready with AAPP level-1 processing on NOAA scene: " + str(fname))
        #LOG.info("working dir: self.working_dir = " + str(config['aapp_processes'][config.process_name]['working_dir']))
    except Exception as err:
        LOG.error("Failed because of: {}".format(err))
        
    return True

def read_arguments():
    """
    Read command line arguments
    Return
    name of the station, environment, config file and log file
    """
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument('-c', '--config_file',
                        type=str,
                        dest='config_file',
                        default='',
                        help="The file containing " +
                        "configuration parameters e.g. aapp_runner.cfg")
    parser.add_argument("-s", "--station",
                        help="Name of the station",
                        dest="station",
                        type=str,
                        default="unknown")
    parser.add_argument("-e", "--environment",
                        dest="environment",
                        type=str,
                        help="Name of the environment (e.g. dev, test, oper)")
    parser.add_argument("-v", "--verbose",
                        help="print debug messages too",
                        action="store_true")
    parser.add_argument("-l", "--log", help="File to log to",
                        type=str,
                        default=None)

    args = parser.parse_args()

    if args.config_file == '':
        print "Configuration file required! aapp_runner.py <file>"
        sys.exit()
    if args.station == '':
        print "Station required! Use command-line switch -s <station>"
        sys.exit()
    else:
        station = args.station.lower()
    if not args.environment:
        print ("Environment required! " +
               "Use command-line switch -e <environment> e.g. de, test")
        sys.exit()
    else:
        env = args.environment.lower()

    if 'template' in args.config_file:
        print "Template file given as master config, aborting!"
        sys.exit()

    return station, env, args.config_file, args.log


def remove(path):
    """
    Remove the file or directory
    """
    if os.path.isdir(path):
        try:
            os.rmdir(path)
            LOG.debug("Removing dir: " + path)
        except OSError:
            LOG.warning("Unable to remove folder: " + path)
    else:
        try:
            if os.path.exists(path):
                LOG.debug("Removing file:" + path)
                os.remove(path)
        except OSError:
            LOG.debug("Unable to remove file: " + path)


def cleanup(number_of_days, path):
    """
    Removes files from the passed in path that are older than or equal
    to number_of_days
    """
    time_in_secs = _time() - number_of_days * 24 * 60 * 60
    for root, dirs, files in os.walk(path, topdown=False):
        LOG.debug("root dirs files: " + root)
        for file_ in files:
            full_path = os.path.join(root, file_)
            stat = os.stat(full_path)

            if stat.st_mtime <= time_in_secs:
                LOG.debug("Removing: " + full_path)
                remove(full_path)

            if not os.listdir(root):
                LOG.debug("Removing root: " + root)
                remove(root)



def setup_logging(config, log_file):
    """
    Init and setup logging
    """

    if log_file is not None:
        try:
            ndays = int(config['logging']["log_rotation_days"])
            ncount = int(config['logging']["log_rotation_backup"])
        except KeyError as err:
            print err.args, \
                "is missing. Please, check your config ",\
                config
            #FIXME Make the errorhandeling better
            raise IOError("Config was given but doesn't " +
                          "know how to backup and rotate")

        handler = handlers.TimedRotatingFileHandler(log_file,
                                                    when='midnight',
                                                    interval=ndays,
                                                    backupCount=ncount,
                                                    encoding=None,
                                                    delay=False,
                                                    utc=True)

        handler.doRollover()
    else:
        handler = logging.StreamHandler(sys.stderr)

    if (config['logging']["logging_mode"] and
            config['logging']["logging_mode"] == "DEBUG"):
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO

    handler.setLevel(loglevel)
    logging.getLogger('').setLevel(loglevel)
    logging.getLogger('').addHandler(handler)

    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger('posttroll').setLevel(logging.INFO)

    LOG = logging.getLogger('aapp_runner')
    
    return LOG
        
def check_message(msg, server):
    """
    Check the message for neccessary stuff:
    message type
    providing server
    """
    
    if msg is None:
        #LOG.debug("Message is None.")
        return False
    elif ( msg.type != 'file' and msg.type != 'dataset' ):
        LOG.warning("Message type is not a file or collection {}".format(msg.type))
        return False
    else:
        try:
            urlobj = []
            if 'uri' in msg.data:
                urlobj.append(urlparse(msg.data['uri']))
            elif 'dataset' in msg.data:
                for file in msg.data['dataset']:
                    urlobj.append(urlparse(file['uri']))
            else:
                LOG.error("Failed to find neccessary filename(s) in message.")
                return False                     
        except KeyError as ke:
            LOG.error("Key error: {}".format(ke))
                
        for obj in urlobj:
            url_ip = socket.gethostbyname(obj.netloc)
            if obj.netloc and (url_ip not in get_local_ips()):
                LOG.warning("Server %s not the current one: %s",
                            str(obj.netloc),
                            socket.gethostname())
                return False

    return True

def check_satellite(msg, config):
    """
    Check if the satellite in message is a valid satellite for this processing
    """
    try:
        if (msg.data['platform_name'] not in config['aapp_static_configuration']['supported_noaa_satellites'] and
            msg.data['platform_name'] not in config['aapp_static_configuration']['supported_metop_satellites']):
                LOG.info("Not a NOAA/Metop scene: " + str(msg.data['platform_name']) + ". Continue...")
                return False
                # FIXME:
    except Exception, err:
        LOG.warning(str(err))
        return False
    
    LOG.debug("Accepting satellite: " + str(msg.data['platform_name']) + " as valid platform.")
    return True

def check_pass_length(msg,config):
    """
    Check if start and end time is ok
    And check if passlength is ok
    """
    config['starttime'] = msg.data['start_time']

    try:
        config['endtime'] = msg.data['end_time']
    except KeyError:
        #TODO Can we handle this better?
        LOG.warning("No end_time in message! Guessing start_time + 14 minutes...")
        config['endtime'] = msg.data['start_time'] + timedelta(seconds=60 * 14)

    # Test if the scene is longer than minimum required:
    pass_length = config['endtime'] - config['starttime']
    if pass_length < timedelta(seconds=60 * config['aapp_processes'][config.process_name]['passlength_threshold']):
        LOG.info("Pass is too short: Length in minutes = %6.1f", pass_length.seconds / 60.0)
        return False

    LOG.debug("Start and end time ok, and passlength is longer than treshold")
    return True

def generate_process_config(msg, config):
    """
    Check sensors to process and setup config for this
    
    Need to check if it is a collection or file message. Then get sensor information from this.
    """
    
    #All possible instruments to process initialized to false.
    config['process_amsua'] = False
    config['process_amsub'] = False
    config['process_hirs'] = False
    config['process_avhrr'] = False
    config['process_msu'] = False
    config['process_mhs'] = False
    config['process_dcs'] = False

    #Check sensors and file as given in the incomming message
    #Note: zip iterates two list at the same time
    if 'dataset' in msg.data:
        for sensor, sensor_filename in zip(msg.data['sensor'], msg.data['dataset']):
            #print sensor, sensor_filename['uri']
            process_name = "process_{}".format(SENSOR_NAME_CONVERTER.get(sensor,sensor))
            config[process_name] = True

            #Name of the input file for given instrument
            input_file_name = "input_{}_file".format(SENSOR_NAME_CONVERTER.get(sensor,sensor))
            #print urlparse(sensor_filename['uri']).path
            config[input_file_name] = urlparse(sensor_filename['uri']).path
 
    elif 'uri' in msg.data:
        for sensor in msg.data['sensor']:
            process_name = "process_{}".format(SENSOR_NAME_CONVERTER.get(sensor,sensor))
            print sensor, process_name
            config[process_name] = True
        config['input_hrpt_file'] = msg.data['uri']
            
    else:
        LOG.error("Could not find needed dataset or uri in message. Can not handle.")
        return False 
    config['calibration_location'] = "-c -l"
    config['a_tovs'] = list("ATOVS")
    config['orbit_number'] = int(msg.data['orbit_number'])
    #How to give the platform name?
    #Which format?
    #Used are for Metop:
    #Metop-A/'Metop A'/'METOP A'
    #M02
    #metop02
    #Throughout this processing the last convention is used!
    if msg.data['platform_name'] in config['aapp_static_configuration']['platform_name_aliases']:
        config['platform_name'] = config['aapp_static_configuration']['platform_name_aliases'][msg.data['platform_name']]
        #print config['platform_name']
        #TODO Should not use satellite_name
        
        config['satellite_name'] = config['platform_name']
    else:
        LOG.error("Failed to replace platform_name: {}. Can not continue.".format(msg.data['platform_name']))
        return False
    
    config['start_time'] = msg.data['start_time']
    
    return True

def create_and_check_scene_id(msg, config):
    """
    Create a scene specific ID to identify the scene process for later
    """
    # Use sat id, start and end time as the unique identifier of the scene!
    if config['platform_name'] in config.job_register and len(config.job_register[config['platform_name']]) > 0:
        # Go through list of start,end time tuples and see if the current
        # scene overlaps with any:
        status = overlapping_timeinterval((config['starttime'], config['endtime']), config.job_register[config['platform_name']])
        if status:
            LOG.warning("Processing of scene " + config['platform_name'] +
                        " " + str(status[0]) + " " + str(status[1]) +
                        " with overlapping time has been"
                        " launched previously")
            LOG.info("Skip it...")
            return False
        else:
            LOG.debug("No overlap with any recently processed scenes...")

    scene_id = (str(config['platform_name']) + '_' +
                config['starttime'].strftime('%Y%m%d%H%M%S') +
                '_' + config['endtime'].strftime('%Y%m%d%H%M%S'))
    LOG.debug("scene_id = " + str(scene_id))
    return scene_id
    
def setup_aapp_processing(config):
    """
    Setup various env variables needed for the aapp processing
    and set eg working dir
    """

    if not 'working_dir' in config['aapp_processes'][config.process_name] and 'use_dyn_work_dir' in config['aapp_processes'][config.process_name]:
        try:
            config['aapp_processes'][config.process_name]['working_dir'] = tempfile.mkdtemp(dir=config['aapp_processes'][config.process_name]['aapp_workdir'])
            LOG.debug("working dir set based on aapp_oarkdir and tmp " + str(config['aapp_processes'][config.process_name]['working_dir']))
        except OSError:
            config['aapp_processes'][config.process_name]['working_dir'] = tempfile.mkdtemp()
        finally:
            LOG.info("Create new working dir...")
    elif not 'working_dir' in config['aapp_processes'][config.process_name]:
        config['aapp_processes'][config.process_name]['working_dir'] = config['aapp_processes'][config.process_name]['aapp_workdir']

    LOG.info("Working dir = " + str(config['aapp_processes'][config.process_name]['working_dir']))

    os.environ["AAPP_PREFIX"] = config['aapp_processes'][config.process_name]['aapp_prefix']
    
    aapp_atovs_conf = os.path.join(os.environ["AAPP_PREFIX"],config['aapp_processes'][config.process_name]['aapp_environment_file'])
    status, returncode, out, err = run_shell_command("bash -c \"source {}\";env".format(aapp_atovs_conf))
    if not status:
        LOG.error("Failed to run the bash source env command for " +str(aapp_atovs_conf))
        return False
    else:
        for line in out.splitlines():
            if line:
                (key,_,value) = line.partition("=")
                os.environ[key]=value

    return True

def process_aapp(msg, config):
    """
    Do the various processing steps of aapp for each instruments
    """

    try:
        starttime = config['starttime']
        platform_name = config['platform_name']
        #working_dir = config['aapp_processes'][config.process_name]['working_dir']
        #tle_indir = config['aapp_processes'][config.process_name]['tle_indir']
    
        #DO tle
        tle_proc_ok = True
        if not do_tleing(config, starttime, platform_name):
            LOG.warning("Tleing failed for some reason. It might be that the processing can continue")
            LOG.warning("Please check the previous log carefully to see if this is an error you can accept.")
            tle_proc_ok = False
            raise TleError("Tleing failed for some reason")
    
        #DO tle satpos
        satpos_proc_ok = True
        if not do_tle_satpos(config, starttime, platform_name):
            LOG.warning("Tle satpos failed for some reason. It might be that the processing can continue")
            LOG.warning("Please check the previous log carefully to see if this is an error you can accept.")
            satpos_proc_ok = False
            raise SatposError("Tle satpos failed for some reason")
    
        #DO decom
        decom_proc_ok = True
        if not do_decommutation(config, msg, starttime):
            LOG.warning("The decommutation failed for some reason. It might be that the processing can continue")
            LOG.warning("Please check the previous log carefully to see if this is an error you can accept.")
            decom_proc_ok = False
            raise DecommutationError("The decommutation failed for some reason")
    
        #DO HIRS
        hirs_proc_ok = True
        from aapp_runner.do_hirs_calibration import do_hirs_calibration
        if not do_hirs_calibration(config, msg, starttime):
            LOG.warning("Tle hirs calibration and location failed for some reason. It might be that the processing can continue")
            LOG.warning("Please check the previous log carefully to see if this is an error you can accept.")
            hirs_proc_ok = False
    
        #DO ATOVS
        atovs_proc_ok = True
        from aapp_runner.do_atovs_calibration import do_atovs_calibration
        if not do_atovs_calibration(config, starttime):
            LOG.warning("The (A)TOVS calibration and location failed for some reason. It might be that the processing can continue")
            LOG.warning("Please check the previous log carefully to see if this is an error you can accept.")
            atovs_proc_ok = False
    
        #DO AVHRR
        avhrr_proc_ok = True
        from aapp_runner.do_avhrr_calibration import do_avhrr_calibration
        if not do_avhrr_calibration(config, msg, starttime):
            LOG.warning("The avhrr calibration and location failed for some reason. It might be that the processing can continue")
            LOG.warning("Please check the previous log carefully to see if this is an error you can accept.")
            avhrr_proc_ok = False
    
        #Do Preprocessing
        atovpp_proc_ok = True
        from aapp_runner.do_atovpp_and_avh2hirs_processing import do_atovpp_and_avh2hirs_processing
        if not do_atovpp_and_avh2hirs_processing(config, starttime):
            LOG.warning("The preprocessing atovin, atopp and/or avh2hirs failed for some reason. It might be that the processing can continue")
            LOG.warning("Please check the previous log carefully to see if this is an error you can accept.")
            atovpp_proc_ok = False
    
        #DO ANA
        ana_proc_ok = True
        from aapp_runner.do_ana_correction import do_ana_correction
        if not do_ana_correction(config, msg, starttime):
            LOG.warning("The ana attitude correction failed for some reason. It might be that the processing can continue")
            LOG.warning("Please check the previous log carefully to see if this is an error you can accept.")
            ana_proc_ok = False
    except KeyError as ke:
        LOG.error("Process aapp failed: {}".format(ke))
        return False
    except TleError as te:
        LOG.error("The tle failed: {}".format(te))
        return False
    except SatposError as se:
        LOG.error("The satpos failed: {}".format(se))
        return False
    except DecommutationError as de:
        LOG.error("The decommutation failed: {}".format(de))
        return False
    except Exception as err:
        LOG.error("Process aapp failed: {}".format(err))
        return False
    else:
        LOG.info("AAPP processing complete.")

    return True

def publish_level1(publisher, config, msg, filelist, station_name, environment):
    """
    Send a publish message, one message per file in the filelist
    """
    
    for file in filelist:
        print file
        msg_to_send = {}
        try:
            msg_to_send = msg.data.copy()
            if 'dataset' in msg_to_send:
                del msg_to_send['dataset']
                
            msg_to_send['uri'] = "file://{}{}".format(config['aapp_processes'][config.process_name]['message_providing_server'], file['file'])

            msg_to_send['filename'] = os.path.basename(file['file'])
            msg_to_send['uid'] = os.path.basename(file['file'])
            msg_to_send['sensor'] = file['sensor']
            msg_to_send['orbit_number'] = config['orbit_number']
            msg_to_send['format'] = "AAPP"
            msg_to_send['type'] = 'Binary'
            msg_to_send['data_processing_level'] = file['level'].upper()
            LOG.debug('level in message: ' + str(msg_to_send['data_processing_level']))
            msg_to_send['start_time'] = config['starttime']
            msg_to_send['end_time'] = config['endtime']
            msg_to_send['station'] = station_name
            msg_to_send['env'] = environment
        except KeyError as ke:
            LOG.error("KeyError, missing key: {}".format(ke))
        except Exception as err:
            LOG.error("Failed to build publish message with error: {}".format(err))
            continue
        
        try:
            publish_to = compose(config['aapp_processes'][config.process_name]['publish_sift_format'],msg_to_send)
        except KeyError as ke:
            LOG.warning("Unknown Key used in format: {}. Check spelling and/or availability.".format(config['aapp_processes'][config.process_name]['publish_sift_format']))
            LOG.warning("Available keys are:")
            for key in to_send:
                LOG.warning("{} = {}".format(key,msg_to_send[key]))
            LOG.error("Can not publish these data!")
            return False
        except ValueError as ve:
            LOG.error("Value Error: {}".format(ve))
            return

        LOG.debug("Publish to:{}".format(publish_to))
        message = Message(publish_to, "file", msg_to_send).encode()
        LOG.debug("sending: " + str(message))
        publisher.send(message)

if __name__ == "__main__":

    """
    Call the various functions that make up the parts of the AAPP processing
    """
    
    #Read the command line argument
    (station_name, environment, config_filename, log_file) = read_arguments()

    if not os.path.isfile(config_filename):
        print "ERROR! Can not find config file: {}".format(config_filename)
        print "Exits!"
        sys.exit()

    config = read_config_file_options(config_filename, station_name, environment)

    #Set up logging
    try:
        LOG = setup_logging(config, log_file)
    except:
        print "Logging setup failed. Check your config"
        #TODO
        #Better error handeling for logging setup
    
    try:
        aapp_config = AappL1Config(config, environment)
    except Exception as err:
        LOG.error("Failed to init AAPP L1 Config object: {}".format(err))
        sys.exit()    

    try:
        with posttroll.subscriber.Subscribe('',
                                            aapp_config.get_parameter('subscribe_topics'),
                                            True) as subscr:
            with Publish('aapp_runner', 0) as publisher:
                while True:
                    for msg in subscr.recv(timeout=90):
                        aapp_config.reset()
                        if not check_message(msg, aapp_config.get_parameter('message_providing_server')):
                            continue
                        
                        if not check_satellite(msg, aapp_config):
                            continue
                    
                        if not check_pass_length(msg, aapp_config):
                            continue
                    
                        if not generate_process_config(msg, aapp_config):
                            continue
                    
                        scene_id =  create_and_check_scene_id(msg, aapp_config)
                        if not scene_id:
                            continue
                    
                        if not setup_aapp_processing(aapp_config):
                            continue
                    
                        if not process_aapp(msg, aapp_config):
                            continue

                        #Rename standard AAPP output file names to usefull ones 
                        #and move files to final location.
                        from aapp_runner.rename_aapp_filenames import rename_aapp_filenames
                        renamed_files = rename_aapp_filenames(aapp_config) 
                        if not renamed_files:
                            LOG.warning("The rename of standard aapp filenames to practical ones returned an empty file list")
                            LOG.warning("This means there are no files to publish")
                        else:
                            publish_level1(publisher, aapp_config, msg, renamed_files, station_name, environment)
                    
                        move_aapp_log_files(aapp_config)
                        cleanup_aapp_logfiles_archive(aapp_config)
                    
                        #cleanup_aapp_workdir(aapp_config)

                        block_before_rerun(aapp_config, msg)
                    
    except KeyboardInterrupt as ki:
        LOG.info("Received keyboard interrupt. Shutting down")
    finally:
        LOG.info("Exiting AAPP runner. See ya")
