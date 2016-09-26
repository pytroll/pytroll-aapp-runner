#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014, 2015, 2016 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <adam.dybbroe@smhi.se>
#   Janne Kotro fmi.fi
#   Trygve Aspenes

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

sys.path.insert(0, "trollduction/")
sys.path.insert(0, "/home/trygveas/git/trollduction-test/aapp_runner")
from read_aapp_config import read_config_file_options
from tle_satpos_prepare import do_tleing
from tle_satpos_prepare import do_tle_satpos
from do_commutation import do_decommutation

import socket
import netifaces
from helper_functions import run_shell_command

LOG = logging.getLogger(__name__)


# ----------------------------
# Default settings for logging
# ----------------------------
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

# -------------------------------
# Default settings for satellites
# -------------------------------
SUPPORTED_NOAA_SATELLITES = ['NOAA-19', 'NOAA-18', 'NOAA-16', 'NOAA-15']
SUPPORTED_METOP_SATELLITES = ['Metop-B', 'Metop-A', 'Metop-C']
SUPPORTED_SATELLITES = SUPPORTED_NOAA_SATELLITES + SUPPORTED_METOP_SATELLITES

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
    'amsua': 'amsu-a', 'amsub': 'amsu-b', 'hirs': 'hirs/4',
    'mhs': 'mhs', 'avhrr': 'avhrt/3'}

METOP_NUMBER = {'b': '01', 'a': '02'}

"""
These are the standard names used by the various AAPP decommutation scripts.
If you change these, you will also have to change the decommutation scripts.
"""
STD_AAPP_OUTPUT_FILESNAMES = {'amsua_file':'aman.l1b',
                              'amsub_file':'ambn.l1b',
                              'hirs_file':'hrsn.l1b',
                              'avhrr_file':'hrpt.l1b'
                              }
# FIXME! This variable should be put in the config file:
SATS_ONLY_AVHRR = []


from urlparse import urlparse
import posttroll.subscriber
from posttroll.publisher import Publish
from posttroll.message import Message
from trollduction.helper_functions import overlapping_timeinterval

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


def nonblock_read(output):
    """An attempt to catch any hangup in reading the output (stderr/stdout)
    from subprocess"""
    import fcntl
    fd = output.fileno()

    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
    try:
        return output.readline()
    except:
        return ''


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


class AappLvl1Processor(object):

    """
    Container for the Metop/NOAA level-1 processing based on AAPP

    """

    def __init__(self, runner_config):
        """
        Init with config file options
        """
        self.noaa_data_out_dir = runner_config['noaa_data_out_dir']
        self.metop_data_out_dir = runner_config['metop_data_out_dir']
        self.noaa_run_script = runner_config['aapp_run_noaa_script']
        self.metop_run_script = runner_config['aapp_run_metop_script']
        self.tle_indir = runner_config['tle_indir']
        self.tle_outdir = runner_config['tle_outdir']
        self.tle_script = runner_config['tle_script']
        self.pps_out_dir = runner_config['pps_out_dir']
        self.pps_out_dir_format = runner_config['pps_out_dir_format']
        self.aapp_prefix = runner_config['aapp_prefix']
        self.aapp_workdir = runner_config['aapp_workdir']
        self.aapp_outdir =  runner_config['aapp_outdir']
        self.aapp_outdir_format =  runner_config['aapp_outdir_format']
        self.copy_data_directories = runner_config['copy_data_directories']
        self.move_data_directory = runner_config['move_data_directory']
        self.use_dyn_work_dir = runner_config['use_dyn_work_dir']
        self.subscribe_topics = runner_config['subscribe_topics']
        self.publish_pps_format = runner_config['publish_pps_format']
        self.publish_l1_format = runner_config['publish_l1_format']
        self.publish_sift_format = runner_config['publish_sift_format']
        self.aapp_log_files_dir = runner_config['aapp_log_files_dir']
        self.aapp_log_files_backup = runner_config['aapp_log_files_backup']
        self.servername = runner_config['servername']
        self.dataserver = runner_config['dataserver']
        self.station = runner_config['station']
        self.environment = runner_config['environment']
        self.locktime_before_rerun = int(
            runner_config.get('locktime_before_rerun', 10))
        self.passlength_threshold = int(runner_config['passlength_threshold'])

        self.fullswath = True  # Always a full swath (never HRPT granules)
        self.working_dir = None
        self.level0_filename = None
        self.starttime = None
        self.endtime = None
        self.platform_name = "Unknown"
        self.satnum = "0"
        self.orbit = "00000"
        self.result_files = None
        self.level0files = None
        self.lvl1_home = self.pps_out_dir
        self.job_register = {}
        self.my_env = os.environ.copy()
        self.check_and_set_correct_orbit_number = False if runner_config['check_and_set_correct_orbit_number'] == 'False' else True
        self.do_ana_correction = False if runner_config['do_ana_correction'] == 'False' else True
        self.initialise()

    def initialise(self):
        """Initialise the processor """
        self.working_dir = None
        self.level0_filename = None
        self.starttime = None
        self.endtime = None
        self.platform_name = "Unknown"
        self.satnum = "0"
        self.orbit = "00000"
        self.result_files = []
        self.level0files = {}
        self.out_dir_config_data = []

    def cleanup_aapp_workdir(self):
        """Clean up the AAPP working dir after processing"""

        filelist = glob('%s/*' % self.working_dir)
        dummy = [os.remove(s) for s in filelist if os.path.isfile(s)]
        filelist = glob('%s/*' % self.working_dir)
        LOG.info("Number of items left after cleaning working dir = " +
                 str(len(filelist)))
        LOG.debug("Files: " + str(filelist))
        shutil.rmtree(self.working_dir)
        return

    def spack_aapplvl1_files(self, subd):
        return spack_aapplvl1_files(self.result_files, self.lvl1_home, subd,
                                    self.satnum)

    def pack_aapplvl1_files(self, subd):
        """ Copy AAPP lvl1 files to PPS source directory
        from input pps sub-directory name generated by crete_pps_subdirname()
        Return a dictionary with destination full path filename, sensor name and
        data processing level"""
        return pack_aapplvl1_files(self.result_files, self.pps_out_dir, subd,
                                   self.satnum)

#    def delete_old_log_files(self):
#        """
#        Clean old AAPP log files
#        """
#        #older_than = int(self.aapp_log_files_backup)*60*60*24
#        LOG.debug("continue...")
#        delete_old_dirs(self.aapp_log_files_dir,
 #                       self.aapp_log_files_backup)
#        return

    def copy_aapplvl1_files(self, subd, out_dir_config_data):
        """Copy AAPP lvl1 files in to data processing level sub-directory
        e.g. metop/level1b
        Input directory is defined in config file metop_data_out_dir and
        noaa_data_out_dir
        Return a dictionary with destination full path filename, sensor name and
        data processing level
        """
        return copy_aapplvl1_files(self.result_files, subd, self.satnum, out_dir_config_data)

    def smove_lvl1dir(self):
        if len(self.result_files) == 0:
            LOG.warning("No files in directory to move!")
            return {}

        # Get the subdirname:
        path = os.path.dirname(self.result_files[0])
        subd = os.path.basename(path)
        LOG.debug("path = " + str(path))
        LOG.debug("lvl1_home = " + str(self.lvl1_home))
        try:
            shutil.move(path, self.lvl1_home)
        except shutil.Error:
            LOG.warning("Directory already exists: " + str(subd))

        if self.orbit == '00000' or self.orbit == None:
            # Extract the orbit number from the sub-dir name:
            dummy, dummy, dummy, self.orbit = subd.split('_')

        # Return a dict with sensor and level for each filename:
        filenames = glob(os.path.join(self.lvl1_home, subd, '*'))
        LOG.info(filenames)

        retv = {}
        for fname in filenames:
            mstr = os.path.basename(fname).split('_')[0]
            if mstr == 'hrpt':
                lvl = '1B'
                instr = 'avhrr/3'
            else:
                lvl = mstr[-2:].upper()
                try:
                    instr = SENSOR_NAME_CONVERTER[mstr[0:-3]]
                except KeyError:
                    LOG.warning("Sensor name will not be converted %s" %
                                str(mstr[0:-3]))
                    LOG.debug("mstr = " + str(mstr))
                    instr = mstr[0:-3]

            retv[fname] = {'level': lvl, 'sensor': instr}

        LOG.info(str(retv))

        return retv

    def move_lvl1dir(self, out_dir):
        """Move sub-directory with AAPP level-1b|c|d files
        Return a dictionary with sensor and data processing level
        for each filename """

        if len(self.result_files) == 0:
            LOG.warning("No files in directory to move!")
            return {}

        # Get the subdirname:
        path = os.path.dirname(self.result_files[0])
        subd = os.path.basename(path)
        LOG.debug("path = " + str(path))
        LOG.debug("out_dir = " + out_dir)
        try:
            shutil.move(path, out_dir)
        except shutil.Error:
            LOG.warning("Directory already exists: " + str(subd))

        if self.orbit == '00000' or self.orbit == None:
            # Extract the orbit number from the sub-dir name:
            dummy, dummy, dummy, self.orbit = subd.split('_')

        filenames = glob(os.path.join(out_dir, subd, '*l1*'))
        LOG.info(filenames)

        retv = {}
        for fname in filenames:
            mstr = os.path.basename(fname).split('_')[0]
            if mstr == 'hrpt':
                lvl = '1b'
                instr = 'avhrr/3'
            else:
                lvl = mstr[-2:]
                try:
                    instr = SENSOR_NAME_CONVERTER[mstr[0:-3]]
                except KeyError:
                    LOG.warning("Sensor name will not be converted %s",
                                str(mstr[0:-3]))
                    LOG.debug("mstr = " + str(mstr))
                    instr = mstr[0:-3]

            retv[fname] = {'level': lvl, 'sensor': instr}

        LOG.info(str(retv))

        return retv

    def move_aapp_log_files(self):
        """ Move AAPP processing log files from AAPP working directory
        in to sub-directory (PPS format).
        The directory path is defined in config file (aapp_log_files)
        """
        try:
            filelist = glob('%s/*.log' % self.working_dir)
            subd = create_pps_subdirname(self.starttime,
                                         self.platform_name,
                                         self.orbit)
            destination = os.path.join(self.aapp_log_files_dir, subd)
            LOG.debug("move_aapp_log_files destination: " + destination)

            if not os.path.exists(destination):
                try:
                    os.makedirs(destination)
                except OSError:
                    LOG.warning("Can't create directory!")
                    return False  # FIXME: Check!
            LOG.debug(
                "Created new directory for AAPP log files:" + destination)

            for file_name in filelist:
                LOG.debug("File_name: " + file_name)
                base_filename = os.path.basename(file_name)
                dst = os.path.join(destination, base_filename)
                LOG.debug("dst: " + dst)
                shutil.move(file_name, dst)
        except OSError as err:
            LOG.error("Moving AAPP log files to " +
                      destination + " failed ", err)

        LOG.info("AAPP log files saved in to " + destination)

        return


# def get_old_dirs(self, dir_path, older_than_days):
#    """
#    return a list of all subfolders under dirPath older than olderThanDays
#    """
#    older_than_days *= 86400 # convert days to seconds
#    present = time.time()
#    directories = []
#    for root, dirs, files in os.walk(dir_path, topdown=False):
#        for name in dirs:
#            sub_dir_path = os.path.join(root, name)
#            if (present - os.path.getmtime(sub_dir_path)) > older_than_days:
#                directories.append(sub_dir_path)
#    return directories

    def create_scene_id(self, keyname):
        # Use sat id, start and end time as the unique identifier of the scene!
        if keyname in self.job_register and len(self.job_register[keyname]) > 0:
            # Go through list of start,end time tuples and see if the current
            # scene overlaps with any:
            status = overlapping_timeinterval((self.starttime, self.endtime),
                                              self.job_register[keyname])
            if status:
                LOG.warning("Processing of scene " + keyname +
                            " " + str(status[0]) + " " + str(status[1]) +
                            " with overlapping time has been"
                            " launched previously")
                LOG.info("Skip it...")
                return True
            else:
                LOG.debug(
                    "No overlap with any recently processed scenes...")

        scene_id = (str(self.platform_name) + '_' +
                    self.starttime.strftime('%Y%m%d%H%M%S') +
                    '_' + self.endtime.strftime('%Y%m%d%H%M%S'))
        LOG.debug("scene_id = " + str(scene_id))
        return scene_id

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

    def run(self, msg):
        """Start the AAPP level 1 processing on either a NOAA HRPT file or a
        set of Metop HRPT files"""

        try:

            # Avoid 'collections' and other stuff:
            if msg is None or msg.type != 'file':
                return True

            LOG.debug("Received message: " + str(msg))
            # msg.data['platform_name'] = "NOAA-19"
            LOG.debug(
                "Supported Metop satellites: " + str(SUPPORTED_METOP_SATELLITES))
            LOG.debug(
                "Supported NOAA satellites: " + str(SUPPORTED_NOAA_SATELLITES))

            try:
                if (msg.data['platform_name'] not in
                        SUPPORTED_NOAA_SATELLITES and
                        msg.data['platform_name'] not in
                        SUPPORTED_METOP_SATELLITES):

                    LOG.info("Not a NOAA/Metop scene. Continue...")
                    return True
                # FIXME:
            except Exception, err:
                LOG.warning(str(err))
                return True

            self.platform_name = msg.data['platform_name']
            LOG.debug("Satellite = " + str(self.platform_name))

            LOG.debug("")
            LOG.debug("\tMessage:")
            LOG.debug(str(msg))
            urlobj = urlparse(msg.data['uri'])
            url_ip = socket.gethostbyname(urlobj.netloc)
            if urlobj.netloc and (url_ip not in get_local_ips()):
                LOG.warning("Server %s not the current one: %s",
                            str(urlobj.netloc),
                            socket.gethostname())
                return True

            LOG.info("Ok... " + str(urlobj.netloc))
            self.servername = urlobj.netloc
            LOG.info("Sat and Sensor: " + str(msg.data['platform_name'])
                     + " " + str(msg.data['sensor']))

            self.starttime = msg.data['start_time']
            try:
                self.endtime = msg.data['end_time']
            except KeyError:
                LOG.warning(
                    "No end_time in message! Guessing start_time + 14 minutes...")
                self.endtime = msg.data[
                    'start_time'] + timedelta(seconds=60 * 14)

            # Test if the scene is longer than minimum required:
            pass_length = self.endtime - self.starttime
            if pass_length < timedelta(seconds=60 * self.passlength_threshold):
                LOG.info("Pass is too short: Length in minutes = %6.1f",
                         pass_length.seconds / 60.0)
                return True

            
            #Due to different ways to start the orbit counting, it might be neccessary
            #to correct the orbit number.
            #
            #Default is to check and correct if neccessary
            #Add configuration to turn it off
            
            start_orbnum = None
            if self.check_and_set_correct_orbit_number:
                try:
                    import pyorbital.orbital as orb
                    sat = orb.Orbital(
                        TLE_SATNAME.get(self.platform_name, self.platform_name), tle_file='')
                    start_orbnum = sat.get_orbit_number(self.starttime)
                except ImportError:
                    LOG.warning("Failed importing pyorbital, " +
                                "cannot calculate orbit number")
                except AttributeError:
                    LOG.warning("Failed calculating orbit number using pyorbital")
                    LOG.warning("platform name = " +
                                str(TLE_SATNAME.get(self.platform_name,
                                                    self.platform_name)) +
                                " " + str(self.platform_name))
    
                LOG.info(
                    "Orbit number determined from pyorbital = " + str(start_orbnum))

            try:
                self.orbit = int(msg.data['orbit_number'])
            except KeyError:
                LOG.warning("No orbit_number in message! Set to none...")
                self.orbit = None

            if self.check_and_set_correct_orbit_number:
                if start_orbnum and self.orbit != start_orbnum:
                    LOG.warning("Correcting orbit number: Orbit now = " +
                                str(start_orbnum) + " Before = " + str(self.orbit))
                    self.orbit = start_orbnum
                else:
                    LOG.debug("Orbit number in message determined " +
                              "to be okay and not changed...")

            if self.platform_name in SUPPORTED_METOP_SATELLITES:
                metop_id = SATELLITE_NAME[self.platform_name].split('metop')[1]
                self.satnum = METOP_NUMBER.get(metop_id, metop_id)
            else:
                self.satnum = SATELLITE_NAME[self.platform_name].strip('noaa')

            year = self.starttime.year
            keyname = str(self.platform_name)
            LOG.debug("Keyname = " + str(keyname))
            LOG.debug("Start: job register = " + str(self.job_register))

            scene_id = self.create_scene_id(keyname)
            
            #This means(from the create_scene_id) skipping this scene_is as it is already processed within a onfigured interval
            #See create_scene_id for detailed info
            if scene_id == True:
                return True
            
            scene_id = self.check_scene_id(scene_id)

            LOG.debug("scene_id = " + str(scene_id))
            if scene_id in self.level0files:
                LOG.debug("Level-0 files = " + str(self.level0files[scene_id]))
            else:
                LOG.debug("scene_id = %s: No level-0 files yet...", str(scene_id))

            self.level0_filename = urlobj.path
            dummy, fname = os.path.split(self.level0_filename)

            sensors = []
            if not self.sensors_to_process(msg, sensors):
                return True
            
            if not self.available_sensors(msg, sensors, scene_id):
                return True
            
            #Need to do this here to add up all sensors for METOP
            for (file,instr) in self.level0files[scene_id]:
                if instr not in sensors:
                    LOG.debug("Adding instrumet to sensors list: {}".format(instr))
                    sensors.append(str(instr))
                
                
            if not self.working_dir and self.use_dyn_work_dir:
                try:
                    self.working_dir = tempfile.mkdtemp(dir=self.aapp_workdir)
                except OSError:
                    self.working_dir = tempfile.mkdtemp()
                finally:
                    LOG.info("Create new working dir...")
            elif not self.working_dir:
                self.working_dir = self.aapp_workdir

            LOG.info("Working dir = " + str(self.working_dir))

            # AAPP requires ENV variables
            #my_env = os.environ.copy()
            #my_env['AAPP_PREFIX'] = self.aapp_prefix
            if self.use_dyn_work_dir:
                self.my_env['DYN_WRK_DIR'] = self.working_dir

            LOG.info("working dir: self.working_dir = " + str(self.working_dir))
            LOG.info("Using AAPP_PREFIX:" + str(self.aapp_prefix))

            for envkey in self.my_env:
                LOG.debug("ENV: " + str(envkey) + " " + str(self.my_env[envkey]))


            aapp_outdir_config_format = ""
            if self.platform_name in SUPPORTED_SATELLITES:
                LOG.info("This is a supported scene. Start the AAPP processing!")
                # FIXME:            LOG.info("Process the scene " +
                #                     self.platform_name + self.orbit)
                # TypeError: coercing to Unicode: need string or buffer, int
                # found
                LOG.info("Process the file " + str(self.level0_filename))


                """
                COnfiguration for the various AAPP processing
                
                This dict is passed to each module doing the actual processing.
                
                The processing of each level is overridden by the available sensors retrived from the message
                
                Meaning if processing of avhrr is set to True in the configuration but is not a mandatory sensor,
                nor contained in the sensor list, then the processing av avhrr is overridden and set to False.
                
                """
                
                
                process_config = {}
                try:
                    process_config['platform'] = SATELLITE_NAME.get(self.platform_name,self.platform_name)
                    process_config['orbit_number'] = int(msg.data['orbit_number'])
                    process_config['working_directory'] = self.working_dir
                    process_config['process_amsua'] = False
                    process_config['process_amsub'] = False
                    process_config['process_hirs'] = False
                    process_config['process_avhrr'] = False
                    process_config['process_msu'] = False
                    process_config['process_dcs'] = False
                    process_config['process_ana'] = self.do_ana_correction
                    process_config['a_tovs'] = list("ATOVS")
                    process_config['hirs_file'] = STD_AAPP_OUTPUT_FILESNAMES['hirs_file']
                    process_config['amsua_file'] = STD_AAPP_OUTPUT_FILESNAMES['amsua_file']
                    process_config['amsub_file'] = STD_AAPP_OUTPUT_FILESNAMES['amsub_file']
                    process_config['avhrr_file'] = STD_AAPP_OUTPUT_FILESNAMES['avhrr_file']
                    process_config['calibration_location'] = "-c -l"
                except KeyError as ke:
                    LOG.error("Could not initialize one or more process config parameters: {}.".format(ke))
                    return True #Meaning: can not process this.

                print str(self.level0files[scene_id])

                if 'metop' in process_config['platform']:
                    sensor_filename = {}
                    for (fname, instr) in self.level0files[scene_id]:
                        sensor_filename[instr] = fname

                    for instr in sensor_filename.keys():
                        print "instr: ",instr
                        if instr not in SENSOR_NAMES:
                            LOG.error("Sensor name mismatch! name = " + str(instr))
                            return True

                    if "avhrr/3" in sensor_filename:
                        process_config['input_avhrr_file'] = sensor_filename['avhrr/3']
                    if "amsu-a" in sensor_filename:
                        process_config['input_amsua_file'] = sensor_filename['amsu-a']
                    if "mhs" in sensor_filename:
                        process_config['input_amsub_file'] = sensor_filename['mhs']
                    if "hirs/4" in sensor_filename:
                        process_config['input_hirs_file'] = sensor_filename['hirs/4']

                _platform = SATELLITE_NAME.get(self.platform_name,self.platform_name)
                #DO tle
                tle_proc_ok = True
                if not do_tleing(self.starttime, _platform, self.working_dir, self.tle_indir):
                    LOG.warning("Tleing failed for some reason. It might be that the processing can continue")
                    LOG.warning("Please check the previous log carefully to see if this is an error you can accept.")
                    tle_proc_ok = False
                
                #DO tle satpos
                satpos_proc_ok = True
                if not do_tle_satpos(self.starttime, _platform, self.tle_indir):
                    LOG.warning("Tle satpos failed for some reason. It might be that the processing can continue")
                    LOG.warning("Please check the previous log carefully to see if this is an error you can accept.")
                    satpos_proc_ok = False
                
                #DO decom
                decom_proc_ok = True
                if not do_decommutation(process_config, sensors, self.starttime, self.level0_filename):
                    LOG.warning("The decommutaion failed for some reason. It might be that the processing can continue")
                    LOG.warning("Please check the previous log carefully to see if this is an error you can accept.")
                    decom_proc_ok = False
                    return True #Meaning can not complete this and skip the rest of the processing
                
                #DO HIRS
                hirs_proc_ok = True
                from do_hirs_calibration import do_hirs_calibration
                if not do_hirs_calibration(process_config, self.starttime):
                    LOG.warning("Tle hirs calibration and location failed for some reason. It might be that the processing can continue")
                    LOG.warning("Please check the previous log carefully to see if this is an error you can accept.")
                    hirs_proc_ok = False
                
                #DO ATOVS
                atovs_proc_ok = True
                from do_atovs_calibration import do_atovs_calibration
                if not do_atovs_calibration(process_config, self.starttime):
                    LOG.warning("The (A)TOVS calibration and location failed for some reason. It might be that the processing can continue")
                    LOG.warning("Please check the previous log carefully to see if this is an error you can accept.")
                    atovs_proc_ok = False
                
                #DO AVHRR
                avhrr_proc_ok = True
                from do_avhrr_calibration import do_avhrr_calibration
                if not do_avhrr_calibration(process_config, self.starttime):
                    LOG.warning("The avhrr calibration and location failed for some reason. It might be that the processing can continue")
                    LOG.warning("Please check the previous log carefully to see if this is an error you can accept.")
                    avhrr_proc_ok = False
                
                #Do Preprocessing
                atovpp_proc_ok = True
                from do_atovpp_and_avh2hirs_processing import do_atovpp_and_avh2hirs_processing
                if not do_atovpp_and_avh2hirs_processing(process_config, self.starttime):
                    LOG.warning("The preprocessing atovin, atopp and/or avh2hirs failed for some reason. It might be that the processing can continue")
                    LOG.warning("Please check the previous log carefully to see if this is an error you can accept.")
                    atovpp_proc_ok = False
                
                #DO IASI
                iasi_proc_ok = True
                from do_iasi_calibration import do_iasi_calibration
                if not do_iasi_calibration(process_config, self.starttime):
                    LOG.warning("The iasi calibration and location failed for some reason. It might be that the processing can continue")
                    LOG.warning("Please check the previous log carefully to see if this is an error you can accept.")
                    iasi_proc_ok = False
            
                #DO ANA
                ana_proc_ok = True
                from do_ana_correction import do_ana_correction
                if not do_ana_correction(process_config, self.starttime):
                    LOG.warning("The ana attitude correction failed for some reason. It might be that the processing can continue")
                    LOG.warning("Please check the previous log carefully to see if this is an error you can accept.")
                    ana_proc_ok = False
                
                #FIXME
                #Need a general check to fail run of some of the AAPP scripts fails fatal.
                
                #This is fallback choice if configured dir format fails
                aapp_outdir_pps_format = os.path.join(self.aapp_outdir,"{0:}_{1:%Y%m%d}_{1:%H%M}_{2:05d}"\
                                                      .format(SATELLITE_NAME.get(self.platform_name, self.platform_name),
                                                              self.starttime,
                                                              int(msg.data['orbit_number'])))
                
                #Make a copy of the msg.data so new needed variables can be added to this as needed
                self.out_dir_config_data = msg.data
                self.out_dir_config_data['satellite_name'] = SATELLITE_NAME.get(self.platform_name, self.platform_name)
                self.out_dir_config_data['orbit_number'] = int(msg.data['orbit_number'])
                try:
                    aapp_outdir_config_format = compose(self.aapp_outdir_format,self.out_dir_config_data)
                except KeyError as ke:
                    LOG.warning("Unknown Key used in format: {}. Check spelling and/or availability.".format(self.aapp_outdir_format))
                    LOG.warning("Available keys are:")
                    for key in self.out_dir_config_data:
                        LOG.warning("{} = {}".format(key,self.out_dir_config_data[key]))
                    LOG.warning("Will continue with directory name format as used by SAFNWC PPS...")
                    aapp_outdir_config_format = aapp_outdir_pps_format
                except ValueError as ve:
                    LOG.warning("value error        : {}".format(ve))
                    LOG.warning("aapp_outdir_format : {}".format(self.aapp_outdir_format))
                    LOG.warning("out_dir_config_data: {}".format(self.out_dir_config_data))

                aapp_outdir_config_format = os.path.join(self.aapp_outdir,aapp_outdir_config_format)
                LOG.info("aapp outdir config format: " + aapp_outdir_config_format)
                
                if not os.path.exists(aapp_outdir_config_format):
                    LOG.info("Create selected aapp_outdir: {}".format(aapp_outdir_config_format))
                    try:
                        os.mkdir(aapp_outdir_config_format)
                    except OSError as oe:
                        LOG.error("Could not create directory: {} with {}".format(aapp_outdir_config_format,oe))
                        
                else:
                    #FIXME Should we delete this directory if exists?
                    LOG.warning("The selected AAPP outdir for this processing exists already: " + aapp_outdir_config_format +". This can cause problems ....")

                #Rename standard AAPP output file names to usefull ones 
                #and move files to final location.
                from rename_aapp_filenames import rename_aapp_filenames
                if not rename_aapp_filenames(process_config, self.starttime, aapp_outdir_config_format):
                    LOG.warning("The rename of standard aapp filenames to practical ones failed for some reason. It might be that the processing can continue")
                    LOG.warning("Please check the previous log carefully to see if this is an error you can accept.")
        
            else:
                LOG.warning("This satellite: {}, is not supported.".format(self.platform_name))
                LOG.warning("Must be one of: {}".format("".join(SUPPORTED_SATELLITES)))


            # Add to job register to avoid this to be run again
            if keyname not in self.job_register.keys():
                self.job_register[keyname] = []

            self.job_register[keyname].append((self.starttime, self.endtime))
            LOG.debug("End: job register = " + str(self.job_register))

            # Block any future run on this scene for time_to_block_before_rerun
            # (e.g. 10) minutes from now:
            t__ = threading.Timer(self.locktime_before_rerun,
                                  reset_job_registry, args=(self.job_register,
                                                            str(self.platform_name),
                                                            (self.starttime,
                                                             self.endtime)))
            t__.start()

            LOG.debug("After timer call: job register = " + str(self.job_register))

            LOG.info("Ready with AAPP level-1 processing on NOAA scene: " + str(fname))
            LOG.info("working dir: self.working_dir = " + str(self.working_dir))

            globstr = os.path.join(self.aapp_outdir,
                                   str(SATELLITE_NAME.get(self.platform_name, self.platform_name)) +
                                   "_*" + str(int(msg.data['orbit_number'])))
            globstr = aapp_outdir_config_format
            LOG.debug("Glob string = " + str(globstr))
            dirlist = glob(globstr)
            if len(dirlist) != 1:
                LOG.error("Cannot find output files in working dir!")
                self.result_files = []
            else:
                self.result_files = get_aapp_lvl1_files(dirlist[0], msg.data['platform_name'])

            LOG.info("Output files: " + str(self.result_files))

        except:
            LOG.exception("Failed in run...")
            raise

        return False


def aapp_rolling_runner(runner_config):
    """The AAPP runner. Listens and triggers processing on Metop/NOAA HRPT
    level 0 files dispatched from reception."""
    LOG.info("*** Start the NOAA/Metop HRPT AAPP runner:")
    LOG.info("-" * 50)

    os.environ["AAPP_PREFIX"] = runner_config['aapp_prefix']
    aapp_atovs_conf = runner_config['aapp_prefix'] + "/ATOVS_ENV7"
    status, returncode, out, err = run_shell_command("bash -c \"source {}\";env".format(aapp_atovs_conf))
    if not status:
        print "Command failed"
    else:
        for line in out.splitlines():
            if line:
                (key,_,value) = line.partition("=")
                os.environ[key]=value
        
    # init
    aapp_proc = AappLvl1Processor(runner_config)
    
    with posttroll.subscriber.Subscribe('',
                                        aapp_proc.subscribe_topics,
                                        True) as subscr:
        with Publish('aapp_runner', 0) as publisher:
            while True:
                skip_rest = False
                aapp_proc.initialise()
                for msg in subscr.recv(timeout=90):
                    status = aapp_proc.run(msg)
                    if not status:
                        #skip_rest = True
                        break  # end the loop and reinitialize!
                if skip_rest:
                    skip_rest = False
                    continue
                
                tobj = aapp_proc.starttime
                LOG.info("Time used in sub-dir name: " +
                         str(tobj.strftime("%Y-%m-%d %H:%M")))

                #Start internal distribution of data
                #Copy data to destinations if configured
                if runner_config['copy_data_directories']:
                    for dest_dir in runner_config['copy_data_directories'].split(','):
                        level1_files = aapp_proc.copy_aapplvl1_files(dest_dir, aapp_proc.out_dir_config_data)

                        publish_level1(publisher,
                                       aapp_proc.servername,
                                       aapp_proc.station,
                                       aapp_proc.environment,
                                       aapp_proc.publish_pps_format,
                                       level1_files,
                                       aapp_proc.orbit, 
                                       aapp_proc.starttime,
                                       aapp_proc.endtime,
                                       msg.data,
                                       aapp_proc.publish_sift_format)

                        
                #move data to last destination if configured
                if runner_config['move_data_directory']:
                    try:
                        move_dir = compose(runner_config['move_data_directory'],aapp_proc.out_dir_config_data)
                    except KeyError as ke:
                        LOG.warning("Unknown Key used in format: {}. Check spelling and/or availability.".format(runner_config['move_data_directory']))
                        LOG.warning("Available keys are:")
                        for key in aapp_proc-out_dir_config_data:
                            LOG.warning("{} = {}".format(key,aapp_proc.out_dir_config_data[key]))
                        LOG.error("Skipping this directory ... ")
                        continue
                    except TypeError as te:
                        LOG.error("Type Error: {}".format(te))

                    LOG.debug("Move into directory: {}".format(runner_config['move_data_directory']))
                    level1_files = aapp_proc.move_lvl1dir(runner_config['move_data_directory'])

                    publish_level1(publisher,
                                   aapp_proc.servername,
                                   aapp_proc.station,
                                   aapp_proc.environment,
                                   aapp_proc.publish_pps_format,
                                   level1_files,
                                   aapp_proc.orbit, 
                                   aapp_proc.starttime,
                                   aapp_proc.endtime,
                                   msg.data,
                                   aapp_proc.publish_sift_format)

                    
                if False:
                    # Site specific processing
                    LOG.info("Station = " + str(aapp_proc.station))
                    if ('norrkoping' in aapp_proc.station or
                            'nkp' in aapp_proc.station):
                        if aapp_proc.platform_name.startswith('Metop'):
                            subd = create_pps_subdirname(tobj, aapp_proc.platform_name,
                                                         aapp_proc.orbit)
                            LOG.info("Create sub-directory for level-1 files: " +
                                     str(subd))
                            level1_files = aapp_proc.smove_lvl1dir()
                            # level1_files = aapp_proc.spack_aapplvl1_files(subd)
                        else:
                            LOG.info("Move sub-directory with NOAA level-1 files")
                            LOG.debug(
                                "Orbit BEFORE call to move_lvl1dir: " + str(aapp_proc.orbit))
                            level1_files = aapp_proc.smove_lvl1dir()
                            LOG.debug(
                                "Orbit AFTER call to smove_lvl1dir: " + str(aapp_proc.orbit))
    
                        publish_level1(publisher,
                                       aapp_proc.servername,
                                       aapp_proc.station,
                                       aapp_proc.environment,
                                       aapp_proc.publish_pps_format,
                                       level1_files,
                                       aapp_proc.orbit,
                                       aapp_proc.starttime,
                                       aapp_proc.endtime,
                                       msg.data)
    
                    elif (aapp_proc.station == 'helsinki' or
                            aapp_proc.station == 'kumpula'):
                        data_out_dir = ""
                        LOG.debug("aapp_proc.platform_name" +
                                  aapp_proc.platform_name)
                        if (aapp_proc.platform_name.startswith('Metop') and
                                aapp_proc.metop_data_out_dir):
                            data_out_dir = aapp_proc.metop_data_out_dir
    
                        if (aapp_proc.platform_name.startswith('NOAA') and
                                aapp_proc.noaa_data_out_dir):
                            data_out_dir = aapp_proc.noaa_data_out_dir
    
                        LOG.debug("DATA_OUT_DIR:" + data_out_dir)
                        if aapp_proc.pps_out_dir:
                            subd = create_pps_subdirname(tobj,
                                                         aapp_proc.platform_name,
                                                         aapp_proc.orbit)
                            LOG.info("Created PPS sub-directory "
                                     "for level-1 files: " + str(subd))
                            level1_files = aapp_proc.pack_aapplvl1_files(subd)
                            if level1_files is not None:
                                LOG.debug("PPS_OUT_DIR: level1_files: ")
                                for file_line in level1_files:
                                    LOG.debug(str(file_line))
    
                                publish_level1(publisher,
                                               aapp_proc.servername,
                                               aapp_proc.station,
                                               aapp_proc.environment,
                                               aapp_proc.publish_pps_format,
                                               level1_files,
                                               aapp_proc.orbit,
                                               aapp_proc.starttime,
                                               aapp_proc.endtime,
                                               msg.data)
                            else:
                                LOG.error("No files copied to " + subd)
    
                        # FIXED: If 'NoneType' object is not iterable
                        #       = no files to publish!
                        if data_out_dir:
                            LOG.info("Copying level-1 files to " + data_out_dir)
                            level1_files = aapp_proc.copy_aapplvl1_files(
                                data_out_dir)
                            if level1_files is not None:
                                LOG.debug("aapp_proc.publish_l1_format:" +
                                          aapp_proc.publish_l1_format)
                                LOG.debug("level1_files: ")
                                publish_level1(publisher,
                                               aapp_proc.servername,
                                               aapp_proc.station,
                                               aapp_proc.environment,
                                               aapp_proc.publish_l1_format,
                                               level1_files,
                                               aapp_proc.orbit,
                                               aapp_proc.starttime,
                                               aapp_proc.endtime,
                                               msg.data)
                            else:
                                LOG.error("Nofile copied to " + data_out_dir)
                #End site specific part.

                if (aapp_proc.working_dir and
                        not aapp_proc.aapp_log_files_dir == ""):
                    LOG.info("Move AAPP log files")
                    aapp_proc.move_aapp_log_files()
                    LOG.info("Cleaning old log files...")
                    path_to_clean = aapp_proc.aapp_log_files_dir
                    older_than_days = int(aapp_proc.aapp_log_files_backup)
                    cleanup(older_than_days, path_to_clean)
                    LOG.info("Cleaning up directory " +
                             str(aapp_proc.working_dir))
                    # aapp_proc.cleanup_aapp_workdir()
                elif aapp_proc.working_dir:
                    LOG.info("NOT Cleaning up directory %s",
                             aapp_proc.working_dir)
                   # aapp_proc.cleanup_aapp_workdir()

                #LOG.info("Do the tleing now that aapp has finished...")
                #do_tleing(aapp_proc.aapp_prefix,
                #          aapp_proc.tle_indir, aapp_proc.tle_outdir,
                #          aapp_proc.tle_script)
                #LOG.info("...tleing done")
            
        
    return


def publish_level1(publisher,
                   server,
                   env,
                   station,
                   publish_format,
                   result_files,
                   orbit, start_t, end_t, mda, publish_sift_format):
    """Publish the messages that AAPP lvl1 files are ready
    """
    # Now publish:
    for key in result_files:
        resultfile = key
        LOG.debug("File: " + str(os.path.basename(resultfile)))
        filename = os.path.split(resultfile)[1]
        to_send = mda.copy()
        to_send['uri'] = ('ssh://%s%s' % (server, resultfile))
        to_send['filename'] = filename
        to_send['uid'] = filename
        to_send['sensor'] = result_files[key]['sensor']
        to_send['orbit_number'] = int(orbit)
        to_send['format'] = publish_format
        to_send['type'] = 'Binary'
        to_send['data_processing_level'] = result_files[key]['level'].upper()
        LOG.debug('level in message: ' + str(to_send['data_processing_level']))
        to_send['start_time'], to_send['end_time'] = start_t, end_t
        to_send['station'] = station
        to_send['env'] = env
        try:
            publish_to = compose(publish_sift_format,to_send)
        except KeyError as ke:
            LOG.warning("Unknown Key used in format: {}. Check spelling and/or availability.".format(publish_sift_format))
            LOG.warning("Available keys are:")
            for key in to_send:
                LOG.warning("{} = {}".format(key,to_send[key]))
            LOG.error("Can not publish these data!")
            return
        except ValueError as ve:
            LOG.error("Value Error: {}".format(ve))
            return

        LOG.debug("Publish to:{}".format(publish_to))
        msg = Message(publish_to, "file", to_send).encode()
        #msg = Message('/' + str(to_send['format']) + '/' +
        #              str(to_send['data_processing_level']) +
        #              '/' + station + '/' + env +
        #             '/polar/direct_readout/',
        #              "file", to_send).encode()
        LOG.debug("sending: " + str(msg))
        publisher.send(msg)


def get_aapp_lvl1_files(level1_dir, satid):
    """Get the aapp level-1 filenames for the NOAA/Metop direct readout
    swath"""

    if satid in SUPPORTED_METOP_SATELLITES:
        lvl1_files = (glob(os.path.join(level1_dir, '*.l1b')) +
                      glob(os.path.join(level1_dir, '*.l1c')) +
                      glob(os.path.join(level1_dir, '*.l1d')))

    else:
        lvl1_files = (glob(os.path.join(level1_dir, "*%s*.l1b"
                                        % (SATELLITE_NAME.get(satid, satid)))) +
                      glob(os.path.join(level1_dir, "*%s*.l1c"
                                        % (SATELLITE_NAME.get(satid, satid)))) +
                      glob(os.path.join(level1_dir, "*%s*.l1d"
                                        % (SATELLITE_NAME.get(satid, satid)))))

    return lvl1_files

# FIXME:
   # if MODE == 'SMHI_MODE':
    #    if satid in SUPPORTED_METOP_SATELLITES:
    #       # Level 1b/c data:
    #        lvl1_files = (glob(os.path.join(level1_dir, '*.l1b')) +
    #                      glob(os.path.join(level1_dir, '*.l1c')) +
    #                      glob(os.path.join(level1_dir, '*.l1d')))
    #    else:
    #        # SUBDIR example: noaa18_20140826_1108_47748
    #        LOG.debug(
    #                  'level1_dir = ' + str(level1_dir) + ' satid  = ' + str(satid))
    #        # /home/users/satman/tmp/hrpt_noaa18_20150421_1425_51109.l1b
    #        matchstr = os.path.join(
    #                   level1_dir, + '*' + SATELLITE_NAME.get(satid, satid) + '_????????_????_?????/') + '*'
    #        LOG.debug(matchstr)
    #        lvl1_files = glob(matchstr)
    #        LOG.debug('get_aapp_lvl1_files: ' + str(lvl1_files))

    # if MODE == 'test':
    #    # AAPP convention
    # LOG.debug('
    #           get_aapp_lvl1_files: ' + str(lvl1_files))


def create_pps_subdirname(obstime, satid, orbnum):
    """Generate the pps subdirectory name from the start observation time, ex.:
    'noaa19_20120405_0037_02270'"""
    return (SATELLITE_NAME.get(satid, satid) +
            obstime.strftime('_%Y%m%d_%H%M_') +
            '%.5d' % orbnum)


def spack_aapplvl1_files(aappfiles, base_dir, subdir, satnum):
    """Copy the AAPP lvl1 files to the sub-directory under the pps directory
    structure"""
    # aman => amsua
    # ambn => amsub (satnum <= 17)
    # ambn => mhs (satnum > 17)
    # hrsn => hirs
    # msun => msu

    # Store the sensor name and the level corresponding to the file:
    sensor_and_level = {}

    name_converter = {'avhr': 'avhrr',
                      'aman': 'amsua',
                      'hrsn': 'hirs',
                      'msun': 'msu',
                      'hrpt': 'hrpt'
                      }
    not_considered = ['dcsn', 'msun']
    path = os.path.join(base_dir, subdir)
    if not os.path.exists(path):
        os.mkdir(path)

    LOG.info("Number of AAPP lvl1 files: " + str(len(aappfiles)))
    # retvl = []
    for aapp_file in aappfiles:
        fname = os.path.basename(aapp_file)
        in_name, ext = fname.split('.')
        if in_name in not_considered:
            continue

        if in_name == 'ambn':
            instr = 'mhs'
            try:
                if int(satnum) <= 17:
                    instr = 'amsub'
            except ValueError:
                pass
            firstname = instr + ext
            level = ext.strip('l').upper()
        elif in_name == 'hrpt':
            firstname = name_converter.get(in_name)
            instr = 'avhrr/3'
            # Could also be 'avhrr'. Will anyhow be converted below...
            level = '1B'
        else:
            instr = name_converter.get(in_name, in_name)
            LOG.debug("Sensor = " + str(instr) + " from " + str(in_name))
            firstname = instr + ext
            level = ext.strip('l').upper()

        newfilename = os.path.join(path, "%s_%s.%s" % (firstname,
                                                       subdir, ext))
        LOG.info("Copy aapp-file to destination: " + newfilename)
        shutil.copy(aapp_file, newfilename)
        # retvl.append(newfilename)
        sensor_and_level[newfilename] = {
            'sensor': SENSOR_NAME_CONVERTER.get(instr, instr),
            'level': level}

    return sensor_and_level
    # return retvl


def pack_aapplvl1_files(aappfiles, base_dir, subdir, satnum):
    """
    Copy the AAPP lvl1 files to the sub-directory under the pps directory
    structure
    """
    # aman => amsua
    # ambn => amsub (satnum <= 17)
    # ambn => mhs (satnum > 17)
    # hrsn => hirs
    # msun => msu

    # Store the sensor name and the level corresponding to the file:
    sensor_and_level = {}

#    name_converter = {'avhr': 'avhrr',
#                      'aman': 'amsua',
#                      'hrsn': 'hirs',
#                      'msun': 'msu',
#                      'hrpt': 'hrpt'
#                      }
#    not_considered = ['dcsn', 'msun']
    LOG.debug(" pack_aapplvl1_files subdir: " + subdir)
    path = os.path.join(base_dir, subdir)
    LOG.debug("path: " + path)
    if not os.path.exists(path):
        LOG.debug("mkdir")
        os.makedirs(path)
    # FIXME: OSError: [Errno 2] No such file or directory:
    LOG.info("Number of AAPP lvl1 files: " + str(len(aappfiles)))

    for aapp_file in aappfiles:
        LOG.debug("Processing aapp_file: " + aapp_file)
#        fname = os.path.basename(aapp_file)
        filename = os.path.basename(aapp_file)
        in_name, ext = filename.split('.')
#
#        if in_name in not_considered:
#            LOG.debug("File NOT consired: " + in_name)
#            continue

        if in_name.startswith('mhs'):
            instr = 'mhs'
            try:
                if int(satnum) <= 17 and int(satnum) >= 15:
                    instr = 'amsub'
            except ValueError:
                pass
#            firstname = instr + ext
#            level = ext.strip('l')
        elif in_name.startswith('hrpt'):
            #            firstname = name_converter.get(in_name)
            instr = 'avhrr/3'
            # Could also be 'avhrr'. Will anyhow be converted below...
#            level = '1b'
        elif in_name.startswith('hirs'):
            instr = 'hirs'
        elif in_name.startswith('amsua'):
            instr = 'amsua'
        elif in_name.startswith('amsub'):
            instr = 'amsub'
        else:
            LOG.debug("File not consired: " + filename)
            continue
#            instr = name_converter.get(in_name, in_name)
#            LOG.debug("Sensor = " + str(instr) + " from " + str(in_name))
#            firstname = instr + ext
#            level = ext.strip('l')
        level = ext.strip('l')
        # LOG.debug("Firstname " + firstname)
#        newfilename = os.path.join(path, "%s_%s.%s" % (firstname,
#                                                       subdir, ext))
        newfilename = os.path.join(path, filename)

        LOG.info("Copy aapp-file to destination: " + newfilename)
        shutil.copy(aapp_file, newfilename)
        sensor_and_level[newfilename] = {
            'sensor': SENSOR_NAME_CONVERTER.get(instr, instr),
            'level': level}

    return sensor_and_level

# AAPP output:
# METOP:
# hrpt_M01_20150428_1857_13540.l1b amsual1b_M01_20150428_1857_13540.l1b
# NOAA:
# hrpt_noaa18_20150428_1445_51208.l1b hirsl1b_noaa18_20150428_1445_51208.l1b
#


def copy_aapplvl1_files(aappfiles, output_data_basepath, satnum, out_dir_config_data):
    """
    Copy AAPP lvl1 files to the sub-directories (level1b,
    level1c, level1d)
    Metop data under the metop_data_out
    and in case of Noaa data under the directory noaa_data_out
    Output format is defined in scripts AAPP_RUN_NOAA and AAPP_RUN_METOP
    """

    LOG.info("Start copy level1 files to directory")
    # Store the sensor name and the level corresponding to the file:
    sensor_and_level = {}
 #   name_converter = {'avhr': 'avhrr',
 #                     'aman': 'amsua',
 #                     'hrsn': 'hirs',
 #                     'msun': 'msu',
 #                     'hrpt': 'hrpt'
 #                     }

    dir_name_converter = {'l1b': 'level1b',
                          'l1c': 'level1c',
                          'l1d': 'level1d'
                          }

#    not_considered = ['dcsn', 'msun']
    if len(aappfiles) == 0:
        LOG.warning("No files in input directory to copy!")
        return

    errors = []
    for aapp_file in aappfiles:
        filename = os.path.basename(aapp_file)
        in_name, ext = filename.split('.')
        LOG.debug("in_name: " + in_name)
#        if in_name in not_considered:
#            LOG.debug("File NOT consired:" + in_name)
#            continue
        if in_name.startswith('mhs'):
            instr = 'mhs'
            try:
                if int(satnum) <= 17 and int(satnum) >= 15:
                    instr = 'amsub'
            except ValueError:
                pass
#            firstname = instr + ext
#            level = ext.strip('l')
        elif in_name.startswith('hrpt'):
            #            firstname = name_converter.get(in_name)
            instr = 'avhrr/3'
            # Could also be 'avhrr'. Will anyhow be converted below...
#            level = '1b'
        elif in_name.startswith('hirs'):
            instr = 'hirs'
        elif in_name.startswith('amsua'):
            instr = 'amsua'
        elif in_name.startswith('amsub'):
            instr = 'amsub'
        else:
            LOG.debug("File not consired:" + filename)
            continue

#           level = '1c'
#            instr = name_converter.get(in_name, in_name)

            LOG.debug("Sensor = " + str(instr) + " from " + str(in_name))
 #           firstname = instr + ext
 #           level = ext.strip('l')
        level = ext.strip('l')
 #           LOG.debug("Firstname " + firstname)


        out_dir_config_data['level_of_data'] = dir_name_converter.get(ext)

        try:
            directory = compose(output_data_basepath, out_dir_config_data)
        except KeyError as ke:
            LOG.warning("Unknown Key used in format: {}. Check spelling and/or availability.".format(output_data_basepath))
            LOG.warning("Available keys are:")
            for key in out_dir_config_data:
                LOG.warning("{} = {}".format(key,out_dir_config_data[key]))
            LOG.error("Skipping this directory ... ")
            return

        LOG.debug("Copy into directory: {}".format(directory))

        if not os.path.exists(directory):
            LOG.info("Create new directory:" + directory)
            try:
                os.makedirs(directory)
            except OSError as err:
                # FIXME: error or fatal?
                LOG.error("Couldn't make new directory " + directory + err)
                return
        else:
            LOG.info("Directory already exists.")

        destination_file = os.path.join(directory, filename)
        LOG.debug("Destination_file: " + destination_file)
        try:
            shutil.copy(aapp_file, destination_file)
        except (IOError, os.error) as err:
            errors.append((aapp_file, destination_file, str(err)))
            LOG.error(in_name + "copy failed %s", err.strerror)
            #            except Error as err:
#                errors.extend(err.args[0])
        if errors:
            LOG.error("Too many errors!")

        sensor_and_level[destination_file] = {
            'sensor': SENSOR_NAME_CONVERTER.get(instr, instr),
            'level': level}

    LOG.debug("--------------------------")
    for key in sensor_and_level:
        LOG.debug("Filename: " + key)

    LOG.info("All files copied.")

    return sensor_and_level


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


def delete_old_dirs(dir_path, older_than_days):
    """
    Delete old directories
    """
    LOG.debug("delete_old_dirs in progress..." + older_than_days)
    older_than = older_than_days * 86400  # convert days to seconds
    time_now = _time()
    LOG.debug("after: " + dir_path)
    for path, folders, files in os.walk(dir_path):
        LOG.debug("path, folders, files:" + path + folders + files)
        for folder in folders[:]:
            folder_path = os.path.join(path, folder)
            if (time_now - os.path.getmtime(folder_path)) > older_than:
                yield folder_path
                LOG.debug("Deleting folder " + folder)
                # folders.remove(folder)

if __name__ == "__main__":

    # Read config file
    #
    # pylint: disable=C0103
    # C0103: Invalid name "%s" (should match %s)
    # Used when the name doesn't match the regular expression
    # associated to its type (constant, variable, class...).

    config = RawConfigParser()

    (station_name, environment, config_filename, log_file) = read_arguments()

    if not os.path.isfile(config_filename):
        #        config.read(config_filename)
        #    else:
        print "ERROR: ", config_filename, ": No such config file."
        sys.exit()

    run_options = read_config_file_options(config_filename,
                                           station_name, environment)
    if not isinstance(run_options, dict):
        print "Reading config file failed: ", config_filename
        sys.exit()

    # Logging
    config.read(config_filename)
    logging_cfg = dict(config.items("logging"))
    print "----------------------------------------\n"
    print logging_cfg

    if log_file is not None:
        try:
            ndays = int(logging_cfg["log_rotation_days"])
            ncount = int(logging_cfg["log_rotation_backup"])
        except KeyError as err:
            print err.args, \
                "is missing. Please, check your config file",\
                config_filename

            raise IOError("Log file was given but doesn't " +
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

    if (logging_cfg["logging_mode"] and
            logging_cfg["logging_mode"] == "DEBUG"):
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

    if run_options['pps_out_dir'] == '':
        LOG.warning("No pps_out_dir specified.")

    for key in run_options:
        print key, "=", run_options[key]

    aapp_rolling_runner(run_options)
