#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2015 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c20671.ad.smhi.se>
#   Trygve Aspenes <trygveas@met.no>
#
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

"""Prepare tle and satpos (and ephe) files for AAPP, using the tleing and
alleph scripts from AAPP

The task of finding the correct TLE is challeging. The user can supply its own
directory in the config file and the filename convetion. The the modele
will try to find the correct one.
If data is reprocessed it should use the closest tle
if data is from Direct Broadcast the only newer tle files than the
timestamp if the index file should be processed.
"""

import logging
from glob import glob
import os
from datetime import datetime
from shutil import copy
from subprocess import Popen, PIPE
from aapp_runner.helper_functions import run_shell_command
from trollsift.parser import compose
import re

LOG = logging.getLogger(__name__)

def _do_6_matches(m):
    return datetime.strptime(m.group(1)+m.group(2)+m.group(3)+m.group(4)+m.group(5)+m.group(6),"%Y%m%d%H%M%S")

def _do_5_matches(m):
    return datetime.strptime(m.group(1)+m.group(2)+m.group(3)+m.group(4)+m.group(5),"%Y%m%d%H%M")

def _do_4_matches(m):
    return datetime.strptime(m.group(1)+m.group(2)+m.group(3)+m.group(4),"%Y%m%d%H")

def _do_3_matches(m):
    return datetime.strptime(m.group(1)+m.group(2)+m.group(3),"%Y%m%d")

def _do_3_matchesYY(m):
    return datetime.strptime(m.group(1)+m.group(2)+m.group(3),"%y%m%d")
        

def do_tleing(config, timestamp, satellite):
    """Get the tle-file and copy them to the AAPP data structure 
       and run the AAPP tleing script and executable"""
    
    return_status = True
    
    #This function relays on beeing in a working directory
    current_dir = os.getcwd() #Store the dir to change back to after function complete
    os.chdir(config['aapp_processes'][config.process_name]['working_dir'])

    tle_match_tests = (('.*(\d{4})(\d{2})(\d{2})_?-?T?(\d{2})(\d{2})(\d{2}).*',_do_6_matches),
                       ('.*(\d{4})(\d{2})(\d{2})_?-?T?(\d{2})(\d{2}).*',_do_5_matches),
                       ('.*(\d{4})(\d{2})(\d{2})_?-?T?(\d{2}).*',_do_4_matches),
                       ('.*(\d{4})(\d{2})(\d{2}).*',_do_3_matches),
                       ('.*(\d{2})(\d{2})(\d{2}).*',_do_3_matchesYY))

    # SATellite IDentification mandatory
    # so take care of default values 
    SATID_FILE=os.getenv('SATID_FILE', 'satid.txt')

    if 'tle_indir' in config['aapp_processes'][config.process_name]:
        tle_indir = config['aapp_processes'][config.process_name]['tle_indir']
        LOG.warning("Override the env variable set in AAPP_ENV7 DIR_NAVIGATION from {} to {}.".format(os.environ['DIR_NAVIGATION'], tle_indir))
        os.environ['DIR_NAVIGATION'] = tle_indir
    else:
        """This is the default directory for the tles"""
        tle_indir = os.getenv('DIR_NAVIGATION')

    # variables for the TLE HOME directory
    DIR_DATA_TLE=os.path.join(os.getenv('DIR_NAVIGATION'),'tle_db')

    #This is needed by AAPP tleing. Try other of not existing
    if not os.path.exists(DIR_DATA_TLE):
        DIR_DATA_TLE=os.path.join(os.getenv('DIR_NAVIGATION'),'orb_elem')
        
    FIC_WRK=os.path.join(os.getenv('DIR_DATA_TLE'),'tmp_tle_',"{}".format(os.getpid()))
    
    #LISTESAT=os.getenv('LISTESAT',os.getenv('PAR_NAVIGATION_DEFAULT_LISTESAT_INGEST_TLE'))

    if 'tle_file_to_data_diff_limit_days' in config['aapp_processes'][config.process_name]:
        select_closest_tle_file_to_data = True
        min_closest_tle_file = int(config['aapp_processes'][config.process_name]['tle_file_to_data_diff_limit_days'])*24*60*60
    else:
        select_closest_tle_file_to_data = False


    TLE_INDEX=os.path.join(DIR_DATA_TLE,"tle_{}.index".format(satellite))

    tle_file_list = []
    if not select_closest_tle_file_to_data:
        if os.path.exists(TLE_INDEX):
            #Loop over all tle files, and only do tle 
            tle_files = [s for s in glob(os.path.join(DIR_DATA_TLE,'tle*txt')) if os.path.isfile(os.path.join(DIR_DATA_TLE, s))]
            tle_files.sort(key=lambda s: os.path.getctime(os.path.join(DIR_DATA_TLE,s)))
            
            tle_index_mtime = os.path.getmtime(TLE_INDEX)
            for s in tle_files:
                if os.path.getmtime(os.path.join(DIR_DATA_TLE,s)) > tle_index_mtime:
                    tle_file_list.append(s)
                    
            if len(tle_file_list) == 0:
                import time
                LOG.warning("No newer tle files than last update of the index file. Last update of index file is {:d}s. If more than a few days you should check.".format(int(time.time()-tle_index_mtime)))
            else:
                LOG.info("Will use tle files {}".format(tle_file_list))
        else:
            LOG.warning("index file does not exist. If this is the first run of AAPP tleing.exe it is ok, elsewise it is a bit suspisiuos.")
            tle_files = [s for s in os.listdir(DIR_DATA_TLE) if os.path.isfile(os.path.join(DIR_DATA_TLE, s))]
            tle_files.sort(key=lambda s: os.path.getctime(os.path.join(DIR_DATA_TLE,s)))
            tle_file_list = tle_files

            
    else:
        #dict to hold needed tle keys
        tle_dict = {}
        tle_dict['timestamp'] = timestamp
        try:
            infile = compose(config['aapp_processes'][config.process_name]['tle_infile_format'],tle_dict)
        except KeyError as ke:
            LOG.error("Key error: {}".format(ke))
            LOG.error("Valid keys :")
            for key in tle_dict.keys():
                LOG.error("{}".format(key))
            raise
        except:
            raise
        
        LOG.debug("tle file name: {}".format(infile))

        #Check if I can read the tle file.
        try:
            with open(os.path.join(DIR_DATA_TLE, infile)) as tle_file:
                del tle_file_list[:]
                tle_file_list.append(os.path.join(DIR_DATA_TLE, infile))
                pass
        except IOError as e:
            LOG.warning("Could not find tle file: {}. Try find closest ... ".format(infile))
            infile = None
            tle_file_list = glob(os.path.join(DIR_DATA_TLE,'*'))
            #print "tle file list: {}".format(tle_file_list)
            #print tle_file_list
            infile_closest = ""
            for tle_file_name in tle_file_list:
                for regex, test in tle_match_tests:
                    m = re.match(regex, tle_file_name)
                    if m:
                        #print "{} {}".format(tle_file_name, test(m))
                        delta = timestamp - test(m)
                        if ( abs(delta.total_seconds()) < min_closest_tle_file):
                            min_closest_tle_file = abs(delta.total_seconds()) 
                            infile_closest = os.path.basename(tle_file_name)
                            LOG.debug("Closest tle infile so far: {}".format(infile_closest))

            if infile_closest:
                del tle_file_list[:]
                tle_file_list.append(infile_closest)
            else:
                LOG.error("Could not find tle file close enough to timestamp {} with limit {}".format(timestamp, min_closest_tle_file))
                LOG.error("Update your TLE files or adjust the limit(Not recomended!).")
                
        if tle_file_list:
            LOG.debug("Use this: {} {}".format(tle_file_list, min_closest_tle_file))
    
    if not tle_file_list:
        LOG.error("Found no tle files.")
        return_status = False
    else:    
        for tle_file in tle_file_list:
            archive=False
            if not os.path.exists(os.path.join(tle_indir,'tle_db',tle_file)):
                LOG.error("Could not find the tle file: {}".format(tle_indir + "/" + tle_file))
                return_status = False
            else:
                """Dont use the tle_indir because this is handeled by the tleing script"""
                status = False
                returncode = 0
                stdout = ""
                stderr = ""
                cmd="tleing.exe"
                try:
                    status, returncode, stdout, stderr = run_shell_command(cmd,stdin="{}\n{}\n{}\n{}\n".format(DIR_DATA_TLE,
                                                                                                               os.path.basename(tle_file),
                                                                                                               satellite,
                                                                                                               TLE_INDEX))
                except:
                    LOG.error("Failed running command: {} with return code: {}".format(cmd,returncode))
                    LOG.error("stdout: {}".format(stdout))
                    LOG.error("stderr: {}".format(stderr))
                    return_status = False
                else:
                    if returncode != 0:
                        LOG.debug("Running command: {} with return code: {}".format(cmd,returncode))
                        LOG.debug("stdout: {}".format(stdout))
                        LOG.debug("stderr: {}".format(stderr))
                    elif not os.path.exists(TLE_INDEX):
                        LOG.error("index file: {} does not exist after tleing. Something is wrong.".format(TLE_INDEX))
                        LOG.debug("Running command: {} with return code: {}".format(cmd,returncode))
                        LOG.debug("stdout: {}".format(stdout))
                        LOG.debug("stderr: {}".format(stderr))
                    else:
                        LOG.debug("Running command: {} with return code: {}".format(cmd,returncode))
                        LOG.debug("stdout: {}".format(stdout))
                        LOG.debug("stderr: {}".format(stderr))

                        #When a index file is generated above one line is added for each tle file.
                        #If several tle files contains equal TLEs each of these TLEs generate one line in the index file
                        #To avoid this, sort the index file keeping only unique lines(skipping the tle filename at the end
                    
                        #The sort options +0b -3b is guessed to be sort from column 0 to 3, but this is not documented
                        #Could cause problems with future version of sort. See eg. http://search.cpan.org/~sdague/ppt-0.12/bin/sort
                        #cmd="sort -u -o {} +0b -3b {}".format(os.path.join(DIR_DATA_TLE, "{}.sort".format(TLE_INDEX)),os.path.join(DIR_DATA_TLE, TLE_INDEX))
                        if os.path.exists(TLE_INDEX):
                            cmd="sort -u -o {} +0b -3b {}".format("{}.sort".format(TLE_INDEX), TLE_INDEX)
                            try:
                                status, returncode, stdout, stderr = run_shell_command(cmd)
                            except:
                                LOG.error("Failed running command: {} with return code: {}".format(cmd,returncode))
                                LOG.error("stdout: {}".format(stdout))
                                LOG.error("stderr: {}".format(stderr))
                                return_status = False
                            else:
                                if returncode == 0 and os.path.exists("{}.sort".format(TLE_INDEX)):
                                    try:
                                        os.remove(TLE_INDEX)
                                    except OSError as e:
                                        LOG.error("Failed to remove unsorted and duplicated index file: {}".format(TLE_INDEX))
                                    else:
                                        try:
                                            os.rename("{}.sort".format(TLE_INDEX),TLE_INDEX)
                                            archive=True
                                        except:
                                            LOG.error("Failed to rename sorted index file to original name.")
                                else:
                                    LOG.error("Returncode other than 0: {} or tle index sort file does exists.".format(returncode, "{}.sort".format(TLE_INDEX)))
                        else:
                            LOG.error("tle index file: {} does not exists after tleing before sort. This can not happen.")
            
            #If a new tle is used and archive dir is given in config, copy TLEs to archive
            if archive and ('tle_archive_dir' in config['aapp_processes'][config.process_name]):
                archive_dict = {}
                archive_dict['tle_indir'] = config['aapp_processes'][config.process_name]['tle_indir']
                for tle_file_name in tle_file_list:
                    for regex, test in tle_match_tests:
                        m = re.match(regex, tle_file_name)
                        if m:
                            archive_dict['timestamp'] = test(m) 
                            tle_archive_dir = compose(config['aapp_processes'][config.process_name]['tle_archive_dir'],archive_dict)
                            if not os.path.exists(tle_archive_dir):
                                try:
                                    os.makedirs(tle_archive_dir)
                                except:
                                    LOG.error("Failed to make archive dir: {}".format(tle_archive_dir))
                                
                            try:
                                copy(tle_file_name, tle_archive_dir)
                            except:
                                LOG.error("Failed to copy TLE file: {} to archive: {}".format(tle_file_name, tle_archive_dir))
                                        
    #Change back after this is done
    os.chdir(current_dir)

    return return_status

def do_tle_satpos(config, timestamp, satellite):
    
    return_status = True
    
    LOG.info("satpos files is stored under the given directory/satpos")
    if 'tle_indir' in config['aapp_processes'][config.process_name]:
        satpos_dir = os.path.join(config['aapp_processes'][config.process_name]['tle_indir'], "satpos" )
    else:
        satpos_dir = os.path.join(os.environ['DIR_NAVIGATION'], "satpos" )
        
    if not os.path.exists(satpos_dir):
        try:
            os.makedirs(satpos_dir)
        except:
            LOG.error("Could not create satpos directory: {}".format(satpos_dir))
            return_status = False
        
    file_satpos = os.path.join(satpos_dir, "satpos_{}_{:%Y%m%d}.txt".format(satellite,timestamp))
    
    if (not os.path.exists(file_satpos) or os.stat(file_satpos).st_size == 0) and return_status:
        """Usage is: satpostle  [ -o] [-s satellite] [-S station] [-d start date] [-n number of days] [-i increment in seconds] [-c search criteria] 

        -o -s -S -d -n -i –c are optional. 

        If no parameter is specified as an option, defaults are : noaa14, Lannion, today 0h, 1.0, 120.0, n (n= nearest, p = preceding). 

        The option -o specifies that the data will be stored in the file satpos_noaxx_yyyymmdd.txt.
         
        Output default is the standard output.. 
        """
        cmd="satpostle -o -s {} -d {:%d/%m/%y} -n 1.2".format(satellite,timestamp)
        try:
            status, returncode, std, err = run_shell_command(cmd)
        except:
            LOG.error("Failed to run command: {}".format(cmd))
            return_status = False
        else:
            if returncode != 0:
                LOG.error("cmd: {} failed with returncode: {}".format(cmd,returncode))
                return_status = False
            elif not os.path.exists(file_satpos):
                LOG.error("file: {} does not exists after satpostle run.".format(file_satpos))
                return_status = False
    else:
        LOG.info("satpos file already there. Use this")
        
    return return_status
