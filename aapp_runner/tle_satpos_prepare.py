#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2015 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c20671.ad.smhi.se>

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

"""

import logging
from glob import glob
import os
from datetime import datetime
import shutil
from subprocess import Popen, PIPE
from helper_functions import run_shell_command

LOG = logging.getLogger(__name__)

def _do_6_matches(m):
    return datetime.strptime(m.group(1)+m.group(2)+m.group(3)+m.group(4)+m.group(5)+m.group(6),"%Y%m%d%H%M%S")

def _do_5_matches(m):
    return datetime.strptime(m.group(1)+m.group(2)+m.group(3)+m.group(4)+m.group(5),"%Y%m%d%H%M")

def _do_4_matches(m):
    return datetime.strptime(m.group(1)+m.group(2)+m.group(3)+m.group(4),"%Y%m%d%H")

def _do_3_matches(m):
    return datetime.strptime(m.group(1)+m.group(2)+m.group(3),"%Y%m%d")
        

def do_tleing(timestamp, satellite, workdir, tle_indir=None, select_closest_tle_file_to_data=False):
    """Get the tle-file and copy them to the AAPP data structure 
       and run the AAPP tleing script and executable"""
    
    #This function relays on beeing in a working directory
    current_dir = os.getcwd() #Store the dir to change back to after function complete
    os.chdir(workdir)

    # SATellite IDentification mandatory
    # so take care of default values 
    SATID_FILE=os.getenv('SATID_FILE', 'satid.txt')

    if tle_indir == None:
        """This is the default directory for the tles"""
        tle_indir = os.getenv('DIR_NAVIGATION')
    else:
        LOG.warning("Override the env variable set in AAPP_ENV7 DIR_NAVIGATION from {} to {}.".format(os.environ['DIR_NAVIGATION'], tle_indir))
        os.environ['DIR_NAVIGATION'] = tle_indir

    # variables for the TLE HOME directory
    DIR_DATA_TLE=os.path.join(os.getenv('DIR_NAVIGATION'),'tle_db')

    #This is needed by AAPP tleing. Try other of not existing
    if not os.path.exists(DIR_DATA_TLE):
        DIR_DATA_TLE=os.path.join(os.getenv('DIR_NAVIGATION'),'orb_elem')
        
    FIC_WRK=os.path.join(os.getenv('DIR_DATA_TLE'),'tmp_tle_',"{}".format(os.getpid()))
    
    #LISTESAT=os.getenv('LISTESAT',os.getenv('PAR_NAVIGATION_DEFAULT_LISTESAT_INGEST_TLE'))

    #print LISTESAT

    TLE_INDEX=os.path.join(DIR_DATA_TLE,"tle_{}.index".format(satellite))

    tle_file_list = []
    if not select_closest_tle_file_to_data:
        if os.path.exists(TLE_INDEX):
            #tle_files = [s for s in os.listdir(DIR_DATA_TLE) if os.path.isfile(os.path.join(DIR_DATA_TLE, s))]
            #_tle_file_list = glob(os.path.join(DIR_DATA_TLE,'tle*txt'))
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
        #FIXME add as config
        tle_file_timestamp_format = "%Y%m%dT%H%M"

        infile = "tle-{0:{1}}.txt".format(timestamp,tle_file_timestamp_format)
    
        LOG.debug("tle file name: {}".format(infile))
    
        min_closest_tle_file = 3*24*60*60
        #Check if I can read the tle file.
        try:
            with open(os.path.join(tle_indir, infile)) as tle_file:
                pass
        except IOError as e:
            LOG.warning("Could not find tle file: {}. Try find closest ... ".format(infile))
            infile = None
            #tle_file_list = glob(os.path.join(tle_indir,'tle*'))
            #print "tle file list: {}".format(tle_file_list)
            import re
            tle_match_tests = (('.*(\d{4})(\d{2})(\d{2})_?-?T?(\d{2})(\d{2})(\d{2}).*',_do_6_matches),
                               ('.*(\d{4})(\d{2})(\d{2})_?-?T?(\d{2})(\d{2}).*',_do_5_matches),
                               ('.*(\d{4})(\d{2})(\d{2})_?-?T?(\d{2}).*',_do_4_matches),
                               ('.*(\d{4})(\d{2})(\d{2}).*',_do_3_matches))
            #print tle_file_list
            for tle_file_name in tle_files:
                for regex, test in tle_match_tests:
                    m = re.match(regex, tle_file_name)
                    if m:
                        delta = timestamp - test(m)
                        if ( abs(delta.total_seconds()) < min_closest_tle_file):
                            min_closest_tle_file = abs(delta.total_seconds()) 
                            infile = os.path.basename(tle_file_name)
                        break
        

        tle_file_list.append(infile)                
        LOG.debug("Use this: {} {}".format(tle_file_list,min_closest_tle_file))

        #print "{}".format(tle_file_list)
        #infile = "tle-{0:{1}}.txt".format(timestamp,tle_file_timestamp_format)
        
    for tle_file in tle_file_list:
        if not os.path.exists(os.path.join(tle_indir,'tle_db',tle_file)):
            print "Could not find the tle file: {}".format(tle_indir + "/" + tle_file)
            return False
        else:
            """Dont use the tle_indir because this is handleled by the tleing script"""
            tle_cmd = open("tle_commands", 'w')
            tle_cmd.write("{}\n".format(DIR_DATA_TLE))
            tle_cmd.write("{}\n".format(tle_file))
            tle_cmd.write("{}\n".format(satellite))
            tle_cmd.write("{}\n".format(TLE_INDEX))
            tle_cmd.close()
            LOG.info("TLE file ok. Do the calc for {} ... ".format(satellite))
            cmd="tleing.exe"
            try:
                status, returncode, stdout, stderr = run_shell_command(cmd,stdin="{}\n{}\n{}\n{}\n".format(DIR_DATA_TLE, tle_file, satellite, TLE_INDEX))
            except:
                LOG.error("Failed running command: {} with return code: {}".format(cmd,returncode))
                LOG.error("stdout: {}".format(stdout))
                LOG.error("stderr: {}".format(stderr))
                return False
            else:
                if returncode != 0:
                    LOG.debug("Running command: {} with return code: {}".format(cmd,returncode))
                    LOG.debug("stdout: {}".format(stdout))
                    LOG.debug("stderr: {}".format(stderr))
                else:

                    #When a index file is generated above one line is added for each tle file.
                    #If several tle files contains equal TLEs each of these TLEs generate one line in the index file
                    #To avoid this, sort the index file keeping only unique lines(skipping the tle filename at the end
                    
                    #The sort options +0b -3b is guessed to be sort from column 0 to 3, but his is not documented
                    #Could cause problems with future version of sort. http://search.cpan.org/~sdague/ppt-0.12/bin/sort
                    cmd="sort -u -o {} +0b -3b {}".format(os.path.join(DIR_DATA_TLE, "{}.sort".format(TLE_INDEX)),os.path.join(DIR_DATA_TLE, TLE_INDEX))
                    try:
                        status, returncode, stdout, stderr = run_shell_command(cmd)
                    except:
                        LOG.error("Failed running command: {} with return code: {}".format(cmd,returncode))
                        LOG.error("stdout: {}".format(stdout))
                        LOG.error("stderr: {}".format(stderr))
                        return False
                    else:
                        if returncode == 0:
                            try:
                                os.remove(os.path.join(DIR_DATA_TLE, TLE_INDEX))
                            except OSError as e:
                                LOG.error("Failed to remove unsorted and duplicated index file: {}".format(os.path.join(DIR_DATA_TLE, TLE_INDEX)))
                            else:
                                try:
                                    os.rename(os.path.join(DIR_DATA_TLE, "{}.sort".fromat(TLE_INDEX)),os.path.join(DIR_DATA_TLE, TLE_INDEX))
                                except:
                                    LOG.error("Failed to rename sorted index file to original name.")
                                        
    #Change back after this is done
    os.chdir(current_dir)

    return True

def do_tle_satpos(timestamp, satellite, satpos_dir=None):
    
    LOG.info("satpos files is stored under the given directory/satpos")
    if satpos_dir == None:
        satpos_dir = os.path.join(os.environ['DIR_NAVIGATION'], "satpos" )
    else:
        satpos_dir = os.path.join(satpos_dir, "satpos" )
        
    if not os.path.exists(satpos_dir):
        try:
            os.makedirs(satpos_dir)
        except:
            LOG.error("Could not create satpos directory: {}".format(satpos_dir))
            return False
        
    file_satpos = os.path.join(satpos_dir, "satpos_{}_{:%Y%m%d}.txt".format(satellite,timestamp))
    
    if not os.path.exists(file_satpos) or os.stat(file_satpos).st_size == 0:
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
            return False
        else:
            if returncode != 0:
                LOG.error("cmd: {} failed with returncode: {}".format(cmd,returncode))
                return False
            elif not os.path.exists(file_satpos):
                LOG.error("file: {} does not exists after satpostle run.".format(file_satpos))
                return False
    else:
        LOG.info("satpos file already there. Use this")
        
    return True