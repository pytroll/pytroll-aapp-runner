#!/usr/bin/env python
# -*- coding: utf-8 -*-
import cmd
from mx.DateTime.DateTime import Timestamp
from cmd import Cmd

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



def do_tleing(aapp_prefix, timestamp, satellite, tle_indir=None):
    """Get the tle-file and copy them to the AAPP data structure 
       and run the AAPP tleing script and executable"""
    
    #FIXME add as config
    tle_file_timestamp_format = "%Y%m%dT%H%M"
    
    if tle_indir == None:
        """This is the default directory for the tles"""
        tle_indir = os.environ['DIR_NAVIGATION']
    else:
        LOG.warning("Reset the already set DIR_NAVIGATION from {} to {}.".format(os.environ['DIR_NAVIGATION'], tle_indir))
        os.environ['DIR_NAVIGATION'] = tle_indir
        
    #This is needed by tleing. create if not exists
    if not os.path.exists(tle_indir+ "/tle_db"):
        try:
            os.makedirs(tle_indir + "/tle_db")
        except:
            LOG.error("Could not create directory {}".format(tle_indir + "/tle_db"))
            return False
        
    infile = "tle-{0:{1}}.txt".format(timestamp,tle_file_timestamp_format)
    
    print "tle file name: {}".format(infile)
    
    if not os.path.exists(tle_indir + "/" + infile):
        print "Could not find the tle file: {}".format(tle_indir + "/" + infile)
        return False
    else:
        """Dont use the tle_indir because this is handleled by the tleing script"""
        print "TLE file ok. Do the calcfor {} ... ".format(satellite)
        cmd="tleing -s {} -f {}".format(satellite,infile)
        try:
            run_shell_command(cmd)
        except:
            print "Failed running command: {}".format(cmd)
            return False
        
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