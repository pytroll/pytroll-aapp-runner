#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2016

# Author(s):

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

"""Run the ana attitude correction script for AVHRR
Relay on several other steps before this can be DONE
"""

import os
import logging
from datetime import datetime

from helper_functions import run_shell_command

LOG = logging.getLogger(__name__)

def do_ana_correction(process_config, timestamp):

    """The software ANA is 3party. User will have to get it themself.
    Further the ana binaries and scripts must be in the PATH,
    preferable in the primary AAPP bin directory
   
    Also it relays on reference_landmarks directory under ENV(DIR_NAVIGATE)/ana dir
    This needs to be copied/installed here by the user 
    """


    return_status = True
    
    #This function relays on beeing in a working directory
    current_dir = os.getcwd() #Store the dir to change back to after function complete
    os.chdir(process_config['working_directory'])

    #Must check of the ana dir exists
    ana_dir = os.path.join(os.getenv('DIR_NAVIGATION'),'ana')
    if not os.path.exists(ana_dir):
        try:
            os.makedirs(ana_dir)
        except OSError in e:
            LOG.error("Failed to create directory: {}. This is needed to run the ANA software.")
            return_status = False
    
    if return_status:
        #Find all matching landmarks
        cmd = "ana_lmk_loc -D {}".format(process_config['avhrr_file'])
        try:
            status, returncode, std, err = run_shell_command(cmd, stdout_logfile="ana_lmk_loc.log",stderr_logfile="ana_lmk_loc.err")
        except:
            import sys
            LOG.error("Command {} failed with {}.".format(cmd,sys.exc_info()[0]))
            if not status:
                return_status = False
        else:
            if not status or returncode != 0:
                LOG.error("Command {} failed with {}".format(cmd, returncode))
                _ana_file = open('ana_lmk_loc.err',"w")
                _ana_file.write(std)
                _ana_file.write(err)
                _ana_file.close()
                return_status = False

    if return_status:
        #Check of ana landmark location file with correct time stamp exist. If not try to find correct timestamp and copy to this. 
        expected_ana_loc_file = "lmkloc_{}_{:%Y%m%d_%H%M}_{:05d}.txt".format(process_config['platform'], timestamp, process_config['orbit_number'])
        if not os.path.exists(os.path.join(ana_dir,"{:%Y-%m}".format(timestamp),expected_ana_loc_file)):
            #Need to run this AAPP command to detect the start of the hrpt.l1b(avhrr data) file
            #Because this might differ from the time stamp in file name
            cmd = "l1bidf.exe"
            try:
                status, returncode, std, err = run_shell_command(cmd,stdin="{}\n".format(process_config['avhrr_file']))
            except:
                import sys
                LOG.error("Command {} failed with {}.".format(cmd,sys.exc_info()[0]))
                if not status:
                    return_status = False
            else:
                if not status or returncode != 0:
                    LOG.error("Command {} failed with {}".format(cmd, returncode))
                    _ana_file = open('ana_lmk_loc.err',"w")
                    _ana_file.write(std)
                    _ana_file.write(err)
                    _ana_file.close()
                    return_status = False
                else:
                    l1bidf_list = std.split()
                    l1bidf_aapp_datetime = datetime.strptime(l1bidf_list[1]+l1bidf_list[2],"%Y%m%d%H%M")
                    print l1bidf_aapp_datetime
                    #estatt_file_name =     lmkloc_metop02_20160617_0952_50132.txt
                    ana_loc_file = "lmkloc_{}_{:%Y%m%d_%H%M}_{:05d}.txt".format(l1bidf_list[0], l1bidf_aapp_datetime, int(l1bidf_list[3]))
                    if os.path.exists(os.path.join(ana_dir,"{:%Y-%m}".format(l1bidf_aapp_datetime),ana_loc_file)):
                        LOG.debug("timestamp {} l1bidf_aapp_datetime: {}".format(timestamp, l1bidf_aapp_datetime))
                        from shutil import copy2
                        copy2(os.path.join(ana_dir,"{:%Y-%m}".format(timestamp),ana_loc_file),
                              os.path.join(ana_dir,"{:%Y-%m}".format(l1bidf_aapp_datetime),expected_ana_loc_file))
                    else:
                        LOG.warning("Could not find ana loc file: {}".format(ana_loc_file))
        else:
            LOG.debug("File ok: {}".format(os.path.join(ana_dir,"{:%Y-%m}".format(timestamp),expected_ana_loc_file)))
        
    import hashlib
    if return_status:
        print hashlib.sha256(open(process_config['avhrr_file'], 'rb').read()).hexdigest()
    
        #Calculate correction from landmarks and update avhrr_file with these new attitude coefisients.
        cmd = "ana_estatt -s {0} -d {1:%Y%m%d} -h {1:%H%M} -n {2:5d}".format(process_config['platform'], timestamp, process_config['orbit_number'])
        try:
            status, returncode, std, err = run_shell_command(cmd)
        except:
            import sys
            LOG.error("Command {} failed with {}.".format(cmd,sys.exc_info()[0]))
            if not status:
                return_status = False
        else:
            if not status or returncode != 0:
                LOG.error("Command {} failed with {}".format(cmd, returncode))
                _ana_file = open('ana_lmk_loc.err',"w")
                _ana_file.write(std)
                _ana_file.write(err)
                _ana_file.close()
                return_status = False
    
        print hashlib.sha256(open(process_config['avhrr_file'], 'rb').read()).hexdigest()
    
        import hashlib
        sha256_before_correction = hashlib.sha256(open(process_config['avhrr_file'], 'rb').read()).hexdigest()

    if return_status:
        #Recalculate the location in the avhhr data file with the new correction attitude coefisients.
        from do_avhrr_calibration import do_avhrr_calibration
        if not do_avhrr_calibration(process_config, timestamp):
            LOG.warning("The avhrr location with ana correction failed for some reason. It might be that the processing can continue")
            LOG.warning("Please check the previous log carefully to see if this is an error you can accept.")
            return_status = False
            
    if return_status:
        sha256_after_correction = hashlib.sha256(open(process_config['avhrr_file'], 'rb').read()).hexdigest()
        print "Before: " + sha256_before_correction
        print "After : " + sha256_after_correction
        if ( sha256_before_correction == sha256_after_correction):
            LOG.warning("The correction of the avhrr location with data from ANA did not take place for some reason, or the correction was 0 for all pitch yaw and roll.")
        else:
            LOG.info("The correction of AVHRR location with data from ANA was performed.")
    
    #Change back after this is done
    os.chdir(current_dir)

    LOG.info("do_ana_correction complete!")

    return return_status
