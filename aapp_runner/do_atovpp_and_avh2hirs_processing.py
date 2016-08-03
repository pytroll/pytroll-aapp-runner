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

"""
Rename standard AAPP files to proctical ones  ...
"""

import os
import logging
from helper_functions import run_shell_command

LOG = logging.getLogger(__name__)

def do_atovpp_and_avh2hirs_processing(process_config, timestamp):
    LOG.debug("Do preprocessing atovpp and avh2hirs ... ")

    return_status = True
    
    #This function relays on beeing in a working directory
    current_dir = os.getcwd() #Store the dir to change back to after function complete
    os.chdir(process_config['working_directory'])

    instruments = "AMSU-A AMSU-B HIRS"
    if "".join(process_config['a_tovs']) == 'TOVS':
        instruments = "MSU HIRS"
    
    cmd = "atovin {}".format(instruments)
    try:
        status, returncode, std, err = run_shell_command(cmd)
    except:
        LOG.error("Command {} failed.".format(cmd))
    else:
        if returncode != 0:
            LOG.error("Command {} failed with return code {}.".format(cmd, returncode))
            LOG.error(std)
            LOG.error(err)
            return_status = False
        else:
            LOG.info("Command {} complete.".format(cmd))
            LOG.debug(std)
            LOG.debug(err)

    if return_status:
        cmd = "atovpp -i \"{}\" -g HIRS ".format(instruments)
        try:
            status, returncode, std, err = run_shell_command(cmd)
        except:
            LOG.error("Command {} failed.".format(cmd))
        else:
            if returncode != 0:
                LOG.error("Command {} failed with return code {}.".format(cmd, returncode))
                LOG.error(std)
                LOG.error(err)
                return_status = False
            else:
                LOG.info("Command {} complete.".format(cmd))

    #THis will not work since the avh2hirs script sources the ATOVS_CONF at start
    #and relay on WRK dir, which is a static dir
    #
    #Skip for now
    if 0:
        cmd = "avh2hirs {}".format("".join(process_config['a_tovs']))
        try:
            status, returncode, std, err = run_shell_command(cmd)
        except:
            LOG.error("Command {} failed.".format(cmd))
        else:
            if returncode != 0:
                LOG.error("Command {} failed with return code {}.".format(cmd, returncode))
                LOG.error(std)
                LOG.error(err)
                return_status = False
            else:
                LOG.info("Command {} complete.".format(cmd))
    else:
        LOG.warning("avh2hirs is not implemented in the processing.")
        
    #Change back after this is done
    os.chdir(current_dir)

    LOG.info("atovpp and avh2hirs complete!")
    return return_status
