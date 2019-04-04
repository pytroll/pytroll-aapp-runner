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

"""Run the calibration script from AAPP for AVHRR data.
Relay on several other steps before this can be DONE
"""

import os
import logging
from aapp_runner.helper_functions import run_shell_command

LOG = logging.getLogger(__name__)


def do_avhrr_calibration(process_config, msg, timestamp):

    if not process_config['process_avhrr']:
        LOG.debug("Skipping avhrr processing due to config or missing data.")
        return True

    accepted_return_codes_avhrcl = [0]

    return_value = True
    LOG.debug("Do the avhrr calibration")

    # This function relays on beeing in a working directory
    current_dir = os.getcwd()  # Store the dir to change back to after function complete
    os.chdir(process_config['aapp_processes'][process_config.process_name]['working_dir'])

    # calibration_location = "-c -l"

    try:
        cmd = "avhrcl {0} -s {1} -d {2:%Y%m%d} -h {2:%H%M} -n {3:05d} {4}".format(process_config['calibration_location'],
                                                                                  process_config['platform_name'],
                                                                                  timestamp,
                                                                                  int(process_config['orbit_number']),
                                                                                  process_config['aapp_static_configuration']['decommutation_files']['avhrr_file'])
    except Exception as err:
        LOG.error("Failed to build avhrcl command: {}".format(err))

    try:
        status, returncode, std, err = run_shell_command(cmd)
    except:
        LOG.error("Command {} failed.".format(cmd))
    else:
        if returncode in accepted_return_codes_avhrcl:
            LOG.info("Command {} complete.".format(cmd))
        else:
            LOG.error("Command {} failed with return code {}.".format(cmd, returncode))
            return_value = False

    # Change back after this is done
    os.chdir(current_dir)

    LOG.info("do_avhrr_calibration complete!")
    return return_value

#    # AVHRR l1b contents before AVHRR calibration/navigation
#    if debug:
#        LOG.debug("RUN prl1bavh")
#        """
#        From the nwpsaf documentation:
#        For  AVHRR,  HIRS  and  MSU,  before  and  after  navigation/calibration  task,  AAPP_RUN  calls
#       tools  (prhavh,  prhirs,  prhmsu)  to  write  level  1B  headers  and  first  records  into
#       ASCII  files (phavh_before_calib.log, phavh_before_calib.log , ...).
#        At the end, it renames all output files to include
#        information in the file names: Satellite name, date and time, orbit number.
#
#        But Be aware that the prhxxx scripts now is renamed to prl1bxxx
#        """
#        cmd ="prl1bavh -s 1 -e 1 hrpt.l1b"
#        run_shell_command(cmd,stdout_logfile="prl1bavh_before_calib.log")
#        prl1bavh -s 1 -e 1 hrpt.l1b > prl1bavh_before_calib.log \
#  || { log_error "prl1bavh failed"; exit 1; }
# fi

# AVHRR calibration and navigation
# log_info "Running avhrcl"
# avhrcl $callocflag -s $SATIMG -d $YYYYMMDD -h $HHMN  -n $NNNNN hrpt.l1b \
# || { log_error "avhrcl failed"; exit 1; }

# AVHRR l1b contents after AVHRR calibration/navigation
# if [ -n "${DEBUG}" ] ; then
#  prl1bavh -s 1 -e 1 hrpt.l1b > prl1bavh_after_calib.log \
#  || { log_error "prl1bavh failed"; exit 1; }
# fi
# fi
