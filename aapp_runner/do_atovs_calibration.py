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

"""Run the calibration script from AAPP for TOVS or ATOVS.

Rely on several other steps before this can be DONE.
"""

import logging
import os

from aapp_runner.helper_functions import run_shell_command

LOG = logging.getLogger(__name__)


def do_atovs_calibration(process_config, timestamp):
    """Do the the ATOVS calibration."""
    if(not process_config['process_amsua'] and
       not process_config['process_amsub'] and
       not process_config['process_mhs'] and
       not process_config['process_msu']):
        LOG.debug("Skipping atovs processing")
        return True

    accepted_return_codes_msucl = [0]
    accepted_return_codes_amsuacl = [0]
    accepted_return_codes_amsubcl = [0]

    return_value = True

    # This function relays on beeing in a working directory
    current_dir = os.getcwd()  # Store the dir to change back to after function complete
    os.chdir(process_config['aapp_processes'][process_config.process_name]['working_dir'])

    # calibration_location = "-c -l"
    if "".join(process_config['a_tovs']) == 'TOVS':
        cmd = "msucl {0} -s {1} -d {2:%Y%m%d} -h {2:%H%M} -n {3:05d} {4}".format(process_config['calibration_location'],
                                                                                 process_config['platform_name'],
                                                                                 timestamp,
                                                                                 int(process_config['orbit_number']),
                                                                                 process_config['aapp_static_configuration']['decommutation_files']['msun_file'])
        try:
            status, returncode, std, err = run_shell_command(cmd)
        except Exception:
            LOG.error("Command {} failed.".format(cmd))
        else:
            if returncode in accepted_return_codes_msucl:
                LOG.info("Command {} complete.".format(cmd))
            else:
                LOG.error("Command {} failed with return code {}.".format(cmd, returncode))
                return_value = False

    elif "".join(process_config['a_tovs']) == 'ATOVS':
        if process_config['process_amsua']:
            cmd = "amsuacl {0} -s {1} -d {2:%Y%m%d} -h {2:%H%M} -n {3:05d} {4}".format(process_config['calibration_location'],
                                                                                       process_config['platform_name'],
                                                                                       timestamp,
                                                                                       int(process_config['orbit_number']),
                                                                                       process_config['aapp_static_configuration']['decommutation_files']['amsua_file'])
            try:
                status, returncode, std, err = run_shell_command(cmd)
            except Exception:
                LOG.error("Command {} failed.".format(cmd))
            else:
                if returncode in accepted_return_codes_amsuacl:
                    LOG.info("Command {} complete.".format(cmd))
                else:
                    LOG.error("Command {} failed with return code {}.".format(cmd, returncode))
                    return_value = False

        if (process_config['process_amsub'] or process_config['process_mhs']) and return_value:
            amsub_script = "mhscl"
            try:
                if 'noaa' in process_config['platform_name'] and int(process_config['platform_name'][-2:]) <= 17:
                    amsub_script = "amsubcl"
                    process_config['process_mhs'] = False
                else:
                    process_config['process_amsub'] = False

            except ValueError:
                LOG.warning("Could not exctract satellite number from the last two characters in the platform name: "
                            "{} and convert it to a number.".format(process_config['platform_name']))

            cmd = "{0} {1} -s {2} -d {3:%Y%m%d} -h {3:%H%M} -n {4:05d} {5}".format(amsub_script,
                                                                                   process_config['calibration_location'],
                                                                                   process_config['platform_name'],
                                                                                   timestamp,
                                                                                   int(process_config['orbit_number']),
                                                                                   process_config['aapp_static_configuration']['decommutation_files']['amsub_file'])
            try:
                status, returncode, std, err = run_shell_command(cmd)
            except Exception:
                LOG.error("Command {} failed.".format(cmd))
            else:
                if returncode in accepted_return_codes_amsubcl:
                    LOG.info("Command {} complete.".format(cmd))
                else:
                    LOG.error("Command {} failed with return code {}.".format(cmd, returncode))
                    return_value = False

    else:
        LOG.error("Unknown A|TOVS key string: {}".format("".join(process_config['a_tovs'])))
        return_value = False

    # Change back after this is done
    os.chdir(current_dir)

    LOG.info("do_atovs_calibration complete!")
    return return_value
