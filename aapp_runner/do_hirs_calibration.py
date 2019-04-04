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

"""Run the calibration script from AAPP for HIRS
Relay on several other steps before this can be DONE
"""

import os
import logging

from aapp_runner.helper_functions import run_shell_command

LOG = logging.getLogger(__name__)


def do_hirs_calibration(process_config, msg, timestamp):

    return_status = True

    if not process_config['process_hirs']:
        return True

    # A list of accepted return codes for the various scripts/binaries run in this function
    accepted_return_codes_hirs_historic_file_manage = [0]

    # This function relays on beeing in a working directory
    current_dir = os.getcwd()  # Store the dir to change back to after function complete
    os.chdir(process_config['aapp_processes'][process_config.process_name]['working_dir'])
    hirs_version_use = None
    hirs_version = os.getenv('HIRSCL_VERSION', 0)
    hirs_version_list = hirs_version.split()
    hirs_sats = os.getenv('HIRSCL_SAT', 'default')
    hirs_sat_list = hirs_sats.split()
    index = 0
    for sat in hirs_sat_list:
        if sat in process_config['platform_name']:
            hirs_version_use = hirs_version_list[index]
        else:
            hirs_version_def = hirs_version_list[index]

        index += 1

    if hirs_version_use is None:
        hirs_version_use = hirs_version_def

    hirs_script = "hirscl"
    hirs_err_file = "hirscl.err"
    calibration_location = process_config['calibration_location']

    LOG.debug("hirs_version_use {}".format(hirs_version_use))

    # pdb.set_trace()

    if int(hirs_version_use) > 1:  # no calibration, just navigation
        calibration_location = "-l"
    elif int(hirs_version_use) == 0 or "".join(process_config['a_tovs']) == 'TOVS':
        calibration_location = "-c -l"
    elif int(hirs_version_use) == 1:
        file_historic = os.path.join(os.getenv('PAR_CALIBRATION_MONITOR'),
                                     process_config['platform_name'], "hirs_historic.txt")
        if os.path.exists(file_historic):
            cmd = "hirs_historic_file_manage -m {} -r {} -n {} {}".format(os.getenv('HIST_SIZE_HIGH'),
                                                                          os.getenv('HIST_SIZE_LOW'),
                                                                          os.getenv('HIST_NMAX'),
                                                                          file_historic)
            try:
                status, returncode, std, err = run_shell_command(cmd)
            except:
                LOG.error("Command {} failed.".format(cmd))
                return_status = False
            else:
                if returncode in accepted_return_codes_hirs_historic_file_manage:
                    LOG.debug("Command complete.")
                else:
                    LOG.error("Command {} failed with returncode {}".format(cmd, returncode))
                    LOG.error("stdout was: {}".format(std))
                    LOG.error("stderr was: {}".format(err))
                    return_status = False

        if return_status:
            cmd = "hcalcb1_algoV4 -s {0} -y {1:%Y} -m {1:%m} -d {1:%d} -h {1:%H} -n {1:%M}".format(process_config['platform_name'], timestamp)
            try:
                status, returncode, std, err = run_shell_command(cmd)
            except:
                import sys
                LOG.error("Command {} failed with {}.".format(cmd, sys.exc_info()[0]))
            else:
                if returncode != 0:
                    LOG.error("Command {} failed with {}".format(cmd, returncode))
                    _hirs_file = open(hirs_err_file, "w")
                    _hirs_file.write(std)
                    _hirs_file.write(err)
                    _hirs_file.close()
                    return_status = False

            hirs_script = "hirscl_algoV4"
            hirs_err_file = "hirscl_algoV4.err"
            calibration_location = "-c -l"
    else:
        LOG.error("Can not figure out which hirs calibration algo version to use.")
        return_status = False

    if return_status:

        try:
            cmd = "{} {} -s {} -d {:%Y%m%d} -h {:%H%M} -n {:05d} {}".format(hirs_script,
                                                                            calibration_location,
                                                                            process_config['platform_name'],
                                                                            timestamp,
                                                                            timestamp,
                                                                            int(process_config['orbit_number']),
                                                                            process_config['aapp_static_configuration']
                                                                            ['decommutation_files']['hirs_file'])
        except KeyError as ke:
            LOG.error("Building command string failed with key error: {}".format(ke))

        try:
            status, returncode, out, err = run_shell_command(cmd, stdout_logfile="{}.log".format(hirs_script),
                                                             stderr_logfile="{}".format(hirs_err_file))
        except:
            import sys
            LOG.error("Command {} failed {}.".format(cmd, sys.exc_info()[0]))
        else:
            if (returncode != 0):
                LOG.error("Command {} failed with {}".format(cmd, returncode))
                _hirs_file = open(hirs_err_file, "w")
                _hirs_file.write(out)
                _hirs_file.write(err)
                _hirs_file.close()
                return_status = False

    # Change back after this is done
    os.chdir(current_dir)

    LOG.info("do_hirs_calibration complete!")

    return return_status
