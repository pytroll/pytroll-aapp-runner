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
from aapp_runner.helper_functions import run_shell_command

LOG = logging.getLogger(__name__)


def do_atovpp_and_avh2hirs_processing(process_config, timestamp):
    if(not process_config['process_amsua'] and
       not process_config['process_amsub'] and
       not process_config['process_hirs']):
        LOG.debug("Skipping atovpp and avh2hirs processing.")
        return True

    LOG.debug("Do preprocessing atovpp and avh2hirs ... ")

    return_status = True

    # This function relays on beeing in a working directory
    current_dir = os.getcwd()  # Store the dir to change back to after function complete
    os.chdir(process_config['aapp_processes'][process_config.process_name]['working_dir'])

    instruments = "AMSU-A AMSU-B HIRS"
    grids = "AMSU-A AMSU-B HIRS"
    if not process_config['process_hirs']:
        instruments = "AMSU-A AMSU-B"
        grids = "AMSU-A AMSU-B"
    if "".join(process_config['a_tovs']) == 'TOVS':
        instruments = "MSU HIRS"
        grids = "MSU HIRS"

    if 'do_atovpp' in process_config['aapp_processes'][process_config.process_name]:
        process_config['do_atovpp'] = process_config['aapp_processes'][process_config.process_name]['do_atovpp']
    else:
        process_config['do_atovpp'] = False

    if 'do_avh2hirs' in process_config['aapp_processes'][process_config.process_name]:
        process_config['do_avh2hirs'] = process_config['aapp_processes'][process_config.process_name]['do_avh2hirs']
    else:
        process_config['do_avh2hirs'] = False

    if process_config['do_atovpp']:
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
            cmd = "atovpp -i \"{}\" -g \"{}\"".format(instruments, grids)
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

    if return_status and process_config['do_avh2hirs'] and process_config['process_hirs']:
        if os.path.exists("./{}".format(process_config['aapp_static_configuration']['decommutation_files']['avhrr_file'])):
            os.symlink("./{}".format(process_config['aapp_static_configuration']['decommutation_files']['avhrr_file']),
                       "{}11".format(os.environ["FORT"]))
        else:
            LOG.error("Could not find file: ./{}".format(process_config['aapp_static_configuration']['decommutation_files']['avhrr_file']))
            return_status = False

        if os.path.exists("./hirs.l1d"):
            os.symlink("./hirs.l1d", "{}12".format(os.environ["FORT"]))
        else:
            LOG.error("Could not find file: {}".format("./hirs.l1d"))
            return_status = False

        if return_status:
            cmd = "l1didf -i hirs.l1d"
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
                    satimg, yyyymmdd, hhmn, orbit, instr, loc1, loc2 = std.split()

        if return_status:
            if "noaa" in satimg:
                satnb = int(satimg[4:6])
            else:
                satnb = int(satimg[5:7])
            LOG.debug("Using satnb: {}".format(satnb))
            unit = satnb + 50
            mmc = int(yyyymmdd[4:6])
            LOG.debug("Using unit: {} and mmc: {}".format(unit, mmc))

            if os.path.exists(os.path.join(os.environ["DIR_PREPROC"], "cor_{}.dat".format(satimg))):
                os.symlink(os.path.join(os.environ["DIR_PREPROC"], "cor_{}.dat".format(satimg)),
                           "{}{}".format(os.environ["FORT"], unit))
            else:
                LOG.error("Failed to find {}".format(os.path.join(os.environ["DIR_PREPROC"],
                                                                  "cor_{}.dat".format(satimg))))
                return_status = False

        if return_status:
            os.environ["SATIMG"] = satimg
            os.environ["YYYYMMDD"] = yyyymmdd
            os.environ["HHMN"] = hhmn
            os.environ["DIR_MAIA2_ATLAS"] = os.path.join(os.environ["DIR_PREPROC"], "atlas")
            os.environ["DIR_MAIA2_THRESHOLDS"] = os.path.join(os.environ["DIR_PREPROC"], "thresholds")
            if "".join(process_config['a_tovs']) == 'TOVS':
                cmd = "maia2_env;maia2_environment;avh2hirs.exe 2>&1"
            else:
                cmd = "bash -c \"source liblog.sh;source maia2_env;maia2_environment\";avh2hirs_atovs.exe 2>&1"

            LOG.debug("Before running command: {}".format(cmd))
            try:
                status, returncode, std, err = run_shell_command(cmd,
                                                                 stdout_logfile="avh2hirs.log",
                                                                 use_shlex=False,
                                                                 use_shell=True)
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
            import glob
            for fortfile in glob.glob("./fort*"):
                os.remove(fortfile)
            os.remove("./albedo")
            os.remove("./sst")
            os.remove("./wv")

    # Change back after this is done
    os.chdir(current_dir)

    LOG.info("atovpp and avh2hirs complete!")
    return return_status
