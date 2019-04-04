#!/usr/bin/python
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

"""Run the decommutation script from AAPP for HRPT data
Relay on several other steps before this can be DONE
"""
import logging
import os
import shutil
import re
import datetime

from aapp_runner.helper_functions import run_shell_command

LOG = logging.getLogger(__name__)


def do_decommutation(process_config, msg, timestamp):
    """
    decommutation ${A_TOVS}  decommutation.par  ${FILE}

    The 3 arguments are obligatory.

    ${A_TOVS} = TOVS for satellite number < or = 14
    ${A_TOVS} = ATOVS for satellite number > 14

    In the decommutation.par file, options are written in this order:
    $1,$2,$3,$4,$5, $6, $7, $8,$9,$10,$11,$12 !OPTION NUMBERS
    $lu1,$lu2,$lu3,$lu4,$lu5,$lu6,$lu7,$lu8,$lu9,$lu10,$lu11,$lu12   !STREAM NO.S
    ${YEAR} ! year of the data
    0  ! operational mode
    ${NNNNN},${NNNNN} ! start and end orbit numbers

    $1 = 0 or 1 for level of error logging
    $2 = 0 or 1 for HIRS/3 or HIRS/4 (1 indicates extract HIRS/3 or HIRS/4 data)
    $3 = 0 or 1 for AMSU-A1 (1 indicates extract AMSU-A1 data)
    $4 = 0 or 1 for AMSU-A2 (1 indicates extract AMSU-A2 data)
    $5 = 0 or 1 for AMSU-B/MHS (1 indicates extract AMSU-B/MHS data)
    $6 = 0 or 1 for HIRS/2 (1 indicates extract HIRS/2 data)
    $7 = 0 or 1 for MSU (1 indicates extract MSU data)
    $8 = 0 or 1 for DCS (1 indicates extract DCS data)
    $9 = 0 or 1 for SEM (1 indicates extract SEM data)
    $10= 0 or 1 for SBUV (1 indicates extract SBUV data)
    $11= 0 or 1 for SAR (1 indicates extract SAR data)
    $12= 0 or 1 for AVHRR (1 indicates extract AVHRR data)
    $lu1 is the logical unit of the log file
    $lu2 is the logical unit of the HIRS/3 or HIRS/4.l1a output file
    $lu3 is the logical unit of the AMSU-A1.l1a output file
    $lu4 is the logical unit of the AMSU-A2.l1a output file
    $lu5 is the logical unit of the AMSU-B.l1a output file
    $lu6 is the logical unit of the HIRS/2.l1a output file
    $lu7 is the logical unit of the MSU.l1a output file
    $lu8 is the logical unit of the DCS.l1a output file
    $lu9 is the logical unit of the SEM.l1a output file
    $lu10 is the logical unit of the SBUV.l1a output file
    $lu11 is the logical unit of the SAR.l1a output file
    $lu12 is the logical unit of the HRPT.l1a output file
    """

    # A list of accepted return codes for the various scripts/binaries run in this function
    accepted_return_codes_decom_hrpt = [0]
    accepted_return_codes_chk1btime = [0]
    accepted_return_codes_decom_amsua_metop = [0]
    accepted_return_codes_decom_amsub_metop = [0]
    accepted_return_codes_decom_hirs_metop = [0]
    accepted_return_codes_decom_avhrr_metop = [0]

    return_status = True

    # This function relays on beeing in a working directory
    current_dir = os.getcwd()  # Store the dir to change back to after function complete
    os.chdir(process_config['aapp_processes'][process_config.process_name]['working_dir'])

    # for sensor in sensors:
    #    if str(sensor) in "amsu-a":
    #        process_config['process_amsua'] = True
    #    elif str(sensor) in ("amsu-b", "mhs"):
    #        process_config['process_amsub'] = True
    #   elif str(sensor) in ("hirs/3","hirs/4"):
    #       process_config['process_hirs'] = True
    #    elif str(sensor) in "avhrr/3":
    #        process_config['process_avhrr'] = True
    #    elif str(sensor) in "msu":
    #        process_config['process_msu'] = True
    #    elif str(sensor) in "dcs":
    #        process_config['process_dcs'] = True
    #    else:
    #        LOG.warning("Instrument/sensor {} not recognised.".format(sensor))

    if 'noaa' in process_config['platform_name']:
        LOG.debug("Do the commutation for NOAA")
        # a_tovs = "ATOVS"

        # PROCESS IN THIS ORDER
        # ama = 0 # AMSU-A
        # amb = 0 # AMSU-B
        # hrs = 0 # HIRS
        # avh = 0 # avhrr
        # msu = 0
        # dcs = 0

        try:
            decom_file = "decommutation.par"
            # Needs to find platform number for A/TOVS
            decom = open(decom_file, 'w')

            if int(process_config['platform_name'][4:6]) <= 14:
                del process_config['a_tovs'][0]

                decom.write("1,0,0,0,0,{},{},{},0,0,0,{}               ! OPTION NUMBERS\n".format(1 if process_config['process_hirs'] else 0,
                                                                                                  1 if process_config['process_msu'] else 0,
                                                                                                  1 if process_config['process_dcs'] else 0,
                                                                                                  1 if process_config['process_avhrr'] else 0))
                decom.write("10,0,0,0,0,11,12,13,0,0,0,14              ! STREAM NO.S\n")
            else:
                decom.write("1,{},{},{},{},0,0,{},0,0,0,{}               ! OPTION NUMBERS\n".format(1 if process_config['process_hirs'] else 0,
                                                                                                    1 if process_config['process_amsua'] else 0,
                                                                                                    1 if process_config['process_amsua'] else 0,
                                                                                                    1 if process_config['process_amsub'] else 0,
                                                                                                    1 if process_config['process_dcs'] else 0,
                                                                                                    1 if process_config['process_avhrr'] else 0))
                decom.write("10,11,15,15,16,0,0,13,0,0,0,14              ! STREAM NO.S\n")

            decom.write("{:%Y}                                     ! year of the data\n".format(timestamp))
            decom.write("0                                         ! operational mode\n")
            decom.write("{0:05d},{0:05d}                           ! start and end orbit numbers\n".format(int(process_config['orbit_number'])))
            decom.close()
        except Exception as err:
            LOG.error("Building decommutation command string failed: {}".format(err))
            return False

        # Read decommitation input into variable
        decom = open(decom_file, 'r')
        decom_input = decom.read()
        decom.close()

        os.environ['FILE_COEF'] = os.path.join(os.environ.get('PAR_CALIBRATION_COEF'), 'amsua', 'amsua_clparams.dat')
        os.symlink(os.environ['FILE_COEF'], "{}50".format(os.environ['FORT']))

        cmd = "decommutation.exe"  # .format("".join(process_config['a_tovs']),decom_file, process_config['input_hrpt_file'])
        try:
            status, returncode, std, err = run_shell_command(cmd, stdin="{}\n{}{}\n".format(process_config['input_hrpt_file'], decom_input, os.getenv('STATION_ID', 'ST')),
                                                             stdout_logfile='decommutation.log',
                                                             stderr_logfile='decommutation.log')
        except:
            LOG.error("Command {} failed.".format(cmd))
        else:
            if returncode in accepted_return_codes_decom_hrpt:
                LOG.info("Decommutation command {} complete.".format(cmd))
            else:
                LOG.error("Command {} failed with return code {}.".format(cmd, returncode))
                return_status = False

        # Need to check the decommutation output and rename fort files
        if return_status:
            # hrpt 14
            if os.path.exists("{}14".format(os.environ['FORT'])):
                shutil.move("{}14".format(os.environ['FORT']), process_config['aapp_static_configuration']['decommutation_files']['avhrr_file'])
                cmd = "chk1btime.exe"
                try:
                    status, returncode, std, err = run_shell_command(cmd, stdin="{}\n".format(process_config['aapp_static_configuration']['decommutation_files']['avhrr_file']))
                except:
                    LOG.error("Failed to execute command {}. Something wrong with the command.".format(cmd))
                else:
                    if returncode in accepted_return_codes_chk1btime:
                        LOG.debug("chk1btime command {} ok.".format(cmd))
                        LOG.debug("std: {}".format(std))
                    else:
                        LOG.error("This means that the start of the data are bad,"
                                  " and that the processing for this data later will fail.")
                        LOG.debug("Return code: {}".format(returncode))
                        LOG.debug("std: {}".format(std))
                        LOG.debug("err: {}".format(err))
                        LOG.debug("status: {}".format(status))
                        process_config['process_avhrr'] = False

            elif process_config['process_avhrr']:
                LOG.warning("Fort file for avhrr does not exist after decommutation. Skip processing this.")
                process_config['process_avhrr'] = False

            # hrsn 11
            if os.path.exists("{}11".format(os.environ['FORT'])):
                shutil.move("{}11".format(os.environ['FORT']), process_config['aapp_static_configuration']['decommutation_files']['hirs_file'])
                cmd = "chk1btime.exe"
                try:
                    status, returncode, std, err = run_shell_command(cmd, stdin="{}\n".format(process_config['aapp_static_configuration']['decommutation_files']['hirs_file']))
                except:
                    LOG.error("Failed to execute command {}. Something wrong with the command.".format(cmd))
                else:
                    if returncode in accepted_return_codes_chk1btime:
                        LOG.debug("chk1btime command {} ok.".format(cmd))
                    else:
                        LOG.error("This means that the start of the data are bad,"
                                  " and that the processing for this data later will fail.")
                        LOG.debug("Return code: {}".format(returncode))
                        LOG.debug("std: {}".format(std))
                        LOG.debug("err: {}".format(err))
                        LOG.debug("status: {}".format(status))
                        process_config['process_hirs'] = False
            elif process_config['process_hirs']:
                LOG.warning("Fort file for hirs does not exist after decommutation. Skip processing this.")
                process_config['process_hirs'] = False

            # msun 12
            if os.path.exists("{}12".format(os.environ['FORT'])):
                shutil.move("{}12".format(os.environ['FORT']), process_config['aapp_static_configuration']['decommutation_files']['msu_file'])
                cmd = "chk1btime.exe"
                try:
                    status, returncode, std, err = run_shell_command(cmd, stdin="{}\n".format(process_config['aapp_static_configuration']['decommutation_files']['msu_file']))
                except:
                    LOG.error("Failed to execute command {}. Something wrong with the command.".format(cmd))
                else:
                    if returncode in accepted_return_codes_chk1btime:
                        LOG.debug("chk1btime command {} ok.".format(cmd))
                    else:
                        LOG.error("This means that the start of the data are bad, "
                                  " and that the processing for this data later will fail.")
                        LOG.debug("Return code: {}".format(returncode))
                        LOG.debug("std: {}".format(std))
                        LOG.debug("err: {}".format(err))
                        LOG.debug("status: {}".format(status))
                        process_config['process_msu'] = False
            elif process_config['process_msu']:
                LOG.warning("Fort file for msu does not exist after decommutation. Skip processing this.")
                process_config['process_msu'] = False

            # dcsn 13
            if os.path.exists("{}13".format(os.environ['FORT'])):
                shutil.move("{}13".format(os.environ['FORT']), process_config['aapp_static_configuration']['decommutation_files']['dcs_file'])
                cmd = "chk1btime.exe"
                try:
                    status, returncode, std, err = run_shell_command(cmd, stdin="{}\n".format(process_config['aapp_static_configuration']['decommutation_files']['dcs_file']))
                except:
                    LOG.error("Failed to execute command {}. Something wrong with the command.".format(cmd))
                else:
                    if returncode in accepted_return_codes_chk1btime:
                        LOG.debug("chk1btime command {} ok.".format(cmd))
                    else:
                        LOG.error("This means that the start of the data are bad,"
                                  " and that the processing for this data later will fail.")
                        LOG.debug("Return code: {}".format(returncode))
                        LOG.debug("std: {}".format(std))
                        LOG.debug("err: {}".format(err))
                        LOG.debug("status: {}".format(status))
                        process_config['process_dcs'] = False
            elif process_config['process_dcs']:
                LOG.warning("Fort file for dcs does not exist after decommutation. Skip processing this.")
                process_config['process_dcs'] = False

            # aman 15
            if os.path.exists("{}15".format(os.environ['FORT'])):
                shutil.move("{}15".format(os.environ['FORT']), process_config['aapp_static_configuration']['decommutation_files']['amsua_file'])
                cmd = "chk1btime.exe"
                try:
                    status, returncode, std, err = run_shell_command(cmd,
                                                                     stdin="{}\n".format(process_config['aapp_static_configuration']['decommutation_files']['amsua_file']))
                except:
                    LOG.error("Failed to execute command {}. Something wrong with the command.".format(cmd))
                else:
                    if returncode in accepted_return_codes_chk1btime:
                        LOG.debug("chk1btime command {} ok.".format(cmd))
                    else:
                        LOG.error("This means that the start of the data are bad, "
                                  "and that the processing for this data later will fail?")
                        LOG.debug("Return code: {}".format(returncode))
                        LOG.debug("std: {}".format(std))
                        LOG.debug("err: {}".format(err))
                        LOG.debug("status: {}".format(status))
                        process_config['process_amsua'] = False
            elif process_config['process_amsua']:
                LOG.warning("Fort file for amsu-a does not exist after decommutation. Skip processing this.")
                process_config['process_amsua'] = False

            # ambn 16
            if os.path.exists("{}16".format(os.environ['FORT'])):
                shutil.move("{}16".format(os.environ['FORT']), process_config['aapp_static_configuration']['decommutation_files']['amsub_file'])
                cmd = "chk1btime.exe"
                try:
                    status, returncode, std, err = run_shell_command(cmd, stdin="{}\n".format(process_config['aapp_static_configuration']['decommutation_files']['amsub_file']))
                except:
                    LOG.error("Failed to execute command {}. Something wrong with the command.".format(cmd))
                else:
                    if returncode in accepted_return_codes_chk1btime:
                        LOG.debug("chk1btime command {} ok.".format(cmd))
                    else:
                        LOG.error("This means that the start of the data are bad, "
                                  "and that the processing for this data later will fail.")
                        LOG.debug("Return code: {}".format(returncode))
                        LOG.debug("std: {}".format(std))
                        LOG.debug("err: {}".format(err))
                        LOG.debug("status: {}".format(status))
                        process_config['process_amsub'] = False
            elif process_config['process_amsub']:
                LOG.warning("Fort file for amsu-b does not exist after decommutation. Skip processing this.")
                process_config['process_amsub'] = False

        if os.path.exists("decommutation.log"):
            dd = None
            tt = None
            with open("decommutation.log") as decomlog:
                while True:
                    line = decomlog.readline()
                    if not line:
                        break
                    p_date = re.compile(".*avhrr\send\sdata\sday\s([0-9]{2})/([0-9]{2})/([0-9]{2})")
                    p_time = re.compile(".*avhrr\send\sdata\stime\s([0-9]{2}):([0-9]{2}):([0-9]{2})\.([0-9]{0,3})")
                    se = p_date.search(line)
                    if se:
                        dd = datetime.date(year=(int(se.group(3)) + 2000), month=int(se.group(2)), day=int(se.group(1)))
                    te = p_time.search(line)
                    if te:
                        tt = datetime.time(hour=int(te.group(1)),
                                           minute=int(te.group(2)),
                                           second=int(te.group(3)),
                                           microsecond=int(te.group(4)) * 1000)
                    if tt and dd:
                        et = datetime.datetime.combine(dd, tt)
                        if abs((et - process_config['endtime']).total_seconds()) < 20 * 60:
                            LOG.debug("Adjusting the endtime with data from decommutaion.log.")
                            LOG.debug("Before {}".format(process_config['endtime']))
                            process_config['endtime'] = et
                            LOG.debug("After  {}".format(process_config['endtime']))
                        break
    elif 'METOP' in process_config['platform_name'].upper():
        LOG.info("Do the metop decommutation")
        if process_config['process_amsua']:
            cmd = "decom-amsua-metop {} {} ".format(process_config['input_amsua_file'],
                                                    process_config['aapp_static_configuration']['decommutation_files']['amsua_file'])
            try:
                status, returncode, std, err = run_shell_command(cmd, stdout_logfile="decom-amsua-metop.log")
            except:
                LOG.error("Command {} failed.".format(cmd))
            else:
                if returncode in accepted_return_codes_decom_amsua_metop:
                    LOG.info("Command {} complete.".format(cmd))
                    if not os.path.exists(process_config['aapp_static_configuration']['decommutation_files']['amsua_file']):
                        LOG.warning("Decom gave OK status, but no data is produced. Something is wrong")
                else:
                    LOG.error("Command {} failed with return code {}.".format(cmd, returncode))
                    return_status = False

        if process_config['process_mhs'] and return_status:
            cmd = "decom-mhs-metop {} {} {} ".format("-ignore_degraded_inst_mdr -ignore_degraded_proc_mdr",
                                                     process_config['input_mhs_file'],
                                                     process_config['aapp_static_configuration']['decommutation_files']['mhs_file'])
            try:
                status, returncode, std, err = run_shell_command(cmd, stdout_logfile="decom-mhs-metop.log")
            except:
                LOG.error("Command {} failed.".format(cmd))
            else:
                if returncode in accepted_return_codes_decom_amsub_metop:
                    LOG.info("Command {} complete.".format(cmd))
                else:
                    LOG.error("Command {} failed with return code {}.".format(cmd, returncode))
                    return_status = False

        if process_config['process_hirs'] and return_status:
            cmd = "decom-hirs-metop {} {} {} ".format("-ignore_degraded_inst_mdr -ignore_degraded_proc_mdr",
                                                      process_config['input_hirs_file'],
                                                      process_config['aapp_static_configuration']['decommutation_files']['hirs_file'])
            try:
                status, returncode, std, err = run_shell_command(cmd, stdout_logfile="decom-hirs-metop.log")
            except:
                LOG.error("Command {} failed.".format(cmd))
            else:
                if returncode in accepted_return_codes_decom_hirs_metop:
                    LOG.info("Command {} complete.".format(cmd))
                else:
                    LOG.error("Command {} failed with return code {}.".format(cmd, returncode))
                    return_status = False

        if process_config['process_avhrr'] and return_status:
            cmd = "decom-avhrr-metop {} {} {} ".format("-ignore_degraded_inst_mdr -ignore_degraded_proc_mdr",
                                                       process_config['input_avhrr_file'],
                                                       process_config['aapp_static_configuration']
                                                       ['decommutation_files']['avhrr_file'])
            try:
                status, returncode, std, err = run_shell_command(cmd, stdout_logfile="decom-avhrr-metop.log")
            except:
                LOG.error("Command {} failed.".format(cmd))
            else:
                if returncode in accepted_return_codes_decom_avhrr_metop:
                    LOG.info("Command {} complete.".format(cmd))
                else:
                    LOG.error("Command {} failed with return code {}.".format(cmd, returncode))
                    return_status = False

    else:
        LOG.error("Unknown platform: {}".format(process_config['platform_name']))
        return_status = False

    # print os.listdir("./")
    # Change back after this is done
    os.chdir(current_dir)

    LOG.info("All decommutations complete.")
    return return_status
