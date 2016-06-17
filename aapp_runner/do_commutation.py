#!/home/users/satman/current/bin/python
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

from helper_functions import run_shell_command

LOG = logging.getLogger(__name__)


def do_decommutation(process_config, sensors, timestamp, hrpt_file):
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

    #This function relays on beeing in a working directory
    current_dir = os.getcwd() #Store the dir to change back to after function complete
    os.chdir(process_config['working_directory'])
    
    print "decom file: ", hrpt_file
    for sensor in sensors:
        if str(sensor) in "amsu-a":
            process_config['process_amsua'] = True
        elif str(sensor) in ("amsu-b", "mhs"):
            process_config['process_amsub'] = True
        elif str(sensor) in ("hirs/3","hirs/4"):
            process_config['process_hirs'] = True
        elif str(sensor) in "avhrr/3":
            process_config['process_avhrr'] = True
        elif str(sensor) in "msu":
            process_config['process_msu'] = True
        elif str(sensor) in "dcs":
            process_config['process_dcs'] = True
        else:
            LOG.warning("Instrument/sensor {} not recognised.".format(sensor))

    if 'noaa' in process_config['platform']:
        print "Do the commutaion for NOAA"
        #a_tovs = "ATOVS"
        
        #PROCESS IN THIS ORDER
        #ama = 0 #AMSU-A
        #amb = 0 #AMSU-B
        #hrs = 0 #HIRS
        #avh = 0 #avhrr
        #msu = 0 
        #dcs = 0 
                 
        decom_file = "decommutation.par"
        #Needs to find platform number for A/TOVS
        decom = open(decom_file, 'w')

        if int(process_config['platform'][4:6]) <= 14:
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
        decom.write("{0:05d},{0:05d}                           ! start and end orbit numbers\n".format(process_config['orbit_number']))
        decom.close()
            
        cmd="decommutation {} {} {}".format("".join(process_config['a_tovs']),decom_file, hrpt_file)
        run_shell_command(cmd)
        
    elif 'metop' in process_config['platform']:
        LOG.info("Do the metop decommutation")
        if process_config['process_amsua']:
            cmd="decom-amsua-metop {} {} ".format(process_config['input_amsua_file'],process_config['amsua_file'])
            try:
                status, returncode, std, err = run_shell_command(cmd,stdout_logfile="decom-amsua-metop.log")
            except:
                LOG.error("Command {} failed.".format(cmd))
            else:
                if returncode != 0:
                    LOG.error("Command {} failed with return code {}.".format(cmd, returncode))
                    return False
                else:
                    LOG.info("Command {} complete.".format(cmd))

        if process_config['process_amsub']:
            cmd="decom-mhs-metop {} {} {} ".format("-ignore_degraded_inst_mdr -ignore_degraded_proc_mdr", 
                                                   process_config['input_amsub_file'],
                                                   process_config['amsub_file'])
            try:
                status, returncode, std, err = run_shell_command(cmd,stdout_logfile="decom-mhs-metop.log")
            except:
                LOG.error("Command {} failed.".format(cmd))
            else:
                if returncode != 0:
                    LOG.error("Command {} failed with return code {}.".format(cmd, returncode))
                    return False
                else:
                    LOG.info("Command {} complete.".format(cmd))

        if process_config['process_hirs']:
            cmd="decom-hirs-metop {} {} {} ".format("-ignore_degraded_inst_mdr -ignore_degraded_proc_mdr", 
                                                   process_config['input_hirs_file'],
                                                   process_config['hirs_file'])
            try:
                status, returncode, std, err = run_shell_command(cmd,stdout_logfile="decom-hirs-metop.log")
            except:
                LOG.error("Command {} failed.".format(cmd))
            else:
                if returncode != 0:
                    LOG.error("Command {} failed with return code {}.".format(cmd, returncode))
                    return False
                else:
                    LOG.info("Command {} complete.".format(cmd))

        if process_config['process_avhrr']:
            cmd="decom-avhrr-metop {} {} {} ".format("-ignore_degraded_inst_mdr -ignore_degraded_proc_mdr", 
                                                   process_config['input_avhrr_file'],
                                                   process_config['avhrr_file'])
            try:
                status, returncode, std, err = run_shell_command(cmd,stdout_logfile="decom-avhrr-metop.log")
            except:
                LOG.error("Command {} failed.".format(cmd))
            else:
                if returncode != 0:
                    LOG.error("Command {} failed with return code {}.".format(cmd, returncode))
                    return False
                else:
                    LOG.info("Command {} complete.".format(cmd))

    else:
        print "Unknown platform: {}".format(process_config['platform'])
        return False
    
    
    #Change back after this is done
    os.chdir(current_dir)

    LOG.info("Decommutation complete.")
    return True