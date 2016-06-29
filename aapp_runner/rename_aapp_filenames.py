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
import shutil

LOG = logging.getLogger(__name__)

def rename_aapp_filenames(process_config, timestamp, aapp_outdir_selected=".", file_name_prefix_avhrr_data="hrpt"):
    LOG.debug("Rename AAPP filenames ... ")

    #This function relays on beeing in a working directory
    current_dir = os.getcwd() #Store the dir to change back to after function complete
    os.chdir(process_config['working_directory'])

    if process_config['process_avhrr']:
        if os.path.exists(process_config['avhrr_file']):
            new_name = "{0:}_{1:}_{2:%Y%m%d}_{2:%H%M}_{3:5d}.l1b".format(file_name_prefix_avhrr_data,
                                                                         process_config['platform'],
                                                                         timestamp,
                                                                         process_config['orbit_number'])
            try:
                shutil.move(process_config['avhrr_file'],os.path.join(aapp_outdir_selected,new_name))
            except OSError as e:
                LOG.error("Failed to rename {} to {}. {}".format(process_config['avhrr_file'],os.path.join(aapp_outdir_selected,new_name),e))
                LOG.error("Please check previous processing")
        else:
            LOG.error("Excpected file {} does not exists. Please check previous processing.".format(process_config['avhrr_file']))

    if process_config['process_hirs']:
        if os.path.exists(process_config['hirs_file']):
            new_name = "{0:}_{1:}_{2:%Y%m%d}_{2:%H%M}_{3:5d}.l1b".format("hirsl1b",
                                                                         process_config['platform'],
                                                                         timestamp,
                                                                         process_config['orbit_number'])
            try:
                shutil.move(process_config['hirs_file'],os.path.join(aapp_outdir_selected,new_name))
            except OSError as e:
                LOG.error("Failed to rename {} to {}. {}".format(process_config['hirs_file'], os.path.join(aapp_outdir_selected,new_name),e))
                LOG.error("Please check previous processing")
        else:
            LOG.error("Excpected file {} does not exists. Please check previous processing.".format(process_config['hirs_file']))

        if os.path.exists('hrsn.l1c'):
            new_name = "{0:}_{1:}_{2:%Y%m%d}_{2:%H%M}_{3:5d}.l1c".format("hirsl1c",
                                                                         process_config['platform'],
                                                                         timestamp,
                                                                         process_config['orbit_number'])
            try:
                shutil.move('hrsn.l1c',os.path.join(aapp_outdir_selected,new_name))
            except OSError as e:
                LOG.error("Failed to rename {} to {}. {}".format('hrsn.l1c', os.path.join(aapp_outdir_selected,new_name),e))
                LOG.error("Please check previous processing")
        else:
            LOG.error("Excpected file {} does not exists. Please check previous processing.".format('hrsn.l1c'))

        #Note: HIRS l1d uses name: hirs, not hrsn as l1b and l1c files
        if os.path.exists('hirs.l1d'):
            new_name = "{0:}_{1:}_{2:%Y%m%d}_{2:%H%M}_{3:5d}.l1d".format("hirsl1d",
                                                                         process_config['platform'],
                                                                         timestamp,
                                                                         process_config['orbit_number'])
            try:
                shutil.move('hirs.l1d',os.path.join(aapp_outdir_selected,new_name))
            except OSError as e:
                LOG.error("Failed to rename {} to {}. {}".format('hirs.l1d', os.path.join(aapp_outdir_selected,new_name),e))
                LOG.error("Please check previous processing")
        else:
            LOG.error("Excpected file {} does not exists. Please check previous processing.".format('hirs.l1d'))

    if process_config['process_amsua']:
        if os.path.exists(process_config['amsua_file']):
            new_name = "{0:}_{1:}_{2:%Y%m%d}_{2:%H%M}_{3:5d}.l1b".format("amsual1b",
                                                                         process_config['platform'],
                                                                         timestamp,
                                                                         process_config['orbit_number'])
            try:
                shutil.move(process_config['amsua_file'],os.path.join(aapp_outdir_selected,new_name))
            except OSError as e:
                LOG.error("Failed to rename {} to {}. {}".format(process_config['amsua_file'],os.path.join(aapp_outdir_selected,new_name),e))
                LOG.error("Please check previous processing")
        else:
            LOG.error("Excpected file {} does not exists. Please check previous processing.".format(process_config['amsua_file']))

        if os.path.exists('aman.l1c'):
            new_name = "{0:}_{1:}_{2:%Y%m%d}_{2:%H%M}_{3:5d}.l1c".format("amsual1c",
                                                                         process_config['platform'],
                                                                         timestamp,
                                                                         process_config['orbit_number'])
            try:
                shutil.move('aman.l1c',os.path.join(aapp_outdir_selected,new_name))
            except OSError as e:
                LOG.error("Failed to rename {} to {}. {}".format('aman.l1c',os.path.join(aapp_outdir_selected,new_name),e))
                LOG.error("Please check previous processing")
        else:
            LOG.error("Expected file {} does not exists. Please check previous processing.".format('aman.l1c'))

        if os.path.exists('aman.l1d'):
            new_name = "{0:}_{1:}_{2:%Y%m%d}_{2:%H%M}_{3:5d}.l1d".format("amsual1d",
                                                                         process_config['platform'],
                                                                         timestamp,
                                                                         process_config['orbit_number'])
            try:
                shutil.move('aman.l1d',os.path.join(aapp_outdir_selected,new_name))
            except OSError as e:
                LOG.error("Failed to rename {} to {}. {}".format('aman.l1d',os.path.join(aapp_outdir_selected,new_name),e))
                LOG.error("Please check previous processing")
        #else:
        #    LOG.error("Excpected file {} does not exists. Please check previous processing.".format('aman.l1d'))

    if process_config['process_amsub']:
        if 'noaa' in process_config['platform'] and int(process_config['platform'][:-2]) <= 17:
            if os.path.exists(process_config['amsub_file']):
                new_name = "{0:}_{1:}_{2:%Y%m%d}_{2:%H%M}_{3:5d}.l1b".format("amsubl1b",
                                                                             process_config['platform'],
                                                                             timestamp,
                                                                             process_config['orbit_number'])
                try:
                    shutil.move(process_config['amsub_file'],os.path.join(aapp_outdir_selected,new_name))
                except OSError as e:
                    LOG.error("Failed to rename {} to {}. {}".format(process_config['amsub_file'],os.path.join(aapp_outdir_selected,new_name),e))
                    LOG.error("Please check previous processing")
            else:
                LOG.error("Excpected file {} does not exists. Please check previous processing.".format(process_config['amsub_file']))
    
            if os.path.exists('ambn.l1c'):
                new_name = "{0:}_{1:}_{2:%Y%m%d}_{2:%H%M}_{3:5d}.l1c".format("amsubl1c",
                                                                             process_config['platform'],
                                                                             timestamp,
                                                                             process_config['orbit_number'])
                try:
                    shutil.move('ambn.l1c',os.path.join(aapp_outdir_selected,new_name)) 
                except OSError as e:
                    LOG.error("Failed to rename {} to {}. {}".format('ambn.l1c',os.path.join(aapp_outdir_selected,new_name),e))
                    LOG.error("Please check previous processing")
            else:
                LOG.error("Excpected file {} does not exists. Please check previous processing.".format('ambn.l1c'))
        else: #Assume metop and noaa > 17
            if os.path.exists(process_config['amsub_file']):
                new_name = "{0:}_{1:}_{2:%Y%m%d}_{2:%H%M}_{3:5d}.l1b".format("mhsl1b",
                                                                             process_config['platform'],
                                                                             timestamp,
                                                                             process_config['orbit_number'])
                try:
                    shutil.move(process_config['amsub_file'],os.path.join(aapp_outdir_selected,new_name))
                except OSError as e:
                    LOG.error("Failed to rename {} to {}. {}".format(process_config['amsub_file'], os.path.join(aapp_outdir_selected,new_name),e))
                    LOG.error("Please check previous processing")
            else:
                LOG.error("Excpected file {} does not exists. Please check previous processing.".format(process_config['amsub_file']))
    
            if os.path.exists('ambn.l1c'):
                new_name = "{0:}_{1:}_{2:%Y%m%d}_{2:%H%M}_{3:5d}.l1c".format("mhsl1c",
                                                                             process_config['platform'],
                                                                             timestamp,
                                                                             process_config['orbit_number'])
                try:
                    shutil.move('ambn.l1c',os.path.join(aapp_outdir_selected,new_name)) 
                except OSError as e:
                    LOG.error("Failed to rename {0:} to {1:}. {2:}".format('ambn.l1c', os.path.join(aapp_outdir_selected,new_name),e))
                    LOG.error("Please check previous processing")
            else:
                LOG.error("Excpected file {} does not exists. Please check previous processing.".format('ambn.l1c'))
            
        if process_config['process_msu']:
            if os.path.exists(process_config['msu_file']):
                new_name = "{0:}_{1:}_{2:%Y%m%d}_{2:%H%M}_{3:5d}.l1b".format("msul1b",
                                                                             process_config['platform'],
                                                                             timestamp,
                                                                             process_config['orbit_number'])
                try:
                    shutil.move(process_config['msu_file'], os.path.join(aapp_outdir_selected,new_name)) 
                except OSError as e:
                    LOG.error("Failed to rename {0:} to {1:}. {2:}".format(process_config['msu_file'], os.path.join(aapp_outdir_selected,new_name),e)) 
                    LOG.error("Please check previous processing")
            else:
                LOG.error("Excpected file {} does not exists. Please check previous processing.".format(process_config['msu_file']))
    
            if os.path.exists('msun.l1c'):
                new_name = "{0:}_{1:}_{2:%Y%m%d}_{2:%H%M}_{3:5d}.l1c".format("msul1c",
                                                                             process_config['platform'],
                                                                             timestamp,
                                                                             process_config['orbit_number'])
                try:
                    shutil.move('msun.l1c', os.path.join(aapp_outdir_selected,new_name))
                except OSError as e:
                    LOG.error("Failed to rename {0:} to {1:}. {2:}".format('msun.l1c',os.path.join(aapp_outdir_selected,new_name),e))  
                    LOG.error("Please check previous processing")
            else:
                LOG.error("Excpected file {} does not exists. Please check previous processing.".format('msun.l1c'))

        if process_config['process_dcs']:
            if os.path.exists(process_config['dcs_file']):
                new_name = "{0:}_{1:}_{2:%Y%m%d}_{2:%H%M}_{3:5d}.l1b".format("dcsl1b",
                                                                             process_config['platform'],
                                                                             timestamp,
                                                                             process_config['orbit_number'])
                try:
                    shutil.move(process_config['dcs_file'], os.path.join(aapp_outdir_selected,new_name))
                except OSError in e:
                    LOG.error("Failed to rename {0:} to {1:}. {2:}".format(process_config['dcs_file'], os.path.join(aapp_outdir_selected,new_name),e))
                    LOG.error("Please check previous processing")
            else:
                LOG.error("Excpected file {} does not exists. Please check previous processing.".format(process_config['dcs_file']))

    #Change back after this is done
    os.chdir(current_dir)

    LOG.info("Rename aapp files complete!")

    return True