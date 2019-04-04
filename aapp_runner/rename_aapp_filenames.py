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
import shutil
from trollsift.parser import compose

LOG = logging.getLogger(__name__)

def rename_file(process_config, values):
    """
    Do the actual renaming and checking
    """
    sensor = next(iter(values))
    process_file = "process_{}".format(sensor)
    try:
        process_config[process_file]
    except KeyError as err:
        LOG.error("No such key: {}".format(err))
        return False
    
    #Need to build a dict to use in trollsift compose
    tmp_process_config = process_config.config.copy()
    tmp_process_config.update(tmp_process_config['aapp_processes'][process_config.process_name])
    #for key, value in tmp_process_config['aapp_processes'][process_config.process_name].iteritems():
        #print key,value
    #    tmp_process_config[key] = value

    #tmp_process_config.update(tmp_process_config['aapp_processes'][process_config.process_name]['rename_aapp_files'])
    tmp_process_config['data_type'] = values[sensor]['data_type']
    tmp_process_config['data_level'] = values[sensor]['data_level']
    
    #for key, value in tmp_process_config['aapp_processes'][process_config.process_name]['rename_aapp_files'].iteritems():
    #    #print key,value
        
    new_name = ""
    
    if process_config[process_file]:
        if os.path.exists(values[sensor]['aapp_file']):
            try:
                _outdir = compose(tmp_process_config['aapp_outdir_format'],tmp_process_config)
                dir = os.path.join(tmp_process_config['aapp_outdir_base'], _outdir)
                _new_name = compose(process_config['aapp_processes'][process_config.process_name]['rename_aapp_compose'], tmp_process_config)
                new_name = os.path.join(dir,_new_name)
            except KeyError as err:
                LOG.error("Failed to compose new filename: {}. Missing key:{}".format(process_config['aapp_processes'][process_config.process_name]['rename_aapp_compose'],err))
                return False
            except Exception as err:
                LOG.error("Failed to compose new filename: {}. Error:{}".format(process_config['aapp_processes'][process_config.process_name]['rename_aapp_compose'],err))
                return False

            try:
                 
                if not os.path.exists(os.path.dirname(new_name)):
                    LOG.debug("Need to create directory: {}".format(os.path.dirname(new_name)))
                    os.makedirs(os.path.dirname(new_name))
            except OSError as oe:
                LOG.error("Could not create directory: {} with {}".format(os.path.dirname(new_name),oe))
                return False
   
            try:
                #shutil.move(process_config['aapp_static_configuration']['decommutation_files'][inputfile],new_name)
                shutil.move(values[sensor]['aapp_file'], new_name)
                #LOG.debug("Renamed: {} to {}".format(process_config['aapp_static_configuration']['decommutation_files'][inputfile], new_name))
                LOG.debug("Renamed: {} to {}".format(values[sensor]['aapp_file'], new_name))
            except OSError as e:
                LOG.error("Failed to rename {} to {}. {}".format(process_config[inputfile],new_name,e))
                LOG.error("Please check previous processing")
                return False
        else:
            LOG.error("Excpected file {} does not exists. Please check previous processing.".format(values[sensor]['aapp_file']))
            return False
    else:
        return False
    
    _tmp = {}
    _tmp['file'] = new_name
    _tmp['sensor'] = sensor
    _tmp['level'] = tmp_process_config['data_level']

    return _tmp

def rename_aapp_filenames(process_config):
    LOG.debug("Rename AAPP filenames ... ")

    #This function relays on beeing in a working directory
    current_dir = os.getcwd() #Store the dir to change back to after function complete
    os.chdir(process_config['aapp_processes'][process_config.process_name]['working_dir'])

    files = []
    for values in process_config['aapp_processes'][process_config.process_name]['rename_aapp_files']:
        #print values
        #for data_type, data_level in value.iteritems():
        #    print "INNER: ",data_type, data_level
        #    process_instrument = "process_{}".format(instrument)
        #    process_file = "{}_file".format(instrument)
            
        file = rename_file(process_config, values)
            
        if file:
            files.append(file)

            
    #Change back after this is done
    os.chdir(current_dir)

    if len(files) > 0:
        LOG.info("Renamed aapp files complete into: {}!".format(os.path.dirname(files[0]['file'])))
    return files
