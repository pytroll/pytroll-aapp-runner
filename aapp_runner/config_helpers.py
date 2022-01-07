#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021 Pytroll Community

# Author(s):

#   Adam.Dybbroe <adam.dybbroe@smhi.se>

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

"""Helper functions for the processing configuration."""

import logging
from urllib.parse import urlparse

from aapp_runner.aapp_runner_tools import set_collection_area_id

LOG = logging.getLogger(__name__)


def update_process_config_sensors_and_filenames_dataset(config, msg_data):
    """Update the processing config parameters regarding sensors and filenames - input is a dataset."""
    LOG.debug("Checking dataset")
    for sensor, sensor_filename in zip(msg_data['sensor'], msg_data['dataset']):
        LOG.debug("{} {}".format(sensor, sensor_filename['uri']))
        process_name = "process_{}".format(
            config['aapp_static_configuration']['sensor_name_converter'].get(sensor, sensor))
        config[process_name] = True

        # Name of the input file for given instrument
        input_file_name = "input_{}_file".format(
            config['aapp_static_configuration']['sensor_name_converter'].get(sensor, sensor))
        # print urlparse(sensor_filename['uri']).path
        config[input_file_name] = urlparse(sensor_filename['uri']).path


def update_process_config_sensors_and_filenames_uri(config, msg_data):
    """Update the processing config parameters regarding sensors and filenames - input is an uri."""
    LOG.debug("Checking uri")
    # Need to force list
    if type(msg_data["sensor"]) not in (tuple, list, set):
        msg_data["sensor"] = [msg_data["sensor"]]

    LOG.debug("sensor: {}".format(msg_data["sensor"]))

    for sensor in msg_data['sensor']:
        process_name = "process_{}".format(
            config['aapp_static_configuration']['sensor_name_converter'].get(sensor, sensor))
        LOG.debug("{} {}".format(sensor, process_name))
        config[process_name] = True

        # For POES 18 and 19 and the METOPs there are MHS. but no AMSU-B.
        # AAPP processing handles MHS as AMSU-B
        if (('NOAA' in msg_data['platform_name'].upper() and int(msg_data['platform_name'][-2:]) >= 18) or
                ('METOP' in msg_data['platform_name'].upper())) and config['process_mhs']:
            config['process_amsub'] = True

        # Name of the input file for given instrument
        # Needed for METOP processing
        input_file_name = "input_{}_file".format(
            config['aapp_static_configuration']['sensor_name_converter'].get(sensor, sensor))
        config[input_file_name] = urlparse(msg_data['uri']).path

    # Needed for POES processing
    config['input_hrpt_file'] = urlparse(msg_data['uri']).path


def generate_process_config(msg, config):
    """Check sensors to process and setup config for this.

    Need to check if it is a collection or file message. Then get sensor information from this.
    """
    # All possible instruments to process initialized to false.
    config['process_amsua'] = False
    config['process_amsub'] = False
    config['process_hirs'] = False
    config['process_avhrr'] = False
    config['process_msu'] = False
    config['process_mhs'] = False
    config['process_dcs'] = False

    # Check sensors and file as given in the incomming message
    # Note: zip iterates two list at the same time
    if 'dataset' in msg.data:
        update_process_config_sensors_and_filenames_dataset(config, msg.data)
    elif 'uri' in msg.data:
        update_process_config_sensors_and_filenames_uri(config, msg.data)
    else:
        LOG.error(
            "Could not find needed dataset or uri in message. Can not handle.")
        return False

    # Be sure to set MHS process to False for NOAA15 as there is no MHS, but amsu-b
    platform_name = msg.data['platform_name'].upper()
    is_noaa15 = platform_name.startswith('NOAA') and platform_name.endswith('15')
    if is_noaa15:
        config['process_mhs'] = False

    # Check if processing for this platform should be altered
    # due to config.
    if 'instrument_skipped_in_processing' in config['aapp_processes'][config.process_name]:
        for platform_name in config['aapp_processes'][config.process_name]['instrument_skipped_in_processing']:
            _platform_name = list(platform_name)[0]
            if _platform_name.upper() == msg.data['platform_name'].upper():
                for sensor in msg.data['sensor']:
                    for skip_sensor in platform_name[_platform_name]:
                        if skip_sensor == sensor:
                            process_name = "process_{}".format(
                                config['aapp_static_configuration']['sensor_name_converter'].get(sensor, sensor))
                            if config[process_name]:
                                LOG.debug("Skipping processing of sensor: {} as of config.".format(skip_sensor))
                                config[process_name] = False

    config['calibration_location'] = "-c -l"
    config['a_tovs'] = list("ATOVS")

    if 'keep_orbit_number_from_message' in config['aapp_processes'][config.process_name] and 'orbit_number' in msg.data:
        config['orbit_number'] = int(msg.data['orbit_number'])
    else:
        # Check the case of no orbit number in message, typically EARS stream
        start_orbnum = None
        try:
            import pyorbital.orbital as orb
            LOG.debug("platform_name: {}".format(msg.data['platform_name']))
            sat = orb.Orbital(config['aapp_static_configuration']['tle_platform_name_aliases'].get(
                msg.data['platform_name'], msg.data['platform_name']))
            start_orbnum = sat.get_orbit_number(msg.data['start_time'])
        except ImportError:
            LOG.warning("Failed importing pyorbital, " +
                        "cannot calculate orbit number")
        except AttributeError:
            LOG.warning("Failed calculating orbit number using pyorbital")
            LOG.warning("platform name in msg and config = " +
                        str(config['aapp_static_configuration'][
                            'tle_platform_name_aliases'].get(msg.data['platform_name'],
                                                             msg.data['platform_name'])) +
                        " " + str(config['platform_name']))
        LOG.info(
            "Orbit number determined from pyorbital = " + str(start_orbnum))
        try:
            config['orbit_number'] = int(msg.data['orbit_number'])
        except KeyError:
            LOG.warning("No orbit_number in message! Set to none...")
            config['orbit_number'] = None

        if start_orbnum and config['orbit_number'] != start_orbnum:
            LOG.warning("Correcting orbit number: Orbit now = " +
                        str(start_orbnum) + " Before = " + str(config['orbit_number']))
            config['orbit_number'] = start_orbnum
        else:
            LOG.debug("Orbit number in message determined"
                      "to be okay and not changed...")
            config['orbit_number'] = int(msg.data['orbit_number'])

    # How to give the platform name?
    # Which format?
    # Used are for Metop:
    # Metop-A/'Metop A'/'METOP A'
    # M02
    # metop02
    # Throughout this processing the last convention is used!
    if msg.data['platform_name'] in config['aapp_static_configuration']['platform_name_aliases']:
        config['platform_name'] = config['aapp_static_configuration'][
            'platform_name_aliases'][msg.data['platform_name']]
        # print config['platform_name']
        # TODO Should not use satellite_name

        config['satellite_name'] = config['platform_name']
    else:
        LOG.error("Failed to replace platform_name: {}. Can not continue.".format(
            msg.data['platform_name']))
        return False

    config['start_time'] = msg.data['start_time']
    set_collection_area_id(msg.data, config)

    return True
