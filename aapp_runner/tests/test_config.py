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

"""Unit tests for reading and manipulating configuration parameters
"""

import unittest
from unittest.mock import patch

import pytest
import yaml
from posttroll.message import Message

from aapp_runner.aapp_runner_tools import set_collection_area_id
from aapp_runner.config_helpers import generate_process_config
from aapp_runner.read_aapp_config import (VALID_CONFIGURATION, AappL1Config,
                                          AappProcessKeyMissing,
                                          AappRunnerConfig, AappWorkDirNotSet,
                                          ConfigFileOptionsError,
                                          EnvironmentError, StaticConfigError,
                                          StationError,
                                          check_config_file_options,
                                          check_dir_permissions)

EARS_MESSAGE_INPUT = """pytroll://HRPT/0/NOAA-19/ collection safusr.t@lxserv2338.smhi.se 2021-11-19T16:52:06.089639 v1.01 application/json {"sensor": ["avhrr/3", "mhs", "amsu-a", "amsu-b", "hirs/4"], "format": "HRPT", "data_processing_level": "0", "variant": "EARS", "platform_name": "NOAA-19", "start_time": "2021-11-19T16:34:00", "origin": "172.18.0.249:9108", "end_time": "2021-11-19T16:46:00", "collection_area_id": "euron1", "collection": [{"start_time": "2021-11-19T16:34:00", "end_time": "2021-11-19T16:35:00", "uri": "ssh://lxserv2338.smhi.se/san1/polar_in/regional/avhrr/lvl0/avhrr_20211119_163400_noaa19.hrp", "uid": "avhrr_20211119_163400_noaa19.hrp"}, {"start_time": "2021-11-19T16:35:00", "end_time": "2021-11-19T16:36:00", "uri": "ssh://lxserv2338.smhi.se/san1/polar_in/regional/avhrr/lvl0/avhrr_20211119_163500_noaa19.hrp", "uid": "avhrr_20211119_163500_noaa19.hrp"}, {"start_time": "2021-11-19T16:36:00", "end_time": "2021-11-19T16:37:00", "uri": "ssh://lxserv2338.smhi.se/san1/polar_in/regional/avhrr/lvl0/avhrr_20211119_163600_noaa19.hrp", "uid": "avhrr_20211119_163600_noaa19.hrp"}, {"start_time": "2021-11-19T16:37:00", "end_time": "2021-11-19T16:38:00", "uri": "ssh://lxserv2338.smhi.se/san1/polar_in/regional/avhrr/lvl0/avhrr_20211119_163700_noaa19.hrp", "uid": "avhrr_20211119_163700_noaa19.hrp"}, {"start_time": "2021-11-19T16:38:00", "end_time": "2021-11-19T16:39:00", "uri": "ssh://lxserv2338.smhi.se/san1/polar_in/regional/avhrr/lvl0/avhrr_20211119_163800_noaa19.hrp", "uid": "avhrr_20211119_163800_noaa19.hrp"}, {"start_time": "2021-11-19T16:39:00", "end_time": "2021-11-19T16:40:00", "uri": "ssh://lxserv2338.smhi.se/san1/polar_in/regional/avhrr/lvl0/avhrr_20211119_163900_noaa19.hrp", "uid": "avhrr_20211119_163900_noaa19.hrp"}, {"start_time": "2021-11-19T16:40:00", "end_time": "2021-11-19T16:41:00", "uri": "ssh://lxserv2338.smhi.se/san1/polar_in/regional/avhrr/lvl0/avhrr_20211119_164000_noaa19.hrp", "uid": "avhrr_20211119_164000_noaa19.hrp"}, {"start_time": "2021-11-19T16:41:00", "end_time": "2021-11-19T16:42:00", "uri": "ssh://lxserv2338.smhi.se/san1/polar_in/regional/avhrr/lvl0/avhrr_20211119_164100_noaa19.hrp", "uid": "avhrr_20211119_164100_noaa19.hrp"}, {"start_time": "2021-11-19T16:42:00", "end_time": "2021-11-19T16:43:00", "uri": "ssh://lxserv2338.smhi.se/san1/polar_in/regional/avhrr/lvl0/avhrr_20211119_164200_noaa19.hrp", "uid": "avhrr_20211119_164200_noaa19.hrp"}, {"start_time": "2021-11-19T16:43:00", "end_time": "2021-11-19T16:44:00", "uri": "ssh://lxserv2338.smhi.se/san1/polar_in/regional/avhrr/lvl0/avhrr_20211119_164300_noaa19.hrp", "uid": "avhrr_20211119_164300_noaa19.hrp"}, {"start_time": "2021-11-19T16:44:00", "end_time": "2021-11-19T16:45:00", "uri": "ssh://lxserv2338.smhi.se/san1/polar_in/regional/avhrr/lvl0/avhrr_20211119_164400_noaa19.hrp", "uid": "avhrr_20211119_164400_noaa19.hrp"}, {"start_time": "2021-11-19T16:45:00", "end_time": "2021-11-19T16:46:00", "uri": "ssh://lxserv2338.smhi.se/san1/polar_in/regional/avhrr/lvl0/avhrr_20211119_164500_noaa19.hrp", "uid": "avhrr_20211119_164500_noaa19.hrp"}]}"""

DR_MESSAGE_INPUT = """pytroll://HRPT/0/nkp/dev/polar/direct_readout file safusr.t@lxserv2338.smhi.se 2021-11-19T17:19:30.973158 v1.01 application/json {"start_time": "2021-11-19T17:06:44", "end_time": "2021-11-19T17:19:26", "orbit_number": 85045, "platform_name": "NOAA-18", "type": "binary", "format": "HRPT", "sensor": ["avhrr/3", "mhs", "amsu-a", "hirs/4"], "data_processing_level": "0", "uid": "20211119170644_NOAA_18.hmf", "uri": "ssh://172.29.4.28/san1/polar_in/direct_readout/hrpt/lvl0/20211119170644_NOAA_18.hmf", "variant": "DR"}"""


TEST_YAML_CONTENT_OK = """
logging:
  log_rotation_days: 1
  log_rotation_backup: 30
  logging_mode: DEBUG

aapp_static_configuration:
  decommutation_files:
    hirs_file: hrsn.l1b
    amsua_file: aman.l1b
    amsub_file: ambn.l1b
    mhs_file: ambn.l1b
    avhrr_file: hrpt.l1b
    msu_file: msun.l1b

  supported_noaa_satellites:
    - NOAA-18
    - NOAA-19

  supported_metop_satellites:
    - Metop-C
    - Metop-B
    - Metop-A

  platform_name_aliases:
    NOAA-19: noaa19
    NOAA-18: noaa18
    Metop-A: metop02
    Metop-B: metop01
    Metop-C: metop03
    METOP-A: metop02
    METOP-B: metop01
    METOP-C: metop03
    'METOP A': metop02
    'METOP B': metop01
    'METOP C': metop03
    M01: metop01
    M02: metop02
    M03: metop03

  satellite_sensor_name_aliases:
    'amsua': 'amsu-a'
    'amsub': 'amsu-b'
    'hirs': 'hirs/4'
    'mhs': 'mhs'
    'avhrr': 'avhrr/3'

  tle_platform_name_aliases:
    'NOAA-19': 'NOAA 19'
    'noaa19': 'NOAA 19'
    'NOAA-18': 'NOAA 18'
    'Metop-A': 'METOP-A'
    'Metop-B': 'METOP-B'
    'Metop-C': 'METOP-C'

  sensor_name_converter:
    'amsua': 'amsu-a'
    'amsub': 'amsu-b'
    'hirs': 'hirs/4'
    'mhs': 'mhs'
    'avhrr': 'avhrr/3'
    'amsu-a': 'amsua'
    'amsu-b': 'amsub'
    'hirs/4': 'hirs'
    'hirs/3': 'hirs'
    'avhrr/3': 'avhrr'

aapp_processes:
  xl-band:
    description: 'Text describing this processing config'
    name: xl-band
    subscribe_topics:
      - /XLBANDANTENNA/HRPT/L0
      - /XLBANDANTENNA/METOP/L0

    collection_area_id: euron1

    tle_indir: /disk2/AAPP/orbelems
    tle_archive_dir: '{tle_indir:s}/tle-archive/{timestamp:%Y%m}'
    tle_infile_format: 'tle{timestamp:%y%m%d}.txt'
    download_tle_files: False
    tle_download:
      - {url: 'http://www.celestrak.com/NORAD/elements/weather.txt'}
      - {url: 'http://oiswww.eumetsat.org/metopTLEs/html/data_out/latest_m01_tle.txt'}

    tle_file_to_data_diff_limit_days: 3

    locktime_before_rerun: 10

    publish_sift_format: '/{format:s}/{data_processing_level:s}/polar/direct_readout'

    keep_orbit_number_from_message: True

    aapp_prefix: /disk2/AAPP
    aapp_environment_file: ATOVS_ENV8
    aapp_workdir: /run/shm/aapp-workdir
    working_dir: <path to working dir>

    use_dyn_work_dir: True

    aapp_outdir_base: /disk2/aapp-runner-data
    aapp_outdir_format: '{satellite_name:s}_{start_time:%Y%m%d}_{start_time:%H%M}_{orbit_number:05d}'

    passlength_threshold: 5

    aapp_log_files_archive_dir: /disk2/aapp-runner-log
    aapp_log_outdir_format: '{satellite_name:s}_{start_time:%Y%m%d}_{start_time:%H%M}_{orbit_number:05d}'
    aapp_log_files_archive_length: 1

    message_providing_server: satproc2.met.no

    do_ana_correction: True
    do_atovpp: True
    do_avh2hirs: True

    instrument_skipped_in_processing:
      - NOAA-15:
        - hirs/3
        - amsu-a
        - amsu-b
      - METOP-C:
        - hirs/4

    rename_aapp_compose: '{data_type:s}_{satellite_name:s}_{start_time:%Y%m%d}_{start_time:%H%M}_{orbit_number:5d}.{data_level:s}'
    rename_aapp_files:
      - avhrr: {aapp_file: hrpt.l1b, data_type: hrpt, data_level: l1b}
      - hirs: {aapp_file: hrsn.l1b, data_type: hirsl1b, data_level: l1b}
      - hirs: {aapp_file: hrsn.l1c, data_type: hirsl1c, data_level: l1c}
      - hirs: {aapp_file: hirs.l1d, data_type: hirsl1d, data_level: l1d}
      - amsua: {aapp_file: aman.l1b, data_type: amsual1b, data_level: l1b}
      - amsub: {aapp_file: ambn.l1b, data_type: amsubl1b, data_level: l1b}
      - mhs: {aapp_file: ambn.l1b, data_type: mhsl1b, data_level: l1b}

    monitor_message:
      send: True
      topic: 'some fantastic topic'
"""


TEST_YAML_CONTENT_MANDATORY = """
aapp_processes:
  xl-band:
    description: 'Text describing this processing config'
    name: xl-band
    subscribe_topics:
      - /XLBANDANTENNA/HRPT/L0
      - /XLBANDANTENNA/METOP/L0

    publish_sift_format: '/{format:s}/{data_processing_level:s}/polar/direct_readout'

    aapp_prefix: /disk2/AAPP
    aapp_environment_file: ATOVS_ENV8

    aapp_outdir_base: /disk2/aapp-runner-data
    aapp_outdir_format: '{satellite_name:s}_{start_time:%Y%m%d}_{start_time:%H%M}_{orbit_number:05d}'

    aapp_log_files_archive_dir: /disk2/aapp-runner-log
    aapp_log_outdir_format: '{satellite_name:s}_{start_time:%Y%m%d}_{start_time:%H%M}_{orbit_number:05d}'
    aapp_log_files_archive_length: 1

    rename_aapp_compose: '{data_type:s}_{satellite_name:s}_{start_time:%Y%m%d}_{start_time:%H%M}_{orbit_number:5d}.{data_level:s}'
    rename_aapp_files:
      - avhrr: {aapp_file: hrpt.l1b, data_type: hrpt, data_level: l1b}

    aapp_workdir: /tmp/myworkdir/

"""


class DummyAappRunnerConfig(object):

    """
    Dummy container for the run configuration for AAPP
    """

    def __init__(self, config, process_name):
        """
        Init the config
        """
        self.config = config
        self.job_register = {}
        self.process_name = process_name

    def __getitem__(self, key):
        try:
            _it = self.config[key]
        except KeyError:
            _it = None
        return _it

    def __setitem__(self, key, value):
        self.config[key] = value


def create_config_from_yaml(yaml_content_str):
    """Create aapp-runner config dict from a yaml file."""
    return yaml.load(yaml_content_str, Loader=yaml.FullLoader)


class TestCheckConfig(unittest.TestCase):
    """Test functions checking the validity of the conguration"""

    def setUp(self):
        self.configuration = {'station': 'norrkoping',
                              'environment': 'xl-band',
                              'description': 'Text describing this processing config',
                              'name': 'xl-band',
                              'aapp_prefix': '/disk2/AAPP',
                              'aapp_environment_file': 'ATOVS_ENV8',
                              'aapp_outdir_base': '/disk2/aapp-runner-data',
                              'aapp_outdir_format': '{satellite_name:s}_{start_time:%Y%m%d}_{start_time:%H%M}_{orbit_number:05d}',
                              'subscribe_topics': ['/XLBANDANTENNA/HRPT/L0',
                                                   '/XLBANDANTENNA/METOP/L0'],
                              'publish_sift_format': '/{format:s}/{data_processing_level:s}/polar/direct_readout',
                              'aapp_log_files_archive_dir': '/disk2/aapp-runner-log',
                              'aapp_log_files_archive_length': 1,
                              'aapp_log_outdir_format': '{satellite_name:s}_{start_time:%Y%m%d}_{start_time:%H%M}_{orbit_number:05d}',
                              'rename_aapp_compose': '{data_type:s}_{satellite_name:s}_{start_time:%Y%m%d}_{start_time:%H%M}_{orbit_number:5d}.{data_level:s}',
                              'rename_aapp_files': [{'avhrr': {'aapp_file': 'hrpt.l1b',
                                                               'data_type': 'hrpt',
                                                               'data_level': 'l1b'}}, ],
                              'tle_indir': '/disk2/AAPP/orbelems',
                              'tle_archive_dir': '{tle_indir:s}/tle-archive/{timestamp:%Y%m}',
                              'tle_infile_format': 'tle{timestamp:%y%m%d}.txt',
                              'download_tle_files': False,
                              'tle_file_to_data_diff_limit_days': 3,
                              'locktime_before_rerun': 10,
                              'keep_orbit_number_from_message': True,
                              'aapp_workdir': '/run/shm/aapp-workdir',
                              'working_dir': '<path to working dir>',
                              'use_dyn_work_dir': True,
                              'passlength_threshold': 5,
                              'message_providing_server': 'satproc2.met.no',
                              'do_ana_correction': True,
                              'do_atovpp': True,
                              'do_avh2hirs': True,
                              'instrument_skipped_in_processing': [{'NOAA-15': ['hirs/3', 'amsu-a', 'amsu-b']},
                                                                   {'METOP-C': ['hirs/4']}],
                              'monitor_message': {'send': True, 'topic': 'some fantastic topic'}}

        self.valid_config = VALID_CONFIGURATION

        self.valid_dir_permissions = [
            ('aapp_prefix', 'r', 'm'),
            ('aapp_outdir_base', 'rw', 'm'),
            ('aapp_log_files_archive_dir', 'rw', 'm')]
        self.valid_dir_permissions_not_mandatory = [
            ('aapp_prefix', 'r', 'n'),
            ('aapp_outdir_base', 'rw', 'n'),
            ('aapp_log_files_archive_dir', 'rw', 'n')]

    def test_check_config_file_options(self):
        """Test the function to check config file options"""

        with patch('aapp_runner.read_aapp_config.check_dir_permissions', return_value=True):
            result = check_config_file_options(self.configuration, self.valid_config)

        self.assertTrue(result)
        with patch('aapp_runner.read_aapp_config.check_dir_permissions', return_value=False):
            result = check_config_file_options(self.configuration, self.valid_config)

        self.assertFalse(result)

    def test_check_dir_permissions(self):
        """Test the function to check dir permissions"""

        with patch('aapp_runner.read_aapp_config.check_dir', return_value=True) as mypatch:
            result = check_dir_permissions(self.configuration, self.valid_dir_permissions)

        self.assertEqual(mypatch.call_count, 3)
        assert result

        with patch('aapp_runner.read_aapp_config.check_dir', return_value=False):
            result = check_dir_permissions(self.configuration, self.valid_dir_permissions)

        self.assertFalse(result)

        with patch('aapp_runner.read_aapp_config.check_dir', return_value=True) as mypatch:
            result = check_dir_permissions(self.configuration, self.valid_dir_permissions_not_mandatory)

        mypatch.assert_not_called()


class TestProcessConfig(unittest.TestCase):
    """Test setting the processing config."""

    def setUp(self):
        self.config_complete = create_config_from_yaml(TEST_YAML_CONTENT_OK)
        self.config_mandatory = create_config_from_yaml(TEST_YAML_CONTENT_MANDATORY)
        self.message1 = DR_MESSAGE_INPUT
        self.message2 = EARS_MESSAGE_INPUT

    def test_set_collection_area_id_present_in_config_but_not_in_message(self):
        """Test setting the collection area id."""

        msg = Message.decode(self.message1)
        config = self.config_complete
        runconfig = DummyAappRunnerConfig(config, 'xl-band')
        set_collection_area_id(msg.data, runconfig)
        assert config['collection_area_id'] == 'euron1'

    def test_set_collection_area_id_present_in_message_but_not_in_config(self):
        """Test setting the collection area id."""

        msg = Message.decode(self.message2)
        config = self.config_mandatory
        runconfig = DummyAappRunnerConfig(config, 'xl-band')
        set_collection_area_id(msg.data, runconfig)
        assert config['collection_area_id'] == 'euron1'

    def test_set_collection_area_id_not_in_message_and_not_in_config(self):
        """Test setting the collection area id."""

        msg = Message.decode(self.message1)
        config = self.config_mandatory
        runconfig = DummyAappRunnerConfig(config, 'xl-band')
        set_collection_area_id(msg.data, runconfig)
        assert config['collection_area_id'] is None


class TestGetConfig(unittest.TestCase):
    """Test getting the yaml config from file"""

    def setUp(self):
        self.config_complete = create_config_from_yaml(TEST_YAML_CONTENT_OK)
        self.config_mandatory = create_config_from_yaml(TEST_YAML_CONTENT_MANDATORY)

    @patch('aapp_runner.read_aapp_config.load_config_from_file')
    @patch('aapp_runner.read_aapp_config.check_static_configuration')
    @patch('aapp_runner.read_aapp_config.check_config_file_options')
    def test_read_config(self, file_options, static_config, config):
        """Test loading and initialising the yaml config"""
        config.return_value = self.config_complete
        static_config.return_value = True
        file_options.return_value = True

        myfilename = "/tmp/mytestfile"
        cfg_obj = AappRunnerConfig(myfilename, 'norrkoping', 'xl-band')
        cfg_obj.check_config()
        result = cfg_obj.config

        log_conf = result['logging']
        log_expected = {'log_rotation_days': 1, 'log_rotation_backup': 30, 'logging_mode': 'DEBUG'}
        self.assertDictEqual(log_expected, log_conf)

        aapp_static_conf = result['aapp_static_configuration']
        aapp_static_expected = {'decommutation_files': {'hirs_file': 'hrsn.l1b',
                                                        'amsua_file': 'aman.l1b',
                                                        'amsub_file': 'ambn.l1b',
                                                        'mhs_file': 'ambn.l1b',
                                                        'avhrr_file': 'hrpt.l1b',
                                                        'msu_file': 'msun.l1b'},
                                'supported_noaa_satellites': ['NOAA-18', 'NOAA-19'],
                                'supported_metop_satellites': ['Metop-C', 'Metop-B', 'Metop-A'],
                                'platform_name_aliases': {'NOAA-19': 'noaa19', 'NOAA-18': 'noaa18',
                                                          'Metop-A': 'metop02', 'Metop-B': 'metop01',
                                                          'Metop-C': 'metop03', 'METOP-A': 'metop02',
                                                          'METOP-B': 'metop01', 'METOP-C': 'metop03',
                                                          'METOP A': 'metop02', 'METOP B': 'metop01',
                                                          'METOP C': 'metop03', 'M01': 'metop01',
                                                          'M02': 'metop02', 'M03': 'metop03'},
                                'satellite_sensor_name_aliases': {'amsua': 'amsu-a',
                                                                  'amsub': 'amsu-b',
                                                                  'hirs': 'hirs/4',
                                                                  'mhs': 'mhs',
                                                                  'avhrr': 'avhrr/3'},
                                'tle_platform_name_aliases': {'NOAA-19': 'NOAA 19',
                                                              'noaa19': 'NOAA 19',
                                                              'NOAA-18': 'NOAA 18',
                                                              'Metop-A': 'METOP-A',
                                                              'Metop-B': 'METOP-B',
                                                              'Metop-C': 'METOP-C'},
                                'sensor_name_converter': {'amsua': 'amsu-a',
                                                          'amsub': 'amsu-b',
                                                          'hirs': 'hirs/4',
                                                          'mhs': 'mhs',
                                                          'avhrr': 'avhrr/3',
                                                          'amsu-a': 'amsua',
                                                          'amsu-b': 'amsub',
                                                          'hirs/4': 'hirs',
                                                          'hirs/3': 'hirs',
                                                          'avhrr/3': 'avhrr'}}
        self.assertDictEqual(aapp_static_expected, aapp_static_conf)

        aapp_proc_config = result['aapp_processes']
        aapp_proc_expected = {'xl-band': {'description': 'Text describing this processing config',
                                          'name': 'xl-band',
                                          'subscribe_topics': ['/XLBANDANTENNA/HRPT/L0', '/XLBANDANTENNA/METOP/L0'],
                                          'tle_indir': '/disk2/AAPP/orbelems',
                                          'tle_archive_dir': '{tle_indir:s}/tle-archive/{timestamp:%Y%m}',
                                          'tle_infile_format': 'tle{timestamp:%y%m%d}.txt',
                                          'download_tle_files': False,
                                          'tle_download': [{'url': 'http://www.celestrak.com/NORAD/elements/weather.txt'},
                                                           {'url': 'http://oiswww.eumetsat.org/metopTLEs/html/data_out/latest_m01_tle.txt'}],
                                          'tle_file_to_data_diff_limit_days': 3,
                                          'locktime_before_rerun': 10,
                                          'collection_area_id': 'euron1',
                                          'publish_sift_format': '/{format:s}/{data_processing_level:s}/polar/direct_readout',
                                          'keep_orbit_number_from_message': True,
                                          'aapp_prefix': '/disk2/AAPP',
                                          'aapp_environment_file': 'ATOVS_ENV8',
                                          'aapp_workdir': '/run/shm/aapp-workdir',
                                          'working_dir': '<path to working dir>',
                                          'use_dyn_work_dir': True,
                                          'aapp_outdir_base': '/disk2/aapp-runner-data',
                                          'aapp_outdir_format': '{satellite_name:s}_{start_time:%Y%m%d}_{start_time:%H%M}_{orbit_number:05d}',
                                          'passlength_threshold': 5,
                                          'aapp_log_files_archive_dir': '/disk2/aapp-runner-log',
                                          'aapp_log_outdir_format': '{satellite_name:s}_{start_time:%Y%m%d}_{start_time:%H%M}_{orbit_number:05d}',
                                          'aapp_log_files_archive_length': 1,
                                          'message_providing_server': 'satproc2.met.no',
                                          'do_ana_correction': True,
                                          'do_atovpp': True,
                                          'do_avh2hirs': True,
                                          'instrument_skipped_in_processing': [{'NOAA-15': ['hirs/3', 'amsu-a', 'amsu-b']},
                                                                               {'METOP-C': ['hirs/4']}],
                                          'rename_aapp_compose': '{data_type:s}_{satellite_name:s}_{start_time:%Y%m%d}_{start_time:%H%M}_{orbit_number:5d}.{data_level:s}',
                                          'rename_aapp_files': [{'avhrr': {'aapp_file': 'hrpt.l1b', 'data_type': 'hrpt', 'data_level': 'l1b'}},
                                                                {'hirs': {'aapp_file': 'hrsn.l1b',
                                                                          'data_type': 'hirsl1b', 'data_level': 'l1b'}},
                                                                {'hirs': {'aapp_file': 'hrsn.l1c',
                                                                          'data_type': 'hirsl1c', 'data_level': 'l1c'}},
                                                                {'hirs': {'aapp_file': 'hirs.l1d',
                                                                          'data_type': 'hirsl1d', 'data_level': 'l1d'}},
                                                                {'amsua': {'aapp_file': 'aman.l1b',
                                                                           'data_type': 'amsual1b', 'data_level': 'l1b'}},
                                                                {'amsub': {'aapp_file': 'ambn.l1b',
                                                                           'data_type': 'amsubl1b', 'data_level': 'l1b'}},
                                                                {'mhs': {'aapp_file': 'ambn.l1b',
                                                                         'data_type': 'mhsl1b', 'data_level': 'l1b'}},
                                                                ],
                                          'monitor_message': {'send': True, 'topic': 'some fantastic topic'}}}

        self.assertDictEqual(aapp_proc_config, aapp_proc_expected)
        self.assertEqual(result['station'], 'norrkoping')
        self.assertEqual(result['environment'], 'xl-band')

    @patch('aapp_runner.read_aapp_config.load_config_from_file')
    @patch('aapp_runner.read_aapp_config.check_static_configuration')
    @patch('aapp_runner.read_aapp_config.check_config_file_options')
    def test_config_env_mismatch(self, file_options, static_config, config):
        """Test something with the yaml config..."""
        config.return_value = self.config_complete
        static_config.return_value = True
        file_options.return_value = True

        myfilename = "/tmp/mytestfile"

        with pytest.raises(EnvironmentError) as exec_info:
            cfg_obj = AappRunnerConfig(myfilename, 'norrkoping', 'unknown')

        exception_raised = exec_info.value
        self.assertEqual(str(exception_raised), "Environment unknown not configured in config. Please check.")

    def test_config_station(self):
        """Test something with the yaml config..."""

        myfilename = "/tmp/mytestfile"
        with patch('aapp_runner.read_aapp_config.load_config_from_file', return_value=self.config_complete):
            cfg_obj = AappRunnerConfig(myfilename, 'norrkoping', 'xl-band')
            result = cfg_obj.config

        self.assertEqual(result['station'], 'norrkoping')

        myconfig = self.config_complete.copy()
        with patch('aapp_runner.read_aapp_config.load_config_from_file', return_value=myconfig):
            with pytest.raises(StationError) as exec_info:
                cfg_obj = AappRunnerConfig(myfilename, 'dundee', 'xl-band')

        exception_raised = exec_info.value
        self.assertEqual(str(exception_raised),
                         "Station from command line: dundee does not match with configured station: norrkoping")

        del myconfig['station']
        with patch('aapp_runner.read_aapp_config.load_config_from_file', return_value=myconfig):
            cfg_obj = AappRunnerConfig(myfilename, 'dundee', 'xl-band')
            result = cfg_obj.config

        self.assertEqual(result['station'], 'dundee')

    @patch('aapp_runner.read_aapp_config.load_config_from_file')
    def test_config_aapp_processes(self, config):
        """Test something with the yaml config..."""
        myconfig = self.config_complete.copy()
        del myconfig['aapp_processes']
        config.return_value = myconfig

        myfilename = "/tmp/mytestfile"
        with pytest.raises(AappProcessKeyMissing) as exec_info:
            cfg_obj = AappRunnerConfig(myfilename, 'norrkoping', 'xl-band')

        exception_raised = exec_info.value
        self.assertEqual(str(exception_raised),
                         "Can not find main section 'aapp_processes' in yaml file. Please check your config.")

    @patch('aapp_runner.read_aapp_config.check_static_configuration')
    @patch('aapp_runner.read_aapp_config.check_config_file_options')
    def test_mandatory_variables(self, file_options, static_config):
        """Test that the configuration contains mandatory variables"""
        static_config.return_value = True
        file_options.return_value = True

        myfilename = "/tmp/mytestfile"

        myconfig = self.config_mandatory.copy()
        with patch('aapp_runner.read_aapp_config.load_config_from_file', return_value=myconfig):
            cfg_obj = AappRunnerConfig(myfilename, 'norrkoping', 'xl-band')
            cfg_obj.check_config()
            result = cfg_obj.config

        expected_dict = {'aapp_processes':
                         {'xl-band': {'description': 'Text describing this processing config',
                                      'name': 'xl-band',
                                      'subscribe_topics': ['/XLBANDANTENNA/HRPT/L0',
                                                           '/XLBANDANTENNA/METOP/L0'],
                                      'publish_sift_format': '/{format:s}/{data_processing_level:s}/polar/direct_readout',
                                      'aapp_prefix': '/disk2/AAPP',
                                      'aapp_environment_file': 'ATOVS_ENV8',
                                      'aapp_outdir_base': '/disk2/aapp-runner-data',
                                      'aapp_outdir_format': '{satellite_name:s}_{start_time:%Y%m%d}_{start_time:%H%M}_{orbit_number:05d}',
                                      'aapp_log_files_archive_dir': '/disk2/aapp-runner-log',
                                      'aapp_log_outdir_format': '{satellite_name:s}_{start_time:%Y%m%d}_{start_time:%H%M}_{orbit_number:05d}',
                                      'aapp_log_files_archive_length': 1,
                                      'rename_aapp_compose': '{data_type:s}_{satellite_name:s}_{start_time:%Y%m%d}_{start_time:%H%M}_{orbit_number:5d}.{data_level:s}',
                                      'rename_aapp_files': [{'avhrr': {'aapp_file': 'hrpt.l1b',
                                                                       'data_type': 'hrpt',
                                                                       'data_level': 'l1b'}}],
                                      'aapp_workdir': '/tmp/myworkdir/'}},
                         'environment': 'xl-band', 'station': 'norrkoping'}

        self.assertDictEqual(expected_dict, result)

    def test_aapp_workdir(self):
        """Test that the aapp-process-env configuration contains an aapp_workdir setting"""
        from aapp_runner.read_aapp_config import load_config_from_file

        myfilename = "/tmp/mytestfile"
        myconfig = self.config_mandatory.copy()
        del myconfig['aapp_processes']['xl-band']['aapp_workdir']
        with patch('aapp_runner.read_aapp_config.load_config_from_file', return_value=myconfig):
            with pytest.raises(AappWorkDirNotSet) as exec_info:
                AappRunnerConfig(myfilename, 'norrkoping', 'xl-band')

        exception_raised = exec_info.value
        self.assertEqual(str(exception_raised),
                         "You must give either 'aapp_workdir' or 'working_dir in config.")


TEST_INPUT_MSG_DSET = """pytroll://collection/EPS/0/metop-b dataset safusr.u@lxserv1043.smhi.se 2021-12-09T09:13:35.291601 v1.01 application/json {"start_time": "2021-12-09T08:58:01", "end_time": "2021-12-09T09:13:20", "processing_start_time": "2021-12-09T08:58:17", "orbit_number": 47873, "platform_name": "Metop-B", "format": "EPS", "type": "binary", "data_processing_level": "0", "variant": "DR", "dataset": [{"uri": "/san1/polar_in/direct_readout/metop/MHSx_HRP_00_M01_20211209085800Z_20211209091317Z_N_O_20211209085817Z", "uid": "MHSx_HRP_00_M01_20211209085800Z_20211209091317Z_N_O_20211209085817Z"}, {"uri": "/san1/polar_in/direct_readout/metop/AMSA_HRP_00_M01_20211209085800Z_20211209091312Z_N_O_20211209085817Z", "uid": "AMSA_HRP_00_M01_20211209085800Z_20211209091312Z_N_O_20211209085817Z"}, {"uri": "/san1/polar_in/direct_readout/metop/AVHR_HRP_00_M01_20211209085803Z_20211209091327Z_N_O_20211209085817Z", "uid": "AVHR_HRP_00_M01_20211209085803Z_20211209091327Z_N_O_20211209085817Z"}], "sensor": ["mhs", "amsu-a", "avhrr/3"]}"""

TEST_INPUT_MSG_URI = """pytroll://HRPT/0/nkp/dev/polar/direct_readout file safusr.u@lxserv1043.smhi.se 2021-12-13T12:38:56.119263 v1.01 application/json {"start_time": "2021-12-13T12:26:08", "end_time": "2021-12-13T12:38:50", "orbit_number": 85372, "platform_name": "NOAA-18", "type": "binary", "format": "HRPT", "sensor": ["avhrr/3", "mhs", "amsu-a", "hirs/4"], "data_processing_level": "0", "uid": "20211213122608_NOAA_18.hmf", "uri": "ssh://172.29.1.52/san1/polar_in/direct_readout/hrpt/20211213122608_NOAA_18.hmf", "variant": "DR"}"""


class TestUpdateProcessConfig(unittest.TestCase):
    """Test various functions updating the (non-static) config during processing."""
    
    def setUp(self):
        self.config_complete = create_config_from_yaml(TEST_YAML_CONTENT_OK)
        self.input_msg_dset = Message.decode(rawstr=TEST_INPUT_MSG_DSET)
        self.input_msg_uri = Message.decode(rawstr=TEST_INPUT_MSG_URI)

    @patch('aapp_runner.read_aapp_config.load_config_from_file')
    def test_update_process_config_sensors_and_filenames_input_dataset(self, config):
        """Test update the process-config for sensors and filenames when input is a dataset."""
        config.return_value = self.config_complete
        myfilename = "/tmp/mytestfile"
        aapp_run_config = AappRunnerConfig(myfilename, 'norrkoping', 'xl-band')
        aapp_config = AappL1Config(aapp_run_config.config, 'xl-band')
        inmsg = self.input_msg_dset
        config = aapp_config

        assert config.config.get('process_mhs') is None

        result = generate_process_config(inmsg, config)
        assert result
        assert config.config['process_mhs']
        assert config.config['process_amsua']
        assert config.config['process_avhrr']
        assert config.config['process_amsub'] is False
        assert config.config['process_hirs'] is False
        assert config.config['process_msu'] is False

        assert config.config.get('input_amsub_file') is None
        assert config.config.get('input_hirs_file') is None
        assert config.config.get('input_msu_file') is None

        expected_filename = "/san1/polar_in/direct_readout/metop/AVHR_HRP_00_M01_20211209085803Z_20211209091327Z_N_O_20211209085817Z"
        self.assertEqual(config.config['input_avhrr_file'], expected_filename)

        expected_filename = "/san1/polar_in/direct_readout/metop/AMSA_HRP_00_M01_20211209085800Z_20211209091312Z_N_O_20211209085817Z"
        self.assertEqual(config.config['input_amsua_file'], expected_filename)

        expected_filename = "/san1/polar_in/direct_readout/metop/MHSx_HRP_00_M01_20211209085800Z_20211209091317Z_N_O_20211209085817Z"
        self.assertEqual(config.config['input_mhs_file'], expected_filename)
        

    @patch('aapp_runner.read_aapp_config.load_config_from_file')
    def test_update_process_config_sensors_and_filenames_input_uri(self, config):
        """Test update the process-config for sensors and filenames when input is an uri (one file)."""        
        config.return_value = self.config_complete

        myfilename = "/tmp/mytestfile"
        aapp_run_config = AappRunnerConfig(myfilename, 'norrkoping', 'xl-band')
        aapp_config = AappL1Config(aapp_run_config.config, 'xl-band')
        inmsg = self.input_msg_uri
        config = aapp_config

        assert config.config.get('process_mhs') is None

        result = generate_process_config(inmsg, config)
        assert result
        assert config.config['process_mhs']
        assert config.config['process_amsua']
        assert config.config['process_avhrr']
        assert config.config['process_amsub']
        assert config.config['process_hirs']
        assert config.config['process_msu'] is False
        
        assert config.config.get('input_amsub_file') is None
        assert config.config.get('input_msu_file') is None

        expected_filename = "/san1/polar_in/direct_readout/hrpt/20211213122608_NOAA_18.hmf"
        self.assertEqual(config.config['input_avhrr_file'], expected_filename)
        self.assertEqual(config.config['input_amsua_file'], expected_filename)
        self.assertEqual(config.config['input_mhs_file'], expected_filename)
        self.assertEqual(config.config['input_hirs_file'], expected_filename)
        
