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

import pytest
from unittest.mock import patch, Mock, MagicMock
import unittest
import yaml
from aapp_runner.read_aapp_config import VALID_CONFIGURATION
from aapp_runner.read_aapp_config import check_config_file_options
from aapp_runner.read_aapp_config import check_dir_permissions
from aapp_runner.read_aapp_config import AappRunnerConfig
from aapp_runner.read_aapp_config import (EnvironmentError, StaticConfigError,
                                          ConfigFileOptionsError, AappWorkDirNotSet,
                                          AappProcessKeyMissing, StationError)


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
