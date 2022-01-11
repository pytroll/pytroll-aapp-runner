#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2022 Pytroll developers

# Author(s):

#   Adam Dybbroe <Firstname.Lastname @ smhi.se>

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

"""Unittesting the helper functions for the AAPP-runner.
"""

import logging
import unittest
from datetime import datetime
from unittest.mock import patch

from aapp_runner.helper_functions import check_if_scene_is_unique
from aapp_runner.read_aapp_config import AappL1Config, AappRunnerConfig
from aapp_runner.tests.test_config import (TEST_YAML_CONTENT_OK,
                                           create_config_from_yaml)


class TestProcessConfigChecking(unittest.TestCase):
    """Test various functions checking on the (non-static) config during processing."""

    def setUp(self):
        self.config_complete = create_config_from_yaml(TEST_YAML_CONTENT_OK)

    @patch('aapp_runner.read_aapp_config.load_config_from_file')
    def test_check_if_scene_is_unique_return_value(self, config):
        """Test checking if the current scene is unique or if it has been processed earlier."""
        config.return_value = self.config_complete
        myfilename = "/tmp/mytestfile"
        aapp_run_config = AappRunnerConfig(myfilename, 'norrkoping', 'xl-band')
        aapp_config = AappL1Config(aapp_run_config.config, 'xl-band')

        aapp_config['platform_name'] = 'metop03'
        aapp_config['collection_area_id'] = 'euron1'
        aapp_config['starttime'] = datetime(2022, 1, 8, 12, 49, 50)
        aapp_config['endtime'] = datetime(2022, 1, 8, 13, 0, 26)

        aapp_config.job_register = {}

        result = check_if_scene_is_unique(aapp_config)
        assert result

        aapp_config.job_register = {'metop03': [(datetime(2022, 1, 8, 12, 49, 50),
                                                 datetime(2022, 1, 8, 13, 0, 26), 'euron1')]}
        # An EARS scene (same platform and overlapping time interval and over
        # the same area of interest) arrives shortly after:
        aapp_config['platform_name'] = 'metop03'
        aapp_config['collection_area_id'] = 'euron1'
        aapp_config['starttime'] = datetime(2022, 1, 8, 12, 50)
        aapp_config['endtime'] = datetime(2022, 1, 8, 13, 0)

        result = check_if_scene_is_unique(aapp_config)
        assert not result

    @patch('aapp_runner.read_aapp_config.load_config_from_file')
    def test_check_if_scene_is_unique_logging(self, config):
        """Test the logging when checking if the current scene is unique."""
        config.return_value = self.config_complete
        myfilename = "/tmp/mytestfile"
        aapp_run_config = AappRunnerConfig(myfilename, 'norrkoping', 'xl-band')
        aapp_config = AappL1Config(aapp_run_config.config, 'xl-band')

        aapp_config.job_register = {'metop03': [(datetime(2022, 1, 8, 12, 49, 50),
                                                 datetime(2022, 1, 8, 13, 0, 26), 'euron1')]}
        # An EARS scene (same platform and overlapping time interval and over
        # the same area of interest) arrives shortly after:
        aapp_config['platform_name'] = 'metop03'
        aapp_config['collection_area_id'] = 'euron1'
        aapp_config['starttime'] = datetime(2022, 1, 8, 12, 50)
        aapp_config['endtime'] = datetime(2022, 1, 8, 13, 0)

        expected_logging = ['INFO:aapp_runner.helper_functions:first message',
                            'INFO:aapp_runner.helper_functions:Processing of scene metop03 2022-01-08 12:49:50 2022-01-08 13:00:26 with overlapping time has been launched previously. Skip it!']

        with self.assertLogs('aapp_runner.helper_functions', level='INFO') as cm:
            logging.getLogger('aapp_runner.helper_functions').info('first message')
            _ = check_if_scene_is_unique(aapp_config)

        self.assertEqual(cm.output, expected_logging)

        with self.assertLogs('aapp_runner.helper_functions', level='WARNING') as cm:
            logging.getLogger('aapp_runner.helper_functions').warning('first message')
            _ = check_if_scene_is_unique(aapp_config)

        self.assertEqual(len(cm.output), 1)

        # Scene is different (different satellite) from previous:
        aapp_config['platform_name'] = 'metop01'
        aapp_config['collection_area_id'] = 'euron1'
        aapp_config['starttime'] = datetime(2022, 1, 8, 12, 50)
        aapp_config['endtime'] = datetime(2022, 1, 8, 13, 0)

        with self.assertLogs('aapp_runner.helper_functions', level='INFO') as cm:
            logging.getLogger('aapp_runner.helper_functions').info('first message')
            result = check_if_scene_is_unique(aapp_config)

        assert result
        self.assertEqual(len(cm.output), 1)
