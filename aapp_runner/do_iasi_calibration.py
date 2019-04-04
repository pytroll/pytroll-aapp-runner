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

"""Run the calibration script from AAPP for IASI
Relay on several other steps before this can be DONE
"""

import os
import logging

from aapp_runner.helper_functions import run_shell_command

LOG = logging.getLogger(__name__)

def do_iasi_calibration(process_config, timestamp):
    #Not implemented.
    #Need the optional software OPS-LRS, not included in normal AAPP download.
    return True
