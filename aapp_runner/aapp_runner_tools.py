#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c21856.ad.smhi.se>

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
"""


def set_collection_area_id(msg_data, config):
    """Setting the collection_area_id to the processing config.

    The collection_area_id is taken from the message if avalable. If not
    present in the message it will we be taken from the static config. If not
    present in config it will be set to None.
    """
    default_collection_area_id = config['aapp_processes'][config.process_name].get('collection_area_id')
    config['collection_area_id'] = msg_data.get('collection_area_id', default_collection_area_id)
