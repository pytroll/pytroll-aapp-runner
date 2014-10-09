#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014

# Author(s):

#   Panu Lahtinen <pnuu+git@iki.fi>

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

'''XML reader for Trollduction system and product configuration files.
'''

import xml.etree.ElementTree as etree
import os

def get_root(fname):
    '''Read XML file and return the root tree.
    '''

    tree = etree.parse(fname)
    root = tree.getroot()

    return root


def parse_xml(tree, also_empty=False):
    '''Parse the given XML file to dictionary.
    '''
    xml_dict = {}

    # this tags will always be lists, if they are present and non-empty
    listify = ['area', 'product', 'valid_satellite', 'invalid_satellite', 
               'pattern', 'file_tag', 'directory']
    children = list(tree)

    if len(children) == 0:
        try:
            xml_dict = tree.text.strip()
        except AttributeError:
            pass

    for child in children:
        new_val = parse_xml(child, also_empty=also_empty)
        if len(new_val) == 0:
            if also_empty:
                xml_dict[child.tag] = ''
                continue
            else:
                continue
        if child.tag in xml_dict:
            if not isinstance(xml_dict[child.tag], list):
                xml_dict[child.tag] = [xml_dict[child.tag]]
            xml_dict[child.tag].append(new_val)
        else:
            if len(new_val) > 0:
                if child.tag in listify:
                    xml_dict[child.tag] = [new_val]
                else:
                    xml_dict[child.tag] = new_val

    return xml_dict


def get_filepattern_config(fname=None):
    '''Retrieves the filepattern configuration file for trollstalker,
    and returns the parsed XML as a dictionary.  Optional argument
    *fname* can be used to specify the file.  If *fname* is None, the
    systemwide file is read.
    '''
    
    if fname is None:
        fname = os.path.realpath(__file__).split(os.path.sep)[:-2]
        fname.append('etc')
        fname.append('filepattern_config.xml')
        fname = os.path.sep.join(fname)

    return parse_xml(get_root(fname), also_empty=True)
