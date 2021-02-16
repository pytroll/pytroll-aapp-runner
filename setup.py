#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2013 - 2021 Pytroll Community

# Author(s):

#   Martin Raspaud <martin.raspaud@smhi.se>
#   Adam Dybbroe <adam.dybbroe@smhi.se>

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

"""Setup for aapp-runner.
"""

import sys
from setuptools import setup, find_packages
import os.path

try:
    # HACK: https://github.com/pypa/setuptools_scm/issues/190#issuecomment-351181286
    # Stop setuptools_scm from including all repository files
    import setuptools_scm.integration
    setuptools_scm.integration.find_files = lambda _: []
except ImportError:
    pass


with open('./README.md', 'r') as fd:
    long_description = fd.read()

description = 'Pytroll runner for AAPP'

requires = ['posttroll', 'netifaces', 'trollsift', 'pytroll-schedule', 'setuptools_scm']
test_requires = ['mock']


setup(name="aapp_runner",
      description=description,
      author='The Pytroll Team',
      author_email='pytroll@googlegroups.com',
      classifiers=["Development Status :: 3 - Alpha",
                   "Intended Audience :: Science/Research",
                   "License :: OSI Approved :: GNU General Public License v3 " +
                   "or later (GPLv3+)",
                   "Operating System :: OS Independent",
                   "Programming Language :: Python",
                   "Topic :: Scientific/Engineering"],
      url="https://github.com/pytroll/pytroll-aapp-runner",
      long_description=long_description,
      license='GPLv3',
      packages=find_packages(),
      install_requires=requires,
      scripts=['bin/aapp_dr_runner.py', ],
      data_files=[],
      tests_require=test_requires,
      python_requires='>=3.7',
      zip_safe=False,
      )
