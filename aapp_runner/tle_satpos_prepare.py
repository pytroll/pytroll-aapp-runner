#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2015, 2017 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c20671.ad.smhi.se>

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

"""Prepare tle and satpos (and ephe) files for AAPP, using the tleing and
alleph scripts from AAPP

"""

import logging
from glob import glob
import os
import tempfile
from datetime import datetime
import shutil
from subprocess import Popen, PIPE

LOG = logging.getLogger(__name__)


def do_tleing(aapp_prefix, tle_in, tle_out, tle_call):
    """Get the tle-file and copy them to the AAPP data structure 
       and run the AAPP tleing script and executable"""

    infiles = glob("%s/tle-*" % (tle_in))
    copy_done = False
    for filename in infiles:
        name = os.path.basename(filename)
        try:
            dtobj = datetime.strptime(name, "tle-%Y%m%d%H%M.txt")
        except ValueError:
            try:
                dtobj = datetime.strptime(name, "tle-%Y%m%d.txt")
            except ValueError:
                LOG.warning("Skip file, %s", str(filename))
                continue

        subdirname = dtobj.strftime('%Y_%m')
        outfile = "%s/%s/%s" % (tle_out, subdirname, name.replace('-', '_'))
        LOG.debug("OUTPUT file = %s", str(outfile))

        subdir = "%s/%s" % (tle_out, subdirname)
        if not os.path.exists(subdir):
            os.mkdir(subdir)
        tmp_filepath = tempfile.mktemp(suffix='_' + os.path.basename(outfile),
                                       dir=os.path.dirname(outfile))
        LOG.debug("tmp-filepath = %s", tmp_filepath)
        shutil.copy(filename, tmp_filepath)
        LOG.debug("File copied: %s -> %s", filename, tmp_filepath)
        os.rename(tmp_filepath, outfile)
        LOG.debug("Rename: %s -> %s", tmp_filepath, outfile)
        copy_done = True

    if copy_done:
        LOG.info("tle files have been found and copied. Do the tleing...")
        my_env = os.environ.copy()
        my_env['AAPP_PREFIX'] = aapp_prefix
        for key in my_env:
            LOG.debug("ENV: " + str(key) + ": " + str(my_env[key]))
        import shlex
        myargs = shlex.split(str(tle_call))
        LOG.debug('Command sequence= ' + str(myargs))
        process = Popen(myargs, shell=False, env=my_env,
                        stderr=PIPE, stdout=PIPE)
        stdout, stderr = process.communicate()
        LOG.debug("communicate called...")

        for item in stdout.split('\n'):
            LOG.info(item)

        for item in stderr.split('\n'):
            LOG.info(item)

    else:
        LOG.info("No tle-files copied. No tleing will be done...")

    return
