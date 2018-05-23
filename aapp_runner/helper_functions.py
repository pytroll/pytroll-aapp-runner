# -*- coding: utf-8 -*-
#
# Copyright (c) 2014-2018 PyTroll community
#
# Author(s):
#
#   Panu Lahtinen <panu.lahtinen@fmi.fi>
#   Martin Raspaud <martin.raspaud@smhi.se>
#
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

'''Helper functions for aapp runner
'''

import numpy as np
import os
import logging
from ConfigParser import ConfigParser

LOGGER = logging.getLogger(__name__)


def overlapping_timeinterval(start_end_times, timelist):
    """From a list of start and end times check if the current time interval
    overlaps with one or more"""

    starttime, endtime = start_end_times
    for tstart, tend in timelist:
        if ((tstart <= starttime and tend > starttime) or
                (tstart < endtime and tend >= endtime)):
            return tstart, tend
        elif (tstart >= starttime and tend <= endtime):
            return tstart, tend

    return False


def run_shell_command(command, use_shell=False, use_shlex=True, my_cwd=None,
                      my_env=None, stdout_logfile=None, stderr_logfile=None, stdin=None, my_timeout=24 * 60 * 60):
    """Run the given command as a shell and get the return code, stdout and stderr
        Returns True/False and return code.
    """
    from subprocess import Popen, PIPE

    if use_shlex:
        import shlex
        myargs = shlex.split(str(command))
        LOGGER.debug('Command sequence= ' + str(myargs))
    else:
        myargs = command

    try:
        proc = Popen(myargs,
                     cwd=my_cwd, shell=use_shell, env=my_env,
                     stderr=PIPE, stdout=PIPE, stdin=PIPE, close_fds=True)

        LOGGER.debug("Process pid: {}".format(proc.pid))
    except OSError as e:
        LOGGER.error("Popen failed for command: {} with {}".format(myargs, e))
        return False
    except ValueError as e:
        LOGGER.error("Popen called with invalid arguments.")
        return False
    except:
        LOGGER.error("Popen failed for an unknown reason.")
        return False

    # proc.poll
    #LOGGER.info("Before call to communicate:")
    #out, err = proc.communicate()
    #return_value = proc.returncode

    #lines = out.splitlines()
    # for line in lines:
    #    LOGGER.info(line)

    #lines = err.splitlines()
    # for line in lines:
    #    LOGGER.info(line)

    import signal

    class Alarm(Exception):
        pass

    def alarm_handler(signum, frame):
        raise Alarm

    signal.signal(signal.SIGALRM, alarm_handler)
    signal.alarm(my_timeout)
    try:
        LOGGER.debug("Before call to communicate:")
        if stdin == None:
            out, err = proc.communicate()
        else:
            out, err = proc.communicate(input=stdin)

        return_value = proc.returncode
        signal.alarm(0)
    except Alarm:
        LOGGER.error(
            "Command: {} took to long time(more than {}s) to complete. Terminates the job.".format(command, my_timeout))
        proc.terminate()
        return False

    LOGGER.debug("communicate complete")
    lines = out.splitlines()
    if stdout_logfile == None:
        for line in lines:
            LOGGER.debug(line)
    else:
        try:
            _stdout = open(stdout_logfile, 'w')
            for line in lines:
                _stdout.write(line + "\n")
            _stdout.close()
        except IOError as e:
            LOGGER.error("IO operation to file stdout_logfile: {} failed with {}".format(stdout_logfile, e))
            return False

    errlines = err.splitlines()
    if (stderr_logfile == None):
        for errline in errlines:
            LOGGER.debug(errline)
    else:
        try:
            _stderr = open(stderr_logfile, 'w')
            for errline in errlines:
                _stderr.write(errline + "\n")
            _stderr.close()
        except IOError as e:
            LOGGER.error("IO operation to file stderr_logfile: {} failed with {}".format(stderr_logfile, e))
            return False

    return True, return_value, out, err
