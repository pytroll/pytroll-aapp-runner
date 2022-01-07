# -*- coding: utf-8 -*-
#
# Copyright (c) 2014 - 2022 Pytroll Community
#
# Author(s):
#
#   Panu Lahtinen <panu.lahtinen@fmi.fi>
#   Martin Raspaud <martin.raspaud@smhi.se>
#   Adam Dybbroe <adam.dybbroe@smhi.se>
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

import logging

LOGGER = logging.getLogger(__name__)


def check_if_scene_is_unique(config):
    """Check if the Scene is unique.

    The scene is checked against the register (holding already processed
    scenes).  If it overlaps in time with a previously processed scene and the
    area-id's (used to collect the data (granules) are the same, then return
    False - the scene is then not unique and should not be processed further.

    """
    LOGGER.debug("config.job_register: %s", str(config.job_register))
    LOGGER.debug("config platform_name: %s", str(config['platform_name']))
    LOGGER.debug("config - collection_area_id: %s", str(config['collection_area_id']))

    # Use sat id, start and end time and area_id as the unique identifier of the scene!
    if (config['platform_name'] in config.job_register and
            len(config.job_register[config['platform_name']]) > 0):

        # Go through list of start,end time tuples and see if the current
        # scene overlaps with any - only if the area ids are the same

        # Get registered start and end times with area id equal to current area_id
        registered_times = []
        for start_t, end_t, area_id in config.job_register[config['platform_name']]:
            if area_id == config['collection_area_id']:
                registered_times.append((start_t, end_t))

        # Get overlap status
        status = overlapping_timeinterval(
            (config['starttime'], config['endtime']), registered_times)

        if status:
            info_msg = ("Processing of scene " + config['platform_name'] +
                        " " + str(status[0]) + " " + str(status[1]) +
                        " with overlapping time has been"
                        " launched previously. Skip it!")
            LOGGER.info(info_msg)
            return False

        LOGGER.debug("No overlap with any recently processed scenes...")

    return True


def create_scene_id(config):
    """Create a unique scene specific ID to identify the scene process for later.

    The id is created from the platform name and start and end times of the
    scene available in the process config dictionary.

    """
    scene_id = (str(config['platform_name']) + '_' +
                config['starttime'].strftime('%Y%m%d%H%M%S') +
                '_' + config['endtime'].strftime('%Y%m%d%H%M%S'))
    LOGGER.debug("scene_id = " + str(scene_id))
    return scene_id


def overlapping_timeinterval(start_end_times, timelist):
    """From a list of start and end times check if the current time interval
    overlaps with one or more"""

    starttime, endtime = start_end_times
    for tstart, tend in timelist:
        if ((tstart <= starttime and tend > starttime) or
                (tstart < endtime and tend >= endtime)):
            return tstart, tend
        if (tstart >= starttime and tend <= endtime):
            return tstart, tend

    return False


def run_shell_command(command, use_shell=False, use_shlex=True, my_cwd=None,
                      my_env=None, stdout_logfile=None, stderr_logfile=None, stdin=None, my_timeout=24 * 60 * 60):
    """Run the given command as a shell and get the return code, stdout and stderr
        Returns True/False and return code.
    """
    from subprocess import PIPE, Popen

    if stdin is not None:
        stdin = stdin.encode('utf-8')

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
    except ValueError:
        LOGGER.error("Popen called with invalid arguments.")
        return False
    except:
        LOGGER.error("Popen failed for an unknown reason.")
        return False

    import signal

    class Alarm(Exception):
        pass

    def alarm_handler(signum, frame):
        raise Alarm

    signal.signal(signal.SIGALRM, alarm_handler)
    signal.alarm(my_timeout)
    try:
        LOGGER.debug("Before call to communicate:")
        if stdin is None:
            out, err = proc.communicate()
        else:
            out, err = proc.communicate(input=stdin)

        out = out.decode('utf-8')
        err = err.decode('utf-8')

        return_value = proc.returncode
        signal.alarm(0)
    except Alarm:
        LOGGER.error(
            "Command: {} took to long time(more than {}s) to complete. Terminates the job.".format(command, my_timeout))
        proc.terminate()
        return False

    LOGGER.debug("communicate complete")
    lines = out.splitlines()
    if stdout_logfile is None:
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
    if stderr_logfile is None:
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
