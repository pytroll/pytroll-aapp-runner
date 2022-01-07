#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2015 - 2022 Pytroll developers

# Author(s):

#   Adam.Dybbroe <a000680@c20671.ad.smhi.se>
#   Trygve Aspenes <trygveas@met.no>
#
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

The task of finding the correct TLE is challenging. The user can supply its own
directory in the config file and the filename convetion. Then the module
will try to find the correct one.
If data is reprocessed it should use the closest tle
If data is from Direct Broadcast the only newer tle files than the
timestamp if the index file should be processed.
"""

import logging
import os
import re
import shutil
import tempfile
import time
from datetime import datetime
from glob import glob
from shutil import copy

from trollsift.parser import Parser, compose, globify

from aapp_runner.helper_functions import run_shell_command

LOG = logging.getLogger(__name__)


def _do_6_matches(m):
    return datetime.strptime(m.group(1) + m.group(2) + m.group(3) + m.group(4) + m.group(5) + m.group(6), "%Y%m%d%H%M%S")


def _do_5_matches(m):
    return datetime.strptime(m.group(1) + m.group(2) + m.group(3) + m.group(4) + m.group(5), "%Y%m%d%H%M")


def _do_4_matches(m):
    return datetime.strptime(m.group(1) + m.group(2) + m.group(3) + m.group(4), "%Y%m%d%H")


def _do_3_matches(m):
    return datetime.strptime(m.group(1) + m.group(2) + m.group(3), "%Y%m%d")


def _do_3_matchesYY(m):
    return datetime.strptime(m.group(1) + m.group(2) + m.group(3), "%y%m%d")


tle_match_tests = ((r'.*(\d{4})(\d{2})(\d{2})_?-?T?(\d{2})(\d{2})(\d{2}).*', _do_6_matches),
                   (r'.*(\d{4})(\d{2})(\d{2})_?-?T?(\d{2})(\d{2}).*', _do_5_matches),
                   (r'.*(\d{4})(\d{2})(\d{2})_?-?T?(\d{2}).*', _do_4_matches),
                   (r'.*(\d{4})(\d{2})(\d{2}).*', _do_3_matches),
                   (r'.*(\d{2})(\d{2})(\d{2}).*', _do_3_matchesYY))


def download_tle(config, timestamp, dir_data_tle):

    user = os.getenv("PAR_NAVIGATION_TLE_USER", "xxxxxx")
    passwd = os.getenv("PAR_NAVIGATION_TLE_PASSWD", "xxxxxx")
    url = os.getenv("PAR_NAVIGATION_TLE_URL_DOWNLOAD")
    timeout = 60
    catalogue = "25338,26536,27453,28654,33591,37849,29499,38771,27431,32958,37214,25994,27424"

    tle_infile = ""
    tle_dict = {}
    tle_dict['timestamp'] = timestamp

    tle_cnf = []
    try:
        tle_cnf = config['aapp_processes'][config.process_name]['tle_download']
    except KeyError as ke:
        LOG.info("No tle_download config found. Using the default.")
        pass
    finally:
        tle_cnf.append({'url': url, 'user': user, 'passwd': passwd, 'timeout': timeout, 'catalogue': catalogue})

    try:
        tle_infile = compose(config['aapp_processes'][config.process_name]['tle_infile_format'], tle_dict)
    except KeyError as ke:
        if 'tle_infile_format' in ke:
            tle_infile = compose('tle_{timestamp:%Y%m%d_%H%M}.txt', tle_dict)
            LOG.warning("Using default TLE file name format: %s", tle_infile)
            pass
        else:
            LOG.error("Key error: {}".format(ke))
            LOG.error("Valid keys :")
            for key in tle_dict.keys():
                LOG.error("{}".format(key))
            raise
    except:
        raise

    tle_file_list = []
    for cnf in tle_cnf:
        for cnf_url in cnf['url'].split():
            LOG.debug("Will try to download TLE from {}.".format(cnf_url))

            if "space-track" in cnf_url:
                # Do the special space-track login
                status = False
                returncode = 0
                stdout = ""
                stderr = ""
                cmd = "wget -T {} --post-data=\"identity={}&password={}\" --cookies=on ",
                "--keep-session-cookies --save-cookies=cookies_spacetrack \"{}/ajaxauth/login\" -olog".format(
                    cnf['timeout'], cnf['user'], cnf['passwd'], cnf_url)
                try:
                    status, returncode, stdout, stderr = run_shell_command(cmd)
                except:
                    LOG.error("Failed running command: {} with return code: {}".format(cmd, returncode))
                    LOG.error("stdout: {}".format(stdout))
                    LOG.error("stderr: {}".format(stderr))
                    return_status = False
                else:
                    if returncode != 0:
                        LOG.debug("Running command: {} with return code: {}".format(cmd, returncode))
                        LOG.debug("stdout: {}".format(stdout))
                        LOG.debug("stderr: {}".format(stderr))
                    else:
                        cmd = "wget -T {} --keep-session-cookies --load-cookies=cookies_spacetrack -O weather.txt ",
                        "\"{}/basicspacedata/query/class/tle_latest/ORDINAL/1/NORAD_CAT_ID/{}/orderby/TLE_LINE1\"".format(
                            cnf['timeout'], cnf_url, cnf['catalogue'])
                        try:
                            status, returncode, stdout, stderr = run_shell_command(cmd)
                        except:
                            LOG.error("Failed running command: {} with return code: {}".format(cmd, returncode))
                            LOG.error("stdout: {}".format(stdout))
                            LOG.error("stderr: {}".format(stderr))
                        else:
                            if returncode != 0:
                                LOG.debug("Running command: {} with return code: {}".format(cmd, returncode))
                                LOG.debug("stdout: {}".format(stdout))
                                LOG.debug("stderr: {}".format(stderr))
                            else:
                                LOG.debug("TLE download ok")
                if os.path.exists("weather.txt"):
                    try:
                        tle_file = open("weather.txt", 'r')
                        tle_string = tle_file.read()
                        tle_file.close()
                        tle_file_out = os.path.join(dir_data_tle, tle_infile)
                        with open(tle_file_out, "a") as tle_file:
                            tle_file.write(tle_string)

                        if tle_file_out not in tle_file_list:
                            tle_file_list.append(tle_file_out)
                    except Exception as ex:
                        LOG.debug("Failed rename tle download file: {}".format(ex))
                        raise
            else:
                import urllib2
                f = urllib2.urlopen(cnf_url)
                tle_string = f.read()
                f.close()
                tle_file_out = os.path.join(dir_data_tle, tle_infile)
                with open(tle_file_out, "a") as tle_file:
                    tle_file.write(tle_string)
                if tle_file_out not in tle_file_list:
                    tle_file_list.append(tle_file_out)

    return tle_file_list


def fetch_realtime_tles(tle_input_path, tle_output_path, tle_infile_format):
    """Get the recent TLEs and copy them into the AAPP data structure."""
    infiles = glob(os.path.join(tle_input_path, globify(tle_infile_format)))
    p__ = Parser(tle_infile_format)
    for filepath in infiles:
        filename = os.path.basename(filepath)
        res = p__.parse(filename)
        dtobj = res['timestamp']

        subdirname = dtobj.strftime('%Y_%m')
        subdirpath = os.path.join(tle_output_path, subdirname)
        outfile = os.path.join(subdirpath, filename)
        LOG.debug("OUTPUT file = %s", str(outfile))

        if not os.path.exists(subdirpath):
            os.mkdir(subdirpath)
        tmp_filepath = tempfile.mktemp(suffix='_' + os.path.basename(outfile),
                                       dir=os.path.dirname(outfile))
        LOG.debug("tmp-filepath = %s", tmp_filepath)
        shutil.copy(filepath, tmp_filepath)
        LOG.debug("File copied: %s -> %s", filepath, tmp_filepath)
        os.rename(tmp_filepath, outfile)
        LOG.debug("Rename: %s -> %s", tmp_filepath, outfile)


def do_tleing(config, timestamp, satellite):
    """Get the tle-file and copy them to the AAPP data structure
       and run the AAPP tleing script and executable"""

    return_status = True

    # This function relies on beeing in a working directory
    try:
        current_dir = os.getcwd()  # Store the dir to change back to after function complete
    except OSError as ose:
        LOG.error("Failed to get current working dir: {}".format(ose))
        raise

    os.chdir(config['aapp_processes'][config.process_name]['working_dir'])

    _maybe_update_env(config)

    # variables for the TLE HOME directory
    DIR_DATA_TLE = os.getenv('DIR_DATA_TLE', os.path.join(os.getenv('DIR_NAVIGATION'), 'orb_elem'))

    _ensure_tledir(DIR_DATA_TLE)

    # Fetch TLE files from central real-time repo and place them under the AAPP orbelems structure:
    if 'recent_tlefiles_ext_dir' in config['aapp_processes'][config.process_name]:
        extdir = config['aapp_processes'][config.process_name]['recent_tlefiles_ext_dir']
        LOG.debug("Fetch TLEs from %s to %s", extdir, DIR_DATA_TLE)
        tle_file_format = config['aapp_processes'][config.process_name]['tle_infile_format']
        fetch_realtime_tles(extdir, DIR_DATA_TLE, tle_file_format)

    TLE_INDEX = os.path.join(DIR_DATA_TLE, "tle_{}.index".format(satellite))

    (tle_dict, tle_file_list, tle_search_dir) = _search_tle_files(config, DIR_DATA_TLE, TLE_INDEX,
                                                                  timestamp)

    if not tle_file_list and config['aapp_processes'][config.process_name]['download_tle_files']:
        LOG.warning("Found no tle files. Try to download ... ")
        tle_file_list = download_tle(config, timestamp, DIR_DATA_TLE)

    _ingest_and_archive_tle_files(config, tle_file_list, DIR_DATA_TLE, tle_dict,
                                  tle_search_dir, satellite, TLE_INDEX)

    # Change back after this is done
    os.chdir(current_dir)

    return return_status


def _maybe_update_env(config):
    """Potentially update environment based on config.

    Based on the configuration for aapp-runner, potentially update some
    environment variables as expected by AAPP.
    """

    if '7' in os.getenv('AAPP_PREFIX'):
        aapp_env = 'AAPP_ENV7'
    elif '8' in os.getenv('AAPP_PREFIX'):
        aapp_env = 'AAPP_ENV8'
    else:
        aapp_env = 'AAPP_ENV'

    if 'dir_navigation' in config['aapp_processes'][config.process_name]:
        LOG.warning("Override the env variable set in {} DIR_NAVIGATION from {} to {}.".format(
            aapp_env,
            os.environ['DIR_NAVIGATION'], config['aapp_processes'][config.process_name]['dir_navigation']))
        os.environ['DIR_NAVIGATION'] = config['aapp_processes'][config.process_name]['dir_navigation']
        # Need to update DIR_DATA_TLE dir to be sure this is corect if not tle_indir is given
        os.environ['DIR_DATA_TLE'] = os.path.join(os.getenv('DIR_NAVIGATION'), 'tle_db')

    if 'tle_indir' in config['aapp_processes'][config.process_name]:
        tle_indir = config['aapp_processes'][config.process_name]['tle_indir']
        LOG.warning("Override the env variable set in {} DIR_DATA_TLE from {} to {}.".format(
            aapp_env,
            os.environ['DIR_DATA_TLE'], tle_indir))
        os.environ['DIR_DATA_TLE'] = tle_indir


def _ensure_tledir(tledir):
    """Ensure that directory tledir exists, log a warning if it doesn't.
    """
    # This is needed by AAPP tleing. Try other if not existing
    if not os.path.exists(tledir):
        LOG.warning("Dir " + tledir + " does not exist. Create ")
        try:
            os.makedirs(tledir)
        except:
            LOG.error("Failed to create %s. Can not handle TLEs without this", DIR_DATA_TLE)
            raise


def _search_tle_files(config, tle_dir, tle_index, timestamp):
    """Search for a list of TLE files.

    Returns (tle_dict, tle_files, tle_search_dir)
    """

    if 'tle_file_to_data_diff_limit_days' in config['aapp_processes'][config.process_name]:
        select_closest_tle_file_to_data = True
        min_closest_tle_file = int(
            config['aapp_processes'][config.process_name]['tle_file_to_data_diff_limit_days']) * 24 * 60 * 60
    else:
        select_closest_tle_file_to_data = False

    tle_search_dir = tle_dir
    tle_file_list = []
    # dict to hold needed tle keys
    tle_dict = {}
    if not select_closest_tle_file_to_data:
        if os.path.exists(tle_index):
            # Loop over all tle files, and only do tle
            tle_files = [s for s in glob(os.path.join(tle_dir, 'tle*txt'))
                         if os.path.isfile(os.path.join(tle_dir, s))]
            tle_files.sort(key=lambda s: os.path.getctime(os.path.join(tle_dir, s)))

            tle_index_mtime = os.path.getmtime(tle_index)
            for s in tle_files:
                if os.path.getmtime(os.path.join(tle_dir, s)) > tle_index_mtime:
                    tle_file_list.append(s)

            if len(tle_file_list) == 0:
                LOG.warning(("No newer tle files than last update of the index file. " +
                             "Last update of index file is {:d}s. If more than a few days you should check.".format(
                                 int(time.time() - tle_index_mtime))))
            else:
                LOG.info("Will use tle files {}".format(tle_file_list))
        else:
            LOG.warning("index file does not exist. If this is the first run of AAPP tleing.exe it is ok,"
                        " otherwise it is a bit suspicious.")
            try:
                tle_files = [s for s in os.listdir(tle_dir) if
                             os.path.isfile(os.path.join(tle_dir, s))]
                tle_files.sort(key=lambda s:
                               os.path.getctime(os.path.join(tle_dir, s)))
                tle_file_list = tle_files
            except OSError:
                LOG.warning("Found no tle files .... ")

    else:
        # dict to hold needed tle keys
        tle_dict['timestamp'] = timestamp
        try:
            infile = compose(config['aapp_processes'][config.process_name]['tle_infile_format'], tle_dict)
        except KeyError as ke:
            LOG.error("Key error: {}".format(ke))
            LOG.error("Valid keys :")
            for key in tle_dict.keys():
                LOG.error("{}".format(key))
            raise
        except:
            raise

        LOG.debug("tle file name: {}".format(infile))

        # Check if I can read the tle file.
        first_search = True

        # FIXME: In AAPP default get_tle script direcory timestamp is TLE_MONTH=`date +%Y-\%m`
        for tle_search_dir in [compose(os.path.join(tle_dir,
                                                    "{timestamp:%Y_%m}"), tle_dict), tle_dir]:
            if not os.path.exists(tle_search_dir):
                LOG.debug("tle_search_dir {} does not exists.".format(tle_search_dir))
                continue
            LOG.debug("tle_search_dir {}".format(tle_search_dir))
            try:
                with open(os.path.join(tle_search_dir, infile)) as tle_file:
                    del tle_file_list[:]
                    tle_file_list.append(os.path.join(tle_search_dir, infile))
                    min_closest_tle_file = 0
                    pass
            except IOError as e:
                LOG.warning("Could not find tle file: {}. Try find closest ... ".format(infile))
                tle_file_list = glob(os.path.join(tle_search_dir, '*'))
                LOG.debug("tle file list: {}".format(tle_file_list))
                LOG.debug(tle_file_list)
                infile_closest = ""

                for tle_file_name in tle_file_list:
                    for regex, test in tle_match_tests:
                        m = re.match(regex, tle_file_name)
                        if m:
                            try:
                                LOG.debug("{} {}".format(tle_file_name, test(m)))
                                delta = timestamp - test(m)
                                if (abs(delta.total_seconds()) < min_closest_tle_file):
                                    min_closest_tle_file = abs(delta.total_seconds())
                                    # infile_closest = os.path.basename(tle_file_name)
                                    infile_closest = tle_file_name
                                    LOG.debug("Closest tle infile so far: {}".format(infile_closest))
                            except ValueError:
                                pass

                if infile_closest:
                    del tle_file_list[:]
                    tle_file_list.append(infile_closest)
                    break
                else:
                    if not first_search:
                        LOG.error("Could not find tle file close enough to timestamp {} with limit {}".format(
                            timestamp, min_closest_tle_file))
                        LOG.error("Update your TLE files or adjust the limit(Not recomended!).")
                first_search = False
            else:
                break

        # DIR_DATA_TLE = tle_search_dir
        if tle_file_list:
            LOG.debug("Use this: {} offset {}s".format(tle_file_list, min_closest_tle_file))

    return (tle_dict, tle_file_list, tle_search_dir)


def _ingest_and_archive_tle_files(config, tle_file_list, tle_dir, tle_dict,
                                  tle_search_dir, satellite, tle_index):
    for tle_file in tle_file_list:
        archive = False

        # SATellite IDentification mandatory
        # so take care of default values
        os.environ['SATID_FILE'] = os.getenv('SATID_FILE', 'satid.txt')

        """Don't use the tle_indir because this is handeled by the tleing script"""
        if (tle_dir != tle_search_dir):
            tle_filename = compose(os.path.join("{timestamp:%Y_%m}", os.path.basename(tle_file)), tle_dict)
        else:
            tle_filename = os.path.basename(tle_file)
        status = False
        returncode = 0
        stdout = ""
        stderr = ""
        cmd = "tleing.exe"
        stdin = "{}\n{}\n{}\n{}\n".format(tle_dir, tle_filename, satellite,
                                          tle_index)
        LOG.debug('stdin arguments to command: ' + str(stdin))
        try:
            status, returncode, stdout, stderr = run_shell_command(cmd, stdin=stdin)

        except:
            LOG.error("Failed running command: {} with return code: {}".format(cmd, returncode))
            LOG.error("stdout: {}".format(stdout))
            LOG.error("stderr: {}".format(stderr))
            return_status = False
        else:
            if returncode != 0:
                LOG.debug("Running command: {} with return code: {}".format(cmd, returncode))
                LOG.debug("stdout: {}".format(stdout))
                LOG.debug("stderr: {}".format(stderr))
            elif not os.path.exists(tle_index):
                LOG.error("index file: {} does not exist after tleing.  Something is wrong.".format(tle_index))
                LOG.debug("Running command: {} with return code: {}".format(cmd, returncode))
                LOG.debug("stdout: {}".format(stdout))
                LOG.debug("stderr: {}".format(stderr))
            else:
                LOG.debug("Running command: {} with return code: {}".format(cmd, returncode))
                LOG.debug("stdout: {}".format(stdout))
                LOG.debug("stderr: {}".format(stderr))
                LOG.debug("DIR_DATA_TLE : {}".format(tle_dir))
                LOG.debug("tle_file : {}".format(os.path.basename(tle_file)))
                LOG.debug("satellite : {}".format(satellite))
                LOG.debug("TLE_INDEX : {}".format(tle_index))

                # When a index file is generated above one line is added for each tle file.
                # If several tle files contains equal TLEs each of these TLEs generate one line in the index file
                # To avoid this, sort the index file keeping only unique lines(skipping the tle filename at the end

                # The sort options +0b -3b is guessed to be sort from column 0 to 3, but this is not documented
                # Could cause problems with future version of sort.
                # See eg. http://search.cpan.org/~sdague/ppt-0.12/bin/sort
                # cmd="sort -u -o {} +0b -3b {}".format(os.path.join(DIR_DATA_TLE, "{}.sort".format(TLE_INDEX)),
                # os.path.join(DIR_DATA_TLE, TLE_INDEX))
                if os.path.exists(tle_index):
                    archive = _sort_index_file(tle_index)
                else:
                    LOG.error("tle index file: {} does not exists after tleing before sort. This can not happen.")

        # If a new tle is used and archive dir is given in config, copy TLEs to archive
        if archive and ('tle_archive_dir' in config['aapp_processes'][config.process_name]):
            _archive_tles(config, tle_file_list)


def _sort_index_file(tle_index):
    archive = False
    cmd = "sort -u +0b -3b {}".format(tle_index)
    try:
        status, returncode, stdout, stderr = run_shell_command(
            cmd, stdout_logfile="{}.sort1".format(tle_index))
    except:
        LOG.error("Failed running command: {} with return code: {}".format(cmd, returncode))
        LOG.error("stdout: {}".format(stdout))
        LOG.error("stderr: {}".format(stderr))
        return_status = False
    else:
        if returncode == 0 and os.path.exists("{}.sort1".format(tle_index)):
            cmd = "grep -v NaN {}.sort1".format(tle_index)
            try:
                status, returncode, stdout, stderr = run_shell_command(
                    cmd,
                    stdout_logfile="{}.sort".format(tle_index))
            except:
                LOG.error("Failed running command: {} with return code: {}".format(cmd, returncode))
                LOG.error("stdout: {}".format(stdout))
                LOG.error("stderr: {}".format(stderr))
                return_status = False
            else:

                try:
                    os.remove(tle_index)
                    os.remove("{}.sort1".format(tle_index))
                except OSError as e:
                    LOG.error("Failed to remove unsorted and duplicated index file: {} with {}"
                              .format(tle_index, e))
                else:
                    try:
                        os.rename("{}.sort".format(tle_index), tle_index)
                        archive = True
                    except:
                        LOG.error("Failed to rename sorted index file to original name.")
        else:
            LOG.error("Returncode other than 0: {} or tle index sort file does exists.".format(
                returncode, "{}.sort".format(tle_index)))
    return archive


def _archive_tles(config, tle_file_list):
    """Archive TLEs according to configuration."""

    archive_dict = {}
    archive_dict['tle_indir'] = config['aapp_processes'][config.process_name]['tle_indir']
    for tle_file_name in tle_file_list:
        for regex, test in tle_match_tests:
            m = re.match(regex, tle_file_name)
            if m:
                try:
                    archive_dict['timestamp'] = test(m)
                    tle_archive_dir = compose(
                        config['aapp_processes'][config.process_name]['tle_archive_dir'], archive_dict)
                    if not os.path.exists(tle_archive_dir):
                        try:
                            os.makedirs(tle_archive_dir)
                        except:
                            LOG.error("Failed to make archive dir: {}".format(tle_archive_dir))
                except ValueError:
                    LOG.exception('Failed in archive step...')
                    pass

                try:
                    copy(tle_file_name, tle_archive_dir)
                    LOG.debug("Copied {} to {}.".format(tle_file_name, tle_archive_dir))
                    archive = False
                except IOError as ioe:
                    LOG.error("Failed to copy TLE file: {} to archive: {} because {}".format(
                        tle_file_name, tle_archive_dir, ioe))
                    LOG.error("CWD: {}".format(os.getcwd()))
                else:
                    # 2021-01-20 added by Gerrit Holl <gerrit.holl@dwd.de>
                    # to ensure only the most greedy match is used and files
                    # don't get copied multiple times.  I hope it doesn't
                    # break anybody's workflow!
                    break


def do_tle_satpos(config, timestamp, satellite):

    return_status = True

    LOG.info("satpos files is stored under the dir_navigation/satpos")
    satpos_dir = os.path.join(os.environ['DIR_NAVIGATION'], "satpos")

    file_satpos = os.path.join(satpos_dir, "satpos_{}_{:%Y%m%d}.txt".format(satellite, timestamp))

    if (not os.path.exists(file_satpos) or os.stat(file_satpos).st_size == 0) and return_status:
        """Usage is: satpostle  [ -o] [-s satellite] [-S station] [-d start date] [-n number of days]
        [-i increment in seconds] [-c search criteria]

        -o -s -S -d -n -i –c are optional.

        If no parameter is specified as an option, defaults are : noaa14, Lannion, today 0h, 1.0, 120.0,
        n (n= nearest, p = preceding).

        The option -o specifies that the data will be stored in the file satpos_noaxx_yyyymmdd.txt.

        Output default is the standard output..
        """
        cmd = "satpostle -o -s {} -d {:%d/%m/%y} -n 1.2".format(satellite, timestamp)
        try:
            status, returncode, std, err = run_shell_command(cmd)
        except:
            LOG.error("Failed to run command: {}".format(cmd))
            return_status = False
        else:
            if returncode != 0:
                LOG.error("cmd: {} failed with returncode: {}".format(cmd, returncode))
                return_status = False
            elif not os.path.exists(file_satpos):
                LOG.error("file: {} does not exists after satpostle run.".format(file_satpos))
                return_status = False
    else:
        LOG.info("satpos file already there. Use this")

    return return_status
