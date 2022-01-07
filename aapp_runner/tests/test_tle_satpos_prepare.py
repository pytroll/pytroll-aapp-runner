#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2015 - 2022 Pytroll developers

# Author(s):

#   Adam Dybbroe <Firstname.Lastname at smhi.se>
#   Trygve Aspenes <trygveas@met.no>
#   Gerrit Holl
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

"""Testing the preparation of TLE and Satpos files.
"""

import datetime
import inspect
import logging
import os
import pathlib
import sys
import unittest.mock
from glob import glob

from aapp_runner.tle_satpos_prepare import fetch_realtime_tles


def get_config(pth):
    """Get a config with tle_indir in pth."""
    # insert class from aapp_dr_runner because that's where config is
    here = pathlib.Path(inspect.getfile(inspect.currentframe()))
    sys.path.insert(0, str(here.parent.parent / "bin"))
    from aapp_dr_runner import AappL1Config
    return AappL1Config({
        'aapp_processes': {
            'test': {
                'tle_indir': str(pth),
                'tle_archive_dir':
                    '{tle_indir:s}/archive/tle-{timestamp:%Y%m%d}',
                'tle_infile_format': 'weather{timestamp:%Y%m%d%H%M}.tle',
                'download_tle_files': False,
                'tle_file_to_data_diff_limit_days': 3000,
                'working_dir': str(pth)}},
    }, "test")


def mk_tle_files(pth):
    """Make some fake TLE files."""
    pth.mkdir(exist_ok=True, parents=True)
    for f in ["weather202101180325.tle",
              "weather202101180008.tle",
              "weather202101190616.tle",
              "weather202101170000.tle-0",
              "weather202101160000.tle-0",
              "weather202101190000.tle-0"]:
        (pth / f).touch()


def test_tle(tmp_path, monkeypatch, caplog):
    """Test that TLEs are correctly archived.

    Test that the archival of TLEs is as expected, with no exceptions raised
    and no spurious directories with wrong names being created.  Related to
    GH#10.
    """
    import aapp_runner.tle_satpos_prepare
    p = tmp_path / "tle"
    monkeypatch.setenv("AAPP_PREFIX", "invalid")
    monkeypatch.setenv("DIR_DATA_TLE", str(p))
    monkeypatch.setenv("DIR_NAVIGATION", "nav")
    config = get_config(p)
    mk_tle_files(p)

    def fake_run_tleing(cmd, stdin="", stdout_logfile=None):
        if cmd == "tleing.exe":
            data_dir = stdin.split("\n")[0]
            assert data_dir == str(p)
            (p / "tle_noaa19.index").touch()
            return (0, 0, "", "")
        elif stdout_logfile is not None:
            pathlib.Path(stdout_logfile).touch()
            return (0, 0, "", "")
        else:
            # as the tested function is except:-ing everything,
            # I can't think of another way to get a test failure
            breakpoint()

    with unittest.mock.patch(
            "aapp_runner.tle_satpos_prepare.run_shell_command") as atr:
        with caplog.at_level(logging.ERROR):
            atr.side_effect = fake_run_tleing
            aapp_runner.tle_satpos_prepare.do_tleing(
                config, datetime.datetime(2021, 1, 19, 14, 8, 26),
                "noaa19")
            assert caplog.text == ""
    exp_d = (p / "archive" / "tle-20210119")
    assert exp_d.exists()
    assert exp_d.is_dir()
    # confirm no other directories created
    assert len(list(exp_d.parent.iterdir())) == 1
    assert [f.name for f in exp_d.iterdir()] == ["weather202101190616.tle"]


def test_fetch_realtime_tles(tmp_path):
    """Test fetching TLE files and copy them in under the data dir structure as expected by AAPP."""
    mypath = tmp_path / "input"
    mk_tle_files(mypath)

    outpath = tmp_path / "output"
    outpath.mkdir()
    exp_subdir = os.path.join(outpath, "2021_01")

    tle_infile_format = 'weather{timestamp:%Y%m%d%H%M}.tle'

    fetch_realtime_tles(mypath, outpath, tle_infile_format)

    assert os.path.exists(exp_subdir)
    assert os.path.isdir(exp_subdir)
    # confirm no other directories created
    assert len(list(outpath.iterdir())) == 1

    result_files = [os.path.basename(f) for f in glob(os.path.join(exp_subdir, 'weather*'))]
    expected_file_names = ["weather202101180325.tle",
                           "weather202101180008.tle",
                           "weather202101190616.tle"]

    for item in result_files:
        assert item in expected_file_names

    assert len(result_files) == len(expected_file_names)
