#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021 Pytroll

# Author(s):

#   Gerrit Holl <gerrit.holl@dwd.de>
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

"""Unit tests for ANA support functions
"""


import inspect
import pathlib
import unittest.mock
import logging

import datetime
import sys


def get_config(pth):
    """Get a config with ana configured."""
    # insert class from aapp_dr_runner because that's where config is
    here = pathlib.Path(inspect.getfile(inspect.currentframe()))
    sys.path.insert(0, str(here.parent.parent / "bin"))
    from aapp_dr_runner import AappL1Config
    conf = AappL1Config({
        'aapp_static_configuration':
        {'decommutation_files': {'avhrr_file': 'hrpt.l1b'}},
        'aapp_processes': {
            'test': {
                'working_dir': str(pth),
                'do_ana_correction': True}}},
            "test")
    conf["process_avhrr"] = True
    conf["platform_name"] = "scabb"
    conf["orbit_number"] = 42
    return conf


def test_ana(tmp_path, monkeypatch, caplog):
    """Test running ANA while mocking actual ANA."""
    import aapp_runner.do_ana_correction
    import posttroll.message
    ppp = tmp_path / "ana"
    ppp.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("DIR_NAVIGATION", str(ppp / "navdir"))
    monkeypatch.chdir(tmp_path)
    (ppp / "navdir" / "ana" / "reference_landmarks").mkdir(
        parents=True, exist_ok=True)
    config = get_config(ppp)
    msg = posttroll.message.Message(
        rawstr="pytroll://file/noaa/avhrr file pytroll@oflks333.dwd.de "
        "2021-01-20T16:28:42.969489 v1.01 application/json "
        '{"path": "", "start_time": "2021-01-19T14:08:26", '
        '"platform_name": "NOAA-19", "uri": '
        '"/data/pytroll/IN/HRPT/20210119140826_NOAA_19.hmf", '
        '"uid": "20210119140826_NOAA_19.hmf", "sensor": '
        '["avhrr/3"], "orig_platform_name": "NOAA_19"}')

    def fake_run_ana(cmd, stdin="", stdout_logfile=None, stderr_logfile=None):
        if cmd == "ana_lmk_loc -D hrpt.l1b":
            (ppp / "navdir" / "ana" / "lmkloc_scabb_20210119_1408}_42.txt"
             ).touch()
            return (True, 0, "", "")
        elif cmd == "l1bidf.exe":
            return (True, 0, "scabb 20210119 1408 42", "")
        elif cmd == "ana_estatt -s scabb -d 20210119 -h 1408 -n 00042":
            with open("hrpt.l1b", "wt") as fp:
                fp.write("Que j'aime a faire connaitre un nombre "
                         "utile aux sages.\n")
            return (True, 0, "", "")
        else:
            breakpoint()

    def fake_calib_avhr(conf, msg, timestamp):
        with open("hrpt.l1b", "at") as fp:
            fp.write("Now is the time for all good men to come to the "
                     "aid of the Party")
        return True

    with unittest.mock.patch(
        "aapp_runner.do_ana_correction.run_shell_command") as atr, \
            caplog.at_level(logging.ERROR):
        atr.side_effect = fake_run_ana
        fakemod = unittest.mock.MagicMock()
        monkeypatch.setitem(
            sys.modules, "aapp_runner.do_avhrr_calibration", fakemod)
        fakemod.do_avhrr_calibration = fake_calib_avhr
        aapp_runner.do_ana_correction.do_ana_correction(
            config,
            msg,
            datetime.datetime(2021, 1, 19, 14, 8, 26))
        assert caplog.text == ""
