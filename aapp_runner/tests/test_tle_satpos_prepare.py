import pytest
import inspect
import pathlib
import unittest.mock
import logging

import datetime
from satpy.utils import debug_on; debug_on()
import sys
import os


def get_config(pth):
    """Get a config with tle_indir in pth."""
    # insert class from aapp_dr_runner because that's where config is
    here = pathlib.Path(inspect.getfile(inspect.currentframe()))
    sys.path.insert(0, str(here.parent.parent / "bin"))
    from aapp_dr_runner import AappL1Config
    return AappL1Config({'logging': {'log_rotation_days': 1,
    'log_rotation_backup': 30, 'logging_mode': 'DEBUG'},
    'aapp_static_configuration': {'decommutation_files': {'avhrr_file':
        'hrpt.l1b'}, 'supported_noaa_satellites': ['NOAA-18', 'NOAA-19'],
        'supported_metop_satellites': ['Metop-C', 'Metop-B', 'Metop-A'],
        'platform_name_aliases': {'NOAA-19': 'noaa19', 'NOAA-18': 'noaa18',
            'Metop-A': 'metop02', 'Metop-B': 'metop01', 'Metop-C': 'metop03',
            'METOP-A': 'metop02', 'METOP-B': 'metop01', 'METOP-C': 'metop03',
            'METOP A': 'metop02', 'METOP B': 'metop01', 'METOP C': 'metop03',
            'M01': 'metop01', 'M02': 'metop02', 'M03': 'metop03'},
        'satellite_sensor_name_aliases': {'avhrr': 'avhrr/3'},
        'tle_platform_name_aliases': {'NOAA-19': 'NOAA 19', 'noaa19':
        'NOAA 19', 'NOAA-18': 'NOAA 18', 'Metop-A': 'METOP-A', 'Metop-B': 'METOP-B',
        'Metop-C': 'METOP-C'}, 'sensor_name_converter': {'avhrr': 'avhrr/3',
            'avhrr/3': 'avhrr'}}, 'aapp_processes': {'test': {'description':
                'Test processing environment', 'name': 'test',
                'subscribe_topics': ['/file/noaa/avhrr', '/cat/metop/avhrr'],
                'tle_indir': str(pth), 'tle_archive_dir':
                '{tle_indir:s}/archive/tle-{timestamp:%Y%m%d}',
                'tle_infile_format': 'weather{timestamp:%Y%m%d%H%M}.tle',
                'download_tle_files': False, 'tle_download': [{'url':
                    'http://www.celestrak.com/NORAD/elements/weather.txt'},
                    {'url':
                        'http://oiswww.eumetsat.org/metopTLEs/html/data_out/latest_m01_tle.txt'}],
                    'tle_file_to_data_diff_limit_days': 3,
                    'locktime_before_rerun': 10, 'publish_sift_format':
                    '/aapp/avhrr', 'keep_orbit_number_from_message': True,
                    'aapp_prefix': '/opt/pytroll/AAPP',
                    'aapp_environment_file': 'ATOVS_ENV8', 'aapp_workdir':
                    '/tmp/pytroll/TMP', 'working_dir': '/tmp/pytroll/TMP',
                    'use_dyn_work_dir': True, 'aapp_outdir_base':
                    '/tmp/pytroll/OUTBOXES', 'aapp_outdir_format':
                    '{satellite_name:s}_{start_time:%Y%m%d_%H%M}_{orbit_number:05d}',
                    'passlength_threshold': 5, 'aapp_log_files_archive_dir':
                    '/opt/pytroll/pytroll_inst/log', 'aapp_log_outdir_format':
                    '{satellite_name:s}_{start_time:%Y%m%d}_{start_time:%H%M}_{orbit_number:05d}',
                    'aapp_log_files_archive_length': 10, 'do_ana_correction':
                    True, 'do_atovpp': False, 'do_avh2hirs': False,
                    'rename_aapp_compose':
                    '{data_type:s}_{satellite_name:s}_{start_time:%Y%m%d}_{start_time:%H%M}_{orbit_number:5d}.{data_level:s}',
                    'rename_aapp_files': [{'avhrr': {'aapp_file': 'hrpt.l1b',
                        'data_type': 'hrpt', 'data_level': 'l1b'}}]}},
                    'environment': 'test', 'station': 'unknown'} , "test")

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
    import aapp_runner.tle_satpos_prepare
    p = tmp_path / "tle"
    monkeypatch.setenv("AAPP_PREFIX", "invalid")
    monkeypatch.setenv("DIR_DATA_TLE", str(p))
    monkeypatch.setenv("DIR_NAVIGATION", "nav")
    config = get_config(p)
    mk_tle_files(p)
    from satpy.utils import debug_on; debug_on()
#    with caplog.at_level(logging.ERROR):
#        do_tleing(config, datetime.datetime(2021, 1, 19, 14, 8, 26), "noaa19")
#        assert "Failed running command" in caplog.text

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
            breakpoint()  # as the test function is except:-ing everything,
                          # I can't think of another way to get a test failure

    with unittest.mock.patch("aapp_runner.tle_satpos_prepare.run_shell_command") as atr:
       with caplog.at_level(logging.ERROR):
           atr.side_effect = fake_run_tleing
           aapp_runner.tle_satpos_prepare.do_tleing(config, datetime.datetime(2021, 1, 19, 14, 8, 26), "noaa19")
           assert caplog.text == ""
