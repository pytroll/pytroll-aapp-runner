import inspect
import pathlib
import unittest.mock
import logging

import datetime
import sys


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
    # make sure we're failing by not knowing tleing.exe
    with caplog.at_level(logging.ERROR):
        aapp_runner.tle_satpos_prepare.do_tleing(
                config, datetime.datetime(2021, 1, 19, 14, 8, 26), "noaa19")
        assert "Failed running command" in caplog.text

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
    assert exp_d.isdir()
    # confirm no other directories created
    assert len(list(exp_d.parent.iterdir())) == 1
    assert list(exp_d.iterdir()) == ["weather202101190616.tle"]
