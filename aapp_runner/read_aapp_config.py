# -*- coding: utf-8 -*-

# Copyright (c) 2021 Pytroll Community

# Author(s):

#   Adam.Dybbroe <adam.dybbroe@smhi.se>

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

"""Reading and checking yaml file configurations
"""

import copy
import os
from socket import gaierror, gethostbyaddr, gethostname


class StationError(RuntimeError):
    """Mismatching station name on commandline compared to config or wrong station."""
    pass


class EnvironmentError(RuntimeError):
    """Error in the specified runtime environment."""
    pass


class AappProcessKeyMissing(RuntimeError):
    """Some AAPP processing key is missing."""
    pass


class AappWorkDirNotSet(RuntimeError):
    """AAPP Working dir not set."""
    pass


class ConfigFileOptionsError(RuntimeError):
    """Error on configuration file options."""
    pass


class StaticConfigError(RuntimeError):
    pass


MANDATORY = 'm'

SUPPORTED_STATIONS = ['kumpula', 'helsinki', 'norrkoping', 'nkp', 'oslo']

MANDATORY_CONFIG_VARIABLES = [
    'description',
    'name',
    'aapp_prefix',
    'aapp_environment_file',
    'aapp_outdir_base',
    'aapp_outdir_format',
    'subscribe_topics',
    'publish_sift_format',
    'aapp_log_files_archive_dir',
    'aapp_log_files_archive_length',
    'aapp_log_outdir_format',
    'rename_aapp_compose',
    'rename_aapp_files',
]

OPTIONAL_CONFIG_VARIABLES = [
    'aapp_workdir',
    'working_dir',
    'use_dyn_work_dir',
    'keep_orbit_number_from_message',
    'do_ana_correction',
    'do_atovpp',
    'do_avh2hirs',
    'instrument_skipped_in_processing',
    'passlength_threshold',
    'monitor_message',
    'message_providing_server',
    'custom_aapp_dir_navigation',
    'locktime_before_rerun',
    'tle_archive_dir',
    'services',
    'dir_navigation',
    'download_tle_files',
    'tle_indir',
    'tle_infile_format',
    'tle_file_to_data_diff_limit_days',
    'tle_archive_dir'
]

#
# Variables that are directories
# ('variable_name', 'permission: r, rw', 'depends on variable_name')
#
valid_dir_permissions = [
    ('aapp_prefix', 'r', MANDATORY),
    ('aapp_outdir_base', 'rw', MANDATORY),
    ('aapp_log_files_archive_dir', 'rw', MANDATORY)
]

valid_readable_files = ['aapp_run_noaa_script',
                        'aapp_run_metop_script',
                        'alleph_script',
                        'tle_script']

VALID_SERVERS = [
    ('servername', 'host'),
    ('dataserver', 'server')
]

STATIC_VARS = [
    'decommutation_files',
    'supported_noaa_satellites',
    'supported_metop_satellites',
    'platform_name_aliases',
    'satellite_sensor_name_aliases'
]

# Config variable will be replaced by following config variable
# if the variable (first one) is empty in config file


VALID_CONFIGURATION = {
    'supported_stations': SUPPORTED_STATIONS,
    'mandatory_config_variables': MANDATORY_CONFIG_VARIABLES,
    'valid_dir_permissions': valid_dir_permissions,
    'valid_readable_files': valid_readable_files,
    'valid_servers': VALID_SERVERS,
    'optional_config_variables': OPTIONAL_CONFIG_VARIABLES
}


def check_station(config, valid_stations):
    """
    Check if station can be used without modifications.
    """
    if config['station'] in valid_stations:
        return True
    return False


def check_hostserver(host):
    """
    Check the host servername
    """
    current_host = gethostname()
    if current_host == host:
        return True
    return False


def check_dataserver(server):
    """
    Check the dataserver by address
    """
    try:
        name, dummy, addresslist = gethostbyaddr(server)
        if server == name or server == addresslist[0]:
            return True
    except gaierror:
        return False
    return False


def check_bool(value):
    """
    Check if value is boolean
    """
    return type(value) is bool


def check_dir(directory, test):
    """ First check if directory exists and has access
    Second test if directory is writable
    Print error message if fails
    """
    # print("check_dir: test is ", test)
    if test == 'r' or test == 'rw':
        if not (os.path.exists(directory) or
                os.access(directory, os.R_OK)):
            print("ERROR: Directory doesn't exist or " +
                  "it is not readable!:" +
                  directory)
            return False
        if test == 'rw':
            # print("RW test")
            test_file = "tmp_write_test.tmp"
            filename = os.path.join(directory, test_file)
            try:
                test = open(filename, "w")
                test.close()
                os.remove(filename)
            except IOError as ioe:
                print("ERROR: Cannot write to directory! {}:{}".format(directory, ioe))
                return False
    else:
        print("ERROR: Unknown test.")
    return True


def check_dir_permissions(config, dir_permissions):
    """
    Check if directories are as defined in dir_permissions[]
    """

    test_results = []

    for dirname, perm, required in dir_permissions:
        if required == MANDATORY:
            check = check_dir(config[dirname], perm)
        else:
            check = True

        test_results.append(check)

    if all(test_results):
        return True

    print("Number of failures: ", len(test_results) - sum(test_results))
    return False


def check_file(filename):
    '''Checking is file exisiting and readable'''
    return os.path.isfile(filename) and os.access(filename, os.R_OK)


def check_readable_files(config, files_to_check):
    """
    Check files_to_check[] are readable
    """
    test_results = []
    for filename in files_to_check:
        check = check_file(config[filename])
        test_results.append(check)
        print(filename, " is ", check)

    if all(test_results):
        return True

    print("Number of failures: ", len(test_results) - sum(test_results))
    return False


def check_config_file_options(config, valid_config=None):
    """
    Check input config dictionary
    """

    dir_permissions = valid_config['valid_dir_permissions']

    print("Checking directories...")
    if not check_dir_permissions(config, dir_permissions):
        print("Checking directories failed.")
        return False

    return True


def check_static_configuration(config):
    """
    Check if all the needed static configurations variables
    are available
    """

    if 'aapp_static_configuration' not in config:
        print("Missing aapp_static_configuration in config. Can not continue.")
        return False
    else:
        _static_config = {}
        for item in STATIC_VARS:
            try:
                _static_config[item] = config['aapp_static_configuration'][item]
            except KeyError:
                print("{} Is missing in the aapp_static_configuration. Please add.".format(item))
                raise

    return True


def load_config_from_file(filename):
    """Load the yaml config from file, given the file-path"""

    # config = SafeConfigParser()
    import yaml
    with open(filename, 'r') as stream:
        try:
            config = yaml.load(stream, Loader=yaml.FullLoader)
            # import pprint
            # print(type(config))
            # pp = pprint.PrettyPrinter(indent=4)
            # pp.pprint(config)
        except yaml.YAMLError as exc:
            print("Failed reading yaml config file: {} with: {}".format(filename, exc))
            raise yaml.YAMLError

    return config


class AappL1Config(object):

    """
    Container for the configuration for AAPP
    """

    def __init__(self, config, process_name):
        """
        Init the config
        """
        self.orig_config = copy.deepcopy(config)
        self.config = config
        self.process_name = process_name
        self.job_register = {}
        self.local_env = {}

    def __getitem__(self, key):
        try:
            _it = self.config[key]
        except KeyError:
            _it = None
        return _it

    def __setitem__(self, key, value):
        self.config[key] = value

    def reset(self):
        """
        Clear/reset dynamic configuration
        """
        self.config = {}
        self.config = copy.deepcopy(self.orig_config)
        self.local_env = {}
        self.local_env = os.environ.copy()

    def add_process_config_paramenter(self, config_key, config_value):
        """
        Add a config parameter to the running config
        """
        self.config['aapp_processes'][
            self.process_name][config_key] = config_value

    def get_parameter(self, key):
        try:
            _it = self.config['aapp_processes'][self.process_name][key]
        except KeyError:
            _it = None
        return _it


class AappRunnerConfig(object):
    """The AAPP runner configurations"""

    def __init__(self, filename, station, env, valid_config=None):
        """Initialize the aapp-runner config object"""
        self.filename = filename
        self.station = station
        self.environment = env
        self._configuration = {}
        self.config_opts = None
        if valid_config is None:
            self.valid_config = VALID_CONFIGURATION
        else:
            self.valid_config = valid_config

        self._load()
        self._check_aapp_process_key_in_config()
        self._check_station()
        self._check_environment()
        self._check_aapp_workdir()

        self.config['station'] = self.station
        self.config['environment'] = self.environment

    def _load(self):
        """Load the configuration from the yaml file."""
        self.config = load_config_from_file(self.filename)

    def _check_aapp_process_key_in_config(self):
        """Check the yaml file config has the key aapp_process"""
        if 'aapp_processes' not in self.config:
            raise AappProcessKeyMissing("Can not find main section 'aapp_processes' in "
                                        "yaml file. Please check your config.")

    def _check_station(self):
        """Check that station in configuration is consistent and supported"""
        if 'station' in self.config:
            if not self.config['station'] == self.station:
                raise StationError("Station from command line: {} "
                                   "does not match with configured station: {}".format(self.station,
                                                                                       self.config['station']))
        if self.station not in SUPPORTED_STATIONS:
            print("Warning: given station: {} not in supported_stations list.".format(self.station))

    def _check_environment(self):
        """Check the environment provided is consistent with the configuration"""

        if 'environment' in self.config:
            if self.config['environment'] != self.environment:
                msg = "Environment from command line: {} "
                "does not match with configured environment: {}".format(self.environment,
                                                                        self.config['environment'])
                raise EnvironmentError(msg)
        if self.environment not in self.config['aapp_processes']:
            raise EnvironmentError("Environment {} not configured in config. "
                                   "Please check.".format(self.environment))

    def _check_aapp_workdir(self):
        """Check the aapp_process-env section has aapp_workdir defined"""
        config_opts = self.config['aapp_processes'][self.environment]
        if 'aapp_workdir' not in config_opts and 'working_dir' not in config_opts:
            raise AappWorkDirNotSet("You must give either 'aapp_workdir' or 'working_dir in config.")

    def _check_optional_and_mandatory_configuration(self):
        """Check the configuration for mandatory and optional variables"""

        optional_config_variables = self.valid_config['optional_config_variables']
        mandatory_config_variables = self.valid_config['mandatory_config_variables']

        self._configuration['station'] = self.config['station']
        self._configuration['environment'] = self.config['environment']

        config_opts = self.config['aapp_processes'][self.config['environment']]

        # Check for mandatory
        for item in mandatory_config_variables:
            try:
                self._configuration[item] = config_opts[item]
            except KeyError as err:
                print("{} is missing. Please, check your config file {}".format(err.args, self.filename))
                raise KeyError

        # Check if rest of variables are in optional ( and mandatory )
        for item in config_opts:
            if item in optional_config_variables:
                self._configuration[item] = config_opts[item]
            elif item not in mandatory_config_variables:
                print("Variable {} is not recognised as a mandatory nor optional config variable."
                      "This will not be used in the processing.".format(item))

    def check_config(self):
        """Check everything with the configuration is consistent and okay"""

        self._check_optional_and_mandatory_configuration()

        if not check_config_file_options(self._configuration, self.valid_config):
            raise ConfigFileOptionsError('File options not okay"')

        if not check_static_configuration(self.config):
            raise StaticConfigError('Static config not okay')


if __name__ == "__main__":
    station_name = ""
    environment = "xl-band"
    aapp_run_config = AappRunnerConfig("../examples/aapp-processing.yaml-template", station_name, environment)
    aapp_run_config.check_config()
    run_options = aapp_run_config.config
