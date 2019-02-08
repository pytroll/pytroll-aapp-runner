import os

from socket import gethostname, gethostbyaddr, gaierror

MANDATORY = 'm'

supported_stations = ['kumpula', 'helsinki', 'norrkoping', 'nkp', 'oslo']

mandatory_config_variables = [
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
    'rename_aapp_compose',
    'rename_aapp_files',
]

optional_config_variables = [
    'aapp_workdir',
    'working_dir',
    'use_dyn_work_dir',
    'keep_orbit_number_from_message',
    'do_ana_correction',
    'do_atovpp',
    'instrument_skipped_in_processing',
    'passlength_threshold',
    'monitor_message',
    'message_providing_server',
    'custom_aapp_dir_navigation',
    'locktime_before_rerun',
    'tle_archive_dir',
    'services',
    'dir_navigation',
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

valid_servers = [
    ('servername', 'host'),
    ('dataserver', 'server')
]

static_vars = [
    'decommutation_files',
    'supported_noaa_satellites',
    'supported_metop_satellites',
    'platform_name_aliases',
    'satellite_sensor_name_aliases'
]

# Config variable will be replaced by following config variable
# if the variable (first one) is empty in config file


VALID_CONFIGURATION = {
    'supported_stations': supported_stations,
    'mandatory_config_variables': mandatory_config_variables,
    'valid_dir_permissions': valid_dir_permissions,
    'valid_readable_files': valid_readable_files,
    'valid_servers': valid_servers,
    'optional_config_variables': optional_config_variables
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
            if config[required]:
                check = check_dir(config[dirname], perm)

            # else:
            #     print("%s %s %s %s %s" % ("ERROR: ", dirname,
            #                               "requires", required,
            #                               "but it's NOT defined!")
            #     check = False
        test_results.append(check)

    if all(test_results):
        return True
    else:
        print("Number of failures: ", len(test_results) - sum(test_results))
        return False


def check_file(filename):
    '''Checking is file exisiting and readable'''
    return os.path.isfile(filename) and os.access(filename, os.R_OK)


def check_readable_files(config, files_to_check):
    """
    Check files_to_check[] are readable
    """
#    print("------------------------------")
    test_results = []
    for filename in files_to_check:
        check = check_file(config[filename])
        test_results.append(check)
        print(filename, " is ", check)

    if all(test_results):
        return True
    else:
        print("Number of failures: ", len(test_results) - sum(test_results))
        return False


def check_config_file_options(config, valid_config=None):
    """
    Check input config dictionary
    """

    dir_permissions = valid_config['valid_dir_permissions']
    readable_files = valid_config['valid_readable_files']
    servers = valid_config['valid_servers']

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
        for item in static_vars:
            try:
                _static_config[item] = config['aapp_static_configuration'][item]
            except KeyError as ke:
                print("{} Is missing in the aapp_static_configuration. Please add.".format(item))
                raise

    return True


def read_config_file_options(filename, station, env, valid_config=None):
    """
    Read and checks config file
    If ok, return configuration dictionary
    """

    # config = SafeConfigParser()
    import yaml
    with open(filename, 'r') as stream:
        try:
            config = yaml.load(stream)
            import pprint
            print(type(config))
            pp = pprint.PrettyPrinter(indent=4)
            pp.pprint(config)
        except yaml.YAMLError as exc:
            print("Failed reading yaml config file: {} with: {}".format(filename, exc))
            raise yaml.YAMLError

    if 'aapp_processes' not in config:
        print("Can not find main section 'aapp_processes' in yaml file. Please check your config.")
        return False

    if valid_config is None:
        valid_config = VALID_CONFIGURATION

    # Config variable will be replaced by following config
    optional_config_variables = valid_config['optional_config_variables']
    mandatory_config_variables = valid_config['mandatory_config_variables']

    configuration = {}
    configuration['station'] = station
    configuration['environment'] = env

    if 'environment' in config:
        if not config['environment'] == env:
            print("Environment from command line: {} "
                  "does not match with configured environment: {}".format(env, config['environment']))
            return False
    else:
        config['environment'] = env
    if config['environment'] not in config['aapp_processes']:
        print("Environment {} not configured in config. Please check.".format(config['environment']))
        return False
    if 'station' in config:
        if not config['station'] == env:
            print("Station from command line: {} "
                  "does not match with configured station: {}".format(env, config['station']))
            return False
    else:
        config['station'] = station
        if not check_station(config, supported_stations):
            print("Warning: given station: {} not in supported_stations list.".format(config['station']))

    config_opts = config['aapp_processes'][configuration['environment']]
    # Check for mandatory
    for item in mandatory_config_variables:
        try:
            configuration[item] = config_opts[item]
        except KeyError as err:
            print("{} is missing. Please, check your config file {}".format(err.args, filename))
            raise KeyError

    # Check if rest of variables are in optional ( and mandatory )
    for item in config_opts:
        if item in optional_config_variables:
            configuration[item] = config_opts[item]
        elif item not in mandatory_config_variables:
            print("Variable {} is not recognised as a mandatory nor optional config variable."
                  "This will not be used in the processing.".format(item))

    if 'aapp_workdir' not in config_opts and 'working_dir' not in config_opts:
        print("You must give either 'aapp_workdir' or 'working_dir in config.")
        return False

    if not check_config_file_options(configuration, valid_config):
        return None

    if not check_static_configuration(config):
        return False

    return config

if __name__ == "__main__":
    station_name = ""
    environment = "xl-band"
    run_options = read_config_file_options("aapp-processing.yaml",
                                           station_name, environment)
