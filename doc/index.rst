===================
PyTROLL AAPP runner
===================

Intended audience
-----------------
The PyTROLL AAPP runner processes with help of the AAPP, Direct Broadcast satellite data from POES and METOP. This is done within a PyTROLL context using posttroll passing information into the runner what data to process.

If you dont have AAPP and posttroll installed and don't have access to DB data (POES and METOP Eumetsat/Eumetcast EARS HRPT and EPS can also be used) then this package is not for you.

Requirements
------------
You need to have latest versjon (7.XX) of AAPP installed. See `NWPSAF`_.

.. _NWPSAF: https://www.nwpsaf.eu/site/software/aapp/

Running arguments:
------------------
-c ( --config_file)
   Valid yaml configuration file 

-s ( --station)
   Name of the station doing the processing
   e.g. osl for Oslo, Norway

-e ( --environment)
   Name of the aapp_processes section in the yaml file to read configuration from.
   The configuration file can contain several environments, e.g. dev, test oper.

-v ( --verbose)
   Override logging level given in the configuration file to level DEBUG.

-l ( --log)
   Sprecify the file to log to, else write to stdout

Configuration
-------------

See this `aapp_config.yaml-template`_ for an example.

.. _aapp_config.yaml-template: https://github.com/pytroll/pytroll-aapp-runner/blob/develop/examples/aapp-processing.yaml-template

Logging
^^^^^^^
log_rotation_days
   How many days before rotation (if log_file option is given)

log_rotation_backup
   How many rotations to keep ( if log_file option is given)

logging_mode
   Logging mode (if verbose is given logging_mode is overriden)

Static configuration
^^^^^^^^^^^^^^^^^^^^
This section contains a lot of information to the processes how to handle various names of platforms and sensors. An how to translate the in the processing. A platform can take many names, like: M01, Metop-B, METOP-B, metopb and so on. Depending of your source of the data adjust this section accordingly. The example listed above is used at Met Norway.

The point of having this configuration here is to avoid to edit the code to make adjustments like this.

aapp_processes
^^^^^^^^^^^^^^
<environment>
   Name of the environment, e.g. test, dev or oper

description
   A nice description of this enviroment

name
   A name of the environment

subscribe_topics
   posttroll topics to subscribe to, must be a list

tle_indir
   Basedir of your tle files.

tle_infile_format
   format of your tle files. Can contain sift encoding

tle_archive_dir
   Where to archive your TLEs

tle_file_to_data_diff_limit_days
   Search for the closest TLE file based on the TLE file format time stamp
   Maximum difference in days is the value configured here.

tle_download
   List of TLE urls in a dictionary to download and append to a tle file.
   The order of the list matters. The first element is in top of the tle
   file and so on.
   A download is only triggered when a tle file of the data timestamp is
   not found and tle_file_to_data_diff_limit_days is not given or the search 
   does not find any tle files.
   Only latest tle files are downloaded, so if you process old data this will not work.
   Valid keys in the dictionary is: url.
   For space-track aditional keys are valid: timeout, user, passwd and catalogue.
   Catalogue is a comma separated string with internatinal satellite numbers.

locktime_before_rerun
   Minutes to lock for similar passes in minutes

publish_sift_format
   posttroll topic to be used when publishing the results. Can contain sift encoding

aapp_prefix
   Base dir of your AAPP installation

aapp_environment_file
   Your AAPP environment file. This is typically ATOVS_ENV7 in you aapp_prefix dir.

aapp_workdir
   The directory where AAPP writes all working files. This or working_dir must be given.

working_dir
   The directory where AAPP writes all working files. This or aapp_workdir must be given.
   
use_dyn_work_dir
   If aapp_workdir is given and this variable is set to True,
   add a random named temporary directory below aapp_wordir
   This is handy if more than one dataset are processed simultaniously

aapp_outdir_base
   AAPP base dir of all the final output data

aapp_outdir_format
   Name of the subdir under aapp_outdir_base where the output data is stored. Can contain sift encoding.

passlength_threshold
   Minimums lenght of a dataset in minutes

aapp_log_files_archive_dir
   Base dir of an archive of the AAPP logs.

aapp_log_files_archive_length
   How many days to keep the AAPP logs before cleanup

message_providing_server
   If you use several servers with posttroll multicast,
   you can give this to specify which server you want to receive messages from.

do_ana_correction
   Do ANA correction. ANA is a separate software package not included in AAPP.

do_atovpp
   Add AAPP processing of level 1c TOVS/ATOVS to level 1d.

do_avh2hirs
   From the AAPP software documenation: 
   applies the calibration coefficients (calculated by AVHRCL) to AVHRR counts and 
   converts radiance into brightness temperature, maps AVHRR data in HIRS FOV, and
   makes the cloud mask MAIA_2.1 for AAPP version 3 and later) in the HIRS ellipse 
   for contaminated pixels discrimination. At the end of this procedure, a level
   1d file exists (HIRS level 1d). 

instrument_skipped_in_processing
   This is a list of satellite names, each with a list of sensor to skip to process.
   Can be handy if you want to skip bad sensors on a specific platform.

rename_aapp_compose
   Tells the runner how to rename the processed data from AAPP to more meaningsfull names.
   This can be a simle file name, but then all sensors from each data level will be renamed
   to the same. So I would say you must use a sift formated variable. See rename_aapp_files
   for details.

rename_aapp_files
   Is a list of dictionaries how to rename the various AAPP processed data. This dictionary
   is applied to rename_aapp_compose.

monitor_message:
  Not implemented.
