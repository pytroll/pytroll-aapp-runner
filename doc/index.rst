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
Input arguments:
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
