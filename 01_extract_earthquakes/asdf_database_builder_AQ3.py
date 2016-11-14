# Script to write ASDF file from miniseed files also build the ASDF SQL data base
# using sqlalchemy (separate database for each station in a network)
# special script for the AQ3 survey and its' unique raw data structure




import pyasdf
from os.path import join, exists, basename
import glob
import sys
from obspy.core import UTCDateTime, read
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker
import time

code_start_time = time.time()

# =========================== User Input Required =========================== #

# Path to the data
data_path = '/media/obsuser/GA-ANU_TRAN/'

# IRIS Virtual Ntework name
virt_net = '_ANU'

# FDSN network identifier (2 Characters)
FDSNnetwork = 'S1'

# =========================================================================== #

path_XML = join(data_path, virt_net, FDSNnetwork, 'network_metadata/stnXML', FDSNnetwork + '.xml')
path_DATA = join(data_path, virt_net, FDSNnetwork, 'raw_DATA/')
path_quakeML = join(data_path, virt_net, FDSNnetwork, 'event_metadata/earthquake/quakeML/')

# Output ASDF file (High Performance Dataset) one file per network
ASDF_out = join(data_path, virt_net, FDSNnetwork, 'ASDF', FDSNnetwork + '.h5')

# Create/open the ASDF file
ds = pyasdf.ASDFDataSet(ASDF_out, compression="gzip-3")

# Add the station XML data to the ASDF file
ds.add_stationxml(path_XML)

quake_files = glob.glob(path_quakeML + '*xml*')

added_quakes_count = 0

if not ds.events.count() == 0:
    # remove earthquake info
    del ds.events

# Add earthquake quakeML data
for quake in quake_files:
    ds.add_quakeml(quake)
    added_quakes_count += 1

# Get a list of service directories
year_dir_list = glob.glob(path_DATA + '*')
year_dir_list.sort()

waveforms_added = 0

# iterate through service directories
for year in year_dir_list:

    print '\r Processing: ', basename(year)

    day_dir_list = glob.glob(year + '/*')
    day_dir_list.sort()

    # iterate through day directories
    for day_path in day_dir_list:

        day_name = basename(day_path)

        seed_files = glob.glob(join(day_path, '*BH*'))

        if seed_files == []:
            continue

        print '\r Working on Day: ', day_name

'''

        waveforms_added += len(seed_files)

        # Iterate through the miniseed files, fix the header values and add waveforms
        for _i, filename in enumerate(seed_files):

            print "\r     Parsing miniseed file ", _i + 1, ' of ', len(seed_files), ' ....',
            sys.stdout.flush()

            # read the miniseed file
            st = read(filename)

            # there will only be one trace in stream because the data is by channels
            tr = st[0]

            # Makes sure header is correct
            tr.stats.network = FDSNnetwork

            # Add waveform to the ASDF file
            ds.add_waveforms(tr, tag="mondo_raw_recording")

            #elif not query == None:
            #    pre_exist_count += 1


del ds
print '\n'
print("--- Execution time: %s seconds ---" % (time.time() - code_start_time))
print '--- Added ', waveforms_added, ' waveforms to ASDF file ---'
print '--- Added ', added_quakes_count, ' quakeML file(s) to ASDF file ---'

'''
