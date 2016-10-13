# Script to ASDF file and use the quakeML (included) metdata to extract earthquake waveforms then store
# them in the ASDF file.

import pyasdf
from os.path import join
from obspy.core import Stream

# =========================== User Input Required =========================== #

#Path to the data
data_path = '/media/obsuser/seismic_data_1/'

#IRIS Virtual Ntework name
virt_net = '_GA_test'

# FDSN network identifier (2 Characters)
FDSNnetwork = 'XX'

# =========================================================================== #

# ASDF file (High Performance Dataset) one file per network
ASDF_in = join(data_path, virt_net, FDSNnetwork, 'ASDF', FDSNnetwork + '.h5')

# Open the ASDF file
ds = pyasdf.ASDFDataSet(ASDF_in)

print ds

# Access the event metadata
event_cat = ds.events

for _j, event in enumerate(event_cat):
    #print '\r  Extracting {0} of {1} Earthquakes....'.format(_j + 1, event_cat.count()),
    #sys.stdout.flush()

    # Get quake origin info
    origin_info = event.preferred_origin() or event.origins[0]

    qtime = origin_info.time.timestamp

    print '...'
    print 'qtime = ', origin_info.time, qtime

    for station in ds.ifilter(ds.q.starttime <= [qtime, qtime+3600]):#, qtime < ds.q.endtime, ds.q.tag == "raw_recording"):
        print station


    break







