# Script to ASDF file and use the quakeML (included) metdata to extract earthquake waveforms then store
# them in the ASDF file.

import pyasdf
from os.path import join
import time

code_start_time = time.time()

# =========================== User Input Required =========================== #

#Path to the data
data_path = '/media/obsuser/GA-ANU_TRAN/'

#IRIS Virtual Ntework name
virt_net = '_ANU'

# FDSN network identifier (2 Characters)
FDSNnetwork = 'S1'

# =========================================================================== #

# ASDF file (High Performance Dataset) one file per network
ASDF_in = join(data_path, virt_net, FDSNnetwork, 'ASDF', FDSNnetwork + '.h5')

# Open the ASDF file
ds = pyasdf.ASDFDataSet(ASDF_in)

# Access the event metadata
event_cat = ds.events

def extract_events(st, inv):
    for _j, event in enumerate(event_cat):
        #print '\r  Extracting {0} of {1} Earthquakes....'.format(_j + 1, event_cat.count()),
        #sys.stdout.flush()

        # Get quake origin info
        origin_info = event.preferred_origin() or event.origins[0]

        qtime = origin_info.time.timestamp

        #print '...'
        #print 'qtime = ', origin_info.time, qtime

        #extract 30 mins before and 2 hours after earthquake origin time for station (mpi)
        st = ds.get_waveforms(network=FDSNnetwork, station='*', location='*', channel='*',
                              starttime=origin_info.time-1750, endtime=origin_info.time+2*3600,
                              tag='mondo_raw_recording')

        if not st.__nonzero__():
            continue

        # add the st back into ASDF file referenced to the quake
        ds.add_waveforms(st, tag='extracted_unproc_quakes', event_id=event)

        break


del ds
print '\n'
print("--- Execution time: %s seconds ---" % (time.time() - code_start_time))



