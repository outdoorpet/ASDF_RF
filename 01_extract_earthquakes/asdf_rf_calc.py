import pyasdf
from os.path import join
from obspy.core import Stream, Trace
from obspy.signal.rotate import rotate_ne_rt
import sys
from operator import itemgetter, attrgetter
from obspy.core import AttribDict
from obspy.geodetics.base import gps2dist_azimuth
from matplotlib.pyplot import plot

from rf import RFStream, rfstats, get_profile_boxes

import time

code_start_time = time.time()


# =========================== User Input Required =========================== #

#Path to the data
data_path = '/media/obsuser/GA-ANU_TRAN/'

#IRIS Virtual Ntework name
virt_net = '_ANU'

# FDSN network identifier (2 Characters)
FDSNnetwork = 'S1'

station = 'AQ3A1'

# =========================================================================== #


# ASDF file (High Performance Dataset) one file per network
ASDF_in = join(data_path, virt_net, FDSNnetwork, 'ASDF', FDSNnetwork + '.h5')

# Open the ASDF file
ds = pyasdf.ASDFDataSet(ASDF_in)

event_cat = ds.events

all_rf_stream = RFStream()

for _j, event in enumerate(event_cat):

    print '\r  Calculating Receiver Functions for {0} of {1} Earthquakes'.format(_j + 1, event_cat.count()),
    sys.stdout.flush()

    # Get quake origin info
    origin_info = event.preferred_origin() or event.origins[0]

    for filtered_waveforms in ds.ifilter(ds.q.event == event, ds.q.station == station):
        st = filtered_waveforms.extracted_unproc_quakes
        inv = filtered_waveforms.StationXML


        if st.__nonzero__():


            rf_stream = RFStream(st)

            stats = rfstats(station=inv[0][0], event=event, phase='P', dist_range=(30,90))

            # Stats might be none if epicentral distance of earthquake is outside dist_range
            if not stats == None:

                for tr in rf_stream:
                    tr.stats.update(stats)

                rf_stream.filter('bandpass', freqmin=0.05, freqmax=1.)
                rf_stream.rf(method='P', trim=(-10,30), downsample=50, deconvolve='time')

                rf_stream.moveout()
                rf_stream.ppoints(pp_depth=30)

                all_rf_stream.extend(rf_stream)



all_rf_stream.sort(keys=['distance'])
all_rf_stream.select(station=station, channel='BHL').plot_rf(fillcolors=(None, 'k'))

#all_rf_stream.profile(boxes=get_profile_boxes(latlon0=(-35.344265, 149.159866), azimuth=10, bins=(0,10,20,30)))

#all_rf_stream.plot_profile()

del ds
print '\n'
print("--- Execution time: %s seconds ---" % (time.time() - code_start_time))