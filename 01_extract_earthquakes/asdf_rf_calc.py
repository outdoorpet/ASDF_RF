import pyasdf
from os.path import join
from obspy.core import Stream, Trace
from obspy.signal.rotate import rotate_ne_rt
import sys
from operator import itemgetter, attrgetter
from obspy.core import AttribDict
from obspy.geodetics.base import gps2dist_azimuth
from matplotlib.pyplot import plot

from rf import RFStream, rfstats

import time

code_start_time = time.time()


# =========================== User Input Required =========================== #

#Path to the data
data_path = '/media/obsuser/seismic_data_1/'

#IRIS Virtual Ntework name
virt_net = '_GA_ANUtest'

# FDSN network identifier (2 Characters)
FDSNnetwork = 'XX'

station = 'GA3'

# =========================================================================== #


#function to rotate N and E component data into Radial Transverse
def ZRT_rot_trace(tr_Z, tr_N, tr_E, baz_actual):

    DN_data = tr_N.data
    DE_data = tr_E.data

    # use rotate to rotate the north and east to radial and transverse
    rotated_data = rotate_ne_rt(DN_data, DE_data, baz_actual)

    tr_rad = Trace(data=rotated_data[0], header=tr_N.stats)
    tr_tran = Trace(data=rotated_data[1], header=tr_N.stats)

    # update channel names
    tr_rad.stats.channel = 'BHR'
    tr_tran.stats.channel = 'BHT'

    rot_st = Stream(traces = [tr_Z, tr_N, tr_E])

    return rot_st




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
                rf_stream.rf(method='P', trim=(-10,30), downsample=50, deconvolve='freq')

                all_rf_stream.extend(rf_stream)

    #if _j > 2:
    #    break


all_rf_stream.select(station=station, channel='BHT').plot_rf(fillcolors=(None, 'k'))







'''

# Access the event metadata
event_cat = ds.events

# Get list of stations in ASDF file
sta_list = ds.waveforms.list()

# Iterate through all stations in ASDF file
for _i, station_name in enumerate(sta_list):
    print '\r'
    print 'Working on Station: ', station_name, ' ....'
    # Get the helper object to access the station waveforms
    #sta_helper = ds.waveforms[station_name]

    #get list of all tags for station
    #tag_list = sta_helper.get_waveform_tags()

    #if 'extracted_unproc_quakes' in tag_list:
        # stream with all event traces for station (i.e. all components)
        #st = sta_helper.extracted_unproc_quakes
        #print st.__str__(extended=True)

    #for event_waveform in ds.ifilter(ds.q.station == station_name, )




    break

'''


del ds
print '\n'
print("--- Execution time: %s seconds ---" % (time.time() - code_start_time))