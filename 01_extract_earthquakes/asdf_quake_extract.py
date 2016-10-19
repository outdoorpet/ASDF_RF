# Script to ASDF file and use the quakeML (included) metdata to extract earthquake waveforms then store
# them in the ASDF file.


import pyasdf
from os.path import join, exists
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy import or_, and_
from obspy.core import Stream, UTCDateTime

from obspy.taup import TauPyModel
from obspy.geodetics.base import gps2dist_azimuth, kilometer2degrees
import time

code_start_time = time.time()


# =========================== User Input Required =========================== #

#Path to the data
data_path = '/media/obsuser/seismic_data_1/'

#IRIS Virtual Ntework name
virt_net = '_GA_test'

# FDSN network identifier (2 Characters)
FDSNnetwork = 'XX'

# travel time model
model = TauPyModel(model="iasp91")

# =========================================================================== #

Base = declarative_base()

class Waveforms(Base):
    __tablename__ = 'waveforms'
    # Here we define columns for the table
    # Notice that each column is also a normal Python instance attribute.
    starttime = Column(Integer)
    endtime = Column(Integer)
    station_id = Column(String(250), nullable=False)
    tag = Column(String(250), nullable=False)
    full_id = Column(String(250), nullable=False, primary_key=True)


#function to create the ASDF waveform ID tag
def make_ASDF_tag(tr, tag):
    data_name = "{net}.{sta}.{loc}.{cha}__{start}__{end}__{tag}".format(
        net=tr.stats.network,
        sta=tr.stats.station,
        loc=tr.stats.location,
        cha=tr.stats.channel,
        start=tr.stats.starttime.strftime("%Y-%m-%dT%H:%M:%S"),
        end=tr.stats.endtime.strftime("%Y-%m-%dT%H:%M:%S"),
        tag=tag)
    return data_name


# Function to seperate the waveform string into seperate fields
def waveform_sep(ws):
    a = ws.split('__')
    starttime = int(UTCDateTime(a[1].encode('ascii')).timestamp)
    endtime = int(UTCDateTime(a[2].encode('ascii')).timestamp)

    # Returns: (station_id, starttime, endtime, waveform_tag)
    return (ws.encode('ascii'), a[0].encode('ascii'), starttime, endtime, a[3].encode('ascii'))



# ASDF file (High Performance Dataset) one file per network
ASDF_in = join(data_path, virt_net, FDSNnetwork, 'ASDF', FDSNnetwork + '.h5')

# Open the ASDF file
ds = pyasdf.ASDFDataSet(ASDF_in)

# Access the event metadata
event_cat = ds.events

# Get list of stations in ASDF file
sta_list = ds.waveforms.list()

files_added = 0

# Iterate through all stations in ASDF file
for _i, station_name in enumerate(sta_list):
    print '\r'
    print 'Working on Station: ', station_name, ' ....'
    # Get the helper object to access the station waveforms
    sta_helper = ds.waveforms[station_name]

    # SQL file for station
    SQL_in = join(data_path, virt_net, FDSNnetwork, 'ASDF', station_name.split('.')[1] + '.db')

    # if the SQL database doesn't exist for the station,
    # then there is no waveforms in the ASDF file for that station
    if not exists(SQL_in):
        print 'No Waveforms for station...'
        continue

    # Initialize the sqlalchemy sqlite engine
    engine = create_engine('sqlite:////' + SQL_in)

    Session = sessionmaker(bind=engine)
    session = Session()

    #for instance in session.query(Waveforms):
    #    print instance.full_id

    for _j, event in enumerate(event_cat):
        print '\r  Extracting {0} of {1} Earthquakes'.format(_j + 1, event_cat.count()),
        sys.stdout.flush()

        # Get quake origin info
        origin_info = event.preferred_origin() or event.origins[0]

        qtime = origin_info.time.timestamp

        #open up new obspy stream object
        st = Stream()

        for matched_waveform in session.query(Waveforms).\
                filter(or_(and_(Waveforms.starttime <= qtime, qtime < Waveforms.endtime),
                           and_(qtime <= Waveforms.starttime, Waveforms.starttime < qtime + 3600)),
                       Waveforms.full_id.like('%raw_recording%')):
            # Now extract all matched waveforms, concatenate using Obspy and write to ASDF with associated event tag
            # Read in the HDF5 matched waveforms into obspy stream (merge them together)
            #print matched_waveform.full_id

            # Open up the waveform into an obspy stream object
            # (this will join to previous waveform if there are multiple mSQL matches)
            st += sta_helper[matched_waveform.full_id]


        if not st.__nonzero__():
            continue

        # Attempt to merge all traces with matching ID'S in place
        st.merge()

        # Now call function to trim each trace so that the earthquake arrives at 900 seconds (15 mins)

        event_latitude = origin_info.latitude
        event_longitude = origin_info.longitude
        event_depth = origin_info.depth

        # Now calculate estimated arrival time of earthquake
        # first calculate distance and azimuthsm, returns (dist (m), faz, baz)
        dist_info = gps2dist_azimuth(event_latitude, event_longitude, sta_helper.coordinates['latitude'], sta_helper.coordinates['longitude'])

        #epicentral distance
        ep_dist = kilometer2degrees(dist_info[0]/1000.0)

        arrivals = model.get_ray_paths(source_depth_in_km=event_depth/1000.0, distance_in_degree=ep_dist, phase_list=["P", "pP", "S"])

        # now trim the st object to make 1 hr period so that the P -wave will arriva at 900 seconds into the trace
        trace_starttime = (origin_info.time + arrivals[0].time) - 900

        st.trim(starttime=trace_starttime, endtime=trace_starttime+3600, pad=True, fill_value=0)

        for tr in st:
            # The ASDF formatted waveform name [full_id, station_id, starttime, endtime, tag]
            waveform_info = waveform_sep(make_ASDF_tag(tr, 'extracted_unproc_quakes'))

            new_waveform = Waveforms(full_id=waveform_info[0], station_id=waveform_info[1], starttime=waveform_info[2],
                                     endtime=waveform_info[3], tag=waveform_info[4])

            query = session.query(Waveforms).filter(Waveforms.full_id == waveform_info[0]).one_or_none()

            if query == None:
                # Add the waveform info to the session
                session.add(new_waveform)
                session.commit()

                # add the traces back into ASDF file referenced to the quake
                ds.add_waveforms(tr, tag='extracted_unproc_quakes', event_id=event)

                files_added += 1



del ds
print '\n'
print("--- Execution time: %s seconds ---" % (time.time() - code_start_time))
print '--- Added ', files_added, ' earthquake waveforms to ASDF file ---'



