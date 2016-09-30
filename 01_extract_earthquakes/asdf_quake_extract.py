# Script to ASDF file and use the quakeML (included) metdata to extract earthquake waveforms then store
# them in the ASDF file.

import pyasdf
from os.path import expanduser, join
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy import or_, and_
from obspy.core import Stream

# =========================== User Input Required =========================== #

#Path to the data
data_path = '/media/obsuser/seismic_data_1/'

#IRIS Virtual Ntework name
virt_net = '_GA_test'

# FDSN network identifier (2 Characters)
FDSNnetwork = 'XX'

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

# ASDF file (High Performance Dataset) one file per network
ASDF_in = join(data_path, virt_net, FDSNnetwork, 'ASDF', FDSNnetwork + '.h5')

# Open the ASDF file
ds = pyasdf.ASDFDataSet(ASDF_in)

# Access the event metadata
event_cat = ds.events

# Get list of stations in ASDF file
sta_list = ds.waveforms.list()

# Iterate through all stations in ASDF file
for _i, station_name in enumerate(sta_list):
    print 'Working on Station: {0}'.format(sta_list[_i])

    # Get the helper object to access the station waveforms
    sta_helper = ds.waveforms[station_name]

    # SQL file for station
    SQL_in = join(data_path, virt_net, FDSNnetwork, 'ASDF', station_name.split('.')[1] + '.db')

    # Initialize the sqlalchemy sqlite engine
    engine = create_engine('sqlite:////' + SQL_in)

    Session = sessionmaker(bind=engine)
    session = Session()

    #for instance in session.query(Waveforms):
    #    print instance.full_id

    for _j, event in enumerate(event_cat):
        #print '\r  Extracting {0} of {1} Earthquakes....'.format(_j + 1, event_cat.count()),
        #sys.stdout.flush()

        # Get quake origin info
        origin_info = event.preferred_origin() or event.origins[0]

        qtime = origin_info.time.timestamp

        print '...'
        print 'qtime = ', origin_info.time, qtime

        #open up new obspy stream object

        st = Stream()

        for matched_waveform in session.query(Waveforms).\
                filter(or_(and_(Waveforms.starttime <= qtime, qtime < Waveforms.endtime), and_(qtime <= Waveforms.starttime, Waveforms.starttime < qtime + 3600))):
            # Now extract all matched waveforms, concatenate using Obspy and write to ASDF with associated event tag
            # Read in the HDF5 matched waveforms into obspy stream (merge them together)
            #print matched_waveform.full_id

            # Open up the waveform into an obspy stream object
            # (this will join to previous waveform if there are multiple mSQL matches)
            st += sta_helper[matched_waveform.full_id]

        # Attempt to merge all traces with matching ID'S in place
        st.merge()

        # Now call function to trim each trace so that the earthquake arrives at 900 seconds (15 mins)

        print sta_helper.StationXML
        event_latitude = origin_info.latitude
        event_longitude = origin_info.longitude


        break




    break








