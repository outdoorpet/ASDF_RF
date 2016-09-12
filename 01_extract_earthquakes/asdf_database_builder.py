# Script to build the ASDF SQL data base using sqlite (seperate database for each station in a network)
# This will be done automatically on ASDF creation in the future

import pyasdf
from os.path import expanduser
import sys
from obspy.core import UTCDateTime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker

#### =========================== User Input Required =========================== ####

# FDSN network identifier (2 Characters)
network = '8B'

#### =========================================================================== ####

Base = declarative_base()
Session = sessionmaker()

class Waveforms(Base):
    __tablename__ = 'waveforms'
    # Here we define columns for the table
    # Notice that each column is also a normal Python instance attribute.
    starttime = Column(Integer)
    endtime = Column(Integer)
    station_id = Column(String(250), nullable=False)
    tag = Column(String(250), nullable=False)
    full_id = Column(String(250), nullable=False, primary_key=True)


# Function to seperate the waveform string into seperate fields
def waveform_sep(ws):
    a = ws.split('__')
    starttime = int(UTCDateTime(a[1].encode('ascii')).timestamp)
    endtime = int(UTCDateTime(a[2].encode('ascii')).timestamp)

    # Returns: (station_id, starttime, endtime, waveform_tag)
    return (ws.encode('ascii'), a[0].encode('ascii'), starttime, endtime, a[3].encode('ascii'))


# ASDF file (High Performance Dataset) one file per network
ASDF_in = expanduser('~') + '/Desktop/DATA/' + network + '/ASDF/' + network + '.h5'
# Open the ASDF file
ds = pyasdf.ASDFDataSet(ASDF_in)



# Get list of stations in ASDF file
sta_list = ds.waveforms.list()

# Iterate through all stations in ASDF file
for _i, station_name in enumerate(sta_list):
    print '\nWorking on Station: {0}'.format(sta_list[_i])

    # Get the waveforms list
    sta = ds.waveforms[station_name]

    sta_waveform_list = sta.list()

    # SQLite database file (per station)
    SQL_out = expanduser('~') + '/Desktop/DATA/' + network + '/ASDF/' + station_name + '.db'

    # Create an engine that stores data
    engine = create_engine('sqlite:////' + SQL_out)

    # Create all tables in the engine
    Base.metadata.create_all(engine)

    # Iterate through the waveforms for the station and write to SQL table
    for _j, waveform in enumerate(sta_waveform_list):

        # Ignore the station XML entry
        if waveform == 'StationXML':
            continue

        print '\r  Writing {0} of {1} Waveforms to SQL database....'.format(_j + 1, len(sta_waveform_list)-1),
        sys.stdout.flush()

        # Call the function to split up the waveform data
        waveform_info = waveform_sep(waveform)

        #print waveform_info

        new_waveform = Waveforms(full_id=waveform_info[0], station_id=waveform_info[1], starttime=waveform_info[2], endtime=waveform_info[3], tag=waveform_info[4])

        # Initiate a session with the SQL database so that we can add data to it
        Session.configure(bind=engine)
        session = Session()

        # Add the waveform info to the session
        session.add(new_waveform)
        session.commit()








