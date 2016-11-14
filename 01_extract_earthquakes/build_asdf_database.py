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

#Path to the data
data_path = '/g/data/ha3/'

#IRIS Virtual Ntework name
virt_net = '_ANU'

# FDSN network identifier (2 Characters)
FDSNnetwork = 'S1'

# =========================================================================== #

# Output ASDF file (High Performance Dataset) one file per network
ASDF_in = join(data_path, virt_net, FDSNnetwork, 'ASDF', FDSNnetwork + '.h5')

# Create/open the ASDF file
ds = pyasdf.ASDFDataSet(ASDF_in, compression="gzip-3")

# Set up the sql waveform databases
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


# Get list of stations in ASDF file
sta_list = ds.waveforms.list()

waveforms_added = 0

# Iterate through all stations in ASDF file
for _i, station_name in enumerate(sta_list):
    print '\r'
    print 'Working on Station: ', station_name, ' ....'
    # Get the helper object to access the station waveforms
    sta_helper = ds.waveforms[station_name]

    waveforms_list = sta_helper.list()

    waveforms_added += len(waveforms_list)

    # Iterate through the miniseed files, fix the header values and add waveforms
    for _i, waveform_tag in enumerate(waveforms_list):

        # ignore stationxml
        if 'StationXML' in waveform_tag:
            continue

        print "\r     Parsing miniseed file ", _i + 1, ' of ', len(waveforms_list), ' ....',
        sys.stdout.flush()

        #SQL filename for station
        SQL_out = join(data_path, virt_net, FDSNnetwork, 'ASDF', station_name + '.db')

        # Open and create the SQL file
        # Create an engine that stores data
        engine = create_engine('sqlite:////' + SQL_out)

        #check if SQL file exists:
        if not exists(SQL_out):
            # Create all tables in the engine
            Base.metadata.create_all(engine)

        # The ASDF formatted waveform name [full_id, station_id, starttime, endtime, tag]
        waveform_info = waveform_sep(waveform_tag)
        #print waveform_info

        new_waveform = Waveforms(full_id=waveform_info[0], station_id=waveform_info[1], starttime=waveform_info[2], endtime=waveform_info[3], tag=waveform_info[4])

        # Initiate a session with the SQL database so that we can add data to it
        Session.configure(bind=engine)
        session = Session()

        # Add the waveform info to the session
        session.add(new_waveform)
        session.commit()


del ds
print '\n'
print("--- Execution time: %s seconds ---" % (time.time() - code_start_time))
print '--- Added ', waveforms_added, ' waveforms to ASDF Database ---'
