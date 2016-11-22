# Script to write ASDF file from miniseed files also build the ASDF SQL data base
# using sqlalchemy (separate database for each station in a network)


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
data_path = '/media/obsuser/seismic_data_1/'

# IRIS Virtual Ntework name
virt_net = '_GA_ANUtest'

# FDSN network identifier (2 Characters)
FDSNnetwork = 'XX'

# =========================================================================== #

path_XML = join(data_path, virt_net, FDSNnetwork, 'network_metadata/stnXML', FDSNnetwork + '.xml')
path_DATA = join(data_path, virt_net, FDSNnetwork, 'raw_DATA/')
path_quakeML = join(data_path, virt_net, FDSNnetwork, 'event_metadata/earthquake/quakeML/')

# Output ASDF file (High Performance Dataset) one file per network
ASDF_out = join(data_path, virt_net, FDSNnetwork, 'ASDF', FDSNnetwork + '.h5')

# text file containing list of service directories already in ASDF file
service_SQL = join(data_path, virt_net, FDSNnetwork, 'ASDF', FDSNnetwork + '_proc.txt')

if not exists(service_SQL):
    service_SQL_entries = []
if exists(service_SQL):
    # open up the text file for appending
    with open(service_SQL, 'r') as f:
        service_SQL_entries = f.readlines()

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


# function to create the ASDF waveform ID tag
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


# Get a list of service directories
service_dir_list = glob.glob(path_DATA + '*service*')

waveforms_added = 0

# iterate through service directories
for service in service_dir_list:

    # Check to see if the service directory has already been processed
    if basename(service) + '\n' in service_SQL_entries:
        continue

    print '\r Processing: ', basename(service)

    # write the service name to the text file
    savefile = open(service_SQL, 'a+')
    savefile.write(basename(service) + '\n')
    savefile.close()

    station_dir_list = glob.glob(service + '/*')


    # iterate through station directories
    for station_path in station_dir_list:

        station_name = basename(station_path)

        seed_files = glob.glob(join(station_path, '*miniSEED/*'))# '*miniSEED/*.mseed*'))

        if seed_files == []:
            continue

        print '\r Working on station: ', station_name

        waveforms_added += len(seed_files)

        # Iterate through the miniseed files, fix the header values and add waveforms
        for _i, filename in enumerate(seed_files):

            print "\r     Parsing miniseed file ", _i + 1, ' of ', len(seed_files), ' ....',
            sys.stdout.flush()

            # read the miniseed file
            st = read(filename)

            # there will only be one trace in stream because the data is by channels
            tr = st[0]

            station_name = tr.stats.station

            # Makes sure header is correct
            tr.stats.network = FDSNnetwork
            tr.stats.station = station_name
            #tr.stats.channel = 'BH' + tr.stats.channel[1:]

            # SQL filename for station
            SQL_out = join(data_path, virt_net, FDSNnetwork, 'ASDF', station_name + '.db')

            # Open and create the SQL file
            # Create an engine that stores data
            engine = create_engine('sqlite:////' + SQL_out)

            # check if SQL file exists:
            if not exists(SQL_out):
                # Create all tables in the engine
                Base.metadata.create_all(engine)

            # The ASDF formatted waveform name [full_id, station_id, starttime, endtime, tag]
            waveform_info = waveform_sep(make_ASDF_tag(tr, "raw_recording"))

            new_waveform = Waveforms(full_id=waveform_info[0], station_id=waveform_info[1], starttime=waveform_info[2],
                                     endtime=waveform_info[3], tag=waveform_info[4])

            # Initiate a session with the SQL database so that we can add data to it
            Session.configure(bind=engine)
            session = Session()

            # Add the waveform info to the session
            session.add(new_waveform)
            session.commit()

            # Add waveform to the ASDF file
            ds.add_waveforms(tr, tag="raw_recording")

del ds
print '\n'
print("--- Execution time: %s seconds ---" % (time.time() - code_start_time))
print '--- Added ', waveforms_added, ' waveforms to ASDF file ---'
print '--- Added ', added_quakes_count, ' quakeML file(s) to ASDF file ---'
