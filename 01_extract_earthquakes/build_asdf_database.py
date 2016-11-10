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
data_path = '/media/obsuser/GA-ANU_TRAN/'

#IRIS Virtual Ntework name
virt_net = '_ANU'

# FDSN network identifier (2 Characters)
FDSNnetwork = 'S1'

# =========================================================================== #

path_DATA = join(data_path, virt_net, FDSNnetwork, 'raw_DATA/')

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




# Get a list of service directories
year_dir_list = glob.glob(path_DATA + '*')
year_dir_list.sort()


waveforms_added = 0

#iterate through service directories
for year in year_dir_list:

    if not basename(year) == '2016':
        continue

    print '\r Processing: ', basename(year)

    day_dir_list = glob.glob(year+'/*')
    day_dir_list.sort()

    #iterate through day directories
    for day_path in day_dir_list:

        day_name = basename(day_path)

        seed_files = glob.glob(join(day_path, '*BH*'))

        if seed_files == []:
            continue

        if not day_name == '011':
            continue

        print '\r Working on Day: ', day_name

        waveforms_added += len(seed_files)

        # Iterate through the miniseed files, fix the header values and add waveforms
        for _i, filename in enumerate(seed_files):

            print "\r     Parsing miniseed file ", _i + 1, ' of ', len(seed_files), ' ....',
            sys.stdout.flush()

            # read the miniseed file
            st = read(filename)

            # there will only be one trace in stream because the data is by channels
            tr = st[0]

            #SQL filename for station
            SQL_out = join(data_path, virt_net, FDSNnetwork, 'ASDF', tr.stats.station + '.db')

            # Open and create the SQL file
            # Create an engine that stores data
            engine = create_engine('sqlite:////' + SQL_out)

            #check if SQL file exists:
            if not exists(SQL_out):
                # Create all tables in the engine
                Base.metadata.create_all(engine)

            # The ASDF formatted waveform name [full_id, station_id, starttime, endtime, tag]
            waveform_info = waveform_sep(make_ASDF_tag(tr, "raw_recording"))
            #print waveform_info

            new_waveform = Waveforms(full_id=waveform_info[0], station_id=waveform_info[1], starttime=waveform_info[2], endtime=waveform_info[3], tag=waveform_info[4])

            # Initiate a session with the SQL database so that we can add data to it
            Session.configure(bind=engine)
            session = Session()

            # Add the waveform info to the session
            session.add(new_waveform)
            session.commit()


print '\n'
print("--- Execution time: %s seconds ---" % (time.time() - code_start_time))
print '--- Added ', waveforms_added, ' waveforms to ASDF Database ---'
