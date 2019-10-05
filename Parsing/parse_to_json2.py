from collections import OrderedDict
from pymongo import MongoClient
import re
import json
import glob
import time
import sys
import datetime


def parse_to_dict(file_path, incr):
    # read in file
    with open(file_path) as f:
        content = f.readlines()

    lines = [line.rstrip('\n') for line in content]

    # parse each line based on 1st character (UW2 pickfile format)
    # build dict at the same time
    pick_file = OrderedDict()
    pick_file['Event_id'] = 'placeholder'
    for line in lines:
        # get 1st character
        if len(line) > 0:
            first = line[0]
        else:
            continue

        # assign value based on 1st character
        # each one has a different format
        # A line is header info (always first)
        # E line is location error info
        # . line is phase info
        if first == 'A':
            if len(line) < 20:
                print("Event " + file_path + " doesn't have location")
            evtype = line[1]
            #reftime0 = line[2:14]
            refyr = int(line[2:6])
            refmo = int(line[6:8])
            refdy = int(line[8:10])
            refhr = int(line[10:12])
            refmn = int(line[12:14])
            refsc = int(line[15:17])
            refms = int(line[18:20])

            time_tmp = datetime.datetime(refyr, refmo, refdy, refhr, refmn)
            ref_unix_time0 = time.mktime(time_tmp.timetuple())

            ref_unix_time = ref_unix_time0 + refsc + refms/1e3

            #reftime1 = line[15:20]
            #reftime = reftime0 + reftime1
            eqlat_raw = line[21:28]
            eqlat = int(eqlat_raw[0:2]) + float(eqlat_raw[3:])/100/60
            eqlon_raw = line[29:37]
            eqlon = -(int(eqlon_raw[0:3]) + float(eqlon_raw[4:])/100/60)
            # sometimes there is a * at the end of depth
            # that needs to be removed
            eqdep = line[38:43].strip('*')
            eqmag = line[44:48].strip()
            nsta = line[48:51].strip()
            npha = int(line[52:55].strip())

            # add values to dict
            pick_file['Type'] = str(evtype)
            pick_file['RefTime'] = str(ref_unix_time)
            pick_file['Latitude'] = float(eqlat)
            pick_file['Longitude'] = float(eqlon)
            try:
                pick_file['Depth'] = float(eqdep)
            except:
                # print("Issue with depth: " + line)
                # pick_file['Depth'] = eqdep
                pick_file['Depth'] = float(0.0)
            try:
                pick_file['Magnitude'] = float(eqmag)
            except:
                # pick_file['Magnitude'] = eqmag
                pick_file['Magnitude'] = float(0.0)
            pick_file['N_sta'] = int(nsta)
            pick_file['N_phase'] = int(npha)
        elif first == '.':
            ltmp = [", ".join(x.split())
                    for x in re.split(r'[()]', line[1:])
                    if x.strip()]
            # start new dict for each sta-chan-type
            # get station and channel from first field
            scfld = ltmp[0].split('.')
            statmp = scfld[0]
            chntmp = scfld[1]

            # Initialize list of observations if it isn't present
            if 'Observations' not in pick_file:
                pick_file['Observations'] = []

            # process observations
            for obs in ltmp[1:]:
                obs_info = obs.split(', ')
                # only process if it has a P at the beginning
                if obs_info[0] == 'P':
                    # This is phase information
                    phase = obs_info[1]
                    first_motion = obs_info[2]
                    arrival_time = float(obs_info[3])
                    pick_quality = obs_info[4]
                    uncertainty = obs_info[5]
                    residual = obs_info[6]

                    # add info to dictionary
                    tmpdict = OrderedDict()
                    tmpdict['Station'] = statmp
                    tmpdict['Channel'] = chntmp
                    tmpdict['Type'] = 'Phase'
                    tmpdict['Phase'] = phase
                    tmpdict['First_motion'] = first_motion
                    tmpdict['Arrival_time'] = ref_unix_time0 + arrival_time
                    tmpdict['Pick_quality'] = int(pick_quality)
                    try:
                        tmpdict['Uncertainty'] = float(uncertainty)
                    except:
                        # print("Issue with uncertainty: " + line)
                        # tmpdict['Uncertainty'] = uncertainty
                        tmpdict['Uncertainty'] = float(0.3)
                    try:
                        tmpdict['Residual'] = float(residual)
                    except:
                        # print("Issue with residual: " + line)
                        # tmpdict['Residual'] = residual
                        tmpdict['Residual'] = float(0.5)
                    pick_file['Observations'].append(tmpdict)
        elif first == 'C':
            words = line.split()
            ONE_MILLION = 1000000
            if len(words) == 4 and words[1] == 'EVENT':
                # this should be the eventid
                pick_file['Event_id'] = int(int(words[3]) + incr*ONE_MILLION*100)

    # return json.dumps(pick_file)
    return pick_file


def write_to_json(dict_file, out_file):
    with open(out_file, 'w') as fp:
        json.dump(dict_file, fp)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: write_to_mongo.py <out_dir> <incr>")
        sys.exit(-1)

    out_dir = sys.argv[1]
    incr = int(sys.argv[2])
    read_dir = '/data/raw'

    start_time = time.time()

    # doc_array = []

    # For testing
#    file_name = '90063011495s'
#    read_dir = '/data'
#    file_path = '{}/{}'.format(read_dir, file_name)
#    print('Reading ' + file_path)
#    doc_dict = parse_to_dict(file_path, incr)
#    print('doc_dict = ')
#    print(doc_dict)
#    print('doc_dict["Event_id"] = ' + str(doc_dict["Event_id"]))
#    out_file = '{}/{}.json'.format(out_dir, doc_dict['Event_id'])

#    write_to_json(doc_dict, out_file)
    #doc_array.append(doc_dict)
#    sys.exit(-1)

    # End testing
    files = glob.glob(read_dir + '/*')
    cnt = 0
    for file_path in files:
        doc_dict = parse_to_dict(file_path, incr)
        out_file = '{}/{}.json'.format(out_dir, doc_dict['Event_id'])
        write_to_json(doc_dict, out_file)
        # if cnt>=20:
        #   break
        # cnt+=1

    end_parse_time = time.time()
    print("Time to parse and write: {}".format(end_parse_time - start_time))

    # print("Writing ID = {}".format(doc_dict['Event_id']))

    end_upload_time = time.time()
    #    print("Time to upload: {}".format(end_upload_time - end_parse_time))

    #    writeToMongo(json_string)
    # print(json_string)
