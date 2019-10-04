import json
import random
import time
import datetime
import numpy as np
import glob
import sys


def read_json(file_path):
    with open(file_path) as json_file:
        data = json.load(json_file)

    return data


# sometimes the seconds are > 60, so we'll have to do this the hard way
# This won't work:
#   time_tmp = datetime.datetime.strptime(time_str, "%Y%m%d%H%M%S.%f")
#   unix_time = time.mktime(time_tmp.timetuple()) \
#       + time_tmp.microsecond/1e6
def parse_time(time_str):
    print('Parsing ' + time_str)
    yr = int(time_str[0:4])
    mo = int(time_str[4:6])
    dy = int(time_str[6:8])
    hr = int(time_str[8:10])
    mn = int(time_str[10:12])
    sc = int(time_str[12:14])
    ms = int(time_str[15:])
    time_tmp = datetime.datetime(yr, mo, dy, hr, mn)
    unix_time_tmp = time.mktime(time_tmp.timetuple())
    unix_time = unix_time_tmp + sc + ms/1e3
    print('{} becomes {}'.format(time_str,unix_time))
    return unix_time


def remove_obs(data):
    # first get the observations
   observations = data['Observations']

   # remove a random observation
   observations = random.sample(observations, len(observations)-1)
   data['Observations']=observations
   return data


def add_random_noise(data):
    # first get the observations
    observations = data['Observations']

    # loop over observations, convert to unix time, and add noise
    new_obs = []
    for obs in observations:
        # only do this if it is phase
        if obs['Type'] == 'Phase':
            # get time
#            unix_time = parse_time(obs['Arrival_time'])

            # add random noise
#            unix_time = unix_time + np.random.normal(0,0.25)

#            obs['Arrival_time'] = unix_time
            unix_time = obs['Arrival_time']
            unix_time = unix_time + np.random.normal(0,0.25)
            unix_str = str(unix_time)
            obs['Arrival_time'] = unix_str

        new_obs.append(obs)

    # apply changes to the input data
    data['Observations'] = new_obs
    return data


def change_id(data, incr):
    data['Event_id'] = int(data['Event_id'] + incr*1e8)
    return data


def write_to_json(data, out_file):
    with open(out_file, 'w') as fp:
        json.dump(data, fp)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: test_json.py <out_dir> <incr>")
        sys.exit(-1)

    t0 = time.time()
    out_dir = sys.argv[1]
    incr = int(sys.argv[2])
#    in_dir = '/data/small_unix/'
    in_dir = '/data/all_w_unix/'
#    file_name = '10265208.json'

    # get filenames
    file_names = glob.glob(in_dir + '*')

    # process each file
    for file_name in file_names:
        #print('Processing ' + file_name)
        # Read in json file
        data = read_json(file_name)

        # Remove one phase observation
        data = remove_obs(data)

        # Convert to unix time and add random noise
        data = add_random_noise(data)

        # Change Event_id
        data = change_id(data, incr)
        #print('    Event_id = {}'.format(data['Event_id']))

        # Write to file
        out_file = '{}/{}.json'.format(out_dir, data['Event_id'])
        write_to_json(data, out_file)

    # Read in json file
#    print('Reading ' + in_dir + file_name)
#    data = read_json(in_dir + file_name)
#    print(json.dumps(data, indent=4))

    # Remove one phase observation
#    print('Removing obs, there were {}'.format(len(data['Observations'])))
#    data = remove_obs(data)
#    print('Now there are {}'.format(len(data['Observations'])))

    # Convert to unix time and add random noise
#    data = add_random_noise(data)

#    data = change_id(data, incr)

#    print(json.dumps(data, indent=4))
#    t1 = time.time()
#    print('It took {:.1f} s'.format(t1-t0))
