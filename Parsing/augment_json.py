'''
Read earthquake observation json files, apply changes, and write back out
Arguments:
    out_dir: the directory to write to
    incr: integer to append to the event_id (so that they are unique)

This is called after parse_to_json.py has been run
Changes include removing random observation, adding random noise
Call using run_augment.sh in order to make multiple copies

Filename: augment_json.py
Cohort: Insight Data Engineering SEA '19C
Name: Carl Ulberg
'''

import json
import random
import time
import numpy as np
import glob
import sys
from parse_to_json import write_to_json


def read_json(file_path):
    with open(file_path) as json_file:
        data = json.load(json_file)

    return data


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


#def write_to_json(data, out_file):
#    with open(out_file, 'w') as fp:
#        json.dump(data, fp)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: augment_json.py <out_dir> <incr>")
        sys.exit(-1)

    t0 = time.time()
    out_dir = sys.argv[1]
    incr = int(sys.argv[2])
    #in_dir = '/data/all_w_unix/'
    in_dir = '/data/testCode1/tmp/'

    # get filenames
    file_names = glob.glob(in_dir + '*')

    # process each file
    for file_name in file_names:
        data = read_json(file_name)

        # Remove one phase observation
        data = remove_obs(data)

        # Convert to unix time and add random noise
        data = add_random_noise(data)

        # Change Event_id
        data = change_id(data, incr)

        # Write to file
        out_file = '{}/{}.json'.format(out_dir, data['Event_id'])
        write_to_json(data, out_file)
