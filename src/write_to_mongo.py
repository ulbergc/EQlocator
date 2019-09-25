from collections import OrderedDict
from pymongo import MongoClient
import re
import json
import glob
import time

def parseFileToJson(filepath):
    # read in file
    with open(filepath) as f:
        content = f.readlines()

    lines = [line.rstrip('\n') for line in content]

    # parse each line based on 1st character (UW2 pickfile format)
    # build dict at the same time
    pickfile=OrderedDict()
    pickfile['Event_id']='placeholder'
    for line in lines:
        # get 1st character
        if len(line)>0:
            first=line[0]
        else:
            continue

        # assign value based on 1st character
        # each one has a different format
        # A line is header info (always first)
        # E line is location error info
        # . line is phase info
        if first == 'A':
            if len(line) < 20:
                print("Event " + filepath + " doesn't have location")
            evtype=line[1]
            reftime0=line[2:14]
            reftime1=line[15:20]
            reftime=reftime0 + reftime1
            eqlat_raw=line[21:28]
            eqlat=int(eqlat_raw[0:2]) + float(eqlat_raw[3:])/100/60
            eqlon_raw=line[29:37]
            eqlon=-(int(eqlon_raw[0:3]) + float(eqlon_raw[4:])/100/60)
            # sometimes there is a * at the end of depth that needs to be removed
            eqdep=line[38:43].strip('*')
            eqmag=line[44:48].strip()
            nsta=line[48:51].strip()
            npha=int(line[52:55].strip())
            
            # add values to dict
            pickfile['Type']=str(evtype)
            pickfile['RefTime']=str(reftime)
            pickfile['Latitude']=str(eqlat)
            pickfile['Longitude']=str(eqlon)
            pickfile['Depth']=str(eqdep)
            pickfile['Magnitude']=str(eqmag)
            pickfile['N_sta']=str(nsta)
            pickfile['N_phase']=str(npha)
        elif first == '.':
            ltmp=[", ".join(x.split())
                  for x in re.split(r'[()]',line[1:])
                  if x.strip()]
            # start new dict for each sta-chan-type
            # get station and channel from first field
            scfld=ltmp[0].split('.')
            statmp=scfld[0]
            chntmp=scfld[1]
            
            # Initialize list of observations if it isn't present
            if 'Observations' not in pickfile:
                pickfile['Observations']=[]

            # process observations
            for obs in ltmp[1:]:
                obs_info=obs.split(', ')
                # only process if it has a P at the beginning
                if obs_info[0]=='P':
                    # This is phase information
                    phase=obs_info[1]
                    first_motion=obs_info[2]
                    arrival_time=obs_info[3]
                    pick_quality=obs_info[4]
                    uncertainty=obs_info[5]
                    residual=obs_info[6]
                    
                    # add info to dictionary
                    tmpdict=OrderedDict()
                    tmpdict['Station']=statmp
                    tmpdict['Channel']=chntmp
                    tmpdict['Type']='Phase'
                    tmpdict['Phase']=phase
                    tmpdict['First_motion']=first_motion
                    tmpdict['Arrival_time']=reftime0 + arrival_time
                    tmpdict['Pick_quality']=pick_quality
                    tmpdict['Uncertainty']=uncertainty
                    tmpdict['Residual']=residual
                    
                    pickfile['Observations'].append(tmpdict)
        elif first == 'C':
            words=line.split()
            if len(words)==4 and words[1]=='EVENT':
                # this should be the eventid
                pickfile['Event_id']=words[3]

    #return json.dumps(pickfile)
    return pickfile

#def writeToMongo(json_string):


if __name__ == "__main__":
    filedir='/data/raw'
#    filename='99123101280c'

    start=time.time()

    doc_array=[]
    files=glob.glob(filedir + '/*')
    for filepath in files:
        #print(filepath)

#    filepath=filedir + '/' + filename
    
    #doc_array=[]
        doc_dict=parseFileToJson(filepath)
        doc_array.append(doc_dict)

    endParse=time.time()
    print("Time to parse: {}".format(endParse - start))

    client = MongoClient('localhost', 27017)
    db = client.tmp0
    collection_name = db.coll0
    #collection_name.insert_one(json_string)
    collection_name.insert_many(doc_array)
    client.close()
   
    endUpload=time.time()
    print("Time to upload: {}".format(endUpload - endParse))
    
#    writeToMongo(json_string)
    #print(json_string)


