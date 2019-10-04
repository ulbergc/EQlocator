import pandas as pd
import numpy as np
import pandas as pd
import numpy as np


def locate_earthquake(df, xflat, yflat, scale=1):
    # drop rows with nan
    df = df.dropna(subset=['Station_Latitude'])

    #print('in the method')
    # only keep P waves
    df = df[df['Phase']=='P']

    # converting lat/lon to 0 (ref point at original location)
    reflat = df.iloc[0]['Latitude']
    reflon = df.iloc[0]['Longitude']

    stalat = df['Station_Latitude']
    stalon = df['Station_Longitude']

    sta_y = (stalat-reflat)*111.1
    sta_x = (stalon-reflon)*111.1*np.cos(reflat*np.pi/180)

    # calculate distance
    # first flatten the arrays to make things easier
    #    xflat = np.ravel(xx)
    #    yflat = np.ravel(yy)
    sta_x = np.ravel(sta_x)
    sta_y = np.ravel(sta_y)

    xflat = xflat[:, np.newaxis]
    yflat = yflat[:, np.newaxis]

    d = np.sqrt((xflat-sta_x)**2 + (yflat-sta_y)**2)

    # get travel time (time = distance / velocity)
    t_t = d/6

    # get arrival time (observed time - reference time)
    t_obs=df['Arrival_time']-df.iloc[0]['RefTime']
    #t_obs = np.ravel(t_obs)
    t_obs = t_obs[np.newaxis, :]

    # for each grid point, calculate origin time that will minimize the misfit
    # [sum(t_obs-t_t)/n_obs]
    t_0 = np.sum(t_obs - t_t, axis=1)/len(t_obs)
    t_0 = t_0[:,np.newaxis]

    # calculate misfit at each location (sum[(t_obs - t_0 - t_t)**2])
    M = np.sum((t_obs - t_0 - t_t)**2, axis=1)

    # minimum index
    ind_min = np.argmin(M)

    x_best = xflat[ind_min]
    y_best = yflat[ind_min]

    #print("Best-fit location at ({},{})".format(x_best,y_best))
    return (x_best, y_best)

def locate_1(df, xx, yy):

    return 1


if __name__ == "__main__":
    #path = 's3a://ulberg-insight/testing/multiline/uncomp/small/small.json'
    path_to_data = '/home/ubuntu/10265208.json'
    df = pd.read_json(path_to_data, lines=True)
    path_to_sta = '/home/ubuntu/EQlocator/seed/stations.csv'
    sta = pd.read_csv(path_to_sta, names=['sta', 'Station_Longitude', 'Station_Latitude', 'Station_Depth'])

    # follow the initial procedures from process_data_s3.py on a single event
    event1 = df.iloc[0]
    ev = event1.copy()

    print(df.head())
    list_of_series=[]
    for obs in ev.Observations:
        evtmp = ev.copy()
        evtmp['obs']=obs
        evtmp['sta']=obs['Station']
        evtmp['Phase']=obs['Phase']
        evtmp['Arrival_time']=obs['Arrival_time']
        evtmp['Channel']=obs['Channel']
        evtmp['Uncertainty']=obs['Uncertainty']
        list_of_series.append(evtmp)

    print(len(list_of_series))
    df1 = pd.DataFrame(list_of_series,columns=evtmp.index)
    print(df1.head())
    print(sta.head(5))
    # merge with station info
    df1_sta = df1.join(sta.set_index('sta'), on='sta')

    # create grid to do the search on
    xgrd = np.arange(-10, 11, 1)
    ygrd = np.arange(-10, 11, 1)
    xx, yy = np.meshgrid(xgrd, ygrd)

    loc = locate_earthquake(df1_sta, np.ravel(xx), np.ravel(yy))
