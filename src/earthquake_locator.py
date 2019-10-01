import pandas as pd
import numpy as np
import pandas as pd
import numpy as np


if __name__ == "__main__":
    #path = 's3a://ulberg-insight/testing/multiline/uncomp/small/small.json'
    path_to_data = '/home/ubuntu/small.json'
    df = pd.read_json(path_to_data, lines=True)
    path_to_sta = '/home/ubuntu/EQlocator/seed/stations.csv'
    sta = pd.read_csv(path_to_sta, names=['Station', 'Longitude', 'Latitude', 'Depth'])

    # follow the initial procedures from process_data_s3.py on a single event
    event1 = df.iloc[0]
    ev = event1.copy()

    print(df.head())
    list_of_series=[]
    for obs in ev.Observations:
        evtmp = ev.copy()
        evtmp['obs']=obs
        list_of_series.append(evtmp)

    print(len(list_of_series))
    df1 = pd.DataFrame(list_of_series,columns=ev.index)
    print(df1.head())
