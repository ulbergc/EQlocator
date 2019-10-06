from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import lit
from pyspark.sql.functions import explode
import time
import sys
import numpy as np
import pandas as pd

from tools import read_data, read_station, write_data, count_station_obs


# Toggle this on or off for testing
WRITE_STATION_INFO = False

# define the columns necessary to calculate an earthquake location
LOCATE_FIELDS = ['Event_id', 'Latitude', 'Longitude', 'Station_Latitude',
        'Station_Longitude', 'sta', 'Type', 'Phase', 'RefTime', 'Arrival_time']

UDF_SCHEMA = 'Event_id long, Longitude double, Latitude double, Method int'

# initialize session so pandas_udf can be created
spark = SparkSession \
        .builder \
        .appName('ProcessData') \
        .getOrCreate()

spark.conf.set('spark.sql.execution.arrow.enabled', 'true')

# make grid search variables that will be always passed to locate2 function
xgrd = np.arange(-10, 11)
ygrd = np.arange(-10, 11)
xx, yy = np.meshgrid(xgrd, ygrd)
xf = np.ravel(xx)
yf = np.ravel(yy)

# broadcast them so that they will stay in memory
xf_bc = spark.sparkContext.broadcast(xf)
yf_bc = spark.sparkContext.broadcast(yf)


def locate0(df):
    '''
    Returns the original earthquake location
    '''
    df_new = df.select('Event_id', 'Latitude', 'Longitude')
    df_new = df_new.withColumn('Method', lit(0))
    return df_new


@pandas_udf(UDF_SCHEMA, PandasUDFType.GROUPED_MAP)
def locate1_udf(df):
    return locate1_pandas(df)


def locate1(df, use_spark=True):
    '''
    This is a naive earthquake location method that just returns the
    average location of stations with observations.

    use_spark can be toggled to test runtime differences between
    using spark native functions vs. pandas_udf
    '''
    if use_spark:
        return locate1_spark(df)
    else:
        return df.groupBy('Event_id').apply(locate1_udf)


def locate1_pandas(df):
    lat_avg = df['Station_Latitude'].mean()
    lon_avg = df['Station_Longitude'].mean()
    ev_id = df['Event_id'].iloc[0]
    return pd.DataFrame([[ev_id, lon_avg, lat_avg, 1]],
            columns=['Event_id', 'Longitude', 'Latitude', 'Method'])


def locate1_spark(df):
    # Get average latitude
    df_avg_lat = df.groupBy('Event_id').avg('Station_Latitude')
    df_avg_lat = df_avg_lat \
        .withColumn('Latitude', df_avg_lat['avg(Station_Latitude)'])
    df_avg_lat = df_avg_lat.drop('avg(Station_Latitude)')

    # Get average longitude
    df_avg_lon = df.groupBy('Event_id').avg('Station_Longitude')
    df_avg_lon = df_avg_lon \
        .withColumn('Longitude', df_avg_lon['avg(Station_Longitude)'])
    df_avg_lon = df_avg_lon.drop('avg(Station_Longitude)')

    # merge average values
    df_avg = df_avg_lat.join(df_avg_lon, 'Event_id')
    df_avg = df_avg.withColumn('Method', lit(1))
    return df_avg


@pandas_udf(UDF_SCHEMA, PandasUDFType.GROUPED_MAP)
def locate2_udf(df):
    return locate2_grid_search(df, xf_bc.value, yf_bc.value)


def locate2(df):
    return df.groupBy('Event_id').apply(locate2_udf)


def locate2_grid_search(df, xflat, yflat, scale=1):
    ev_id = df['Event_id'].iloc[0]
    reflat = df['Latitude'].iloc[0]
    reflon = df['Longitude'].iloc[0]
    reftime = float(df['RefTime'].iloc[0])

    df = df.dropna(subset=['Station_Latitude'])

    # only keep P waves
    df = df[df['Phase'] == 'P']

    stalat = df['Station_Latitude']
    stalon = df['Station_Longitude']

    sta_y = (stalat-reflat)*111.1
    sta_x = (stalon-reflon)*111.1*np.cos(reflat*np.pi/180)

    # calculate distance
    # first flatten the arrays to increase the dimension
    sta_x = np.ravel(sta_x)
    sta_y = np.ravel(sta_y)

    xflat = xflat[:, np.newaxis]
    yflat = yflat[:, np.newaxis]

    d = np.sqrt((xflat-sta_x)**2 + (yflat-sta_y)**2)

    # get travel time (time = distance / velocity)
    t_t = d/6

    # get arrival time (observed time - reference time)
    t_obs = df['Arrival_time'].astype('double')-reftime
    t_obs = t_obs[np.newaxis, :]

    # for each grid point, calculate origin time that will minimize the misfit
    t_0 = np.sum(t_obs - t_t, axis=1)/len(t_obs)
    t_0 = t_0[:, np.newaxis]

    # calculate misfit at each location (sum[(t_obs - t_0 - t_t)**2])
    M = np.sum((t_obs - t_0 - t_t)**2, axis=1)

    # minimum index
    ind_min = np.argmin(M)
    # print("minimum misfit: {}".format(M[ind_min]))
    x_best = xflat[ind_min][0]
    y_best = yflat[ind_min][0]

    # convert back to lat/lon
    lon_best = x_best/(111.1*np.cos(reflat*np.pi/180)) + reflon
    lat_best = y_best/111.1 + reflat

    # print("Best-fit location at ({},{})".format(x_best,y_best))
    return pd.DataFrame([[ev_id, lon_best, lat_best, 2]],
            columns=['Event_id', 'Longitude', 'Latitude', 'Method'])


def main(db_write_name, table_write_name, METHOD):
    # read data from S3
    df0 = read_data(spark)

    # read station information from S3
    sta = read_station(spark)
    sta.printSchema()
    # keep this information cached since it is small and will be used a lot
    sta.cache()

    # Explode observations in df to make them easier to work with
    df = df0.withColumn('obs', explode(df0['Observations']))
    df = df.drop(*['Observations', 'N_sta', 'N_phase'])

    # pull additional info from obs
    df = df.withColumn('sta', df.obs.Station) \
        .withColumn('Arrival_time', df.obs.Arrival_time) \
        .withColumn('Phase', df.obs.Phase) \
        .withColumn('Uncertainty', df.obs.Uncertainty)

    df = df.drop('obs')
    df = df.join(sta, 'sta')
    df = df.select(LOCATE_FIELDS)

    # CHOOSE LOCATION METHOD (0 = original, 1 = avg, 2 = grid)
    if METHOD == 0:
        # Return the original location (input original, non-exploded df)
        df_out = locate0(df0)
    elif METHOD == 1:
        # Return the average of stations with observations for each earthquake
        # uses Spark native methods
        df_out = locate1(df)
        # use this one for testing pandas_udf
        # df_out = locate1(df, use_spark=False)
    elif METHOD == 2:
        # Return the location that minimizes travel-time residual (grid search)
        df_out = locate2(df)
    else:
        print('Use method 0, 1, 2')

    # Write data to MySQL
    write_data(df_out, db_write_name, table_write_name)

    if WRITE_STATION_INFO:
        sta_count = count_station_obs(df)
        sta_count.printSchema()
        sta_count.show(10)
        # merge sta_cnt with sta
        sta_all = sta.join(sta_count, 'sta')
        write_data(sta_all, db_name='tmp5', table_name='sta_month')

    spark.stop()


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print('Usage: process_data.py <dbname> <tablename> <method #>')
        sys.exit(-1)

    starttime = time.time()

    db_write_name = sys.argv[1]
    table_write_name = sys.argv[2]
    METHOD = int(sys.argv[3])
    main(db_write_name, table_write_name, METHOD)

    endtime = time.time()
    print('It took {:.1f} seconds'.format(endtime-starttime))
