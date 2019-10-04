from pyspark.sql import SQLContext
from pyspark.sql.functions import explode
from pyspark.sql.functions import lit
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from pyspark.sql import Row
import time
import sys
import numpy as np
import pandas as pd


WRITE_STATION_INFO = False
WRITE_DB_NAME='final'
WRITE_TABLE_NAME='table7'

LOCATE_STRUCT_FIELDS = [
        T.StructField("Event_id", T.LongType(), True),
        T.StructField("Latitude", T.FloatType(), True),
        T.StructField("Longitude", T.FloatType(), True),
        T.StructField("Station_Latitude", T.FloatType(), True),
        T.StructField("Station_Longitude", T.FloatType(), True),
        T.StructField("sta", T.StringType(), True),
        T.StructField("Type", T.StringType(), True),
        T.StructField("Phase", T.StringType(), True),
        T.StructField("RefTime", T.StringType(), True),
        T.StructField("Arrival_time", T.StringType(), True)
]
LOCATE_FIELDS = ["Event_id", "Latitude", "Longitude", "Station_Latitude",
        "Station_Longitude", "sta", "Type", "Phase", "RefTime", "Arrival_time"]

LOCATE_OUTPUT = [
        T.StructField("Event_id", T.LongType(), True),
        T.StructField("Latitude", T.DoubleType(), True),
        T.StructField("Longitude", T.DoubleType(), True),
]

# initialize session so udf can be created
spark = SparkSession \
        .builder \
        .appName("ProcessData") \
        .getOrCreate()

spark.conf.set("spark.sql.execution.arrow.enabled", "true")
#spark.conf.set("spark.sql.execution.arrow.enabled", "false")

def read_data(spark):
    path = 's3a://ulberg-insight/testing/multiline/uncomp/small/*.json'
#    path = 's3a://ulberg-insight/testing/multiline/uncomp/huge/*.json'
    #archive_path = 's3a://ulberg-insight/archive_small'
    path =  's3a://ulberg-insight/test1set/19800201.json'
    archive_path = 's3a://ulberg-insight/archive'
#    months = [19800101, 19800201, 19800301, 19800401, 19800501, 19800601]
#    months = [19800101]
    molimit = 13 # 13 for all, 2 for single
    yrlimit = 5 # 5 for all, 1 for single
    mo = ['{:02d}01'.format(x) for x in range(1,molimit)]
    allmo=[]
    for i in range(yrlimit):
        tmpmo = ['198{}{}'.format(i,x) for x in mo]
        allmo+=tmpmo
    #months = ['1980{}'.format(x) for x in mo]
    months = allmo
    path = ['{p1}/{mo}/{mo}.json' \
        .format(p1=archive_path, mo=month) for month in months]
    df = spark.read.json(path)
    #df = spark.read \
    #    .load('s3a://ulberg-insight/testing/small/small.tar.gz',
    #    format='com.databricks.spark.csv')
    df.printSchema()

    return df


def read_station(spark):
    sql_context = SQLContext(spark)
    sta = sql_context.read  \
            .load('s3a://ulberg-insight/info/stations_small.csv',
            format='com.databricks.spark.csv')
#    print("{} stations".format(sta.count()))
#    sta.printSchema()
    # change stations schema
    sta = sta.selectExpr("_c0 as sta", "_c1 as Station_Longitude", "_c2 as Station_Latitude", "_c3 as Station_Depth")
    sta = sta.withColumn("Station_Latitude", sta["Station_Latitude"].cast(T.DoubleType()))
    sta = sta.withColumn("Station_Longitude", sta["Station_Longitude"].cast(T.DoubleType()))
    sta = sta.withColumn("Station_Depth", sta["Station_Depth"].cast(T.DoubleType()))
#    sta.printSchema()
    return sta


# write dataframe to destination
# include options argument/config file to define this instead of in code?
def write_data(df, db_name=WRITE_DB_NAME, table_name=WRITE_TABLE_NAME):
    # data_to_write=df.select("Event_id","Latitude","Longitude")
    data_to_write = df
#    print(df.count())

    # check that database is created and table is not
    # define
    mysql_host = "jdbc:mysql://10.0.0.11:3306/"
    #mysql_db = WRITE_DB_NAME
    #mysql_table = WRITE_TABLE_NAME
    mysql_db = db_name
    mysql_table = table_name
    mysql_user = "user"
    mysql_pw = "pw"
    data_to_write.write.format("jdbc").options(
        url=(mysql_host + mysql_db),
        dbtable=mysql_table,
        user=mysql_user,
        password=mysql_pw,
        driver="com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()

    # could add .mode("append") before save to append, otherwise creates new


def test_method(df1):
    columns_to_drop = ['N_sta']
    df2 = df1.drop(*columns_to_drop)
    return df2


def explode_on_val(df, explode_col='Observations', new_col='obs'):
    df = df.withColumn(new_col, explode(df[explode_col]))
    return df


def drop_cols(df, columns_to_drop):
    return df.drop(*columns_to_drop)


def count_station_obs(df):
    # this version counts duplicates if a sta had multiple obs for one  event
#    sta_count = df.groupby('sta').count()
    # this version does it correctly?
    sta_count = df.groupby('sta').agg(countDistinct('Event_id'))
    sta_count = sta_count \
        .withColumn('count', sta_count['count(DISTINCT Event_id)'])
    sta_count = sta_count.drop('count(DISTINCT Event_id)')
    return sta_count


def locate0(df):
    df_new = df.select("Event_id","Latitude","Longitude")
    df_new = df_new.withColumn("Method", lit(0))
    return df_new


@pandas_udf("Event_id long, Longitude double, Latitude double, Method int", PandasUDFType.GROUPED_MAP)
def locate1_wrapper(df):
    return locate1_pandas(df)


def locate1(df):
    return locate1_spark(df)
    #return df.groupBy('Event_id').apply(locate1_wrapper)


def locate1_pandas(df):
    lat_avg = df['Station_Latitude'].mean()
    lon_avg = df['Station_Longitude'].mean()
    ev_id = df['Event_id'].iloc[0]
    return pd.DataFrame([[ev_id, lon_avg, lat_avg, 1]], columns=['Event_id', 'Longitude', 'Latitude', 'Method'])


# this one just returns the average of stations with observations
def locate1_spark(df):
    df_avg_lat = df.groupBy('Event_id').avg('Station_Latitude')
    df_avg_lon = df.groupBy('Event_id').avg('Station_Longitude')
    # rename columns
    df_avg_lat = df_avg_lat.withColumn("Latitude", df_avg_lat["avg(Station_Latitude)"])
    df_avg_lon = df_avg_lon.withColumn("Longitude", df_avg_lon["avg(Station_Longitude)"])
    # drop old columns
    df_avg_lat = drop_cols(df_avg_lat, ['avg(Station_Latitude)'])
    df_avg_lon = drop_cols(df_avg_lon, ['avg(Station_Longitude)'])
    # merge average values
    df_avg = df_avg_lat.join(df_avg_lon, 'Event_id')
    df_avg.printSchema()
    # add method column
    df_avg = df_avg.withColumn("Method", lit(1))
    return df_avg


def locate2(df):
    #df_loc = df.select(LOCATE_FIELDS)
    #df_new_loc = df_loc.groupBy('Event_id').apply(eq_wrapper)
    return df.groupBy('Event_id').apply(eq_wrapper)


@pandas_udf("Event_id long, Longitude double, Latitude double, Method int", PandasUDFType.GROUPED_MAP)
def eq_wrapper(df):
    return locate_earthquake(df)


# make variables that will be always passed to function
xgrd = np.arange(-10,11)
ygrd = np.arange(-10,11)
xx,yy = np.meshgrid(xgrd, ygrd)
xf = np.ravel(xx)
yf = np.ravel(yy)

xf_bc = spark.sparkContext.broadcast(xf)
yf_bc = spark.sparkContext.broadcast(yf)

#@pandas_udf(LOCATE_OUTPUT, PandasUDFType.GROUPED_MAP)
#@pandas_udf("Event_id long, Longitude double, Latitude double", PandasUDFType.GROUPED_MAP)
#def locate_earthquake(df, xflat=xf_bc, yflat=yf_bc, scale=1):
def locate_earthquake(df, scale=1):
#def locate_earthquake(df, scale=1):
    xflat=xf_bc.value
    yflat=yf_bc.value
    #print(type(xflat))
    ev_id = df.iloc[0]['Event_id']
    # drop rows with nan
    #print('Event {}'.format(ev_id))
    reflat = df['Latitude'].iloc[0]
    reflon = df['Longitude'].iloc[0]
    reftime = float(df.iloc[0]['RefTime'])

    sh1=df.shape
    df = df.dropna(subset=['Station_Latitude'])
    if 'Latitude' not in df.columns:
        print("Event {}, {} to {}, doesn't have latitude!".format(ev_id,sh1,df.shape))
    #else:
    #    print('Event {}, {} to {}, reflat={}]'.format(ev_id,sh1,df.shape,reflat))
    #print('in the method')
    # only keep P waves
    df = df[df['Phase']=='P']

    # converting lat/lon to 0 (ref point at original location)
    #reflat = df['Latitude'].iloc[0]
    #reflon = df['Longitude'].iloc[0]

    stalat = df['Station_Latitude']
    stalon = df['Station_Longitude']

    sta_y = (stalat-reflat)*111.1
    sta_x = (stalon-reflon)*111.1*np.cos(reflat*np.pi/180)

    # calculate distance
    # first flatten the arrays to increase the dimension
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
    t_obs=df['Arrival_time'].astype('double')-reftime
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
    #print("minimum misfit: {}".format(M[ind_min]))
    x_best = xflat[ind_min][0]
    y_best = yflat[ind_min][0]

    # convert back to lat/lon
    lon_best = x_best/(111.1*np.cos(reflat*np.pi/180)) + reflon
    lat_best = y_best/111.1 + reflat

    #print("Best-fit location at ({},{})".format(x_best,y_best))
    #return (ev_id, x_best[0], y_best[0])
    return pd.DataFrame([[ev_id, lon_best, lat_best, 2]], columns=['Event_id', 'Longitude', 'Latitude', 'Method'])


#if __name__ == "__main__":
def main():
    # master_DNS="ec2-34-223-143-198.us-west-2.compute.amazonaws.com"

    if len(sys.argv) != 3:
        print("Usage: process_data.py <dbname> <tablename>")
        sys.exit(-1)

    # Define spark session with read configuration
    # !!!change this to read from S3
    dbname = sys.argv[1]  # small-tmp1; large-tmp0
    collname = sys.argv[2]  # small-c0; large-c0
#    spark = SparkSession \
#        .builder \
#        .appName("ProcessData") \
#        .getOrCreate()

    #        .master("spark://" + master_DNS + ":7077") \
    starttime = time.time()

    # drop table that we'll write to
    #check_sql_table()

    # read data from S3
    df0 = read_data(spark)
    # store original
    #df_original = df
    #print("DF has {} records".format(df.count()))

    # read station information from S3
    sta = read_station(spark)
    sta.printSchema()
    # keep this information cached since it is small and will be used a lot
    sta.cache()

    # Process each record
    # explode observations in df to make them easier to work with
    df = explode_on_val(df0, explode_col='Observations', new_col='obs')

    # drop useless columns
    df = drop_cols(df, ['Observations', 'N_sta', 'N_phase'])

    # pull additional info from obs
    df = df.withColumn("sta", df.obs.Station)
    df = df.withColumn("Arrival_time", df.obs.Arrival_time)
    df = df.withColumn("Phase", df.obs.Phase)
    df = df.withColumn("Uncertainty", df.obs.Uncertainty)

    # get clean df with normal types
    df = drop_cols(df, ['obs'])

    # merge data and sta
    df = df.join(sta, 'sta')

    print(df.schema)

    df = df.select(LOCATE_FIELDS)

    print("Partitions in df = {}".format(df.rdd.getNumPartitions()))
    # CHOOSE LOCATION METHOD (2 = grid, 1=avg)
    #df_out = locate2(df)
    #df_out = locate1(df)
    df_out = locate0(df0)
    print("Partitions in df_out = {}".format(df_out.rdd.getNumPartitions()))

    # simple df_out for testing
#    df_out=df.select("Event_id","Latitude","Longitude")

#    df_out.printSchema()
    # Write data to MySQL
    write_data(df_out)

    if WRITE_STATION_INFO:
        sta_cnt = count_station_obs(df)
        sta_cnt.printSchema()
        sta_cnt.show(10)
        # merge sta_cnt with sta
        sta_all = sta.join(sta_cnt, 'sta')
        write_data(sta_all, db_name=WRITE_DB_NAME, table_name='statmp2')

    spark.stop()

    endtime = time.time()
    print("It took {:.1f} seconds".format(endtime-starttime))
    #print(bounding_box)

main()
