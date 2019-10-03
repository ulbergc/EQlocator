from pyspark.sql import SQLContext
from pyspark.sql.functions import explode
from pyspark.sql.functions import lit
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType
# from pyspark.sql.types import StructField, IntegerType, FloatType, StructType
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from pyspark.sql import Row
import time
import sys
import numpy as np
import pandas as pd
#import mysql.connector


WRITE_STATION_INFO = False
WRITE_DB_NAME='tmp5'
WRITE_TABLE_NAME='a2'

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
    molimit = 2 # 13 for all, 2 for single
    yrlimit = 1 # 5 for all, 1 for single
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


delete_table_sql = "DROP TABLE IF EXISTS {};".format(WRITE_TABLE_NAME)


def check_sql_table():
    mysql_host = "jdbc:mysql://10.0.0.11:3306/"
    mysql_db = WRITE_DB_NAME
    mysql_table = WRITE_TABLE_NAME
    mysql_user = "user"
    mysql_pw = "pw"
    cnx = mysql.connector.connect(
        url=(mysql_host + mysql_db),
        dbtable=mysql_table,
        user=mysql_user,
        password=mysql_pw,
        driver="com.mysql.cj.jdbc.Driver")
    cursor = cnx.cursor()
    # drop the table
    cursor.execute(delete_table_sql)


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


# Locate earthquake
# - create separate class to do this and allow for different methods?
def locate_EQ(x, sta_pd):
   # print("Processing event {evid}, {nobs} observations".format(
   #     evid=x.Event_id, nobs=len(x.Observations)))
    # print(type(x))

    # default values
    idtmp = x.Event_id
    lattmp = float(x.Latitude)
    lontmp = float(x.Longitude)

    all_obs = x['Observations']
    print(type(all_obs))
    # test value
#       lattmp = obs[0]["Uncertainty"]
    # loop over each observation, only include if it is a P phase
    all_sta = []
    all_time = []
    for obs in all_obs:
        if obs['Type'] == 'Phase' and obs['Phase'] == 'P':
            sta_name = obs['Station']
            all_sta.append(sta_name)
            all_time.append(obs['Arrival_time'])

    tuple_list = list(zip(all_sta, all_time))
    df_obs = pd.DataFrame(tuple_list, columns=['Station', 'Arrival_time'])

    # merge station and time observations
    sta2 = pd.merge(df_obs, sta, how='inner', on=['Station'])
    #s1 = pd.merge(df1, df2, how='inner', on=['user_id'])
    #s1 = pd.merge(dfA, dfB, how='inner', on=['S', 'T'])

    #lattmp = sta2['Latitude'].mean()
    #lontmp = sta2['Longitude'].mean()

    #return Row(Event_id=idtmp, Latitude=lattmp, Longitude=lontmp)
    return Row(idtmp, lattmp, lontmp)

def test_method(df1):
    columns_to_drop = ['N_sta']
    df2 = df1.drop(*columns_to_drop)
    return df2


def explode_on_val(df, explode_col='Observations', new_col='obs'):
    df = df.withColumn(new_col, explode(df[explode_col]))
    return df


def drop_cols(df, columns_to_drop):
    return df.drop(*columns_to_drop)
#def locate_sta_avg(x):


def count_station_obs(df):
    # this version counts duplicates if a sta had multiple obs for one  event
#    sta_count = df.groupby('sta').count()
    # this version does it correctly?
    sta_count = df.groupby('sta').agg(countDistinct('Event_id'))
    sta_count = sta_count \
        .withColumn('count', sta_count['count(DISTINCT Event_id)'])
    sta_count = sta_count.drop('count(DISTINCT Event_id)')
    return sta_count


def locate1(df):
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
    df_loc = df.select(LOCATE_FIELDS)
    #df_new_loc = df_loc.groupBy('Event_id').apply(eq_wrapper)
    return df_loc.groupBy('Event_id').apply(eq_wrapper)


def locate_simple(x):
    # default values
    idtmp = x.Event_id
    lattmp = float(x.Latitude)
    lontmp = float(x.Longitude)
    return (idtmp, lattmp, lontmp, 0)


@pandas_udf("Event_id long, Station_Latitude double", PandasUDFType.GROUPED_MAP)
def test_udf(pdf):
    tmplat = pdf.Station_Latitude
    return pdf.assign(Station_Latitude=(tmplat+1))


# make variables that will be always passed to function
xgrd = np.arange(-10,11)
ygrd = np.arange(-10,11)
xx,yy = np.meshgrid(xgrd, ygrd)
xf = np.ravel(xx)
yf = np.ravel(yy)

#@pandas_udf(LOCATE_OUTPUT, PandasUDFType.GROUPED_MAP)
#@pandas_udf("Event_id long, Longitude double, Latitude double", PandasUDFType.GROUPED_MAP)
def locate_earthquake(df, xflat=xf, yflat=yf, scale=1):
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


@pandas_udf("Event_id long, Longitude double, Latitude double, Method int", PandasUDFType.GROUPED_MAP)
def eq_wrapper(df):
    return locate_earthquake(df)


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

    # verify id column type, they are too big for Int
    df = df0.withColumn("Event_id", df0["Event_id"].cast(T.LongType()))

    # read station information from S3
    sta = read_station(spark)
    sta.printSchema()
    # keep this information cached since it is small and will be used a lot
    sta.cache()

    # convert sta to rdd so it can be passed to locate_eq
#    sta_pd = sta.toPandas()
#    print(type(sta_pd))

    # this way doesn't work
    #sta_rdd2 = sta.rdd.map(list)
    #print(type(sta_rdd2))

    # Process each record
    # test passing to method
#    df = test_method(df)
    # explode observations in df to make them easier to work with
    df = explode_on_val(df, explode_col='Observations', new_col='obs')
#    df.printSchema()

    # drop useless columns
    df = drop_cols(df, ['Observations', 'N_sta', 'N_phase'])
#    df.printSchema()

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

    # CHOOSE LOCATION METHOD (2 = grid, 1=avg)
    df_out = locate2(df)
    #df_out = locate1(df)


    # test user defined function
#    df.show(1)
#    col_keep = ['Event_id', 'Station_Latitude']
#    df_small = df.select(col_keep)
#    #df_small = df.select("Event_id", "Station_Latitude")
#    sample = df_small.filter(df_small.Event_id == 10265028).toPandas()
#    print("sample")
#    print(sample.head())
#
#    print(df_small.schema)
#
#    sample_new = test_udf.func(sample)
#    print("sample_new")
#    print(sample_new.head())

    # do the location
#    df_loc = df.select(LOCATE_FIELDS)
#    sample2 = df_loc.filter(df_loc.Event_id == 10265028).toPandas()
#    print("sample2")
#    print(sample2.head())
#    df_loc.printSchema()

    #sample2_new = locate_earthquake.func(sample2)
#    sample2_new = eq_wrapper.func(sample2)
#    print("sample2_new type: ")
#    print(type(sample2_new))
#    sample2_new.printSchema()
#    df_loc.show(3)
#    df_new_loc = df_loc.groupBy('Event_id').apply(eq_wrapper)
#    print("df_new_loc type:")
#    print(type(df_new_loc))
#    df_new_loc.printSchema()
#    df_new_loc.show(3)
    #df_new_loc.explain(True)
    # count number of nan values after merge
    #df.agg(count_not_null('Station_Latitude', True)).show()

    # get average values
#    df_avg_lat = df.groupBy('Event_id').avg('Station_Latitude')
#    df_avg_lon = df.groupBy('Event_id').avg('Station_Longitude')

    # rename columns
#    df_avg_lat = df_avg_lat.withColumn("Latitude", df_avg_lat["avg(Station_Latitude)"])
#    df_avg_lon = df_avg_lon.withColumn("Longitude", df_avg_lon["avg(Station_Longitude)"])

    # drop old columns
#    df_avg_lat = drop_cols(df_avg_lat, ['avg(Station_Latitude)'])
#    df_avg_lon = drop_cols(df_avg_lon, ['avg(Station_Longitude)'])

    # merge average values
#    df_avg = df_avg_lat.join(df_avg_lon, 'Event_id')
#    df_avg.printSchema()

    # add method column
#    df_avg = df_avg.withColumn("Method", lit(1))

    # using foreach will not return anything
    # can edit df or write intermediate results?
    # df.foreach(lambda x: locateEQ(x,'fe'))

    # using map will return a transformed rdd
    # if you pass sta in as well it will fail: pickling error???
#    d_rdd = df0.rdd.map(lambda x: locate_simple(x))
#    d_rdd = df.rdd \
#        .map(lambda x: locate_EQ(x, sta_pd))
    # print(d_rdd.count())

    # drop this from memory, don't need it
    # df.unpersist()

    # create schema for transformed rdd
#    fields = [T.StructField("Event_id", T.LongType(), True),
#              T.StructField("Latitude", T.FloatType(), True),
#              T.StructField("Longitude", T.FloatType(), True),
#              T.StructField("Method", T.IntegerType(), True)]
#    sql_schema = T.StructType(fields)

    # d_rdd_with_schema = sqlContext.applySchema(d_rdd,sql_schema)
#    df_out = spark.createDataFrame(d_rdd, sql_schema)
    # This version creates df without defined schema
    #df_out = spark.createDataFrame(d_rdd)

    # simple df_out for testing
#    df_out=df.select("Event_id","Latitude","Longitude")

    # print(d_rdd.first()[0])
    # df2.printSchema()
    # Create table, headers for MySQL? Or do this in write_data?
    # ??

    # Use the average station location as the earthquake's location
#    df_out = df_avg
    #df_out = df_new_loc

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
