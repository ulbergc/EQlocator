from pyspark.sql import SQLContext
from pyspark.sql.functions import explode
# from pyspark.sql.types import StructField, IntegerType, FloatType, StructType
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from pyspark.sql import Row
import time
import sys
import pandas as pd
#import mysql.connector


def read_data(spark):
#    path = 's3a://ulberg-insight/testing/multiline/uncomp/small/*.json'
#    path = 's3a://ulberg-insight/testing/multiline/uncomp/huge/*.json'
    archive_path = 's3a://ulberg-insight/archive'
#    months = [19800101, 19800201, 19800301, 19800401, 19800501, 19800601]
    months = [19800101]
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
            .load('s3a://ulberg-insight/info/stations.csv',
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


tmp_table_name="table1"
delete_table_sql = "DROP TABLE IF EXISTS {};".format(tmp_table_name)


def check_sql_table():
    mysql_host = "jdbc:mysql://10.0.0.11:3306/"
    mysql_db = "tmp4"
    mysql_table = tmp_table_name
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
def write_data(df):
    # data_to_write=df.select("Event_id","Latitude","Longitude")
    data_to_write = df
#    print(df.count())

    # check that database is created and table is not
    # define
    mysql_host = "jdbc:mysql://10.0.0.11:3306/"
    mysql_db = "tmp4"
    mysql_table = tmp_table_name
    mysql_user = "user"
    mysql_pw = "pw"
    data_to_write.write.format("jdbc").options(
        url=(mysql_host + mysql_db),
        dbtable=mysql_table,
        user=mysql_user,
        password=mysql_pw,
        driver="com.mysql.cj.jdbc.Driver") \
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


def locate_simple(x):
    # default values
    idtmp = x.Event_id
    lattmp = float(x.Latitude)
    lontmp = float(x.Longitude)
    return (idtmp, lattmp, lontmp)


if __name__ == "__main__":
    # master_DNS="ec2-34-223-143-198.us-west-2.compute.amazonaws.com"

    if len(sys.argv) != 3:
        print("Usage: process_data.py <dbname> <tablename>")
        sys.exit(-1)

    # Define spark session with read configuration
    # !!!change this to read from S3
    dbname = sys.argv[1]  # small-tmp1; large-tmp0
    collname = sys.argv[2]  # small-c0; large-c0
    spark = SparkSession \
        .builder \
        .appName("ProcessData") \
        .getOrCreate()

    #        .master("spark://" + master_DNS + ":7077") \
    starttime = time.time()

    # drop table that we'll write to
    #check_sql_table()

    # read data from S3
    df0 = read_data(spark)
    # store original
    #df_original = df
    #print("DF has {} records".format(df.count()))

    # change id column type, they are too big for Int
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

    # pull station name from obs
    df = df.withColumn("sta", df.obs.Station)

    # merge data and sta
    df = df.join(sta, 'sta')

    # get average values
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
#    df_avg.printSchema()

    # using foreach will not return anything
    # can edit df or write intermediate results?
    # df.foreach(lambda x: locateEQ(x,'fe'))

    # using map will return a transformed rdd
    # if you pass sta in as well it will fail: pickling error???
    d_rdd = df0.rdd.map(lambda x: locate_simple(x))
#    d_rdd = df.rdd \
#        .map(lambda x: locate_EQ(x, sta_pd))
    # print(d_rdd.count())

    # drop this from memory, don't need it
    # df.unpersist()

    # create schema for transformed rdd
    fields = [T.StructField("Event_id", T.LongType(), True),
              T.StructField("Latitude", T.FloatType(), True),
              T.StructField("Longitude", T.FloatType(), True)]
    sql_schema = T.StructType(fields)

    # d_rdd_with_schema = sqlContext.applySchema(d_rdd,sql_schema)
    df_out = spark.createDataFrame(d_rdd, sql_schema)
    # This version creates df without defined schema
    #df_out = spark.createDataFrame(d_rdd)

    # simple df_out for testing
#    df_out=df.select("Event_id","Latitude","Longitude")

    # print(d_rdd.first()[0])
    # df2.printSchema()
    # Create table, headers for MySQL? Or do this in write_data?
    # ??

    # Use the average station location as the earthquake's location
    df_out = df_avg

    df_out.printSchema()
    # Write data to MySQL
    write_data(df_out)

    spark.stop()

    endtime = time.time()
    print("It took {:.1f} seconds".format(endtime-starttime))
    #print(bounding_box)
