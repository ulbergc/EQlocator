from pyspark.sql import SQLContext
# from pyspark.sql.types import StructField, IntegerType, FloatType, StructType
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from pyspark.sql import Row
import time
import sys
import pandas as pd


def read_data(spark):
    path = 's3a://ulberg-insight/testing/multiline/uncomp/small/*.json'
    df = spark.read.json(path)
    #df = spark.read \
    #    .load('s3a://ulberg-insight/testing/small/small.tar.gz',
    #    format='com.databricks.spark.csv')
    df.printSchema()

    return df


# write dataframe to destination
# include options argument/config file to define this instead of in code?
def write_data(df):
    # data_to_write=df.select("Event_id","Latitude","Longitude")
    data_to_write = df
    print(df.count())

    # check that database is created and table is not
    # define
    mysql_host = "jdbc:mysql://10.0.0.11:3306/"
    mysql_db = "tmp4"
    mysql_table = "table1"
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
    # check if Observations exists, if so, load it
    obsName = "Observations"
    #if obsName in x:
    #    obs = x[obsName]
    #    print(type(obs))
        # test value
        # lattmp = obs[0]["Uncertainty"]

    #return Row(Event_id=idtmp, Latitude=lattmp, Longitude=lontmp)
    return Row(idtmp, lattmp, lontmp)


def locate_simple(x):
    return x


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

    # read data from S3
    df = read_data(spark)
    print("DF has {} records".format(df.count()))

    # read station information from S3
    sql_context = SQLContext(spark)
    sta = sql_context.read  \
            .load('s3a://ulberg-insight/info/stations.csv',
            format='com.databricks.spark.csv')
    print("{} stations".format(sta.count()))

    # keep this information cached since it is small and will be used a lot
    sta.cache()

    # convert sta to rdd so it can be passed to locate_eq
    sta_pd = sta.toPandas()
    print(type(sta_pd))

    # this way doesn't work
    #sta_rdd2 = sta.rdd.map(list)
    #print(type(sta_rdd2))

    # get min/max location values
    #print(sta.printSchema())
    #lat_min = sta.agg({"Latitude": "min"}).head()[0]
    #lat_max = sta.agg({"Latitude": "max"}).head()[0]
    #lon_min = sta.agg({"Longitude": "min"}).head()[0]
    #lon_max = sta.agg({"Longitude": "max"}).head()[0]
    #print('*** Station bounding box= [{:.4f} {:.4f} {:.4f} {:.4f}]'
    #      .format(lat_min, lat_max, lon_min, lon_max))
    #print('*** lat_min={:.4f}'.format(lat_min))
    #bounding_box = [lat_min, lat_max, lon_min, lon_max]

    # Process each record
    # using foreach will not return anything
    # can edit df or write intermediate results?
    # df.foreach(lambda x: locateEQ(x,'fe'))

    # using map will return a transformed rdd
    # if you pass sta in as well it will fail: pickling error???
    #d_rdd = df.rdd.map(lambda x: locate_simple(x))
    d_rdd = df.rdd \
        .map(lambda x: locate_EQ(x, sta_pd))
    # print(d_rdd.count())

    # drop this from memory, don't need it
    # df.unpersist()

    # create schema for transformed rdd
    fields = [T.StructField("Event_id", T.IntegerType(), True),
              T.StructField("Latitude", T.FloatType(), True),
              T.StructField("Longitude", T.FloatType(), True)]
    sql_schema = T.StructType(fields)

    # d_rdd_with_schema = sqlContext.applySchema(d_rdd,sql_schema)
    df_out = spark.createDataFrame(d_rdd, sql_schema)
    # This version creates df without defined schema
    #df_out = spark.createDataFrame(d_rdd)

    df_out.printSchema()

    # simple df_out for testing
    #df_out=df.select("Event_id","Latitude","Longitude")
    # print(d_rdd.first()[0])
    # df2.printSchema()
    # Create table, headers for MySQL? Or do this in write_data?
    # ??

    # Write data to MySQL
    write_data(df_out)

    spark.stop()

    endtime = time.time()
    print("It took {:.1f} seconds".format(endtime-starttime))
    #print(bounding_box)
