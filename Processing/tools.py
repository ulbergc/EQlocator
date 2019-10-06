import pyspark.sql.types as T
from pyspark.sql import SQLContext
from pyspark.sql.functions import countDistinct
import os


def read_data(spark):
    '''
    Read earthquake arrival time data from S3
    Return as Spark DataFrame
    '''
    # path_single is for testing a single 'day' ~37k events
    path_single = 's3a://ulberg-insight/test1set/19800201.json'
    archive_path = 's3a://ulberg-insight/archive'

    molimit = 2  # 13 for all, 2 for single month
    yrlimit = 1  # 5 for all, 1 for single year
    mo = ['{:02d}01'.format(x) for x in range(1, molimit)]

    months = []
    for i in range(yrlimit):
        tmpmo = ['198{}{}'.format(i, x) for x in mo]
        months += tmpmo

    path = ['{p1}/{mo}/{mo}.json'
            .format(p1=archive_path, mo=month) for month in months]
    df = spark.read.json(path)
    df.printSchema()

    return df


def read_station(spark):
    '''
    Read station location information from S3
    Return as Spark DataFrame
    '''
    sql_context = SQLContext(spark)
    sta = sql_context.read  \
        .load('s3a://ulberg-insight/info/stations_small.csv',
            format='com.databricks.spark.csv')

    # change stations schema
    sta = sta.selectExpr("_c0 as sta",
                        "_c1 as Station_Longitude",
                        "_c2 as Station_Latitude",
                        "_c3 as Station_Depth")
    sta = sta.withColumn("Station_Latitude", sta["Station_Latitude"]
                    .cast(T.DoubleType())) \
            .withColumn("Station_Longitude", sta["Station_Longitude"]
                    .cast(T.DoubleType())) \
            .withColumn("Station_Depth", sta["Station_Depth"]
                    .cast(T.DoubleType()))

    return sta


def write_data(df, db_name, table_name):
    '''
    Write Spark DataFrame to database
    '''
    mysql_host = os.environ['MYSQL_PATH']
    mysql_db = db_name
    mysql_table = table_name
    mysql_user = os.environ['MYSQL_USER']
    mysql_pw = os.environ['MYSQL_PASSWORD']
    df.write.format("jdbc").options(
        url='jdbc:mysql://{}/{}'.format(mysql_host, mysql_db),
        dbtable=mysql_table,
        user=mysql_user,
        password=mysql_pw,
        driver="com.mysql.cj.jdbc.Driver") \
        .save()


def count_station_obs(df):
    '''
    Count the number of times each station had an earthquake arrival time
    '''
    sta_count = df.groupby('sta').agg(countDistinct('Event_id'))
    sta_count = sta_count \
        .withColumn('count', sta_count['count(DISTINCT Event_id)'])
    sta_count = sta_count.drop('count(DISTINCT Event_id)')
    return sta_count
