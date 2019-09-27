from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import Row
import time
import sys

def read_data(spark):
    df = spark.read \
        .format("mongo") \
        .load()
    df.printSchema()

    return df

# write dataframe to destination 
# - include options argument/config file to define this instead of in code?
def write_data(df):
    #data_to_write=df.select("_id","Latitude","Longitude")
    data_to_write=df
    #data_to_write.show()

    # check that database is created and table is not

    # define 
    mysqlhost="10.0.0.11:3306/"
    mysqldb="tmp4"
    mysqltable="table1"
    mysqluserpw="?user=user&password=pw"
    data_to_write.write.format("jdbc").options(
        url=("jdbc:mysql://" + mysqlhost + mysqldb + mysqluserpw),
        driver="com.mysql.cj.jdbc.Driver",
        dbtable=mysqltable).save()

    # could add .mode("append") before save to append, otherwise creates new

# Locate earthquake
# - create separate class to do this and allow for different methods?
def locateEQ(x,sta,boundingBox):
    print("Processing event {evid}, {nobs} observations".format(evid=x._id,nobs=len(x.Observations)))
    #print(type(x))

    # default values
    idtmp=x._id
    lattmp=float(x.Latitude)
    lontmp=float(x.Longitude)
    # check if Observations exists, if so, load it
    obsName="Observations"
    if obsName in x:
	obs=x[obsName]
	print(type(obs))  
	# test value
	lattmp=obs[0]["Uncertainty"]


    return (idtmp,lattmp,lontmp)

if __name__ == "__main__":
#    master_DNS="ec2-34-223-143-198.us-west-2.compute.amazonaws.com"

    if len(sys.argv) != 3:
	print("Usage: process_data.py <dbname> <tablename>")
	sys.exit(-1)

    # Define spark session with read configuration
    # !!!change this to read from S3
    dbname=sys.argv[1] # small-tmp1; large-tmp0
    collname=sys.argv[2] # small-c0; large-c0
    spark = SparkSession \
        .builder \
        .appName("ProcessData") \
        .config("spark.mongodb.input.uri","mongodb://10.0.0.5/{db}.{col}".format(db=dbname,col=collname)) \
        .config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.11:2.4.1") \
        .getOrCreate()
    
    
    #        .master("spark://" + master_DNS + ":7077") \
    starttime=time.time()

    # read data from MongoDB
    df=read_data(spark)
    
    # read station information
    sta=spark.read.format("mongo").option("uri","mongodb://10.0.0.5/{db}.{col}".format(db=dbname,col="sta")).load()   
    print("{} stations".format(sta.count()))
    
    # get min/max location values 
    print(sta.printSchema())
    lat_min=sta.agg({"Latitude": "min"}).head()[0]
    lat_max=sta.agg({"Latitude": "max"}).head()[0]
    lon_min=sta.agg({"Longitude": "min"}).head()[0]
    lon_max=sta.agg({"Longitude": "max"}).head()[0]
    print('*** Station bounding box= [{:.4f} {:.4f} {:.4f} {:.4f}]'.format(lat_min,lat_max,lon_min,lon_max))
    print('*** lat_min={:.4f}'.format(lat_min))

    ### Process each record ###
    # using foreach will not return anything - can edit df or write intermediate results?
    #df.foreach(lambda x: locateEQ(x,'fe'))
    
    # using map because it will return a transformed rdd
    d_rdd=df.rdd.map(lambda x: locateEQ(x,sta,[lat_min,lat_max,lon_min,lon_max]))
    #print(d_rdd.count())

    # drop this from memory, don't need it
#    df.unpersist()

    # create schema for transformed rdd
    fields = [StructField("Event_id", IntegerType(), True), 
            StructField("Latitude", FloatType(), True),
            StructField("Longitude", FloatType(), True)]
    sql_schema = StructType(fields)

#    d_rdd_with_schema = sqlContext.applySchema(d_rdd,sql_schema)
    df_out=spark.createDataFrame(d_rdd,sql_schema)
    # This version creates df without defined schema
    #df_out=spark.createDataFrame(d_rdd)

    df_out.printSchema()
    
    # simple df_out for testing
#    df_out=df.select("_id","Latitude","Longitude")
    #print(d_rdd.first()[0])
    ##df2.printSchema()
    # Create table, headers for MySQL? Or do this in write_data?
    #?? 
    
    # Write data to MySQL
    write_data(df_out)

    spark.stop()
    
    endtime=time.time()
    print("It took {:.1f} seconds".format(endtime-starttime))

