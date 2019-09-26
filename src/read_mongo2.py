from pyspark.sql import SQLContext
#from pyspark.sql import StructField 
#from pyspark.sql import StringType 
#from pyspark.sql import StructType
#from pyspark.sql import FloatType
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import Row
import time

def read_data(spark):
    df = spark.read \
        .format("mongo") \
        .load()
    df.printSchema()

#    docs=df.collect()
    
    return df


def write_data(df):
#    df.write.format('jdbc').options(
#        url='jdbc:mysql://localhost/database_name',
#        driver='com.mysql.jdbc.Driver',
#        dbtable='DestinationTableName',
#        user='your_user_name',
#        password='your_password').mode('append').save()

#    data_to_write=df.select("_id","name")
   # data_to_write=df.select("value","extra")
    data_to_write=df
    #data_to_write.show()
    
    mysqlhost="10.0.0.11:3306/"
    mysqldb="tmp"
    mysqluserpw="?user=user&password=pw"
#    data_to_write.write.format("jdbc").options(
#        url=("jdbc:mysql://" + mysqlhost + mysqldb + mysqluserpw),
#        driver="com.mysql.cj.jdbc.Driver",
#        dbtable="table1").save()

    # could add .mode("append") before save to append, otherwise creates new

def locateEQ(x,boundingBox):
    print("Processing event {evid}, {nobs} observations".format(evid=x.Event_id,nobs=len(x.Observations)))
    #print(type(x))
    #print(x)
    #print((float(x.Latitude) + float(x.Longitude))/2)
    return (x.Event_id,x.Latitude,x.Longitude)

if __name__ == "__main__":
#    master_DNS="ec2-34-223-143-198.us-west-2.compute.amazonaws.com"
    dbname="tmp3" # small-tmp1; large-tmp0
    collname="c0" # small-c0; large-c0
    spark = SparkSession \
        .builder \
        .appName("MigrateData") \
        .config("spark.mongodb.input.uri","mongodb://10.0.0.5/{db}.{col}".format(db=dbname,col=collname)) \
        .config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.11:2.4.1") \
        .getOrCreate()
    
    
    #        .master("spark://" + master_DNS + ":7077") \
    starttime=time.time()

    # read data from MongoDB
    df=read_data(spark)
    
    # read station information
    sta=spark.read.format("mongo").option("uri","mongodb://10.0.0.5/{db}.{col}".format(db=dbname,col="sta")).load()   
    # get min/max location values 
    print(sta.printSchema())
    lat_min=sta.agg({"Latitude": "min"}).head()[0]
    lat_max=sta.agg({"Latitude": "max"}).head()[0]
    lon_min=sta.agg({"Longitude": "min"}).head()[0]
    lon_max=sta.agg({"Longitude": "max"}).head()[0]
#    min_max=sta.agg(min("Latitude"),max("Latitude"),min("Longitude"),max("Longitude")).head()
 #   lat_min=min_max.getInt(0)
 #   lat_max=min_max.getInt(1)
#    lon_min=min_max.getInt(2)
#    lon_max=min_max.getInt(3)
    print('*** Station bounding box= [{:.4f} {:.4f} {:.4f} {:.4f}]'.format(lat_min,lat_max,lon_min,lon_max))
    print('*** lat_min={:.4f}'.format(lat_min))
   # process each record
    #df.foreach(lambda x: locateEQ(x,'fe'))
    
    # use map because it will return a transformed rdd
    d_rdd=df.rdd.map(lambda x: locateEQ(x,[lat_min,lat_max,lon_min,lon_max]))
    print(d_rdd.count())

    # create schema for transformed rdd
    fields = [StructField("Event_id", StringType(), True), 
            StructField("Latitude", FloatType(), True),
            StructField("Longitude", FloatType(), True)]
    sql_schema = StructType(fields)

    #d_rdd_with_schema = sqlContext.applySchema(d_rdd,sql_schema)
    df_out=spark.createDataFrame(d_rdd,sql_schema)

    df_out.printSchema()

    #print(d_rdd.first()[0])
    ##df2.printSchema()
    # Create table, headers for MySQL

    print("{} stations".format(sta.count()))
    #print("Sta box: [{} {} {} {}]".format(
#    sta.describe(['Latitude']).show()
    # Write data to MySQL
    write_data(df_out)

    spark.stop()
    
    endtime=time.time()
    print("It took {:.1f} seconds".format(endtime-starttime))
