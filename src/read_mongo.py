
from pyspark.sql import SparkSession
from pyspark.sql import Row

def read_data(spark):
    df = spark.read \
        .format("mongo") \
        .load()
    df.printSchema()
    return df


def write_data(df):
#    df.write.format('jdbc').options(
#        url='jdbc:mysql://localhost/database_name',
#        driver='com.mysql.jdbc.Driver',
#        dbtable='DestinationTableName',
#        user='your_user_name',
#        password='your_password').mode('append').save()

#    data_to_write=df.select("_id","name")
    data_to_write=df.select("name","format")
    data_to_write.show()
    
    mysqlhost="10.0.0.14:3306/"
    mysqldb="tmp"
    mysqluserpw="?user=user2&password=pw"
    data_to_write.write.format("jdbc").options(
        url=("jdbc:mysql://" + mysqlhost + mysqldb + mysqluserpw),
        driver="com.mysql.cj.jdbc.Driver",
        dbtable="test1").save()

    # could add .mode("append") before save to append, otherwise creates new


if __name__ == "__main__":
#    master_DNS="ec2-34-223-143-198.us-west-2.compute.amazonaws.com"
    spark = SparkSession \
        .builder \
        .appName("MigrateData") \
        .config("spark.mongodb.input.uri","mongodb://10.0.0.4/dbsmall.movie") \
        .config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.11:2.4.1") \
        .getOrCreate()
    
    
    #        .master("spark://" + master_DNS + ":7077") \

    # read data from MongoDB
    df=read_data(spark)

    # Create table, headers for MySQL


    # Write data to MySQL
    write_data(df)

    spark.stop()
