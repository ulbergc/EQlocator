
from pyspark.sql import SparkSession
from pyspark.sql import Row

def read_data(spark):
    df = spark.read \
        .format("mongo") \
        .load()
    df.printSchema()


if __name__ == "__main__":
#    master_DNS="ec2-34-223-143-198.us-west-2.compute.amazonaws.com"
    spark = SparkSession \
        .builder \
        .appName("MigrateData") \
        .config("spark.mongodb.input.uri","mongodb://10.0.0.4/dbsmall.movie") \
        .config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.11:2.4.1")
        .getOrCreate()
    
    
    #        .master("spark://" + master_DNS + ":7077") \

    read_data(spark)

    spark.stop()
