from pyspark.sql import SparkSession
import scipy as sp 

if __name__ == "__main__":
    spark = (SparkSession.builder.appName("linkage").getOrCreate())

    prev = spark.read.option("header", "true").option("nullValue", "?").option("inferSchema", "true").csv("datasets/donations")

    prev.show()

    prev.schema
    
