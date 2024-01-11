from pyspark import SparkContext
from pyspark.sql import SparkSession

# Initialize the Spark Context, and set the Log Level to only recieve erros
sc = SparkContext()
sc.setLogLevel("ERROR")

# Initiate the Spark Session
spark = SparkSession.builder.getOrCreate()

# Read the first datafile from the dataset
df1 = spark.read.json("/user/s2484765/project/flightdata.zip/flightlist_20190101_20190131.csv.gz")



print('test')