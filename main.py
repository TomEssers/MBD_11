from pyspark import SparkContext
from pyspark.sql import SparkSession

# Initialize the Spark Context, and set the Log Level to only recieve erros
sc = SparkContext()
sc.setLogLevel("ERROR")

# Initiate the Spark Session
spark = SparkSession.builder.getOrCreate()

# Read the first datafile from the dataset
df1 = spark.read.option("header", "true").csv("/user/s2484765/project/flightdata/flightlist_20200101_20200131.csv.gz")

# Must rename the corrupt record column
df1.show()