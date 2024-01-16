from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import to_timestamp, col, year, month, avg, regexp_extract
from pyspark.sql.types import TimestampType
import subprocess
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Initialize the Spark Context, and set the Log Level to only recieve erros
sc = SparkContext()
sc.setLogLevel("ERROR")

# Initiate the Spark Session
spark = SparkSession.builder.getOrCreate()

#read in the files
command = "hadoop fs -ls /user/s2284456/filtered"
file_list = subprocess.check_output(command, shell=True).decode('utf-8').split('\n')
file_list = [line.split()[-1] for line in file_list if len(line.split()) > 0]
file_list = file_list[2:]

df_stats = None

# Define the schema for the empty DataFrame
schema = StructType([
    StructField("origin", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("time_difference", DoubleType(), True),
    StructField("firstseen", TimestampType(), True),
    StructField("filename", StringType(), True)
])

df = spark.createDataFrame([], schema)

for file in file_list:
    new_df = spark.read.option("header", "true").csv(file)
    new_df = new_df.withColumnRenamed(new_df.columns[0],"origin")
    new_df = new_df.withColumnRenamed(new_df.columns[1],"destination")
    new_df = new_df.withColumnRenamed(new_df.columns[2],"time_difference")
    new_df = new_df.withColumnRenamed(new_df.columns[3],"firstseen")
    new_df = new_df.withColumnRenamed(new_df.columns[4],"filename")
    df = df.unionByName(new_df)
    
df = df.withColumn("year", year("firstseen"))
df = df.withColumn("month", month("firstseen"))
    
    # Calculate the average time_difference for each origin-destination pair per month-year
df1 = df.filter(df.time_difference < 64800.0)
df_avg_time_difference = df1.groupBy("origin", "destination", "year", "month").agg(avg("time_difference").alias("avg_time_difference"))
    
    # Count the number of flights for each origin-destination pair per month-year
df_flight_count = df.groupBy("origin", "destination", "year", "month").count()
    
    # Join df_avg_time_difference with df_flight_count on origin, destination, year, and month columns
df_avg_time_difference = df_avg_time_difference.join(df_flight_count, ["origin", "destination", "year", "month"], "left_outer")
    
    # Append the current DataFrame to the combined DataFrame
if df_stats is None:
    df_stats = df_avg_time_difference
else:
    df_stats = df_stats.union(df_avg_time_difference)

# Show the average time difference and flight count
df_stats.show(10)

# Write the DataFrame to a csv file
df_stats.write.csv("/user/s2284456/stats", header=True, mode="overwrite")

#check size of the file
command = "hadoop fs -du -s /user/s2284456/stats"
file_size_bytes = int(subprocess.check_output(command, shell=True).split()[0])
file_size_mb = file_size_bytes / (1024 * 1024)
print(file_size_mb)
