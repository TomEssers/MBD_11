from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import to_timestamp, col, year, month, avg, regexp_extract
import subprocess

# Initialize the Spark Context, and set the Log Level to only recieve erros
sc = SparkContext()
sc.setLogLevel("ERROR")

# Initiate the Spark Session
spark = SparkSession.builder.getOrCreate()

# Read the first datafile from the dataset
df2 = spark.read.option("header", "true").option("delimiter",";").csv("/user/s2284456/ICAO_europe.csv")

# Extract the relevant column as a list
icao = [row['ICAO'] for row in df2.collect()]

command = "hadoop fs -ls /user/s2484765/project/flightdata/"
file_list = subprocess.check_output(command, shell=True).decode('utf-8').split('\n')

# Remove the first line (it's just a header) and the last line (it's empty) and remove the files that are not gzipped files
file_list = [line.split()[-1] for line in file_list if len(line.split()) > 0]
file_list = [line for line in file_list if line.endswith(".gz")]
file_list = file_list[1:-1]

command = "hadoop fs -test -d /user/s2284456/filtered"
is_directory = subprocess.call(command, shell=True) == 0

if is_directory:
    command = "hadoop fs -rm -r /user/s2284456/filtered"
    subprocess.call(command, shell=True)

for file in file_list:
    df1 = spark.read.option("header", "true").csv(file)
    df1 = df1.filter(df1.origin.startswith("EH") & df1.destination.substr(1,2).isin(icao) & ~df1.origin.isin(["EHVK","EHDB","EHDP","EHGR","EHDS","EHND","EHMZ","EHDR","EHHV","EHLW","EHWO","EHOW"]) & (df1.origin != df1.destination))
    df1 = df1.withColumn("filename", regexp_extract(input_file_name(), r"flightlist_(\d{8}_\d{8})", 1))
    df1 = df1.withColumn("firstseen", to_timestamp(df1.firstseen))
    df1 = df1.withColumn("lastseen", to_timestamp(df1.lastseen))
    df1 = df1.withColumn("time_difference", col("lastseen").cast("long") - col("firstseen").cast("long"))  

    column_names = df1.columns
    df1 = df1.union(spark.createDataFrame([column_names], df1.columns))

    df1.select(df1.origin, df1.destination, df1.time_difference, df1.firstseen, df1.filename).write.mode("append").csv("/user/s2284456/filtered")

#check size of the file
command = "hadoop fs -du -s /user/s2284456/filtered"
file_size_bytes = int(subprocess.check_output(command, shell=True).split()[0])
file_size_mb = file_size_bytes / (1024 * 1024)
print(file_size_mb)
