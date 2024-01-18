from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import to_timestamp, col, regexp_extract

# Initialize the Spark Context, and set the Log Level to only recieve erros
sc = SparkContext()
sc.setLogLevel("ERROR")

# Initiate the Spark Session
spark = SparkSession.builder.getOrCreate()

# Path of all flight data
flight_data_path = "/user/s2484765/project/flightdata"
icao_data_path = "/user/s2484765/project/icao_europe.csv"
aircraft_data_path = "/user/s2484765/project/aircrafts.csv"
airports_data_path = "/user/s2484765/project/airport_codes.csv"
lockdown_dates_data_path = "/user/s2484765/project/lockdown_dates.csv"

# Get a python list of all icao codes 
# This is overkill with PySpark as it is small data, so look at this later
df_icao = spark.read.csv(icao_data_path, header=True, inferSchema=True, sep=';')
icao = df_icao.select('ICAO').rdd.flatMap(lambda x: x).collect()

# Import all flight data of the period Jan 2020 till ... into a dataframe
df_flights = spark.read.csv(flight_data_path, header=True, inferSchema=True)

# Filter for data where the start is the Netherlands (EH) and the destination is europe
# Additionally remove flights that have same origin as destination
df1 = df_flights.filter(
    ~df_flights.origin.isin(["EHVK", "EHDB", "EHDP", "EHGR", "EHDS", "EHND", "EHMZ", "EHDR", "EHHV", "EHLW", "EHWO", "EHOW"]) &
    df_flights.destination.substr(1, 2).isin(icao) & (df_flights.origin != df_flights.destination))


df1 = df1.withColumn("filename", regexp_extract(input_file_name(), r"flightlist_(\d{8}_\d{8})", 1))
df1 = df1.withColumn("firstseen", to_timestamp(df1.firstseen))
df1 = df1.withColumn("lastseen", to_timestamp(df1.lastseen))
df1 = df1.withColumn("time_difference", col("lastseen").cast("long") - col("firstseen").cast("long"))

df1.select(df1.origin, df1.destination, df1.time_difference, df1.firstseen, df1.filename).write.mode("append").csv(
    "/user/s2484765/testing_project")
