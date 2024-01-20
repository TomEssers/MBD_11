from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, to_timestamp, col, regexp_extract

# Initialize the Spark Context, and set the Log Level to only receive errors
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
# TODO This is overkill with PySpark as it is small data, so look at this later
df_icao = spark.read.csv(icao_data_path, header=True, inferSchema=True, sep=';')
icao = df_icao.select('ICAO').rdd.flatMap(lambda x: x).collect()
# Get a list of all aircraft that can carry more 20 passengers or more
df_aircraft = spark.read.csv(aircraft_data_path, header=True, inferSchema=True, sep=';')
df_aircraft = df_aircraft.filter(df_aircraft.Passengers >= 20)
aircraft = df_aircraft.select('CODE').rdd.flatMap(lambda x: x).collect()

# Import all flight data of the period Jan 2020 till May 2022 into a dataframe
df_flights = spark.read.csv(flight_data_path, header=True, inferSchema=True)

# Filter for data where the start is the Netherlands (EH)
# Filter out very small Dutch airports
# Filter for only destinations in our icao list, these are European airports
# Remove flights that have same origin as destination
# Only select flights that had planes that can take at least 20 passengers
df1 = df_flights.filter(
    df_flights.origin.startswith("EH") &
    ~df_flights.origin.isin(
        ["EHVK", "EHDB", "EHDP", "EHGR", "EHDS", "EHND", "EHMZ", "EHDR", "EHHV", "EHLW", "EHWO", "EHOW"]) &
    df_flights.destination.substr(1, 2).isin(icao) & (df_flights.origin != df_flights.destination) &
    df_flights.typecode.isin(aircraft))

# Add new columns filename (time period), first seen, last seen, and time difference
df1 = df1.withColumn("filename", regexp_extract(input_file_name(), r"flightlist_(\d{8}_\d{8})", 1)) \
    .withColumn("firstseen", to_timestamp(df1.firstseen)) \
    .withColumn("lastseen", to_timestamp(df1.lastseen)) \
    .withColumn("time_difference", col("lastseen").cast("long") - col("firstseen").cast("long"))

# Select only the useful columns
final_df = df1.select(df1.origin, df1.destination, df1.time_difference, df1.typecode, df1.firstseen, df1.filename)\

# Save the dataframe on HDFS in CSV files
final_df.write.option("header","true").mode("append").csv("/user/s2484765/testing_project")

