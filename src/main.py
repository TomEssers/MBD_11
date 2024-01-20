from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, to_timestamp, col, regexp_extract


def import_clean_data(spark, icao_data_path, aircraft_data_path, lockdown_data_path, flight_data_path):
    # Read ICAO data and filter relevant column
    df_icao = spark.read.csv(icao_data_path, header=True, inferSchema=True, sep=';')
    icao_list = df_icao.select('ICAO').collect()

    # Read aircraft data, filter, and select relevant column
    df_aircraft = spark.read.csv(aircraft_data_path, header=True, inferSchema=True, sep=';')
    df_aircraft = df_aircraft.filter(df_aircraft.Passengers >= 20)
    aircraft_list = df_aircraft.select('CODE').collect()

    # Read lockdown data
    lockdown_list = spark.read.csv(lockdown_data_path, header=True, inferSchema=True)
    # Add start_date and end_date as timestamps, instead of dates
    lockdown_list = lockdown_list.withColumn("start_date", to_timestamp(col("start_date"), "dd/MM/yyyy")) \
        .withColumn("end_date", to_timestamp(col("end_date"), "dd/MM/yyyy"))

    # TODO: Lockdown_list gebruiken

    # Read flight data
    df_flights = spark.read.csv(flight_data_path, header=True, inferSchema=True)

    # Filter for data where the start is the Netherlands (EH)
    # Filter out very small Dutch airports
    # Filter for only destinations in our icao list, these are European airports
    # Remove flights that have same origin as destination
    # Only select flights that had planes that can take at least 20 passengers
    final_df = (
        df_flights.filter(
            (col("origin").startswith("EH")) &
            ~col("origin").isin(
                ["EHVK", "EHDB", "EHDP", "EHGR", "EHDS", "EHND", "EHMZ", "EHDR", "EHHV", "EHLW", "EHWO", "EHOW"]) &
            col("destination").substr(1, 2).isin([row.ICAO for row in icao_list]) &
            (col("origin") != col("destination")) &
            col("typecode").isin([row.CODE for row in aircraft_list])
        )
        .withColumn("filename", regexp_extract(input_file_name(), r"flightlist_(\d{8}_\d{8})", 1))
        .withColumn("firstseen", to_timestamp(col("firstseen")))
        .withColumn("lastseen", to_timestamp(col("lastseen")))
        .withColumn("time_difference", (col("lastseen").cast("long") - col("firstseen").cast("long")))
        .select("origin", "destination", "time_difference", "typecode", "firstseen", "filename")
    )

    return final_df


def compute_statistics(cleaned_df):
    print('later')


if __name__ == "__main__":
    # Initialize the Spark Session and set the Log Level to only receive errors
    sc = SparkContext()
    sc.setLogLevel("ERROR")

    # Initiate the Spark Session
    spark = SparkSession.builder.getOrCreate()

    # File paths to data
    icao_data_path = "/user/s2484765/project/icao_europe.csv"
    lockdown_data_path = "/user/s2484765/project/lockdown_dates.csv"
    aircraft_data_path = "/user/s2484765/project/aircrafts.csv"
    flight_data_path = "/user/s2484765/project/flightdata"
    result_data_path = "/user/s2484765/project/results"

    # Import the data and clean it
    cleaned_df = import_clean_data(spark, icao_data_path, aircraft_data_path, flight_data_path)

    # Compute the statistics on the partitions
    compute_statistics(cleaned_df)
