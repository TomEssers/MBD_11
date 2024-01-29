from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, to_timestamp, col, regexp_extract, when
from pyspark.sql.types import BooleanType
from datetime import datetime

def import_and_clean_data(spark, icao_data_path, aircraft_data_path, lockdown_data_path, flight_data_path, covid_start_date, covid_end_date):
    # Read ICAO data and filter relevant column, put result in python array (is small data)
    df_icao = spark.read.csv(icao_data_path, header=True, inferSchema=True, sep=';')
    icao_list = df_icao.select('ICAO').collect()

    # Read aircraft data, filter, and select relevant column, put result in python array (is small data)
    df_aircraft = spark.read.csv(aircraft_data_path, header=True, inferSchema=True, sep=';')
    df_aircraft = df_aircraft.filter(df_aircraft.Passengers >= 20)
    aircraft_list = df_aircraft.select('CODE').collect()

    # Read lockdown data, and put in puthon array
    df_lockdown_list = spark.read.csv(lockdown_data_path, header=True, inferSchema=True)
    # Add start_date and end_date as timestamps, instead of dates
    df_lockdown_list = df_lockdown_list.withColumn("start_date", to_timestamp(col("start_date"), "dd/MM/yyyy")) \
        .withColumn("end_date", to_timestamp(col("end_date"), "dd/MM/yyyy"))

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
        .withColumn("is_during_covid", when(
            (col("firstseen") >= datetime.strptime(covid_start_date, "%Y-%m-%d")) & (col("firstseen") <= datetime.strptime(covid_end_date, "%Y-%m-%d")), True)
                    .otherwise(False).cast(BooleanType()))
    )

    # Join the lockdown_list DataFrame to final_df based on 'firstseen' timestamp
    final_df = final_df.join(df_lockdown_list, (col("firstseen") >= df_lockdown_list["start_date"]) & (col("firstseen") <= df_lockdown_list["end_date"]), "left_outer")
    # If there is a match, use the lockdown_name otherwise, leave it empty
    final_df = final_df.withColumn("lockdown_name", when(col("lockdown_name").isNotNull(), col("lockdown_name")).otherwise(""))

    # Return the final dataframe with the following columns
    return final_df.select("origin", "destination", "time_difference", "typecode", "firstseen", "filename", "is_during_covid", "lockdown_name")


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

    # Covid start and end date
    covid_start_date = "2020-01-17"
    covid_end_date = "2022-05-30"

    # Import the data and clean it
    cleaned_df = import_and_clean_data(spark, icao_data_path, aircraft_data_path, lockdown_data_path, flight_data_path, covid_start_date, covid_end_date)

    # Download result
    cleaned_df.write.csv("/user/s2484765/testing_project", header=True)
