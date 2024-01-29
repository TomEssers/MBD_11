# focused on research question: How did the delay times evolve over the COVID period in comparison to the years previous to COVID?
# Eurocontrol flight data columns:
    # ECTRL ID                          Eurocontrol ID, unique for each flight
    # ADEP	                            Departure airport ICAO code
    # ADEP Latitude	                    Departure airport latitude
    # ADEP Longitude                    Departure airport longitude
	# ADES                              Destination airport ICAO code
	# ADES Latitude                     Destination airport latitude
	# ADES Longitude                    Destination airport longitude
	# FILED OFF BLOCK TIME              The planned time of departure
	# FILED ARRIVAL TIME                The planned time of arrival
	# ACTUAL OFF BLOCK TIME             The measured time of departure
	# ACTUAL ARRIVAL TIME	            The measured time of arrival
    # AC Type                           The type of aircraft used for the flight
	# AC Operator                       The ICAO airline code of the organization operating the aircraft
	# AC Registration                   The registration code of the aircraft used
	# ICAO Flight Type                  The type of flight. S = scheduled. N = Not scheduled.
	# STATFOR Market Segment            The market segment this flight was used for. See below for the different market segment options.
	# Requested FL	                    The planned cruising altitude of the flight
    # Actual Distance Flown (nm)        The measured flown distance in nautical miles (nm)
# STATFOR Market Segments (source: https://www.eurocontrol.int/sites/default/files/2022-05/eurocontrol-market-segment-update-2022-05.pdf):
    # Low-Cost Scheduled (32% in 2019)
    # Mainline (36% in 2019)
    # Regional (15% in 2019)
    # Non-scheduled (4% in 2019)
    # Business aviation (6% in 2019)
    # All-Cargo (3% in 2019)
    # Military (1% in 2019)
    # Other (3% in 2019)

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import to_timestamp, col, regexp_extract, when, year, month, avg
from pyspark.sql.types import BooleanType
from datetime import datetime

# resulting csv: ADEP, ADES, filed_time_difference, AC Type, actual_departure, filename, is_during_covid, delay_departure, delay_arrival
def import_and_clean_data_eurocontrol(spark, icao_data_path, aircraft_data_path, lockdown_data_path, flight_data_path, covid_start_date, covid_end_date):
    # Read ICAO data and filter relevant column
    df_icao = spark.read.csv(icao_data_path, header=True, inferSchema=True, sep=';')
    icao_list = df_icao.select('ICAO').collect()

    # Read aircraft data, filter, and select relevant column
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
    # Add dates as timestamps instead of dates
    df_flights = df_flights.withColumn("FILED OFF BLOCK TIME", to_timestamp(col("FILED OFF BLOCK TIME"), "dd-MM-yyyy HH:mm:ss")) \
        .withColumn("ACTUAL OFF BLOCK TIME", to_timestamp(col("ACTUAL OFF BLOCK TIME"), "dd-MM-yyyy HH:mm:ss")) \
        .withColumn("ACTUAL ARRIVAL TIME", to_timestamp(col("ACTUAL ARRIVAL TIME"), "dd-MM-yyyy HH:mm:ss")) \
        .withColumn("FILED ARRIVAL TIME", to_timestamp(col("FILED ARRIVAL TIME"), "dd-MM-yyyy HH:mm:ss"))


    # Filter for data where the start is the Netherlands (EH)
    # Filter out very small Dutch airports
    # Filter for only destinations in our icao list, these are European airports
    # Remove flights that have same origin as destination
    # Only select flights that had planes that can take at least 20 passengers
    final_df = (
        df_flights.filter(
            (col("ADEP").startswith("EH")) &
            ~col("ADEP").isin(
                ["EHVK", "EHDB", "EHDP", "EHGR", "EHDS", "EHND", "EHMZ", "EHDR", "EHHV", "EHLW", "EHWO", "EHOW"]) &
            col("ADES").substr(1, 2).isin([row.ICAO for row in icao_list]) &
            (col("ADEP") != col("ADES")) &
            col("AC Type").isin([row.CODE for row in aircraft_list])
        )
        .withColumn("filename", regexp_extract(input_file_name(), r"Flights_(\d{4}_\d{2})", 1))
        .withColumn("actual_departure", col("ACTUAL OFF BLOCK TIME"))
        .withColumn("delay_departure", (col("ACTUAL OFF BLOCK TIME").cast("long") - col("FILED OFF BLOCK TIME").cast("long")))
        .withColumn("delay_arrival", (col("ACTUAL ARRIVAL TIME").cast("long") - col("FILED ARRIVAL TIME").cast("long")))
        .withColumn("filed_time_difference", (col("FILED ARRIVAL TIME").cast("long") - col("FILED OFF BLOCK TIME").cast("long")))
        .withColumn("is_during_covid", when(
            (col("actual_departure") >= datetime.strptime(covid_start_date, "%Y-%m-%d")) & (col("actual_departure") <= datetime.strptime(covid_end_date, "%Y-%m-%d")), True)
                    .otherwise(False).cast(BooleanType()))
        .select("ADEP", "ADES", "filed_time_difference", "AC Type", "actual_departure", "filename", "is_during_covid", "delay_departure", "delay_arrival")
    )

    return final_df

# Returns a dataframe containing the statistics for each departure airport.
def stats_per_departure_per_year(data_path, is_during_covid):
    # Read data
    df_data = spark.read.csv(data_path, header=True, inferSchema=True)

    # filter and group data
    final_df = (
        df_data.filter(
            col("is_during_covid") == is_during_covid
        )
        .withColumn("year", year(col("actual_departure")))
        .withColumn("total_delay", col("delay_departure") + col("delay_arrival"))
        .select("ADEP", "year", "delay_departure", "delay_arrival", "total_delay")
        .groupBy("ADEP", "year").agg(avg("delay_departure").alias("avg_delay_departure"), avg("delay_arrival").alias("avg_delay_destination"), avg("total_delay").alias("avg_total_delay"))
    )
    return final_df

# Returns a dataframe containing the statistics for each destination airport.
def stats_per_destination_per_year(data_path, is_during_covid):
    # Read data
    df_data = spark.read.csv(data_path, header=True, inferSchema=True)

    # filter and group data
    final_df = (
        df_data.filter(
            col("is_during_covid") == is_during_covid
        )
        .withColumn("year", year(col("actual_departure")))
        .withColumn("total_delay", col("delay_departure") + col("delay_arrival"))
        .select("ADES", "year", "delay_departure", "delay_arrival", "total_delay")
        .groupBy("ADES", "year").agg(avg("delay_departure").alias("avg_delay_departure"), avg("delay_arrival").alias("avg_delay_destination"), avg("total_delay").alias("avg_total_delay"))
    )
    return final_df

# Returns a dataframa containing the statistics for each route.
def stats_per_route_per_year(data_path, is_during_covid):
    # Read data
    df_data = spark.read.csv(data_path, header=True, inferSchema=True)

    # filter and group data
    final_df = (
        df_data.filter(
            col("is_during_covid") == is_during_covid
        )
        .withColumn("year", year(col("actual_departure")))
        .withColumn("total_delay", col("delay_departure") + col("delay_arrival"))
        .withColumn("actual_duration", col("filed_time_difference") + col("total_delay"))
        .select("ADEP", "ADES", "year", "delay_departure", "delay_arrival", "total_delay", "filed_time_difference", "actual_duration")
        .groupBy("ADEP", "ADES", "year").agg(avg("delay_departure").alias("avg_delay_departure"), avg("delay_arrival").alias("avg_delay_destination"), avg("total_delay").alias("avg_total_delay"), avg("filed_time_difference").alias("avg_filed_duration"), avg("actual_duration").alias("avg_actual_duration"))
    )
    return final_df

# Returns a dataframe containing the statistics for each aircraft.
def stats_per_aircraft_per_year(data_path, is_during_covid):
    # Read data
    df_data = spark.read.csv(data_path, header=True, inferSchema=True)

    # filter and group data
    final_df = (
        df_data.filter(
            col("is_during_covid") == is_during_covid
        )
        .withColumn("year", year(col("actual_departure")))
        .withColumn("total_delay", col("delay_departure") + col("delay_arrival"))
        .withColumn("actual_duration", col("filed_time_difference") + col("total_delay"))
        .select("AC Type", "year", "delay_departure", "delay_arrival", "total_delay", "filed_time_difference", "actual_duration")
        .groupBy("AC Type", "year").agg(avg("delay_departure").alias("avg_delay_departure"), avg("delay_arrival").alias("avg_delay_destination"), avg("total_delay").alias("avg_total_delay"), avg("filed_time_difference").alias("avg_filed_duration"), avg("actual_duration").alias("avg_actual_duration"))
    )
    return final_df



# Initialize the Spark Context, and set the Log Level to only recieve erros
sc = SparkContext()
sc.setLogLevel("ERROR")

# Initiate the Spark Session
spark = SparkSession.builder.getOrCreate()

# File paths to data
icao_data_path = "/user/s2484765/project/icao_europe.csv"
lockdown_data_path = "/user/s2484765/project/lockdown_dates.csv"
aircraft_data_path = "/user/s2484765/project/aircrafts.csv"
flight_data_path = "/user/s2484765/project/eurocontrolflightdata"
result_data_path = "/user/s2484765/project/eurocontrolresults"
result_data_test_path = "/user/s2406020/project/eurocontrolresults"
result_stats_path = "/user/s2484765/project/eurocontrolstatsresults"
result_stats_test_path = "/user/s2406020/project/eurocontrolstatsresults"

# Covid start and end date
covid_start_date = "2020-01-17"
covid_end_date = "2022-05-30"

# Running settings
import_and_clean_data = False
create_stats = True
data_test = True
stats_test = True

if data_test:
    result_data_path = result_data_test_path
if stats_test:
    result_stats_path = result_stats_test_path

if import_and_clean_data:
    # import the data and clean it
    cleaned_eurocontrol_df = import_and_clean_data_eurocontrol(spark, icao_data_path, aircraft_data_path, lockdown_data_path, flight_data_path, covid_start_date, covid_end_date)

    # Download result
    cleaned_eurocontrol_df.write.csv(result_data_path, header=True)

if create_stats:
    # Create statistics
    stats_departure_airport_covid = stats_per_departure_per_year(result_data_path, True)
    stats_departure_airport_no_covid = stats_per_departure_per_year(result_data_path, False)
    stats_destination_airport_covid = stats_per_destination_per_year(result_data_path, True)
    stats_destination_airport_no_covid = stats_per_destination_per_year(result_data_path, False)
    stats_routes_covid = stats_per_route_per_year(result_data_path, True)
    stats_routes_no_covid = stats_per_route_per_year(result_data_path, False)
    stats_aircraft_covid = stats_per_aircraft_per_year(result_data_path, True)
    stats_aircraft_no_covid = stats_per_aircraft_per_year(result_data_path, False)

    # Download result
    stats_departure_airport_covid.repartition(1).write.csv(result_stats_path + "/departurecovid", header=True)
    stats_departure_airport_no_covid.repartition(1).write.csv(result_stats_path  + "/departurenocovid", header=True)
    stats_destination_airport_covid.repartition(1).write.csv(result_stats_path + "/destinationcovid", header=True)
    stats_destination_airport_no_covid.repartition(1).write.csv(result_stats_path  + "/destinationnocovid", header=True)
    stats_routes_covid.repartition(1).write.csv(result_stats_path + "/routescovid", header=True)
    stats_routes_no_covid.repartition(1).write.csv(result_stats_path + "/routesnocovid", header=True)
    stats_aircraft_covid.repartition(1).write.csv(result_stats_path + "/aircraftcovid", header=True)
    stats_aircraft_no_covid.repartition(1).write.csv(result_stats_path + "/aircraftnocovid", header=True)