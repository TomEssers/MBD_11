from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, avg

sc = SparkContext()
sc.setLogLevel("DEBUG")

spark = SparkSession.builder.getOrCreate()

def read_csv_files(file_list, schema):
    df_stats = None
    for file in file_list:
        new_df = spark.read.option("header", "true").csv(file)

        for i, column in enumerate(new_df.columns):
            new_df = new_df.withColumnRenamed(column, schema[i].name)

        if df_stats is None:
            df_stats = new_df
        else:
            df_stats = df_stats.union(new_df)

    return df_stats

def filtered_count(df_stats, origin):
    df_filtered = df_stats.filter(df_stats.origin == origin)
    df_flight_count = df_filtered.groupBy("origin", "destination", year("firstseen"), month("firstseen")).count()
    df_flight_count.write.csv("hdfs:///user/s2284456/flight_count_" + origin, header=True, mode="append")

def filtered_avg_td(df_stats, origin, destination):
    df_filtered = df_stats.filter(df_stats.origin == origin).filter(df_stats.destination == destination)
    df_filtered = df_filtered.filter(df_filtered.time_difference < 64800.0)
    df_avg_time_difference = df_filtered.groupBy("origin", "destination", year("firstseen"), month("firstseen")).agg(avg("time_difference").alias("avg_time_difference"))
    output_folder = "hdfs:///user/s2284456/avg_timedif_" + origin + "_" + destination
    df_avg_time_difference.write.csv(output_folder, header=True, mode="append")
    df_avg_time_difference.show(10)

def count(df_stats):
    df_flight_count = df_stats.groupBy("origin", "destination").count()
    df_flight_count.write.csv("hdfs:///user/s2284456/flight_count", header=True, mode="append")

def write_min_max_duration(df_stats):
        df_duration = df_stats.groupBy("origin", "destination", year("firstseen"), month("firstseen")).agg(min("time_difference").alias("min_duration"), max("time_difference").alias("max_duration"))
        output_folder = "hdfs:///user/s2284456/min_max_duration"
        df_duration.write.csv(output_folder, header=True, mode="append")

def write_min_max_numflights(df_stats):
        df_numflights = df_stats.groupBy("origin", "destination", year("firstseen"), month("firstseen")).agg(min("count").alias("min_numflights"), max("count").alias("max_numflights"))
        output_folder = "hdfs:///user/s2284456/min_max_numflights"
        df_numflights.write.csv(output_folder, header=True, mode="append")

def compare_avg_flights_per_month(df_filtered):
    df_lockdown = df_filtered.filter(df_filtered["lockdown_period"] == True)
    df_non_lockdown = df_filtered.filter(df_filtered["lockdown_period"] == False)

    df_avg_lockdown = df_lockdown.groupBy("origin", "destination", year("firstseen"), month("firstseen")).agg(avg("count").alias("avg_flights_per_month_lockdown"))
    df_avg_non_lockdown = df_non_lockdown.groupBy("origin", "destination", year("firstseen"), month("firstseen")).agg(avg("count").alias("avg_flights_per_month_non_lockdown"))

    df_comparison_all_months = df_avg_lockdown.join(
        df_avg_non_lockdown,
        on=["origin", "destination", "year(firstseen)", "month(firstseen)"],
        how="inner"
    )
    output_folder = "hdfs:///user/s2284456/avg_flights_per_month"
    df_comparison_all_months.write.csv(output_folder, header=True, mode="append")

def compare_avg_duration(df_filtered):
    df_lockdown = df_filtered.filter(df_filtered["lockdown_period"] == True)
    df_non_lockdown = df_filtered.filter(df_filtered["lockdown_period"] == False)

    df_avg_lockdown = df_lockdown.groupBy("origin", "destination").agg(avg("time_difference").alias("avg_duration_lockdown"))
    df_avg_non_lockdown = df_non_lockdown.groupBy("origin", "destination").agg(avg("time_difference").alias("avg_duration_non_lockdown"))

    df_comparison = df_avg_lockdown.join(
        df_avg_non_lockdown,
        on=["origin", "destination"],
        how="inner"
    )
    output_folder = "hdfs:///user/s2284456/avg_duration"
    df_comparison.write.csv(output_folder, header=True, mode="append")
    

def main():
    stats_df = spark.read.csv("hdfs:///user/s2484765/project/results", header=True, inferSchema=True)
    stats_df.show(10)

    count(stats_df)

    for x in ["EHAM", "EHEH", "EHRD"]:
        filtered_count(stats_df, x)

    destinations = ["LFPG", "EGLL", "EIDW", "EKCH", "EDDM", "LEBL", "EDDF", "LEMD", "LIRF", "ESSA"]
    for destination in destinations:
        filtered_avg_td(stats_df, "EHAM", destination)

    write_min_max_duration(stats_df)
    write_min_max_numflights(stats_df)
    compare_avg_duration(stats_df)
    compare_avg_flights_per_month(stats_df)


