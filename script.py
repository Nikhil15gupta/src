from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, date_trunc, max, min, to_timestamp
import sys


def main(input_path, output_path, time_bucket):
    # Initializing SparkSession
    spark = SparkSession.builder \
        .appName("Batch Aggregation Challenge") \
        .master("local[*]") \
        .getOrCreate()

    # Limiting logging to ERROR messages only
    spark.sparkContext.setLogLevel("ERROR")

    # Reading the input dataset
    input_path = "./sample_input.csv"
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Converting Timestamp column to a standard format
    df = df.withColumn("Timestamp", to_timestamp(col("Timestamp")))

    # Dynamically determining the aggregation window based on input
    time_format = "day" if time_bucket == "D" else "hour" if time_bucket == "H" else "month"
    df = df.withColumn("TimeBucket", date_trunc(time_format, col("Timestamp")))

    # Aggregating data
    aggregated_df = df.groupBy("Metric", "TimeBucket") \
        .agg(
            avg("Value").alias("AverageValue"),
            min("Value").alias("MinValue"),
            max("Value").alias("MaxValue")
        )

    # Writing the output
    aggregated_df.write.csv(output_path, header=True, mode="overwrite")

    # Stoping the SparkSession
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: script.py <input_path> <output_path> <time_bucket>")
        sys.exit(-1)

    input_path, output_path, time_bucket = sys.argv[1], sys.argv[2], sys.argv[3]
    main(input_path, output_path, time_bucket)
