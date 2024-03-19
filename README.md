# Batch Aggregation Challenge

This repository contains a PySpark script `script.py` designed for the Batch Aggregation Challenge. The script aggregates time series data by metric and time bucket, calculating average, minimum, and maximum values for each metric within each time bucket.

## Prerequisites

- Apache Spark
- Python 3.x
- PySpark

Ensure Apache Spark is installed and properly configured on your system. The PySpark library should be installed within your Python environment.

## Setup

Place the `sample_input.csv` file in the same directory as `script.py`. This ensures the script can easily locate and read the input data.

## Running the Script

To run the script, use the `spark-submit` command from the terminal or command prompt. Navigate to the directory containing `script.py` and `sample_input.csv`, then execute the following command:

```bash
spark-submit script.py sample_input.csv output.csv <time_bucket>
```

Replace `<time_bucket>` with your desired aggregation window. The script supports the following time buckets:

- `D` for daily aggregation
- `H` for hourly aggregation
- any other character for monthly aggregation

For example, to aggregate data on a daily basis, use:

```bash
spark-submit script.py sample_input.csv output.csv D
```

This will aggregate the data from `sample_input.csv` into daily buckets, calculating average, minimum, and maximum values for each metric, and write the results to `output.csv` in the same directory.

## Output

The script writes the aggregated data to a CSV file. The output file will contain the following columns:

- `Metric`: The name of the metric.
- `TimeBucket`: The time bucket for the aggregation.
- `AverageValue`: The average value of the metric in the time bucket.
- `MinValue`: The minimum value of the metric in the time bucket.
- `MaxValue`: The maximum value of the metric in the time bucket.