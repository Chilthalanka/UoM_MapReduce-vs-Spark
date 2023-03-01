# Import libraries
import os
import argparse
import time
import warnings
warnings.filterwarnings('ignore')
from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .master("yarn") \
    .appName("Airline Delay Data Analysis") \
    .config("spark.speculation", True) \
    .config("spark.sql.broadcastTimeout", "1800") \
    .getOrCreate()


if __name__ == '__main__':
    start_time = time.time()
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)

    # Parse input arguments to the script
    parser = argparse.ArgumentParser(description='Airline delay data analysis')
    parser.add_argument(dest='file_name',
                        help='The SQL file name that should be used for the Spark job')
    args = parser.parse_args()

    # Load the data to a spark dataframe and create a temporary view
    df = spark.read.\
        format("csv").\
        option("header", "true").\
        load("s3://airline-delay-analysis/input/DelayedFlights-updated.csv")
    df.createOrReplaceTempView("airline_delay")

    # Read the file containing the SQL query
    sql_script_path = os.path.join(current_dir, "spark-sql/{}".format(args.file_name))
    with open(sql_script_path) as file:
        sql_query = file.read()

    # Run the Spark SQL query
    result = spark.sql(sql_query)
    result.show()

    end_time = time.time()
    print(f"Spark job execution time: {(end_time - start_time):.2f}s")
