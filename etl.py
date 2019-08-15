# ETL pipeline for analysing Weather and pollution data
import pandas as pd
import re
from pyspark.sql import SparkSession
import os
import configparser
from datetime import datetime, timedelta
from pyspark.sql import types as t
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear

config_all = configparser.ConfigParser()
config_all.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """Create a Apache Spark session to process the data.

    Keyword arguments:
    * N/A

    Output:
    * spark -- An Apache Spark session.
    """
    spark = SparkSession \
        .builder\
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport().getOrCreate()
    return spark

def process_config():
    pass

def process_admissions_data():
    pass

def process_countries_data():
    pass

def process_airport_data():
    pass

def process_time_data():
    pass

def process_immigrations_data():
    pass

def main():
    """Load input data (I94 Immigration data) from input_data path,
        process the data to extract dimension and fact tables,
        and store the prepered data to parquet files to output_data path.

    Keyword arguments:
    * NA

    Output:
    * admissions_table   -- directory with admissions_table parquet files
                          stored in output_data path.
    * countries_table    -- directory with countries_table parquet files
                          stored in output_data path.
    * airports_table     -- directory with airports_table parquet files
                          stored in output_data path.
    * time_table         -- directory with time_table parquet files
                          stored in output_data path.
    * immigrations_table -- directory with immigrations_table parquet files
                          stored in output_data path.
    """
    start = datetime.now()
    start_str = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
    print("\nSTARTED ETL pipeline (to process I94 Immigrations data). \
            at {}\n".format(start))

    # Create Spark session for the pipeline.
    spark = create_spark_session()

    # Prepare configs for the pipeline.
    config = process_config(config_all)

    # Process Dimension tables.
    admissions_table = process_admissions_data(spark, start_str)
    countries_table = process_countries_data(spark, start_str)
    airports_table = process_airport_data(spark, start_str)
    time_table = process_time_data(spark, start_str)

    # Process Fact table.
    immigrations_table = process_immigrations_data(spark, start_str):

    print("Finished the ETL pipeline processing.")
    print("ALL DONE.")

    stop = datetime.now()
    print("FINISHED ETL pipeline (to process song_data and log_data) at {}"\
            .format(stop))
    print("TIME: {}".format(stop-start))

if __name__ == "__main__":
    main()
