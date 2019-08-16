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

def create_spark_session():
    """Create a Apache Spark session to process the data.

    Keyword arguments:
    * N/A

    Output:
    * spark -- An Apache Spark session.
    """
    print("Preparing Spark session for the pipeline...")
    spark = SparkSession \
        .builder\
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport().getOrCreate()
    print("Spark session preparation DONE.")

    return spark

def process_config(config_all):
    """Prepare configs for the pipeline.

    Keyword arguments:
    * config_all    -- All configuration settings read from config file.

    Output:
    * PATHS         -- Dictionary of paths used in pipeline.
    """
    print("Preparing PATHs for the pipeline...")
    PATHS = {}

    # Set parameters for the pipeline based on config
    if config_all['COMMON']['DATA_LOCATION'] == "local":
        PATHS["input_data"]        = config_all['LOCAL']['INPUT_DATA_LOCAL']
        PATHS["i94_data"]          = config_all['LOCAL']['INPUT_DATA_I94_LOCAL']
        PATHS["airport_codes"]     = config_all['LOCAL']['INPUT_DATA_AIRPORT_LOCAL']
        PATHS["country_codes"]     = config_all['LOCAL']['INPUT_DATA_COUNTRY_LOCAL']
        PATHS["airport_codes_i94"] = config_all['LOCAL']['INPUT_DATA_AIRPORT_I94_LOCAL']
        PATHS["country_codes_i94"] = config_all['LOCAL']['INPUT_DATA_COUNTRY_I94_LOCAL']
        PATHS["output_data"]       = config_all['LOCAL']['OUTPUT_DATA_LOCAL']
    elif config_all['COMMON']['DATA_LOCATION'] == "server":
        PATHS["input_data"]        = config_all['SERVER']['INPUT_DATA_SERVER']
        PATHS["i94_data"]          = config_all['SERVER']['INPUT_DATA_I94_SERVER']
        PATHS["airport_codes"]     = config_all['SERVER']['INPUT_DATA_AIRPORT_SERVER']
        PATHS["country_codes"]     = config_all['SERVER']['INPUT_DATA_COUNTRY_SERVER']
        PATHS["airport_codes_i94"] = config_all['SERVER']['INPUT_DATA_AIRPORT_I94_SERVER']
        PATHS["country_codes_i94"] = config_all['SERVER']['INPUT_DATA_COUNTRY_I94_SERVER']
        PATHS["output_data"]       = config_all['SERVER']['OUTPUT_DATA_SERVER']
    elif config_all['COMMON']['DATA_LOCATION'] == "aws":
        PATHS["input_data"]        = config_all['AWS']['INPUT_DATA']
        PATHS["i94_data"]          = config_all['AWS']['INPUT_DATA_I94']
        PATHS["airport_codes"]     = config_all['AWS']['INPUT_DATA_AIRPORT']
        PATHS["country_codes"]     = config_all['AWS']['INPUT_DATA_COUNTRY']
        PATHS["airport_codes_i94"] = config_all['AWS']['INPUT_DATA_AIRPORT_I94']
        PATHS["country_codes_i94"] = config_all['AWS']['INPUT_DATA_COUNTRY_I94']
        PATHS["output_data"]       = config_all['AWS']['OUTPUT_DATA']

    if config_all["COMMON"]["DATA_STORAGE"] == "postgresql":
        PATHS["data_storage"]      = config_all["COMMON"]["DATA_STORAGE_SQL"]
    elif config_all["COMMON"]["DATA_STORAGE"] == "parquet":
        PATHS["data_storage"]      = config_all["COMMON"]["DATA_STORAGE"]

    #print(AWS_ACCESS_KEY_ID)
    #print(AWS_SECRET_ACCESS_KEY)

    # Print out paths in PATHS
    print("PATHS preparation DONE.\n")
    print("PATHS:")
    for path in PATHS:
        print(path)

    return PATHS

def process_i94_data(spark, PATHS, start_time):
    """Load input data (i94) from input path,
        read the data to Spark and
        store the data to parquet staging files.

    Keyword arguments:
    * spark             -- reference to Spark session.
    * PATHS             -- paths for input and output data.
    * start_time        -- Datetime when the pipeline was started.
                            Used for name parquet files.

    Output:
    * i94_staging_table -- directory with parquet files
                            stored in output data path.
    """
    print("Processing i94 data ...")
    # Read data to Spark
    i94_df_spark =spark.read\
                        .format('com.github.saurfang.sas.spark')\
                        .load(PATHS["i94_data"])

    # Print schema and data snippet
    print("SCHEMA:")
    i94_df_spark.printSchema()
    print("DATA EXAMPLES:")
    i94_df_spark.show(2, truncate=False)

    # Write data to parquet file:
    i94_df_path = PATHS["output_data"] \
                    + "i94_staging.parquet" \
                    + "_" + start_time
    print(f"OUTPUT: {i94_df_path}")
    print("Writing parquet files ...")
    i94_df_spark.write.mode("overwrite").parquet(i94_df_path)
    print("Writing i94 staging files DONE.\n")

    # Read parquet file back to Spark:
    i94_df_spark = spark.read.parquet(i94_df_path)

    return i94_df_spark

def process_admissions_data(spark, PATHS, start_str):
    """Load input data (i94) from input path,
        process the data to extract admissions table and
        store the prepered data to parquet files.

    Keyword arguments:
    * spark             -- reference to Spark session.
    * PATHS             -- paths for input and output data.
    * start_str         -- Datetime when the pipeline was started.
                        Used to name parquet files.

    Output:
    * admissions_table  -- directory with parquet files
                            stored in output data path.
    """

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
    # Start the clocks
    start = datetime.now()
    start_str = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
    print("\nSTARTED ETL pipeline (to process I94 Immigrations data). \
            at {}\n".format(start))
    # --------------------------------------------------------
    # Prepare configs for the pipeline.
    config_all = configparser.ConfigParser()
    config_all.read('dl.cfg')
    os.environ['AWS_ACCESS_KEY_ID']=config_all['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config_all['AWS']['AWS_SECRET_ACCESS_KEY']

    PATHS = process_config(config_all)
    # --------------------------------------------------------
    # Create Spark session for the pipeline.
    spark = create_spark_session()
    # --------------------------------------------------------
    # Process input data to staging tables.
    i94_df_spark = process_i94_data(spark, PATHS, start_str)
    # --------------------------------------------------------
    # Process Dimension tables.
    #admissions_table = process_admissions_data(spark, PATHS, start_str)
    #countries_table = process_countries_data(spark, PATHS, start_str)
    #airports_table = process_airport_data(spark, PATHS, start_str)
    #time_table = process_time_data(spark, PATHS, start_str)

    # Process Fact table.
    #immigrations_table = process_immigrations_data(spark, start_str):
    # --------------------------------------------------------
    print("Finished the ETL pipeline processing.")
    print("ALL DONE.")

    stop = datetime.now()
    print("FINISHED ETL pipeline (to process song_data and log_data) at {}"\
            .format(stop))
    print("TIME: {}".format(stop-start))

if __name__ == "__main__":
    main()
