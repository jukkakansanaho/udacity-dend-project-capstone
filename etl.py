# ETL pipeline for analysing Weather and pollution data
import pandas as pd
import re
from pyspark.sql import SparkSession
import os
import glob
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
# --------------------------------------------------------
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
        PATHS["country_codes_iso"] = config_all['LOCAL']['INPUT_DATA_COUNTRY_LOCAL']
        PATHS["airport_codes_i94"] = config_all['LOCAL']['INPUT_DATA_AIRPORT_I94_LOCAL']
        PATHS["country_codes_i94"] = config_all['LOCAL']['INPUT_DATA_COUNTRY_I94_LOCAL']
        PATHS["output_data"]       = config_all['LOCAL']['OUTPUT_DATA_LOCAL']
    elif config_all['COMMON']['DATA_LOCATION'] == "server":
        PATHS["input_data"]        = config_all['SERVER']['INPUT_DATA_SERVER']
        PATHS["i94_data"]          = config_all['SERVER']['INPUT_DATA_I94_SERVER']
        PATHS["airport_codes"]     = config_all['SERVER']['INPUT_DATA_AIRPORT_SERVER']
        PATHS["country_codes_iso"] = config_all['SERVER']['INPUT_DATA_COUNTRY_SERVER']
        PATHS["airport_codes_i94"] = config_all['SERVER']['INPUT_DATA_AIRPORT_I94_SERVER']
        PATHS["country_codes_i94"] = config_all['SERVER']['INPUT_DATA_COUNTRY_I94_SERVER']
        PATHS["output_data"]       = config_all['SERVER']['OUTPUT_DATA_SERVER']
    elif config_all['COMMON']['DATA_LOCATION'] == "aws":
        PATHS["input_data"]        = config_all['AWS']['INPUT_DATA']
        PATHS["i94_data"]          = config_all['AWS']['INPUT_DATA_I94']
        PATHS["airport_codes"] = config_all['AWS']['INPUT_DATA_AIRPORT']
        PATHS["country_codes_iso"]     = config_all['AWS']['INPUT_DATA_COUNTRY']
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

# --------------------------------------------------------
def parse_input_files(PATHS, path, extension, start_str):
    """Parse recursively all input files from given directory path.

    Keyword arguments:
    * PATHS     -- PATHS variable with all.
    * path      -- path to parse.
    * extension -- file extension to look for.

    Output:
    * all_files -- List of all valid input files found.
    """
    print(f"PATH: {path}")
    print(f"EXTENSION: {extension}")
    # Get (from directory) the files matching extension
    all_files = []
    for root, dirs, files in os.walk(path):
        files = glob.glob(os.path.join(root, extension))
        for f in files :
            all_files.append(os.path.abspath(f))

    return all_files

# --------------------------------------------------------
def reorder_paths(filepaths):
    """Reorder all input files Jan -> Dec.

    Keyword arguments:
    * filepath  -- List of all filepaths.

    Output:
    * reordered_paths_list_clean -- Ordered lisat of all inout files.
    """
    reordered_paths_list = \
        ["01","02","03","04","05","06","07","08","09","10","11","12"]

    for path in filepaths:
        month, year = parse_year_and_month(path)
        month_order = {
            "jan": 0,
            "feb": 1,
            "mar": 2,
            "apr": 3,
            "may": 4,
            "jun": 5,
            "jul": 6,
            "aug": 7,
            "sep": 8,
            "oct": 9,
            "nov": 10,
            "dec": 11
        }
        loc = month_order.get(month)
        reordered_paths_list[loc] = path

    reordered_paths_list_clean = []
    for str in reordered_paths_list:
        if len(str) != 2:
            reordered_paths_list_clean.append(str)
        else:
            pass

    return reordered_paths_list_clean

# --------------------------------------------------------
def parse_year_and_month(filepath):
    """Parse year and month out from the file name.

    Keyword arguments:
    * filepath  -- List of all filepaths.

    Output:
    * month, year -- Month and year of the input file
                    (based on file name).
    """
    month = filepath[-18:-15]
    year = filepath[-15:-13]

    return month, year

# --------------------------------------------------------
def process_i94_data(spark, PATHS, filepath, start_time):
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
    start_local = datetime.now()
    print("Processing i94 data ...")

    month, year = parse_year_and_month(filepath)
    print(f"YEAR+MONTH: {year} - {month}")

    # Read data to Spark
    i94_df_spark =spark.read\
                        .format('com.github.saurfang.sas.spark')\
                        .load(filepath)

    # Print schema and data snippet
    print("SCHEMA:")
    i94_df_spark.printSchema()
    print("DATA EXAMPLES:")
    i94_df_spark.show(2, truncate=False)

    # Write data to parquet file:
    i94_df_path = PATHS["output_data"] \
                    + "i94_staging" \
                    + "_" + year + "_" + month + "_" \
                    + ".parquet" \
                    + "_" + start_time
    print(f"OUTPUT: {i94_df_path}")
    print("Writing parquet files ...")
    i94_df_spark.write.mode("overwrite").parquet(i94_df_path)
    print("Writing i94 staging files DONE.\n")

    # Read parquet file back to Spark:
    print("Reading parquet files back to Spark...")
    i94_df_spark = spark.read.parquet(i94_df_path)
    print("Reading parquet files back to Spark DONE.")

    stop_local = datetime.now()
    total_local = stop_local - start_local
    print(f"I94 data processing DONE in: {total_local}\n")

    return i94_df_spark

# --------------------------------------------------------
def process_i94_airport_data(spark, PATHS, start_time):
    """Load input data (i94 airports) from input path,
        read the data to Spark and
        store the data to parquet staging files.

    Keyword arguments:
    * spark             -- reference to Spark session.
    * PATHS             -- paths for input and output data.
    * start_time        -- Datetime when the pipeline was started.
                            Used for name parquet files.

    Output:
    * i94_airport_staging_table -- directory with parquet files
                                    stored in output data path.
    """
    start_local = datetime.now()
    print("Processing i94_airport data ...")
    # Read I94 Airport codes data from XLS:
    airport_codes_i94_df = pd.read_excel(PATHS["airport_codes_i94"], \
                                            header=0, index_col=0)
    # --------------------------------------------------------
    # Cleaning I94 Airport data first
    print("Cleaning I94 airport data...")
    ac = {  "i94port_clean": [],
            "i94_airport_name_clean": [],
            "i94_state_clean": []
        }
    codes = []
    names = []
    states = []
    for index, row in airport_codes_i94_df.iterrows():
        y = re.sub("'", "", index)
        x = re.sub("'", "", row[0])
        z = re.sub("'", "", row[0]).split(",")
        y = y.strip()
        z[0] = z[0].strip()

        if len(z) == 2:
            codes.append(y)
            names.append(z[0])
            z[1] = z[1].strip()
            states.append(z[1])
        else:
            codes.append(y)
            names.append(z[0])
            states.append("NaN")

    ac["i94port_clean"] = codes
    ac["i94_airport_name_clean"] = names
    ac["i94_state_clean"] = states

    airport_codes_i94_df_clean = pd.DataFrame.from_dict(ac)
    print("Cleaning I94 airport data DONE.")
    # --------------------------------------------------------
    # Writing clean data to CSV (might be needed at some point)
    print("Writing I94 airport data to CSV...")
    ac_path = PATHS["input_data"] + "/airport_codes_i94_clean.csv"
    airport_codes_i94_df_clean.to_csv(ac_path, sep=',')
    print("Writing I94 airport data to CSV DONE.")
    # --------------------------------------------------------
    # Read data to Spark
    print("Reading I94 airport data to Spark...")
    airport_codes_i94_schema = t.StructType([
                    t.StructField("i94_port", t.StringType(), False),
                    t.StructField("i94_airport_name", t.StringType(), False),
                    t.StructField("i94_airport_state", t.StringType(), False)
                ])
    airport_codes_i94_df_spark = spark.createDataFrame(\
                            airport_codes_i94_df_clean, \
                            schema=airport_codes_i94_schema)
    # --------------------------------------------------------
    # Print schema and data snippet
    print("SCHEMA:")
    airport_codes_i94_df_spark.printSchema()
    print("DATA EXAMPLES:")
    airport_codes_i94_df_spark.show(2, truncate=False)
    # --------------------------------------------------------
    # Write data to parquet file:
    airport_codes_i94_df_path = PATHS["output_data"] \
                                + "airport_codes_i94_staging.parquet" \
                                + "_" + start_time
    print(f"OUTPUT: {airport_codes_i94_df_path}")
    print("Writing parquet files ...")
    airport_codes_i94_df_spark.write.mode("overwrite")\
                                .parquet(airport_codes_i94_df_path)
    print("Writing i94 airport staging files DONE.")

    # Read parquet file back to Spark:
    print("Reading parquet files back to Spark")
    airport_codes_i94_df_spark = spark.read\
                                .parquet(airport_codes_i94_df_path)
    print("Reading parquet files back to Spark DONE.")

    stop_local = datetime.now()
    total_local = stop_local - start_local
    print(f"I94 Airport code processing DONE in: {total_local}\n")

    return airport_codes_i94_df_spark

# --------------------------------------------------------
def process_i94_country_code_data(spark, PATHS, start_time):
    """Load input data (i94 Country Codes) from input path,
        read the data to Spark and
        store the data to parquet staging files.

    Keyword arguments:
    * spark                 -- reference to Spark session.
    * PATHS                 -- paths for input and output data.
    * start_time            -- Datetime when the pipeline was started.
                                Used for name parquet files.

    Output:
    * i94_country_codes_staging_table -- directory with parquet files
                                        stored in output data path.
    """
    start_local = datetime.now()
    print("Processing i94 Country Codes data ...")
    # Read I94 Country codes data from XLS:
    country_codes_i94_df = pd.read_excel(PATHS["country_codes_i94"], \
                                        header=0, index_col=0)
    # --------------------------------------------------------
    # Cleaning I94 Country Code data first
    cc = {"i94cit_clean": [],
          "i94_country_name_clean": [],
          "iso_country_code_clean" : []
          }
    ccodes = []
    cnames = []
    ccodes_iso = []

    for index, row in country_codes_i94_df.iterrows():
        cname = re.sub("'", "", row[0]).strip()
        ccode_iso = row[1]
        ccodes.append(index)
        cnames.append(cname)
        ccodes_iso.append(ccode_iso)

    cc["i94cit_clean"] = ccodes
    cc["i94_country_name_clean"] = cnames
    cc["iso_country_code_clean"] = ccodes_iso

    country_codes_i94_df_clean = pd.DataFrame.from_dict(cc)
    # --------------------------------------------------------
    # Writing clean data to CSV (might be needed at some point)
    print("Writing I94 Country Code data to CSV...")
    cc_path = PATHS["input_data"] + "/country_codes_i94_clean.csv"
    country_codes_i94_df_clean.to_csv(cc_path, sep=',')
    print("Writing I94 Country Code data to CSV DONE.")
    print("Cleaning I94 Country Code data DONE.")
    # --------------------------------------------------------
    # Read data to Spark
    print("Reading I94 Country Code data to Spark...")
    country_codes_i94_schema = t.StructType([
                t.StructField("i94_cit", t.StringType(), False),
                t.StructField("i94_country_name", t.StringType(), False),
                t.StructField("iso_country_code", t.StringType(), False)
            ])
    country_codes_i94_df_spark = spark.createDataFrame(\
                                country_codes_i94_df_clean, \
                                schema=country_codes_i94_schema)
    # --------------------------------------------------------
    # Print schema and data snippet
    print("SCHEMA:")
    country_codes_i94_df_spark.printSchema()
    print("DATA EXAMPLES:")
    country_codes_i94_df_spark.show(2, truncate=False)
    # --------------------------------------------------------
    # Write i94 Country data to parquet file:
    country_codes_i94_df_path = PATHS["output_data"] \
                                + "country_codes_i94_staging.parquet" \
                                + "_" + start_time
    print(f"OUTPUT: {country_codes_i94_df_path}")
    print("Writing parquet files ...")
    country_codes_i94_df_spark.write.mode("overwrite")\
                                .parquet(country_codes_i94_df_path)
    print("Writing i94 Country Code staging files DONE.")
    # --------------------------------------------------------
    # Read parquet file back to Spark:
    print("Reading parquet files back to Spark... ")
    country_codes_i94_df_spark = spark.read.\
                                 parquet(country_codes_i94_df_path)
    print("Reading parquet files back to Spark DONE.")
    # --------------------------------------------------------
    stop_local = datetime.now()
    total_local = stop_local - start_local
    print(f"I94 Country Code processing DONE in: {total_local}\n")

    return country_codes_i94_df_spark

# --------------------------------------------------------
def process_iso_country_code_data(spark, PATHS, start_time):
    """Load input data (ISO-3166 Country Codes) from input path,
        read the data to Spark and
        store the data to parquet staging files.

    Keyword arguments:
    * spark                 -- reference to Spark session.
    * PATHS                 -- paths for input and output data.
    * start_time            -- Datetime when the pipeline was started.
                                Used for name parquet files.

    Output:
    * iso_country_codes_staging_table -- directory with parquet files
                                        stored in output data path.
    """
    start_local = datetime.now()
    print("Processing ISO-3166 Country Codes data ...")
    # Read I94 Country codes data from XLS:
    country_codes_iso_df = pd.read_csv(PATHS["country_codes_iso"], header=0)
    # --------------------------------------------------------
    # Writing clean data to CSV (might be needed at some point)
    print("Writing ISO-3166 Country Code data to CSV...")
    cc_path = PATHS["input_data"] + "/country_codes_iso_clean.csv"
    country_codes_iso_df.to_csv(cc_path, sep=',')
    print("Writing ISO-3166 Country Code data to CSV DONE.")
    print("Cleaning ISO-3166 Country Code data DONE.")
    # --------------------------------------------------------
    # Read data to Spark
    print("Reading ISO-3166 Country Code data to Spark...")
    country_codes_iso_schema = t.StructType([
                t.StructField("name", t.StringType(), False),
                t.StructField("alpha_2", t.StringType(), False),
                t.StructField("alpha_3", t.StringType(), False),
                t.StructField("country_code", t.StringType(), False),
                t.StructField("iso_3166_2", t.StringType(), False),
                t.StructField("region", t.StringType(), True),
                t.StructField("sub_region", t.StringType(), True),
                t.StructField("intermediate_region", t.StringType(), True),
                t.StructField("region_code", t.StringType(), True),
                t.StructField("sub_region_code", t.StringType(), True),
                t.StructField("intermediate_region_code", t.StringType(), True),
            ])
    country_codes_iso_df_spark = spark.createDataFrame(\
                                country_codes_iso_df, \
                                schema=country_codes_iso_schema)
    # --------------------------------------------------------
    # Print schema and data snippet
    print("SCHEMA:")
    country_codes_iso_df_spark.printSchema()
    # print("DATA EXAMPLES:")
    # #country_codes_iso_df_spark.show(2, truncate=False)
    # --------------------------------------------------------
    # Write ISO-3166 Country data to parquet file:
    country_codes_iso_df_path = PATHS["output_data"] \
                                + "country_codes_iso_staging.parquet" \
                                + "_" + start_time
    print(f"OUTPUT: {country_codes_iso_df_path}")
    print("Writing parquet files ...")
    country_codes_iso_df_spark.write.mode("overwrite")\
                                .parquet(country_codes_iso_df_path)
    print("Writing ISO-3166 Country Code staging files DONE.")
    # --------------------------------------------------------
    # Read parquet file back to Spark:
    print("Reading parquet files back to Spark... ")
    country_codes_iso_df_spark = spark.read.\
                                 parquet(country_codes_iso_df_path)
    print("Reading parquet files back to Spark DONE.")
    # --------------------------------------------------------
    stop_local = datetime.now()
    total_local = stop_local - start_local
    print(f"ISO-3166 Country Code processing DONE in: {total_local}\n")

    return country_codes_iso_df_spark

# --------------------------------------------------------
def clean_i94_data(spark, PATHS, i94_df_spark, start_time):
    """Clean i94 data - fill-in empty/null values with "NA"s or 0s.

    Keyword arguments:
    * spark              -- reference to Spark session.
    * PATHS              -- paths for input and output data.
    * start_str          -- Datetime when the pipeline was started.
                            Used to name parquet files.

    Output:
    * i94_df_spark_clean -- clean Spark DataFrame.
    """
    start_local = datetime.now()
    print("Cleaning i94 data...")
    # Filling-in empty/null data with "NA"s or 0's
    i94_df_spark_clean = i94_df_spark\
        .na.fill({'i94mode': 0.0, 'i94addr': 'NA','depdate': 0.0, \
            'i94bir': 'NA', 'i94visa': 0.0, 'count': 0.0, \
            'dtadfile': 'NA', 'visapost': 'NA', 'occup': 'NA', \
            'entdepa': 'NA', 'entdepd': 'NA', 'entdepu': 'NA', \
            'matflag': 'NA', 'biryear': 0.0, 'dtaddto': 'NA', \
            'gender': 'NA', 'insnum': 'NA', 'airline': 'NA', \
            'admnum': 0.0, 'fltno': 'NA', 'visatype': 'NA'
            })
    print("Cleaning i94 data DONE.")

    stop_local = datetime.now()
    total_local = stop_local - start_local
    print(f"I94 data cleaning DONE in: {total_local}\n")

    return i94_df_spark_clean

# --------------------------------------------------------
def process_admissions_data(spark, PATHS, i94_df_spark_clean, start_time):
    """Load input data (i94_clean),
        process the data to extract admissions table and
        store the prepered data to parquet files.

    Keyword arguments:
    * spark               -- reference to Spark session.
    * PATHS               -- paths for input and output data.
    * i94_df_spark_clean  -- cleaned i94 Spark dataframe.
    * start_str           -- Datetime when the pipeline was started.
                            Used to name parquet files.

    Output:
    * admissions_table_df -- directory with parquet files
                            stored in output data path.
    """
    start_local = datetime.now()
    print("Creating admissions_table...")
    # Create table + query
    i94_df_spark_clean.createOrReplaceTempView("admissions_table_DF")
    admissions_table = spark.sql("""
        SELECT  DISTINCT admnum   AS admission_nbr,
                         i94res   AS country_code,
                         i94bir   AS age,
                         i94visa  AS visa_code,
                         visatype AS visa_type,
                         gender   AS person_gender
        FROM admissions_table_DF
        ORDER BY country_code
    """)

    print("SCHEMA:")
    admissions_table.printSchema()
    #print("DATA EXAMPLES:")
    #admissions_table.show(2, truncate=False)
    # --------------------------------------------------------
    print("Writing parquet files ...")
    # Write DF to parquet file:
    admissions_table_path = PATHS["output_data"] \
                            + "admissions_table.parquet" \
                            + "_" + start_time
    print(f"OUTPUT: {admissions_table_path}")
    admissions_table.write.mode("overwrite")\
                        .parquet(admissions_table_path)
    print("Writing admissions_table parquet files DONE.")
    # --------------------------------------------------------
    # Read parquet file back to Spark:
    admissions_table_df = spark.read.parquet(admissions_table_path)
    # --------------------------------------------------------
    stop_local = datetime.now()
    total_local = stop_local - start_local
    print(f"Creating admissions_table DONE in: {total_local}\n")

    return admissions_table_df

# --------------------------------------------------------
def process_countries_data( spark, \
                            PATHS, \
                            country_codes_i94_df_spark, \
                            country_codes_iso_df_spark, \
                            start_time):
    """Load input data (country_codes_clean),
        process the data to extract countries table and
        store the prepered data to parquet files.

    Keyword arguments:
    * spark              -- reference to Spark session.
    * PATHS              -- paths for input and output data.
    * country_codes_i94_df_spark -- cleaned i94 country code
                                    Spark dataframe
    * start_str          -- Datetime when the pipeline was started.
                            Used to name parquet files.

    Output:
    * countries_table_df -- directory with parquet files
                            stored in output data path.
    """
    start_local = datetime.now()
    print("Creating countries_table...")

    country_codes_i94_df_spark_joined = country_codes_i94_df_spark\
        .join(country_codes_iso_df_spark, \
            (country_codes_i94_df_spark.iso_country_code == \
                    country_codes_iso_df_spark.country_code))

    # print("SCHEMA_joined_table (before new table): ")
    # country_codes_i94_df_spark_joined.printSchema()
    # country_codes_i94_df_spark_joined.show(20, truncate=False)

    # Create table + query
    country_codes_i94_df_spark_joined.createOrReplaceTempView("countries_table_DF")
    countries_table = spark.sql("""
        SELECT DISTINCT i94_cit          AS country_code,
                        i94_country_name AS country_name,
                        iso_country_code AS iso_ccode,
                        alpha_2          AS iso_alpha_2,
                        alpha_3          AS iso_alpha_3,
                        iso_3166_2       AS iso_3166_2_code,
                        name             AS iso_country_name,
                        region           AS iso_region,
                        sub_region       AS iso_sub_region,
                        region_code      AS iso_region_code,
                        sub_region_code  AS iso_sub_region_code
        FROM countries_table_DF          AS countries
        ORDER BY country_name
    """)

    print("SCHEMA:")
    countries_table.printSchema()
    # print("DATA EXAMPLES:")
    # countries_table.show(50, truncate=False)
    # --------------------------------------------------------
    print("Writing parquet files ...")
    # Write DF to parquet file:
    countries_table_path = PATHS["output_data"] \
                            + "countries_table.parquet" \
                            + "_" + start_time
    print(f"OUTPUT: {countries_table_path}")
    countries_table.write.mode("overwrite").parquet(countries_table_path)
    print("Writing DONE.")
    print("Writing countries_table parquet files DONE.")
    # --------------------------------------------------------
    # Read parquet file back to Spark:
    countries_table_df = spark.read.parquet(countries_table_path)
    # --------------------------------------------------------
    stop_local = datetime.now()
    total_local = stop_local - start_local
    print(f"Creating countries_table DONE in: {total_local}\n")

    return countries_table_df

# --------------------------------------------------------
def process_airport_data(spark, PATHS, airport_codes_i94_df_spark, start_time):
    """Load input data (airport_codes_clean),
        process the data to extract airports table and
        store the prepered data to parquet files.

    Keyword arguments:
    * spark             -- reference to Spark session.
    * PATHS             -- paths for input and output data.
    * airport_codes_i94_df_spark -- cleaned i94 airport code Spark dataframe
    * start_str         -- Datetime when the pipeline was started.
                            Used to name parquet files.

    Output:
    * airports_table_df -- directory with parquet files
                            stored in output data path.
    """
    start_local = datetime.now()
    print("Creating airports_table...")
    # Create table + query
    airport_codes_i94_df_spark.createOrReplaceTempView("airports_table_DF")
    airports_table = spark.sql("""
        SELECT DISTINCT  i94_port          AS airport_id,
                         i94_airport_name  AS airport_name,
                         i94_airport_state AS airport_state
        FROM airports_table_DF             AS airports
        ORDER BY airport_name
    """)

    print("SCHEMA:")
    airports_table.printSchema()
    #print("DATA EXAMPLES:")
    #airports_table.show(2, truncate=False)
    # --------------------------------------------------------
    print("Writing parquet files ...")
    # Write DF to parquet file:
    airports_table_path = PATHS["output_data"] \
                                + "airports_table.parquet" \
                                + "_" + start_time
    print(f"OUTPUT: {airports_table_path}")
    airports_table.write.mode("overwrite")\
                        .parquet(airports_table_path)
    print("Writing airports_table parquet files DONE.")
    # --------------------------------------------------------
    # Read parquet file back to Spark:
    airports_table_df = spark.read\
                             .parquet(airports_table_path)
    # --------------------------------------------------------
    stop_local = datetime.now()
    total_local = stop_local - start_local
    print(f"Creating airports_table DONE in: {total_local}\n")

    return airports_table_df

# --------------------------------------------------------
def process_time_data(spark, PATHS, i94_df_spark_clean, start_time):
    """Load input data (i94_clean),
        process the data to extract time table and
        store the prepered data to parquet files.

    Keyword arguments:
    * spark              -- reference to Spark session.
    * PATHS              -- paths for input and output data.
    * i94_df_spark_clean -- cleaned i94 Spark dataframe
    * start_str          -- Datetime when the pipeline was started.
                            Used to name parquet files.

    Output:
    * time_table_df      -- directory with parquet files
                            stored in output data path.
    """
    start_local = datetime.now()
    print("Creating time_table...")

    # Add new arrival_ts column
    print("Creating new arrival_ts column...")

    @udf(t.TimestampType())
    def get_timestamp (arrdate):
        arrdate_int = int(arrdate)
        return (datetime(1960,1,1) + timedelta(days=arrdate_int))

    i94_df_spark_clean = i94_df_spark_clean\
                        .withColumn("arrival_time", \
                                    get_timestamp(i94_df_spark_clean.arrdate))
    print("New column creation DONE.")
    # --------------------------------------------------------
    print("Creating time_table query...")
    # Create table + query
    # Extracting detailed data from arrival_ts
    i94_df_spark_clean.createOrReplaceTempView("time_table_DF")
    time_table = spark.sql("""
        SELECT DISTINCT  arrival_time             AS arrival_ts,
                         hour(arrival_time)       AS hour,
                         day(arrival_time)        AS day,
                         weekofyear(arrival_time) AS week,
                         month(arrival_time)      AS month,
                         year(arrival_time)       AS year,
                         dayofweek(arrival_time)  AS weekday
        FROM time_table_DF

    """)

    print("SCHEMA:")
    time_table.printSchema()
    #print("DATA EXAMPLES:")
    #time_table.show(2, truncate=False)
    # --------------------------------------------------------
    print("Writing parquet files ...")
    # Write DF to parquet file:
    time_table_path = PATHS["output_data"] \
                            + "time_table.parquet" \
                            + "_" + start_time
    print(f"OUTPUT: {time_table_path}")
    time_table.write.mode("append").partitionBy("year", "month")\
                    .parquet(time_table_path)
    print("Writing time_table parquet files DONE.")
    # --------------------------------------------------------
    # Read parquet file back to Spark:
    time_table_df = spark.read.parquet(time_table_path)
    # --------------------------------------------------------
    stop_local = datetime.now()
    total_local = stop_local - start_local
    print(f"Creating time_table DONE in: {total_local}\n")

    return time_table_df, i94_df_spark_clean

# --------------------------------------------------------
def process_immigrations_data(spark, \
                              PATHS, \
                              i94_df_spark_clean, \
                              country_codes_i94_df_spark, \
                              airport_codes_i94_df_spark, \
                              time_table_df, \
                              start_time):
    """Load input data (i94_clean, country_code_clean,
        airport_codes_clean),
        process the data to extract immigrations table and
        store the prepered data to parquet files.

    Keyword arguments:
    * spark                 -- reference to Spark session.
    * PATHS                 -- paths for input and output data.
    * i94_df_spark_clean    -- cleaned i94 Spark dataframe
    * country_codes_i94_df_spark -- cleaned i94 country codes dataframe
    * airport_codes_i94_df_spark -- cleaned i94 airport codes dataframe
    * start_str             -- Datetime when the pipeline was started.
                                Used to name parquet files.

    Output:
    * immigrations_table_df -- directory with parquet files
                                stored in output data path.
    """
    start_local = datetime.now()
    print("Creating immigrations_table query...")
    # Join dataframes
    i94_df_spark_joined = i94_df_spark_clean\
        .join(country_codes_i94_df_spark, \
            (i94_df_spark_clean.i94cit == \
                    country_codes_i94_df_spark.i94_cit))\
        .join(airport_codes_i94_df_spark, \
            (i94_df_spark_clean.i94port == \
                    airport_codes_i94_df_spark.i94_port))\
        .join(time_table_df, \
                    i94_df_spark_clean.arrival_time == \
                    time_table_df.arrival_ts)
    # --------------------------------------------------------
    # Add new arrival_ts column
    print("Creating new immigration_id column...")
    i94_df_spark_joined = i94_df_spark_joined\
                            .withColumn("immigration_id", \
                                        monotonically_increasing_id())
    print("New column DONE.")
    # --------------------------------------------------------
    # Create table + query
    @udf(t.TimestampType())
    def get_timestamp2 (depdate):
        if depdate == "null":
            depdate_int = 0
        else:
            depdate_int = int(depdate)
        return (datetime(1960,1,1) + timedelta(days=depdate_int))

    i94_df_spark_joined = i94_df_spark_joined\
                        .withColumn("departure_date", \
                            get_timestamp2(i94_df_spark_joined.depdate))
    print("New column creation DONE.")
    print("i94_joined_SCHEMA:")
    i94_df_spark_joined.printSchema()
    # --------------------------------------------------------
    print("Creating immigrations_table query...")
    # Create table + query
    i94_df_spark_joined.createOrReplaceTempView("immigrations_table_DF")
    immigrations_table = spark.sql("""
        SELECT DISTINCT  immigration_id AS immigration_id,
                         arrival_time   AS arrival_time,
                         year           AS arrival_year,
                         month          AS arrival_month,
                         i94_port       AS airport_id,
                         i94_cit        AS country_code,
                         admnum         AS admission_nbr,
                         i94mode        AS arrival_mode,
                         departure_date AS departure_date,
                         airline        AS airline,
                         fltno          AS flight_nbr

        FROM immigrations_table_DF immigrants
        ORDER BY arrival_time
    """)

    print("SCHEMA:")
    immigrations_table.printSchema()
    #print("DATA EXAMPLES:")
    #time_table.show(2, truncate=False)
    # --------------------------------------------------------
    print("Writing parquet files ...")
    # Write DF to parquet file:
    immigrations_table_path = PATHS["output_data"] \
                                    + "immigrations_table.parquet" \
                                    + "_" + start_time
    print(f"OUTPUT: {immigrations_table_path}")
    immigrations_table.write.mode("append")\
                            .partitionBy("arrival_year", "arrival_month")\
                            .parquet(immigrations_table_path)
    print("Writing immigrations_table parquet files DONE.")
    # --------------------------------------------------------
    # Read parquet file back to Spark:
    immigrations_table_df = spark.read.parquet(immigrations_table_path)
    # --------------------------------------------------------
    stop_local = datetime.now()
    total_local = stop_local - start_local
    print(f"Creating immigrations_table DONE in: {total_local}\n")

    return immigrations_table_df

# --------------------------------------------------------
def check_data_quality( spark, \
                        round_ts, \
                        admissions_table_df, \
                        countries_table_df,
                        airports_table_df, \
                        time_table_df, \
                        immigrations_table_df, \
                        start_time):
    """Check data quality of all dimension and fact tables.

    Keyword arguments:
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

    Output:
    *
    """
    start_local = datetime.now()
    print("Start processing data quality checks...")
    results = { "round_ts": round_ts,
                "admissions_count": 0,
                "admissions": "",
                "countries_count": 0,
                "countries": "",
                "airports_count": 0,
                "airports": "",
                "time_count": 0,
                "time": "",
                "immigrations_count": 0,
                "immigrations": ""}
    # --------------------------------------------------------
    # CHECK admissions table:
    # Check that key fields have valid values (no nulls or empty)
    print("Checking admissions table...")
    admissions_table_df.createOrReplaceTempView("admissions_table_DF")
    admissions_table_check1 = spark.sql("""
        SELECT  COUNT(*)
        FROM admissions_table_DF
        WHERE   admission_nbr IS NULL OR admission_nbr == "" OR
                country_code IS NULL OR country_code == ""
    """)

    # Check that table has > 0 rows
    admissions_table_df.createOrReplaceTempView("admissions_table_DF")
    admissions_table_check2 = spark.sql("""
        SELECT  COUNT(*)
        FROM admissions_table_DF
    """)
    if admissions_table_check1.collect()[0][0] > 0 \
        & admissions_table_check2.collect()[0][0] < 1:
        results['admissions_count'] = admissions_table_check2.collect()[0][0]
        results['admissions'] = "NOK"
    else:
        results['admissions_count'] = admissions_table_check2.collect()[0][0]
        results['admissions'] = "OK"

    print("NULLS:")
    admissions_table_check1.show(1)
    print("ROWS:")
    admissions_table_check2.show(1)
    # --------------------------------------------------------
    # CHECK countries table:
    # Check that key fields have valid values (no nulls or empty)
    print("Checking countries table...")
    countries_table_df.createOrReplaceTempView("countries_table_DF")
    countries_table_check1 = spark.sql("""
        SELECT  COUNT(*)
        FROM countries_table_DF
        WHERE   country_code IS NULL OR country_code == "" OR
                country_name IS NULL OR country_name == "" OR
                iso_ccode IS NULL OR iso_ccode == "" OR
                iso_alpha_2 IS NULL OR iso_alpha_2 == "" OR
                iso_alpha_3 IS NULL OR iso_alpha_3 == "" OR
                iso_3166_2_code IS NULL OR iso_3166_2_code == "" OR
                iso_country_name IS NULL OR iso_country_name == ""
    """)

    # Check that table has > 0 rows
    countries_table_df.createOrReplaceTempView("countries_table_DF")
    countries_table_check2 = spark.sql("""
        SELECT  COUNT(*)
        FROM countries_table_DF
    """)

    if countries_table_check1.collect()[0][0] > 0 \
        & countries_table_check2.collect()[0][0] < 1:
        results['countries_count'] = countries_table_check2.collect()[0][0]
        results['countries'] = "NOK"
    else:
        results['countries_count'] = countries_table_check2.collect()[0][0]
        results['countries'] = "OK"

    print("NULLS:")
    countries_table_check1.show(1)
    print("ROWS:")
    countries_table_check2.show(1)
    # --------------------------------------------------------
    # CHECK airports table:
    # Check that key fields have valid values (no nulls or empty)
    print("Checking airports table...")
    airports_table_df.createOrReplaceTempView("airports_table_DF")
    airports_table_check1 = spark.sql("""
        SELECT  COUNT(*)
        FROM airports_table_DF
        WHERE   airport_id IS NULL OR airport_id == "" OR
                airport_name IS NULL OR airport_name == ""
    """)

    # Check that table has > 0 rows
    airports_table_df.createOrReplaceTempView("airports_table_DF")
    airports_table_check2 = spark.sql("""
        SELECT  COUNT(*)
        FROM airports_table_DF
    """)

    if airports_table_check1.collect()[0][0] > 0 \
        & airports_table_check2.collect()[0][0] < 1:
        results['airports_count'] = airports_table_check2.collect()[0][0]
        results['airports'] = "NOK"
    else:
        results['airports_count'] = airports_table_check2.collect()[0][0]
        results['airports'] = "OK"

    print("NULLS:")
    airports_table_check1.show(1)
    print("ROWS:")
    airports_table_check2.show(1)
    # --------------------------------------------------------
    # CHECK time table:
    # Check that key fields have valid values (no nulls or empty)
    print("Checking time table...")
    time_table_df.createOrReplaceTempView("time_table_DF")
    time_table_check1 = spark.sql("""
        SELECT  COUNT(*)
        FROM time_table_DF
        WHERE   arrival_ts IS NULL OR arrival_ts == ""
    """)


    # Check that table has > 0 rows
    time_table_df.createOrReplaceTempView("time_table_DF")
    time_table_check2 = spark.sql("""
        SELECT  COUNT(*)
        FROM time_table_DF
    """)
    if time_table_check1.collect()[0][0] > 0 \
        & time_table_check2.collect()[0][0] < 1:
        results['time_count'] = time_table_check2.collect()[0][0]
        results['time'] = "NOK"
    else:
        results['time_count'] = time_table_check2.collect()[0][0]
        results['time'] = "OK"

    print("NULLS:")
    time_table_check1.show(1)
    print("ROWS:")
    time_table_check2.show(1)
    # --------------------------------------------------------
    # CHECK immigrations table:
    # Check that key fields have valid values (no nulls or empty)
    print("Checking immigrations table...")
    immigrations_table_df.createOrReplaceTempView("immigrations_table_DF")
    immigrations_table_check1 = spark.sql("""
        SELECT  COUNT(*)
        FROM immigrations_table_DF
        WHERE   immigration_id IS NULL OR immigration_id == "" OR
                arrival_time IS NULL OR arrival_time == "" OR
                arrival_year IS NULL OR arrival_year == "" OR
                arrival_month IS NULL OR arrival_month == "" OR
                airport_id IS NULL OR airport_id == "" OR
                country_code IS NULL OR country_code == "" OR
                admission_nbr IS NULL OR admission_nbr == ""
    """)

    # Check that table has > 0 rows
    immigrations_table_df.createOrReplaceTempView("immigrations_table_DF")
    immigrations_table_check2 = spark.sql("""
        SELECT  COUNT(*)
        FROM immigrations_table_DF
    """)

    if immigrations_table_check1.collect()[0][0] > 0 \
        & immigrations_table_check2.collect()[0][0] < 1:
        results['immigrations_count'] = immigrations_table_check2.collect()[0][0]
        results['immigrations'] = "NOK"
    else:
        results['immigrations_count'] = immigrations_table_check2.collect()[0][0]
        results['immigrations'] = "OK"

    print("NULLS:")
    immigrations_table_check1.show(1)
    print("ROWS:")
    immigrations_table_check2.show(1)
    # --------------------------------------------------------
    stop_local = datetime.now()
    total_local = stop_local - start_local
    print(f"Checking data quality DONE in: {total_local}\n")

    return results
# --------------------------------------------------------
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
    results_all = []
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
    # Parse input data dir
    input_files = parse_input_files(PATHS, \
                                    PATHS["i94_data"], \
                                    "*.sas7bdat", \
                                    start_str)
    input_files_reordered = reorder_paths(input_files)
    PATHS["i94_files"] = input_files_reordered
    print(f"i94_files: {PATHS['i94_files']}")
    # --------------------------------------------------------
    # Process all input
    round = 0
    for filepath in PATHS["i94_files"]:
        round_ts = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
        # Process input data to staging tables.
        i94_df_spark = process_i94_data(spark, PATHS, filepath, start_str)
        airport_codes_i94_df_spark = process_i94_airport_data(  spark, \
                                                                PATHS, \
                                                                start_str)
        country_codes_i94_df_spark = process_i94_country_code_data(spark, \
                                                                PATHS, \
                                                                start_str)
        country_codes_iso_df_spark = process_iso_country_code_data(spark, \
                                                                PATHS, \
                                                                start_str)
        # --------------------------------------------------------
        # Cleaning the data:
        i94_df_spark_clean = clean_i94_data(spark, \
                                            PATHS, \
                                            i94_df_spark, \
                                            start_str)

        # Process Dimension tables.
        admissions_table_df = process_admissions_data(\
                                                spark, \
                                                PATHS, \
                                                i94_df_spark_clean, \
                                                start_str)

        countries_table_df = process_countries_data(\
                                                spark, \
                                                PATHS, \
                                                country_codes_i94_df_spark, \
                                                country_codes_iso_df_spark, \
                                                start_str)

        airports_table_df = process_airport_data(\
                                                spark, \
                                                PATHS, \
                                                airport_codes_i94_df_spark, \
                                                start_str)

        time_table_df, i94_df_spark_clean = process_time_data( \
                                                spark, \
                                                PATHS, \
                                                i94_df_spark_clean, \
                                                start_str)

        # Process Fact table.
        immigrations_table_df = process_immigrations_data( \
                                        spark,
                                        PATHS, \
                                        i94_df_spark_clean, \
                                        country_codes_i94_df_spark, \
                                        airport_codes_i94_df_spark, \
                                        time_table_df, \
                                        start_str)

        print("Checking data quality of created tables.")
        results = check_data_quality( spark, \
                                      round_ts, \
                                      admissions_table_df, \
                                      countries_table_df,
                                      airports_table_df, \
                                      time_table_df, \
                                      immigrations_table_df, \
                                      start_str)
        results_all.append(results)
        print("Data quality checks DONE.")
    print("Finished the ETL pipeline processing.")
    print("RESULTS: ")
    print(results_all)
    # --------------------------------------------------------

    print("ALL work in ETL pipeline is now DONE.")
    stop = datetime.now()
    print("FINISHED ETL pipeline (to process song_data and log_data) at {}"\
            .format(stop))
    print("TOTAL TIME: {}".format(stop-start))


if __name__ == "__main__":
    main()
