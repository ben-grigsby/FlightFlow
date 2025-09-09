# etl/silver/avstack/silver_dept_info.py

from pyspark.sql.functions import col, to_timestamp, length, regexp_replace, upper

# ==================================================================
# Functions
# ==================================================================


def clean_transform_dept_info(df, user, password, host, db_name, latest_processing_time):
    """
    Clean and transform the columns that will go into the avstack.silver_dept_info table.
    """

    print("Starting cleaning and transformation process. avstack.bronze_info -> avstack.silver_dept_info")

    df_clean = df \
    .withColumn("flight_iata", upper(regexp_replace(col("flight_iata"), "[^A-Za-z0-9]", ""))) \
    .withColumn("flight_date", to_timestamp("flight_date")) \
    .filter(col("flight_iata").rlike("^[A-Za-z]+[0-9]+$")) \
    .withColumn("dept_airport_iata", upper(regexp_replace(col("dept_airport_iata"), "[^A-Za-z]", ""))) \
    .filter(col("dept_airport_iata").rlike("^[A-Za-z]+$")) \
    .filter(length(col("dept_airport_iata")) ==  3) \
    .withColumn("dept_airport_icao", upper(regexp_replace(col("dept_airport_icao"), "[^A-Za-z]", ""))) \
    .filter(col("dept_airport_icao").rlike("^[A-Za-z]+$")) \
    .filter(length(col("dept_airport_icao")) ==  4) \
    .withColumn("dept_delay", col("dept_delay").cast("int")) \
    .withColumn("scheduled_dept", to_timestamp("scheduled_dept", "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    .withColumn("estimated_dept", to_timestamp("estimated_dept", "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    .withColumn("actual_dept", to_timestamp("actual_dept", "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    .withColumn("estimated_dept_runway", to_timestamp("estimated_dept_runway", "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    .withColumn("actual_dept_runway", to_timestamp("actual_dept_runway", "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    



    df_dept_info = df_clean.select(
        col("id"),
        col("flight_iata"),
        col("flight_date"),
        col("dept_airport").alias("airport"),
        col("dept_timezone").alias("timezone"),
        col("dept_airport_iata").alias("iata"),
        col("dept_airport_icao").alias("icao"),
        col("dept_airport_terminal").alias("terminal"),
        col("dept_airport_gate").alias("gate"),
        col("dept_delay"),
        col("scheduled_dept"),
        col("estimated_dept"),
        col("actual_dept"),
        col("estimated_dept_runway").alias("estimated_runway"),
        col("actual_dept_runway").alias("actual_runway")
    )#.filter(col("created_at") > latest_processing_time)


    df_dept_info.write \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://postgres:{host}/{db_name}") \
    .option("dbtable", "avstack.silver_dept_info") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

    print("Completed cleaning and tranformation process. avstack.bronze_info -> avstack.silver_dept_info")
    print(f"Original record count: {df.count()}")
    print(f"Cleaned record count: {df_dept_info.count()}")