# etl/silver/avstack/silver_arr_info.py

from pyspark.sql.functions import col, to_timestamp, length, regexp_replace, upper

# ==================================================================
# Functions
# ==================================================================


def clean_transform_arr_info(df, user, password, host, db_name, latest_processing_time):
    """
    Clean and transform the columns that will go into the avstack.silver_arr_info table.
    """

    print("Starting cleaning and transformation process. avstack.bronze_info -> avstack.silver_arr_info")

    df_clean = df \
    .withColumn("flight_iata", upper(regexp_replace(col("flight_iata"), "[^A-Za-z0-9]", ""))) \
    .withColumn("flight_date", to_timestamp("flight_date")) \
    .filter(col("flight_iata").rlike("^[A-Za-z]+[0-9]+$")) \
    .withColumn("arr_airport_iata", upper(regexp_replace(col("arr_airport_iata"), "[^A-Za-z]", ""))) \
    .filter(col("arr_airport_iata").rlike("^[A-Za-z]+$")) \
    .filter(length(col("arr_airport_iata")) ==  3) \
    .withColumn("arr_airport_icao", upper(regexp_replace(col("arr_airport_icao"), "[^A-Za-z]", ""))) \
    .filter(col("arr_airport_icao").rlike("^[A-Za-z]+$")) \
    .filter(length(col("arr_airport_icao")) ==  4) \
    .withColumn("arr_delay", col("arr_delay").cast("int")) \
    .withColumn("scheduled_arr", to_timestamp("scheduled_arr", "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    .withColumn("estimated_arr", to_timestamp("estimated_arr", "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    .withColumn("actual_arr", to_timestamp("actual_arr", "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    .withColumn("estimated_arr_runway", to_timestamp("estimated_arr_runway", "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    .withColumn("actual_arr_runway", to_timestamp("actual_arr_runway", "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    
    


    df_arr_info = df_clean.select(
        col("id"),
        col("flight_iata"),
        col("flight_date"),
        col("arr_airport").alias("airport"),
        col("arr_timezone").alias("timezone"),
        col("arr_airport_iata").alias("iata"),
        col("arr_airport_icao").alias("icao"),
        col("arr_airport_terminal").alias("terminal"),
        col("arr_airport_gate").alias("gate"),
        col("arr_delay"),
        col("arr_airport_baggage").alias("baggage"),
        col("scheduled_arr"),
        col("estimated_arr"),
        col("actual_arr"),
        col("estimated_arr_runway").alias("estimated_runway"),
        col("actual_arr_runway").alias("actual_runway")
    )#.filter(col("created_at") > latest_processing_time)


    df_arr_info.write \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://postgres:{host}/{db_name}") \
    .option("dbtable", "avstack.silver_arr_info") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

    print("Completed cleaning and tranformation process. avstack.bronze_info -> avstack.silver_arr_info")
    print(f"Original record count: {df.count()}")
    print(f"Cleaned record count: {df_arr_info.count()}")