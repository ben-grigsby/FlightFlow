#etl/silver/avstack/silver_flight_info.py

from pyspark.sql.functions import col, to_timestamp, length, regexp_replace, upper


# ==================================================================
# Functions
# ==================================================================


def clean_transform_flight_info(df, user, password, host, db_name, latest_processing_time):
    """
    Clean and transform the columns that will go into the avstack.silver_flight_info table.
    """

    print("Starting cleaning and transformation process. avstack.bronze_info -> avstack.silver_flight_info")

    df_clean = df \
    .withColumn("flight_date", to_timestamp("flight_date")) \
    .filter(length(col("airline_iata")) == 2) \
    .filter(length(col("airline_icao")) == 3) \
    .withColumn("flight_number", col("flight_number").cast("int")) \
    .withColumn("flight_iata", upper(regexp_replace(col("flight_iata"), "[^A-Za-z0-9]", ""))) \
    .filter(col("flight_iata").rlike("^[A-Za-z]+[0-9]+$")) \
    .withColumn("flight_icao", upper(regexp_replace(col("flight_icao"), "[^A-Za-z0-9]", ""))) \
    .filter(col("flight_icao").rlike("^[A-Za-z]+[0-9]+$"))


    df_flight_info = df_clean.select(
        "id",
        "flight_iata",
        "flight_date",
        "flight_icao",
        "flight_number",
        "flight_status",
        "airline_name",
        "airline_iata",
        "airline_icao",
        "aircraft",
        "live",
    )#.filter(col("created_at") > latest_processing_time)


    df_flight_info.write \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://postgres:{host}/{db_name}") \
    .option("dbtable", "avstack.silver_flight_info") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

    print("Completed cleaning and tranformation process. avstack.bronze_info -> avstack.silver_flight_info")
    print(f"Original record count: {df.count()}")
    print(f"Cleaned record count: {df_flight_info.count()}")