# etl/feature_encoding.py

from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import lower, col, to_timestamp, date_format

# ---------------------- Spark Session ----------------------
spark = (
    SparkSession.builder
    .appName("FeatureEncoding")
    .config("spark.driver.memory", "6g")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.shuffle.partitions", "150")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "data/iceberg_warehouse")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------- Load Base Features ----------------------
df = spark.read.parquet("data/model_features_cleaned/")

# --- Extract departure and arrival hour from ISO timestamps ---
df = df.withColumn("dep_hour", date_format(to_timestamp(col("dept_scheduled")), "HH").cast("int"))
df = df.withColumn("arr_hour", date_format(to_timestamp(col("arr_scheduled")), "HH").cast("int"))

# ---------------------- Feature Definitions ----------------------
categorical_cols = ["airline", "dep_airport", "arr_airport"]
numeric_cols = ["month", "day", "dep_hour", "arr_hour"]
target_col = "arr_delay"

# Lowercase all categorical string columns
for c in categorical_cols:
    df = df.withColumn(c, lower(col(c)))

# ---------------------- Encoding ----------------------
indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in categorical_cols]
encoders = [OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_vec") for c in categorical_cols]

assembler_inputs = [f"{c}_vec" for c in categorical_cols] + numeric_cols
assembler = VectorAssembler(
    inputCols=assembler_inputs,
    outputCol="features",
    handleInvalid="keep"
)

# ---------------------- Pipeline ----------------------
pipeline = Pipeline(stages=indexers + encoders + [assembler])
encoded_model = pipeline.fit(df)
encoded_df = encoded_model.transform(df)

# ---------------------- Save the Fitted Pipeline ----------------------
encoded_model.write().overwrite().save("data/models/encoding_pipeline")
print("Encoding pipeline saved to models/encoding_pipeline")

# ---------------------- Save Encoded Data ----------------------
encoded_df.select("features", target_col).write.mode("overwrite").parquet("data/model_encoded/")

print("Feature encoding complete — saved to data/model_encoded/")

if __name__ == '__main__':
    encoded_df.select("features", target_col, "dep_hour", "arr_hour").show(5, truncate=False)