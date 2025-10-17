# etl/future_data/future_data_encoding.py

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel 

# --------------- Spark Session ---------------
spark = (
    SparkSession.builder
    .appName("FutureFeatureEncoding")
    .config("spark.driver.memory", "6g")
    .getOrCreate()
)

# --------------- Load Future Data ---------------
future_df = spark.read.parquet("data/future_parquet/")
print(f"Loaded {future_df.count()} future flights")

# --------------- Load Saved Encoding Pipeline ---------------
pipeline_model = PipelineModel.load("models/encoding_pipeline")
print("Loaded encoding pipeline")

# --------------- Transform Future Data ---------------
encoded_future_df = pipeline_model.transform(future_df)

encoded_future_df.select("features").show(5, truncate=False)
print(f"Encoded {encoded_future_df.count()} future flights")

# --------------- Save Encoded Future Data ---------------
encoded_future_df.select("features").write.mode("overwrite").parquet("data/model_encoded_future/")
print("Saved encoded future features to data/model_encoded_future/")