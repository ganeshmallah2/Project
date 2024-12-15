from pyspark.sql import SparkSession

def load_data(spark, file_path):
    """Load data from a CSV file into a DataFrame."""
    return spark.read.option("header", "true").csv(file_path)
