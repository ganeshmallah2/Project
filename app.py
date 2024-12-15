from pyspark.sql import SparkSession
from src.crash_analysis import CrashAnalysis
from src.etl import ETL
from src.config import Config

def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("CrashAnalysisApp").getOrCreate()
    config = Config()
    #ETL().use()
    # Load data using Spark
    df_Units = spark.read.csv(config.get("input_data_folder")+"Units_use.csv", header=True, inferSchema=True)
    df_Primary_Person = spark.read.csv(config.get("input_data_folder")+"Primary_Person_use.csv", header=True, inferSchema=True)
    df_damages = spark.read.csv(config.get("input_data_folder")+"Damages_use.csv", header=True, inferSchema=True)
    df_charges = spark.read.csv(config.get("input_data_folder")+"Charges_use.csv", header=True, inferSchema=True)

    # Perform analysis
    analysis = CrashAnalysis(df_Units, df_Primary_Person, df_damages, df_charges)
    analysis.write_output(config.get("output_data_folder")+"results.txt")

if __name__ == "__main__":
    main()
