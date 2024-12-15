import os
from pyspark.sql import SparkSession
import logging as log

class ETL:
    def __init__(self):
        self.spark = SparkSession.builder.appName("ETL").getOrCreate()
        log.basicConfig(level=log.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    def clean_data(self, input_file, output_file):
        """
        Cleans a CSV file by removing the PRSN_AGE column using PySpark.

        Args:
            input_file (str): Path to the input CSV file.
            output_file (str): Path to save the cleaned CSV file.
        """
        try:
            log.info(f"Processing file: {input_file}")
            df = self.spark.read.csv(input_file, header=True, inferSchema=True, sep="\t")
            log.info(f"Initial schema: {df.schema.simpleString()}")

            # Drop the specified column
            df_cleaned = df.drop("PRSN_AGE")
            log.info(f"Schema after dropping PRSN_AGE: {df_cleaned.schema.simpleString()}")

            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_file), exist_ok=True)

            # Save the cleaned data
            df_cleaned.write.csv(output_file, header=True, mode="overwrite")
            log.info(f"Cleaned file saved to: {output_file}")
        except Exception as e:
            log.error(f"Error: {e}")
            raise

    def clean_units_data(self, input_file, output_file):
        """
        Cleans the Units_use.csv file by replacing blank values with 'NA' 
        for specific columns.

        Args:
            input_file (str): Path to the input CSV file.
            output_file (str): Path to save the cleaned CSV file.
        """
        try:
            log.info(f"Processing file: {input_file}")
            df = self.spark.read.csv(input_file, header=True, inferSchema=True, sep="\t")
            log.info(f"Initial schema: {df.schema.simpleString()}")

            # Replace blank values with 'NA' for specific columns
            columns_to_replace = [
                "VEH_PARKED_FL", "VEH_HNR_FL", "EMER_RESPNDR_FL", 
                "VEH_INVENTORIED_FL", "VEH_TRANSP_NAME"
            ]
            for column in columns_to_replace:
                df = df.na.fill({column: "NA"})

            log.info(f"Schema after replacing blank values with 'NA': {df.schema.simpleString()}")

            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_file), exist_ok=True)

            # Save the cleaned data
            df.write.csv(output_file, header=True, mode="overwrite")
            log.info(f"Cleaned file saved to: {output_file}")
        except Exception as e:
            log.error(f"Error: {e}")
            raise

    def use(self):
        input_path_primary_person = "data/input/Primary_Person_use.csv"
        output_path_primary_person = "data/processed/"
        input_path_units = "data/input/Units_use.csv"
        output_path_units = "data/processed/"
        self.clean_data(input_path_primary_person, output_path_primary_person)
        self.clean_units_data(input_path_units, output_path_units)

# if __name__ == "__main__":
#     input_path_primary_person = "data/raw/Primary_Person_use.csv"
#     output_path_primary_person = "data/processed/Primary_Person_cleaned"
#     input_path_units = "data/raw/Units_use.csv"
#     output_path_units = "data/processed/Units_cleaned"

#     etl = ETL()
#     etl.clean_data(input_path_primary_person, output_path_primary_person)
#     etl.clean_units_data(input_path_units, output_path_units)
