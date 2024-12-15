import unittest
from pyspark.sql import SparkSession
from src.data_loader import load_data

class TestDataLoader(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

    def test_load_data(self):
        df = load_data(self.spark, 'data/Charges_use.csv')
        self.assertEqual(df.count(), 100)  # Example check

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
