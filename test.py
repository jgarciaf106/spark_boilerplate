from unittest import TestCase
from pyspark.sql import SparkSession
from lib.utils import load_survey_df, count_by_country


class UtilsTestCase(TestCase):
    spark = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .master("local[3]") \
            .appName("HelloSparkTest") \
            .getOrCreate()


    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
