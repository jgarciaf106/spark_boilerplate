import sys
import os
from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import *

if __name__ == "__main__":
    conf = get_spark_app_config()
    
    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()
    
    
    logger = Log4j("boilerplate.spark.app", spark)

    sample_data = os.getcwd() + "/data/sample.csv"

    if not os.path.exists(sample_data):
        logger.error("Sample data does not exits")
        sys.exit(-1)

    logger.info("Starting HelloSpark")

    survey_raw_df = load_survey_df(spark, sample_data)
    partitioned_survey_df = survey_raw_df.repartition(2)
    count_df = count_by_country(partitioned_survey_df)
    count_df.show()

    logger.info("Finished HelloSpark")
    spark.stop()
