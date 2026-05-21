from pyspark.sql import SparkSession
from src.utils.config import IS_DOCKER

def get_spark_session(app_name="YouTubePipeline"):
    """
    Creates and returns a SparkSession with standard configurations.
    """
    builder = SparkSession.builder.appName(app_name)
    
    # Common packages
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.postgresql:postgresql:42.7.3"
    ]
    
    builder = builder.config("spark.jars.packages", ",".join(packages))
    
    # Optimized ivy home for docker
    if IS_DOCKER:
        builder = builder.config("spark.driver.extraJavaOptions", "-Divy.home=/opt/spark/work-dir/.ivy2") \
                         .config("spark.executor.extraJavaOptions", "-Divy.home=/opt/spark/work-dir/.ivy2")
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark
