from pyspark.sql import SparkSession

def get_spark(app_name="ecommerce_batch_pipeline"):
    """
    Function to create and return a reusable SparkSession.

    Parameters:
    - app_name: Name of the Spark application (shows in Spark UI)

    Returns:
    - spark: SparkSession object
    """

    # Create a SparkSession builder
    spark = SparkSession.builder.appName(app_name).master("local[*]").config(
            "spark.sql.shuffle.partitions",
            "4"
    ).getOrCreate()                  

    # Reduce Spark log noise
    spark.sparkContext.setLogLevel("WARN")

    # Return SparkSession so other scripts can use it
    return spark
