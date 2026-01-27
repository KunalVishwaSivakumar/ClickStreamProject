from datetime import datetime
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0,str(project_root))

from pyspark.sql.functions import col, trim
from utils.spark_session import get_spark

def main():
    spark = get_spark(appname="silver_clickstream_tranform")
    #Define Path
    bronze_path = str(project_root / "data" / "bronze")
    silver_path = str(project_root / "data" / "silver")

    bronze_df = spark.read.parquet(bronze_path)

    silver_df = bronze_df.dropna(subset=["user_id", "event_type"])

    product_events = ["product_view", "add_to_cart", "purchase"]

    silver_df = silver_df.filter(
    (col("event_type").isin(product_events) & col("product_id").isNotNull()
    |
    (~col("event_type").isin(product_events)))
    )


    silver_df = silver_df.filter(
        (col("event_type") != "purchase") | col("amount").isNotNull()
    )

    silver_df.write.mode("overwrite").parquet(silver_path)

    spark.close()

if __name__ == "__main__":
    main()


