from datetime import datetime
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0,str(project_root))

from pyspark.sql.functions import col, trim
from utils.spark_session import get_spark

def main():
    spark = get_spark("silver_clickstream_tranform")
    #Define Path
    bronze_path = str(project_root / "data" / "bronze")
    silver_path = str(project_root / "data" / "silver")

    bronze_df = spark.read.parquet(bronze_path)

    silver_df = bronze_df.dropna(subset=["UserID", "EventType"])

    product_events = ["product_view", "add_to_cart", "purchase"]

    silver_df = silver_df.filter(
    (col("EventType").isin(product_events) & col("ProductId").isNotNull()
    |
    (~col("EventType").isin(product_events)))
    )


    silver_df = silver_df.filter(
        (col("EventType") != "purchase") | col("Amount").isNotNull()
    )

    silver_df.write.mode("overwrite").parquet(silver_path)

    spark.stop()

if __name__ == "__main__":
    main()


