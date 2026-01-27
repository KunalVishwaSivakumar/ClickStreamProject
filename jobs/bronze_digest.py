# Import Python standard library for handling dates and paths
from datetime import datetime
from pathlib import Path
import sys

# Ensure project root is on sys.path so `utils` can be imported when
# running this script from the `jobs/` directory.
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import Spark SQL helper functions
from pyspark.sql.functions import lit, current_timestamp

# Import reusable SparkSession creator from project root
from utils.spark_session import get_spark


def main():
    # ---------------------------------------
    # 1. Start Spark (engine ON)
    # ---------------------------------------
    print("Starting Spark session...")
    spark = get_spark(app_name="bronze_clickstream_ingestion")
    print("Spark session started!")

    # ---------------------------------------
    # 2. Define input & output paths (absolute)
    # ---------------------------------------
    input_path = str(project_root / "data" / "raw" / "ecommerce_clickstream_transactions.csv")

    # Capture ingestion date for partitioning
    ingestion_date = datetime.today().strftime("%Y-%m-%d")
    print(f"Ingestion date: {ingestion_date}")

    # Bronze layer output path
    output_path = str(project_root / "data" / "bronze" / f"ingestion_date={ingestion_date}")
    print(f"Input path: {input_path}")
    print(f"Output path: {output_path}")

    # ---------------------------------------
    # 3. Read raw CSV data
    # ---------------------------------------
    print(f"Reading CSV from: {input_path}")
    raw_df = spark.read.csv(
        input_path,
        header=True,        # First row contains column names
        inferSchema=False   # Skip expensive schema inference; read all as strings
    )
    print(f"CSV loaded. Row count estimate: {raw_df.count()}")

    # ---------------------------------------
    # 4. Add ingestion metadata
    # ---------------------------------------
    print("Adding ingestion metadata...")
    bronze_df = raw_df.withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )

    bronze_df = bronze_df.withColumn(
        "source_file",
        lit("clickstream.csv")
    )
    print(f"Metadata added. Bronze schema has {len(bronze_df.columns)} columns.")

    # ---------------------------------------
    # 5. Write data to Bronze layer
    # ---------------------------------------
    print(f"Writing parquet to: {output_path}")
    bronze_df.write.mode("append").parquet(output_path)
    print("Parquet write complete!")

    # ---------------------------------------
    # 6. Stop Spark session
    # ---------------------------------------
    print("Stopping Spark session...")
    spark.stop()
    print("All done!")


# Standard Python entry point
if __name__ == "__main__":
    main()