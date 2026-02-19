# jobs/silver_transform.py
# Silver Layer: Data Quality and Validation
# This job applies business rules and removes bad data so only clean data
# flows downstream to analytics.

from datetime import datetime
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from pyspark.sql.functions import col
from utils.spark_session import get_spark
from utils.logger import setup_logger
from utils.data_quality import DataQualityValidator


def main():
    logger = setup_logger("silver_transform")
    
    try:
        logger.info("=" * 80)
        logger.info("SILVER LAYER TRANSFORMATION - DATA QUALITY")
        logger.info("=" * 80)
        
        # ========================================
        # 1. Initialize Spark
        # ========================================
        logger.info("Initializing Spark session...")
        spark = get_spark("silver_clickstream_transform")
        logger.info("Spark session initialized")
        
        # ========================================
        # 2. Define Paths
        # ========================================
        logger.info("Defining data paths...")
        ingestion_date = datetime.today().strftime("%Y-%m-%d")
        bronze_path = str(project_root / "data" / "bronze" / f"ingestion_date={ingestion_date}")
        silver_path = str(project_root / "data" / "silver")
        logger.info(f"Bronze input: {bronze_path}")
        logger.info(f"Silver output: {silver_path}")
        
        # ========================================
        # 3. Read Bronze Data
        # ========================================
        logger.info("Reading Bronze layer data...")
        try:
            bronze_df = spark.read.parquet(bronze_path)
            bronze_count = bronze_df.count()
            logger.info(f"Bronze data loaded: {bronze_count:,} rows")
        except Exception as e:
            logger.error(f"Failed to read Bronze data: {str(e)}", exc_info=True)
            raise
        
        # ========================================
        # 4. Apply Validation Rules
        # ========================================
        logger.info("Applying data quality rules...")
        
        # Rule 1: Remove null UserIDs
        logger.info("Rule 1: Removing rows with null UserID...")
        before_rule1 = bronze_df.count()
        bronze_df = bronze_df.filter(col("UserID").isNotNull())
        after_rule1 = bronze_df.count()
        removed_rule1 = before_rule1 - after_rule1
        logger.info(f"  Removed {removed_rule1:,} rows (null UserID)")
        
        # Rule 2: Remove null EventTypes
        logger.info("Rule 2: Removing rows with null EventType...")
        before_rule2 = bronze_df.count()
        bronze_df = bronze_df.filter(col("EventType").isNotNull())
        after_rule2 = bronze_df.count()
        removed_rule2 = before_rule2 - after_rule2
        logger.info(f"  Removed {removed_rule2:,} rows (null EventType)")
        
        # Rule 3: Product events must have ProductID
        logger.info("Rule 3: Validating product events have ProductID...")
        product_events = ["product_view", "add_to_cart", "purchase"]
        before_rule3 = bronze_df.count()
        bronze_df = bronze_df.filter(
            (col("EventType").isin(product_events) & col("ProductID").isNotNull()) |
            (~col("EventType").isin(product_events))
        )
        after_rule3 = bronze_df.count()
        removed_rule3 = before_rule3 - after_rule3
        logger.info(f"  Removed {removed_rule3:,} rows (product event without ProductID)")
        
        # Rule 4: Purchase events must have Amount
        logger.info("Rule 4: Validating purchase events have Amount...")
        before_rule4 = bronze_df.count()
        bronze_df = bronze_df.filter(
            (col("EventType") != "purchase") | col("Amount").isNotNull()
        )
        after_rule4 = bronze_df.count()
        removed_rule4 = before_rule4 - after_rule4
        logger.info(f"  Removed {removed_rule4:,} rows (purchase without Amount)")
        
        # ========================================
        # 5. Validate Silver Data Quality
        # ========================================
        logger.info("Validating Silver layer data...")
        validator = DataQualityValidator(spark, logger)
        silver_metrics = validator.validate_silver(bronze_df)
        
        silver_count = bronze_df.count()
        total_removed = bronze_count - silver_count
        retention_pct = (silver_count / bronze_count * 100) if bronze_count > 0 else 0
        
        # ========================================
        # 6. Write Silver Data
        # ========================================
        logger.info(f"Writing data to Silver layer: {silver_path}")
        try:
            bronze_df.write.mode("overwrite").parquet(silver_path)
            logger.info("Silver write completed successfully")
        except Exception as e:
            logger.error(f"Failed to write Silver data: {str(e)}", exc_info=True)
            raise
        
        # ========================================
        # 7. Verify Written Data
        # ========================================
        logger.info("Verifying written data...")
        try:
            silver_written = spark.read.parquet(silver_path)
            verified_count = silver_written.count()
            logger.info(f"Silver verification: {verified_count:,} rows in storage")
        except Exception as e:
            logger.warning(f"Could not verify written data: {str(e)}")
        
        # ========================================
        # 8. Log Summary
        # ========================================
        logger.info("=" * 80)
        logger.info("SILVER TRANSFORMATION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Bronze input rows: {bronze_count:,}")
        logger.info(f"Silver output rows: {silver_count:,}")
        logger.info(f"Total rows removed: {total_removed:,}")
        logger.info(f"Retention rate: {retention_pct:.2f}%")
        logger.info(f"Quality status: {silver_metrics['quality_status']}")
        logger.info("")
        logger.info("Removal breakdown:")
        logger.info(f"  Rule 1 (null UserID): {removed_rule1:,}")
        logger.info(f"  Rule 2 (null EventType): {removed_rule2:,}")
        logger.info(f"  Rule 3 (product events): {removed_rule3:,}")
        logger.info(f"  Rule 4 (purchase events): {removed_rule4:,}")
        logger.info("=" * 80)
        logger.info("SILVER TRANSFORMATION COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error("SILVER TRANSFORMATION FAILED")
        logger.error("=" * 80)
        logger.error(f"Error: {str(e)}", exc_info=True)
        logger.error("=" * 80)
        raise
    
    finally:
        logger.info("Stopping Spark session...")
        spark.stop()
        logger.info("Done!")


if __name__ == "__main__":
    main()
