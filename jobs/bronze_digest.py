# jobs/bronze_digest.py
# Bronze Layer: Raw Data Ingestion with Metadata
# This job reads raw CSV clickstream data and preserves it exactly as-is,
# adding only metadata (ingestion timestamp and source file).

from datetime import datetime
from pathlib import Path
import sys

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from pyspark.sql.functions import lit, current_timestamp
from utils.spark_session import get_spark
from utils.logger import setup_logger
from utils.data_quality import DataQualityValidator


def main():
    # Setup logging FIRST before anything else
    logger = setup_logger("bronze_digest")
    
    try:
        logger.info("=" * 80)
        logger.info("BRONZE LAYER INGESTION - STARTING")
        logger.info("=" * 80)
        
        # ========================================
        # 1. Initialize Spark Session
        # ========================================
        logger.info("Initializing Spark session...")
        spark = get_spark(app_name="bronze_clickstream_ingestion")
        logger.info("Spark session initialized successfully")
        
        # ========================================
        # 2. Define Paths
        # ========================================
        logger.info("Defining data paths...")
        input_path = str(project_root / "data" / "raw" / "ecommerce_clickstream_transactions.csv")
        ingestion_date = datetime.today().strftime("%Y-%m-%d")
        output_path = str(project_root / "data" / "bronze" / f"ingestion_date={ingestion_date}")
        
        logger.info(f"Input CSV: {input_path}")
        logger.info(f"Output Bronze: {output_path}")
        logger.info(f"Ingestion date: {ingestion_date}")
        
        # ========================================
        # 3. Read Raw CSV Data
        # ========================================
        logger.info("Reading raw CSV data...")
        try:
            raw_df = spark.read.csv(
                input_path,
                header=True,
                inferSchema=False  # Read all as strings initially
            )
            row_count = raw_df.count()
            logger.info(f"CSV loaded successfully: {row_count:,} rows")
        except FileNotFoundError:
            logger.error(f"Input file not found: {input_path}")
            raise
        except Exception as e:
            logger.error(f"Failed to read CSV: {str(e)}", exc_info=True)
            raise
        
        # ========================================
        # 4. Validate Raw Data Quality
        # ========================================
        logger.info("Validating raw data...")
        validator = DataQualityValidator(spark, logger)
        quality_metrics = validator.validate_bronze(raw_df)
        
        if quality_metrics['total_rows'] < 100:
            logger.warning(f"Low row count detected: {quality_metrics['total_rows']}")
        
        # ========================================
        # 5. Add Ingestion Metadata
        # ========================================
        logger.info("Adding ingestion metadata...")
        bronze_df = raw_df.withColumn(
            "ingestion_timestamp",
            current_timestamp()
        ).withColumn(
            "source_file",
            lit("ecommerce_clickstream_transactions.csv")
        )
        
        logger.info(f"Metadata added: {len(bronze_df.columns)} columns total")
        logger.debug(f"Columns: {bronze_df.columns}")
        
        # ========================================
        # 6. Write to Bronze Layer
        # ========================================
        logger.info(f"Writing data to Bronze layer: {output_path}")
        try:
            bronze_df.write.mode("overwrite").parquet(output_path)
            logger.info(f"Bronze write completed successfully")
        except Exception as e:
            logger.error(f"Failed to write to Bronze: {str(e)}", exc_info=True)
            raise
        
        # ========================================
        # 7. Verify Written Data
        # ========================================
        logger.info("Verifying written data...")
        try:
            bronze_written = spark.read.parquet(output_path)
            verified_count = bronze_written.count()
            logger.info(f"Bronze verification: {verified_count:,} rows in storage")
            
            if verified_count != row_count:
                logger.warning(f"Row count mismatch: {row_count:,} input vs {verified_count:,} written")
        except Exception as e:
            logger.warning(f"Could not verify written data: {str(e)}")
        
        # ========================================
        # 8. Log Summary Statistics
        # ========================================
        logger.info("=" * 80)
        logger.info("BRONZE INGESTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Rows ingested: {row_count:,}")
        logger.info(f"Null UserIDs: {quality_metrics['null_userids']:,}")
        logger.info(f"Null EventTypes: {quality_metrics['null_eventtypes']:,}")
        logger.info(f"Columns added: {len(bronze_df.columns)}")
        logger.info(f"Quality score: {quality_metrics['quality_score_pct']}%")
        logger.info("=" * 80)
        logger.info("BRONZE INGESTION COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error("BRONZE INGESTION FAILED")
        logger.error("=" * 80)
        logger.error(f"Error: {str(e)}", exc_info=True)
        logger.error("=" * 80)
        raise
    
    finally:
        logger.info("Stopping Spark session...")
        spark.stop()
        logger.info("Spark session stopped")
        logger.info("Done!")


if __name__ == "__main__":
    main()
