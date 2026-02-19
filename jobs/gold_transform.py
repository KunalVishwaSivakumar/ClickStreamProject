# jobs/gold_transform.py
# Gold Layer: Star Schema Construction
# This job transforms cleaned Silver data into a star schema with one fact table
# and five dimensions, optimized for analytical queries.

from datetime import datetime
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from pyspark.sql.functions import (
    col, row_number, max as spark_max, min as spark_min,
    to_timestamp, year, month, dayofmonth, dayofweek,
    count as spark_count, when, lit, current_timestamp, dense_rank
)
from pyspark.sql.window import Window
from utils.spark_session import get_spark
from utils.logger import setup_logger
from utils.data_quality import DataQualityValidator


def create_users_dimension(silver_df, logger):
    """Create Users dimension table."""
    logger.info("Creating Users dimension...")
    users_dim = (
        silver_df
        .groupBy("UserID")
        .agg(
            spark_min(col("Timestamp")).alias("first_seen_date"),
            spark_max(col("Timestamp")).alias("last_seen_date"),
            spark_count(col("SessionID")).alias("total_events")
        )
        .withColumn("user_id", col("UserID"))
        .select(
            "user_id",
            "first_seen_date",
            "last_seen_date",
            "total_events"
        )
        .sort("user_id")
    )
    user_count = users_dim.count()
    logger.info(f"Created {user_count:,} unique users")
    return users_dim


def create_sessions_dimension(silver_df, logger):
    """Create Sessions dimension table."""
    logger.info("Creating Sessions dimension...")
    sessions_dim = (
        silver_df
        .groupBy("SessionID", "UserID")
        .agg(
            spark_min(col("Timestamp")).alias("session_start_time"),
            spark_max(col("Timestamp")).alias("session_end_time"),
            spark_count(col("*")).alias("session_event_count")
        )
        .withColumn("session_id", col("SessionID"))
        .select(
            "session_id",
            "UserID",
            "session_start_time",
            "session_end_time",
            "session_event_count"
        )
        .sort("session_id")
    )
    session_count = sessions_dim.count()
    logger.info(f"Created {session_count:,} unique sessions")
    return sessions_dim


def create_products_dimension(silver_df, logger):
    """Create Products dimension table."""
    logger.info("Creating Products dimension...")
    products_dim = (
        silver_df
        .select("ProductID")
        .where(col("ProductID").isNotNull())
        .distinct()
        .withColumn("product_id", col("ProductID"))
        .select("product_id")
        .sort("product_id")
    )
    product_count = products_dim.count()
    logger.info(f"Created {product_count:,} unique products")
    return products_dim


def create_eventtype_dimension(silver_df, logger):
    """Create EventType dimension table."""
    logger.info("Creating EventType dimension...")
    eventtype_dim = (
        silver_df
        .select("EventType")
        .distinct()
        .withColumn("event_type", col("EventType"))
        .select("event_type")
        .sort("event_type")
    )
    event_count = eventtype_dim.count()
    logger.info(f"Created {event_count:,} unique event types")
    return eventtype_dim


def create_time_dimension(silver_df, logger):
    """Create Time dimension table with temporal attributes."""
    logger.info("Creating Time dimension...")
    df_with_ts = silver_df.withColumn("ts", to_timestamp(col("Timestamp")))

    time_dim = (
        df_with_ts
        .select(col("ts").alias("timestamp"))
        .distinct()
        .withColumn("year", year(col("timestamp")))
        .withColumn("month", month(col("timestamp")))
        .withColumn("day_of_month", dayofmonth(col("timestamp")))
        .withColumn("day_of_week", dayofweek(col("timestamp")))
        .withColumn("is_weekend", when((col("day_of_week") == 1) | (col("day_of_week") == 7), 1).otherwise(0))
        .select(
            "timestamp",
            "year",
            "month",
            "day_of_month",
            "day_of_week",
            "is_weekend"
        )
        .sort("timestamp")
    )
    time_count = time_dim.count()
    logger.info(f"Created {time_count:,} unique timestamps")
    return time_dim


def create_events_fact_table(silver_df, logger):
    """Create Events fact table."""
    logger.info("Creating Events fact table...")
    df_with_ts = silver_df.withColumn("ts", to_timestamp(col("Timestamp")))
    window_spec = Window.orderBy("ts")

    events_fact = (
        df_with_ts
        .withColumn("event_id", row_number().over(window_spec))
        .withColumn("user_id", col("UserID"))
        .withColumn("session_id", col("SessionID"))
        .withColumn("product_id", col("ProductID"))
        .withColumn("event_type", col("EventType"))
        .withColumn("amount", col("Amount"))
        .withColumn("outcome", col("Outcome"))
        .withColumn("timestamp", col("ts"))
        .select(
            "event_id",
            "user_id",
            "session_id",
            "product_id",
            "event_type",
            "amount",
            "outcome",
            "timestamp"
        )
    )
    event_count = events_fact.count()
    logger.info(f"Created {event_count:,} events in fact table")
    return events_fact


def main():
    logger = setup_logger("gold_transform")
    
    try:
        logger.info("=" * 80)
        logger.info("GOLD LAYER TRANSFORMATION - STAR SCHEMA")
        logger.info("=" * 80)
        
        # ========================================
        # 1. Initialize Spark
        # ========================================
        logger.info("Initializing Spark session...")
        spark = get_spark("gold_clickstream_transform")
        logger.info("Spark session initialized")
        
        # ========================================
        # 2. Define Paths
        # ========================================
        logger.info("Defining data paths...")
        silver_path = str(project_root / "data" / "silver")
        gold_path = str(project_root / "data" / "gold")
        logger.info(f"Silver input: {silver_path}")
        logger.info(f"Gold output: {gold_path}")
        
        # ========================================
        # 3. Read Silver Data
        # ========================================
        logger.info("Reading Silver layer data...")
        try:
            silver_df = spark.read.parquet(silver_path)
            silver_count = silver_df.count()
            logger.info(f"Silver data loaded: {silver_count:,} rows")
            logger.info(f"  Columns: {len(silver_df.columns)}")
        except Exception as e:
            logger.error(f"Failed to read Silver data: {str(e)}", exc_info=True)
            raise
        
        # ========================================
        # 4. Create Dimensions
        # ========================================
        logger.info("")
        logger.info("=" * 80)
        logger.info("CREATING DIMENSIONS")
        logger.info("=" * 80)
        
        users_dim = create_users_dimension(silver_df, logger)
        sessions_dim = create_sessions_dimension(silver_df, logger)
        products_dim = create_products_dimension(silver_df, logger)
        eventtype_dim = create_eventtype_dimension(silver_df, logger)
        time_dim = create_time_dimension(silver_df, logger)
        
        # ========================================
        # 5. Create Fact Table
        # ========================================
        logger.info("")
        logger.info("=" * 80)
        logger.info("CREATING FACT TABLE")
        logger.info("=" * 80)
        
        events_fact = create_events_fact_table(silver_df, logger)
        
        # ========================================
        # 6. Write Gold Layer Tables
        # ========================================
        logger.info("")
        logger.info("=" * 80)
        logger.info("WRITING GOLD LAYER")
        logger.info("=" * 80)
        
        try:
            logger.info(f"Writing Users dimension to {gold_path}/users/...")
            users_dim.write.mode("overwrite").parquet(f"{gold_path}/users")
            logger.info("Users written")
            
            logger.info(f"Writing Sessions dimension to {gold_path}/sessions/...")
            sessions_dim.write.mode("overwrite").parquet(f"{gold_path}/sessions")
            logger.info("Sessions written")
            
            logger.info(f"Writing Products dimension to {gold_path}/products/...")
            products_dim.write.mode("overwrite").parquet(f"{gold_path}/products")
            logger.info("Products written")
            
            logger.info(f"Writing EventType dimension to {gold_path}/event_types/...")
            eventtype_dim.write.mode("overwrite").parquet(f"{gold_path}/event_types")
            logger.info("EventTypes written")
            
            logger.info(f"Writing Time dimension to {gold_path}/time/...")
            time_dim.write.mode("overwrite").parquet(f"{gold_path}/time")
            logger.info("Time written")
            
            logger.info(f"Writing Events fact table to {gold_path}/events/...")
            events_fact.write.mode("overwrite").parquet(f"{gold_path}/events")
            logger.info("Events written")
            
        except Exception as e:
            logger.error(f"Failed to write Gold tables: {str(e)}", exc_info=True)
            raise
        
        # ========================================
        # 7. Validation
        # ========================================
        logger.info("")
        logger.info("=" * 80)
        logger.info("VALIDATION - SAMPLE DATA")
        logger.info("=" * 80)
        
        logger.info("Sample Users:")
        users_dim.limit(5).show(truncate=False)
        
        logger.info("Sample Sessions:")
        sessions_dim.limit(5).show(truncate=False)
        
        logger.info("Sample Products:")
        products_dim.limit(5).show(truncate=False)
        
        logger.info("Sample Event Types:")
        eventtype_dim.show(truncate=False)
        
        logger.info("Sample Events:")
        events_fact.limit(5).show(truncate=False)
        
        # ========================================
        # 8. Final Summary
        # ========================================
        logger.info("")
        logger.info("=" * 80)
        logger.info("✓ GOLD LAYER COMPLETE!")
        logger.info("=" * 80)
        logger.info("")
        logger.info("Summary:")
        logger.info(f"  Users: {users_dim.count():,}")
        logger.info(f"  Sessions: {sessions_dim.count():,}")
        logger.info(f"  Products: {products_dim.count():,}")
        logger.info(f"  Event Types: {eventtype_dim.count():,}")
        logger.info(f"  Time entries: {time_dim.count():,}")
        logger.info(f"  Events (Fact): {events_fact.count():,}")
        logger.info("")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error("GOLD TRANSFORMATION FAILED")
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
