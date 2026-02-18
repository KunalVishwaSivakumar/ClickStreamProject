from datetime import datetime
import sys
from pathlib import Path

project_root  = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0,str(project_root))

from pyspark.sql.functions import (
    col, row_number, max as spark_max, min as spark_min,
    to_timestamp, year, month, dayofmonth, dayofweek,
    count as spark_count, when, lit, current_timestamp, dense_rank
    
)
from pyspark.sql.window import Window
from utils.spark_session import get_spark

def create_user_dimensions(silver_df):
    print("Creating Users dimension...")
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
    print(f"Created {users_dim.count()} unique users")
    return users_dim

def create_sessions_dimensions(silver_df):
    print("Creating Sessions dimension...")
    sessions_dim = (
        silver_df
        .groupBy("SessionID","UserID")
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
    print(f"Created {sessions_dim.count()} unique sessions")
    return sessions_dim

def create_products_dimensions(silver_df):
    print("Creating Products dimension...")
    products_dim = (
        silver_df
        .select("ProductID")
        .where(col("ProductID").isNotNull())
        .distinct()
        .withColumn("product_id", col("ProductID"))
        .select("product_id")
        .sort("product_id")
    )
    print(f"Created {products_dim.count()} unique products")
    return products_dim

def create_eventtype_dimension(silver_df):
    print("Creating EventType dimension...")
    eventtype_dim = (
        silver_df
        .select("EventType")
        .distinct()
        .withColumn("event_type", col("EventType"))
        .select("event_type")
        .sort("event_type")
    
    )
    print(f"✓ Created {eventtype_dim.count()} unique event types")
    return eventtype_dim
def create_time_dimension(silver_df):
    print("Creating Time dimension...")
    df_with_ts = silver_df.withColumn("ts",to_timestamp(col("Timestamp")))

    time_dim = (
        df_with_ts
        .select(col("ts").alias("timestamp"))
        .distinct()
        .withColumn("year", year(col("timestamp")))
        .withColumn("month", month(col("timestamp")))
        .withColumn("day_of_month", dayofmonth(col("timestamp")))
        .withColumn("day_of_week", dayofweek(col("timestamp")))
        .withColumn("is_weekend", when((col("day_of_week")==1)|(col("day_of_week")==7),1).otherwise(0))
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
    print(f"Created {time_dim.count()} unique timestamps")
    return time_dim

def create_events_fact_table(silver_df):
    print('Creating Events fact table...')
    df_with_ts = silver_df.withColumn("ts", to_timestamp(col("Timestamp")))
    window_spec = Window.orderBy("ts")

    events_fact = (
        df_with_ts
        .withColumn("event_id", row_number().over(window_spec))
        .withColumn("user_id",col("UserID"))
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
    print(f"Created {events_fact.count()} events in fact table")
    return events_fact

def main():
    print("\n" + "="*70)
    print("GOLD LAYER TRANFORMATION - STAR SCHEMA")
    print("="*70 + "\n")

    spark = get_spark("gold_clickstream_transform")

    silver_path = str(project_root / "data" / "silver")
    gold_path = str(project_root / "data" / "gold")

    print(f"Reading silver data from: {silver_path}")
    silver_df = spark.read.parquet(silver_path)
    print(f"Silver data loaded: {silver_df.count():,} rows, {len(silver_df.columns)} columns")
    print(f"\nSilver schema:")
    silver_df.printSchema()
    print()

    # Create dimensional tables
    print("\n" + "="*70)
    print("CREATING DIMENSIONS")
    print("="*70 + "\n")
    
    users_dim = create_user_dimensions(silver_df)
    sessions_dim = create_sessions_dimensions(silver_df)
    products_dim = create_products_dimensions(silver_df)
    eventtype_dim = create_eventtype_dimension(silver_df)
    time_dim = create_time_dimension(silver_df)
    
    # Create fact table
    print("\n" + "="*70)
    print("CREATING FACT TABLE")
    print("="*70 + "\n")
    
    events_fact = create_events_fact_table(silver_df)
    
    # Write to Gold layer
    print("\n" + "="*70)
    print("WRITING GOLD LAYER")
    print("="*70 + "\n")
    
    print(f"Writing Users dimension to: {gold_path}/users/")
    users_dim.write.mode("overwrite").parquet(f"{gold_path}/users")
    print("✓ Users written")
    
    print(f"Writing Sessions dimension to: {gold_path}/sessions/")
    sessions_dim.write.mode("overwrite").parquet(f"{gold_path}/sessions")
    print("✓ Sessions written")
    
    print(f"Writing Products dimension to: {gold_path}/products/")
    products_dim.write.mode("overwrite").parquet(f"{gold_path}/products")
    print("✓ Products written")
    
    print(f"Writing EventType dimension to: {gold_path}/event_types/")
    eventtype_dim.write.mode("overwrite").parquet(f"{gold_path}/event_types")
    print("✓ EventTypes written")
    
    print(f"Writing Time dimension to: {gold_path}/time/")
    time_dim.write.mode("overwrite").parquet(f"{gold_path}/time")
    print("✓ Time written")
    
    print(f"Writing Events fact table to: {gold_path}/events/")
    events_fact.write.mode("overwrite").parquet(f"{gold_path}/events")
    print("✓ Events written")
    
    # Validate
    print("\n" + "="*70)
    print("VALIDATION - SAMPLE DATA")
    print("="*70 + "\n")
    
    print("Sample Users:")
    users_dim.limit(5).show(truncate=False)
    
    print("\nSample Sessions:")
    sessions_dim.limit(5).show(truncate=False)
    
    print("\nSample Products:")
    products_dim.limit(5).show(truncate=False)
    
    print("\nSample Event Types:")
    eventtype_dim.show(truncate=False)
    
    print("\nSample Events:")
    events_fact.limit(5).show(truncate=False)
    
    print("\n" + "="*70)
    print("✓ GOLD LAYER COMPLETE!")
    print("="*70 + "\n")
    
    print(f"Summary:")
    print(f"  Users: {users_dim.count():,}")
    print(f"  Sessions: {sessions_dim.count():,}")
    print(f"  Products: {products_dim.count():,}")
    print(f"  Event Types: {eventtype_dim.count():,}")
    print(f"  Time entries: {time_dim.count():,}")
    print(f"  Events (Fact): {events_fact.count():,}")
    
    spark.stop()


if __name__ == "__main__":
    main()


