# jobs/advanced_analytics.py
# Advanced Analytics: RFM Analysis, Session Metrics, and Customer Insights
# This job runs advanced analytical queries for deeper customer understanding.

from pathlib import Path
import sys
from datetime import datetime, timedelta

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from utils.spark_session import get_spark
from utils.logger import setup_logger


def setup_tables(spark, gold_path, logger):
    """Register gold layer tables as views."""
    logger.info("Loading gold layer tables...")
    try:
        from pyspark.sql.functions import col

        events = spark.read.parquet(f"{gold_path}/events")
        users = spark.read.parquet(f"{gold_path}/users")
        sessions = spark.read.parquet(f"{gold_path}/sessions")
        products = spark.read.parquet(f"{gold_path}/products")
        time_dim = spark.read.parquet(f"{gold_path}/time")

        # Fix: Cast timestamp columns explicitly to avoid Spark 3.0 strict parser issues
        # with microsecond precision timestamps (e.g. '2024-07-18 04:06:30.792952')
        sessions = sessions \
            .withColumn("session_start_time", col("session_start_time").cast("timestamp")) \
            .withColumn("session_end_time", col("session_end_time").cast("timestamp"))

        events = events \
            .withColumn("timestamp", col("timestamp").cast("timestamp"))

        events.createOrReplaceTempView("events")
        users.createOrReplaceTempView("users")
        sessions.createOrReplaceTempView("sessions")
        products.createOrReplaceTempView("products")
        time_dim.createOrReplaceTempView("time_dim")

        logger.info("All tables loaded and registered")
        return events, users, sessions, products, time_dim

    except Exception as e:
        logger.error(f"Failed to load tables: {str(e)}", exc_info=True)
        raise


def query_rfm_analysis(spark, logger):
    """RFM Analysis: Recency, Frequency, Monetary Value"""
    logger.info("\n" + "=" * 80)
    logger.info("ADVANCED QUERY 1: RFM CUSTOMER SEGMENTATION")
    logger.info("=" * 80)
    try:
        result = spark.sql("""
            WITH purchases AS (
                SELECT
                    user_id,
                    COUNT(*) as purchase_frequency,
                    SUM(amount) as monetary_value,
                    MAX(timestamp) as last_purchase_date
                FROM events
                WHERE event_type = 'purchase' AND amount > 0
                GROUP BY user_id
            ),
            rfm_scored AS (
                SELECT
                    user_id,
                    purchase_frequency,
                    monetary_value,
                    last_purchase_date,
                    CASE
                        WHEN last_purchase_date > DATE_SUB(CURRENT_DATE, 30) THEN 'Recent'
                        WHEN last_purchase_date > DATE_SUB(CURRENT_DATE, 90) THEN 'Active'
                        ELSE 'Inactive'
                    END as recency_segment,
                    CASE
                        WHEN purchase_frequency >= 5 THEN 'Frequent'
                        WHEN purchase_frequency >= 2 THEN 'Regular'
                        ELSE 'Occasional'
                    END as frequency_segment,
                    CASE
                        WHEN monetary_value >= 500 THEN 'High-Value'
                        WHEN monetary_value >= 100 THEN 'Medium-Value'
                        ELSE 'Low-Value'
                    END as monetary_segment
                FROM purchases
            ),
            total_customers AS (
                SELECT COUNT(*) as total_count FROM rfm_scored
            )
            SELECT
                recency_segment,
                frequency_segment,
                monetary_segment,
                COUNT(*) as customer_count,
                ROUND(COUNT(*) * 100.0 / t.total_count, 2) as pct_of_customers,
                COUNT(DISTINCT user_id) as unique_customers,
                ROUND(AVG(monetary_value), 2) as avg_clv,
                ROUND(MAX(monetary_value), 2) as max_clv,
                ROUND(MIN(monetary_value), 2) as min_clv
            FROM rfm_scored
            CROSS JOIN total_customers t
            GROUP BY recency_segment, frequency_segment, monetary_segment, t.total_count
            ORDER BY avg_clv DESC
        """)
        logger.info("RFM Segmentation Results:")
        result.show(truncate=False)
        logger.info("RFM Analysis completed")
        return result
    except Exception as e:
        logger.error(f"RFM Analysis failed: {str(e)}", exc_info=True)
        return None


def query_session_metrics(spark, logger):
    """Session Analysis: Understanding browsing patterns"""
    logger.info("\n" + "=" * 80)
    logger.info("ADVANCED QUERY 2: SESSION BEHAVIOR ANALYSIS")
    logger.info("=" * 80)
    try:
        result = spark.sql("""
            WITH session_details AS (
                SELECT
                    s.session_id,
                    s.session_event_count,
                    (UNIX_TIMESTAMP(s.session_end_time) - UNIX_TIMESTAMP(s.session_start_time)) / 60 as session_duration_minutes,
                    HOUR(s.session_start_time) as hour_of_day,
                    COUNT(CASE WHEN e.event_type = 'purchase' THEN 1 END) as purchases_in_session,
                    SUM(e.amount) as session_revenue
                FROM sessions s
                LEFT JOIN events e ON s.session_id = e.session_id
                GROUP BY s.session_id, s.session_event_count, s.session_start_time, s.session_end_time
            )
            SELECT
                CASE
                    WHEN session_duration_minutes <= 5  THEN 'Quick (<=5 min)'
                    WHEN session_duration_minutes <= 15 THEN 'Short (5-15 min)'
                    WHEN session_duration_minutes <= 30 THEN 'Medium (15-30 min)'
                    ELSE 'Long (30+ min)'
                END as session_length,
                COUNT(*) as session_count,
                ROUND(AVG(session_event_count), 1) as avg_events_per_session,
                ROUND(AVG(session_duration_minutes), 1) as avg_duration_minutes,
                ROUND(SUM(CASE WHEN purchases_in_session > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as conversion_rate_pct,
                ROUND(SUM(session_revenue), 2) as total_revenue,
                ROUND(AVG(session_revenue), 2) as avg_revenue_per_session
            FROM session_details
            GROUP BY session_length
            ORDER BY avg_events_per_session DESC
        """)
        logger.info("Session Behavior Results:")
        result.show(truncate=False)
        logger.info("Session Analysis completed")
        return result
    except Exception as e:
        logger.error(f"Session Analysis failed: {str(e)}", exc_info=True)
        return None


def query_hourly_patterns(spark, logger):
    """Hourly Traffic Patterns: When do customers shop?"""
    logger.info("\n" + "=" * 80)
    logger.info("ADVANCED QUERY 3: HOURLY TRAFFIC PATTERNS")
    logger.info("=" * 80)
    try:
        result = spark.sql("""
            SELECT
                HOUR(e.timestamp) as hour_of_day,
                COUNT(DISTINCT e.user_id) as unique_users,
                COUNT(*) as total_events,
                ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT e.user_id), 2) as avg_events_per_user,
                COUNT(CASE WHEN e.event_type = 'product_view' THEN 1 END) as product_views,
                COUNT(CASE WHEN e.event_type = 'purchase' THEN 1 END) as purchases,
                ROUND(SUM(CASE WHEN e.event_type = 'purchase' THEN e.amount ELSE 0 END), 2) as revenue,
                ROUND(SUM(CASE WHEN e.event_type = 'purchase' THEN e.amount ELSE 0 END) / COUNT(DISTINCT e.user_id), 2) as revenue_per_user
            FROM events e
            GROUP BY HOUR(e.timestamp)
            ORDER BY hour_of_day
        """)
        logger.info("Hourly Traffic Pattern Results:")
        result.show(truncate=False)
        logger.info("Hourly Analysis completed")
        return result
    except Exception as e:
        logger.error(f"Hourly Analysis failed: {str(e)}", exc_info=True)
        return None


def query_product_cross_sell(spark, logger):
    """Cross-Sell Analysis: What products are viewed together?"""
    logger.info("\n" + "=" * 80)
    logger.info("ADVANCED QUERY 4: PRODUCT AFFINITY ANALYSIS")
    logger.info("=" * 80)
    try:
        result = spark.sql("""
            WITH user_products AS (
                SELECT
                    user_id,
                    COLLECT_LIST(product_id) as products_viewed
                FROM events
                WHERE event_type IN ('product_view', 'add_to_cart', 'purchase')
                  AND product_id IS NOT NULL
                GROUP BY user_id
            )
            SELECT
                'Products viewed together' as analysis,
                COUNT(DISTINCT user_id) as users,
                ROUND(AVG(SIZE(products_viewed)), 1) as avg_products_per_user,
                ROUND(MAX(SIZE(products_viewed)), 0) as max_products_viewed
            FROM user_products
        """)
        logger.info("Product Affinity Results:")
        result.show(truncate=False)
        logger.info("Product Affinity Analysis completed")
        return result
    except Exception as e:
        logger.error(f"Product Affinity Analysis failed: {str(e)}", exc_info=True)
        return None


def query_churn_risk(spark, logger):
    """Churn Risk Analysis: Who's at risk of not returning?"""
    logger.info("\n" + "=" * 80)
    logger.info("ADVANCED QUERY 5: CHURN RISK IDENTIFICATION")
    logger.info("=" * 80)
    try:
        result = spark.sql("""
            WITH user_activity AS (
                SELECT
                    user_id,
                    DATEDIFF(CURRENT_DATE, MAX(timestamp)) as days_since_activity,
                    COUNT(*) as total_events,
                    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as purchases
                FROM events
                GROUP BY user_id
            ),
            total_customers AS (
                SELECT COUNT(*) as total_count FROM user_activity
            )
            SELECT
                CASE
                    WHEN days_since_activity <= 7  THEN 'Active'
                    WHEN days_since_activity <= 30 THEN 'At Risk'
                    WHEN days_since_activity <= 90 THEN 'High Risk'
                    ELSE 'Churned'
                END as churn_status,
                COUNT(*) as customer_count,
                ROUND(COUNT(*) * 100.0 / t.total_count, 2) as pct_of_customers,
                ROUND(AVG(days_since_activity), 1) as avg_days_inactive,
                ROUND(AVG(total_events), 1) as avg_lifetime_events,
                ROUND(AVG(purchases), 1) as avg_lifetime_purchases
            FROM user_activity
            CROSS JOIN total_customers t
            GROUP BY churn_status, t.total_count
            ORDER BY avg_days_inactive DESC
        """)
        logger.info("Churn Risk Results:")
        result.show(truncate=False)
        logger.info("Churn Risk Analysis completed")
        return result
    except Exception as e:
        logger.error(f"Churn Risk Analysis failed: {str(e)}", exc_info=True)
        return None


def main():
    logger = setup_logger("advanced_analytics")
    spark = None
    try:
        logger.info("=" * 80)
        logger.info("ADVANCED ANALYTICS - DEEP CUSTOMER INSIGHTS")
        logger.info("=" * 80)

        spark = get_spark(app_name="advanced_analytics")
        gold_path = str(project_root / "data" / "gold")
        logger.info(f"Gold path: {gold_path}\n")

        # Setup tables
        setup_tables(spark, gold_path, logger)

        # Run advanced queries
        results = {}
        results['rfm']      = query_rfm_analysis(spark, logger)
        results['sessions'] = query_session_metrics(spark, logger)
        results['hourly']   = query_hourly_patterns(spark, logger)
        results['affinity'] = query_product_cross_sell(spark, logger)
        results['churn']    = query_churn_risk(spark, logger)

        logger.info("\n" + "=" * 80)
        logger.info("ALL ADVANCED ANALYSES COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)

    except Exception as e:
        logger.error("=" * 80)
        logger.error("ADVANCED ANALYTICS FAILED")
        logger.error("=" * 80)
        logger.error(f"Error: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            logger.info("Stopping Spark session...")
            spark.stop()
            logger.info("Done!")


if __name__ == "__main__":
    main()