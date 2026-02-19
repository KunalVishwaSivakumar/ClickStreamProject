# jobs/analytics.py
# Analytics Layer: Business Intelligence Queries
# This job runs 7 key business queries on the Gold layer to extract insights.

from pathlib import Path
import sys
from pyspark.sql import functions as F

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from utils.spark_session import get_spark
from utils.logger import setup_logger


def setup_tables(spark, gold_path, logger):
    """Register gold layer tables as temporary views for SQL queries."""
    logger.info("Loading gold layer tables...")
    
    try:
        events = spark.read.parquet(f"{gold_path}/events")
        users = spark.read.parquet(f"{gold_path}/users")
        products = spark.read.parquet(f"{gold_path}/products")
        event_types = spark.read.parquet(f"{gold_path}/event_types")
        time_dim = spark.read.parquet(f"{gold_path}/time")
        
        events.createOrReplaceTempView("events")
        users.createOrReplaceTempView("users")
        products.createOrReplaceTempView("products")
        event_types.createOrReplaceTempView("event_types")
        time_dim.createOrReplaceTempView("time_dim")
        
        logger.info("All gold tables loaded and registered as views")
        
        return events, users, products, event_types, time_dim
        
    except Exception as e:
        logger.error(f"Failed to load gold tables: {str(e)}", exc_info=True)
        raise


def query_top_products(spark, logger):
    """Query 1: Top 10 Most Viewed Products"""
    logger.info("")
    logger.info("=" * 80)
    logger.info("QUERY 1: TOP 10 MOST VIEWED PRODUCTS")
    logger.info("=" * 80)
    logger.info("Business Question: Which products drive the most views?")
    logger.info("Strategy: Count product_view events, rank by count")
    logger.info("")
    
    try:
        result = spark.sql("""
            WITH product_stats AS (
                SELECT 
                    e.product_id,
                    COUNT(CASE WHEN e.event_type = 'product_view' THEN 1 END) as view_count,
                    COUNT(CASE WHEN e.event_type = 'add_to_cart' THEN 1 END) as add_to_cart_count,
                    COUNT(CASE WHEN e.event_type = 'purchase' THEN 1 END) as purchase_count
                FROM events e
                GROUP BY e.product_id
            ),
            total_views AS (
                SELECT SUM(view_count) as total_view_count
                FROM product_stats
            )
            SELECT 
                ps.product_id,
                ps.view_count,
                ROUND(ps.view_count * 100.0 / tv.total_view_count, 2) as pct_of_all_views,
                ps.add_to_cart_count,
                ps.purchase_count,
                ROUND(ps.purchase_count * 100.0 / NULLIF(ps.view_count, 0), 2) as view_to_purchase_pct
            FROM product_stats ps
            CROSS JOIN total_views tv
            ORDER BY ps.view_count DESC
            LIMIT 10
        """)
        
        logger.info("Results:")
        result.show(truncate=False)
        logger.info(f"Query 1 completed successfully")
        return result
        
    except Exception as e:
        logger.error(f"Query 1 failed: {str(e)}", exc_info=True)
        return None


def query_conversion_funnel(spark, logger):
    """Query 2: Conversion Funnel (View -> Cart -> Purchase)"""
    logger.info("")
    logger.info("=" * 80)
    logger.info("QUERY 2: CONVERSION FUNNEL (View -> Cart -> Purchase)")
    logger.info("=" * 80)
    logger.info("Business Question: What % of viewers complete purchase?")
    logger.info("Strategy: Track users through journey stages")
    logger.info("")
    
    try:
        result = spark.sql("""
            WITH user_events AS (
                SELECT 
                    user_id,
                    MAX(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) as viewed,
                    MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) as carted,
                    MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchased
                FROM events
                GROUP BY user_id
            )
            SELECT 
                SUM(viewed) as users_who_viewed,
                SUM(carted) as users_who_carted,
                SUM(purchased) as users_who_purchased,
                ROUND(SUM(carted) * 100.0 / SUM(viewed), 2) as view_to_cart_pct,
                ROUND(SUM(purchased) * 100.0 / SUM(carted), 2) as cart_to_purchase_pct,
                ROUND(SUM(purchased) * 100.0 / SUM(viewed), 2) as overall_conversion_pct
            FROM user_events
        """)
        
        logger.info("Results:")
        result.show(truncate=False)
        logger.info(f"Query 2 completed successfully")
        return result
        
    except Exception as e:
        logger.error(f"Query 2 failed: {str(e)}", exc_info=True)
        return None


def query_daily_active_users(spark, logger):
    """Query 3: Daily Active Users"""
    logger.info("")
    logger.info("=" * 80)
    logger.info("QUERY 3: DAILY ACTIVE USERS & TRENDS")
    logger.info("=" * 80)
    logger.info("Business Question: How is traffic trending?")
    logger.info("Strategy: Count unique users and events per day")
    logger.info("")
    
    try:
        result = spark.sql("""
            SELECT 
                t.timestamp as date,
                t.day_of_week,
                CASE 
                    WHEN t.is_weekend = 1 THEN 'Weekend'
                    ELSE 'Weekday'
                END as day_type,
                COUNT(DISTINCT e.user_id) as unique_users,
                COUNT(*) as total_events,
                ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT e.user_id), 2) as avg_events_per_user
            FROM events e
            JOIN time_dim t ON e.timestamp = t.timestamp
            GROUP BY t.timestamp, t.day_of_week, t.is_weekend
            ORDER BY date DESC
            LIMIT 14
        """)
        
        logger.info("Results (last 2 weeks):")
        result.show(truncate=False)
        logger.info(f"Query 3 completed successfully")
        return result
        
    except Exception as e:
        logger.error(f"Query 3 failed: {str(e)}", exc_info=True)
        return None


def query_user_segmentation(spark, logger):
    """Query 4: User Segmentation by Activity"""
    logger.info("")
    logger.info("=" * 80)
    logger.info("QUERY 4: USER SEGMENTATION BY ACTIVITY LEVEL")
    logger.info("=" * 80)
    logger.info("Business Question: Who are our loyal vs one-time customers?")
    logger.info("Strategy: Classify users by event count")
    logger.info("")
    
    try:
        result = spark.sql("""
            WITH user_activity AS (
                SELECT 
                    u.user_id,
                    COUNT(e.event_id) as event_count,
                    COUNT(DISTINCT e.product_id) as products_viewed,
                    SUM(CASE WHEN e.event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
                    SUM(CASE WHEN e.event_type = 'purchase' THEN e.amount ELSE 0 END) as total_spent
                FROM users u
                LEFT JOIN events e ON u.user_id = e.user_id
                GROUP BY u.user_id
            ),
            total_users AS (
                SELECT COUNT(*) as total FROM user_activity
            )
            SELECT 
                CASE 
                    WHEN event_count = 0 THEN 'Never Active'
                    WHEN event_count <= 5 THEN 'Low Activity (1-5 events)'
                    WHEN event_count <= 20 THEN 'Medium Activity (6-20 events)'
                    ELSE 'High Activity (20+ events)'
                END as user_segment,
                COUNT(*) as user_count,
                ROUND(COUNT(*) * 100.0 / t.total, 2) as pct_of_users,
                ROUND(AVG(event_count), 2) as avg_events_per_user,
                ROUND(AVG(purchases), 2) as avg_purchases,
                ROUND(AVG(total_spent), 2) as avg_spent
            FROM user_activity ua
            CROSS JOIN total_users t
            GROUP BY user_segment, t.total
            ORDER BY user_count DESC
        """)
        
        logger.info("Results:")
        result.show(truncate=False)
        logger.info(f"Query 4 completed successfully")
        return result
        
    except Exception as e:
        logger.error(f"Query 4 failed: {str(e)}", exc_info=True)
        return None


def query_purchase_metrics(spark, logger):
    """Query 5: Revenue & Purchase Metrics"""
    logger.info("")
    logger.info("=" * 80)
    logger.info("QUERY 5: REVENUE & PURCHASE METRICS")
    logger.info("=" * 80)
    logger.info("Business Question: How much revenue? Who's loyal?")
    logger.info("Strategy: Aggregate purchases and identify repeat customers")
    logger.info("")
    
    try:
        result = spark.sql("""
            WITH purchases AS (
                SELECT 
                    user_id,
                    event_id,
                    amount,
                    timestamp
                FROM events
                WHERE event_type = 'purchase' AND amount > 0
            ),
            user_purchases AS (
                SELECT 
                    user_id,
                    COUNT(*) as purchase_count,
                    SUM(amount) as total_spent,
                    AVG(amount) as avg_order_value,
                    MIN(timestamp) as first_purchase_date,
                    MAX(timestamp) as last_purchase_date
                FROM purchases
                GROUP BY user_id
            )
            SELECT 
                COUNT(DISTINCT user_id) as total_purchasers,
                COUNT(DISTINCT CASE WHEN purchase_count > 1 THEN user_id END) as repeat_purchasers,
                ROUND(COUNT(DISTINCT CASE WHEN purchase_count > 1 THEN user_id END) * 100.0 / COUNT(DISTINCT user_id), 2) as repeat_purchase_rate_pct,
                SUM(total_spent) as total_revenue,
                ROUND(SUM(total_spent) / COUNT(DISTINCT user_id), 2) as avg_revenue_per_user,
                ROUND(AVG(avg_order_value), 2) as avg_order_value,
                ROUND(AVG(purchase_count), 2) as avg_purchases_per_user
            FROM user_purchases
        """)
        
        logger.info("Results:")
        result.show(truncate=False)
        logger.info(f"Query 5 completed successfully")
        return result
        
    except Exception as e:
        logger.error(f"Query 5 failed: {str(e)}", exc_info=True)
        return None


def query_user_journey(spark, logger):
    """Query 6: User Journey Sample"""
    logger.info("")
    logger.info("=" * 80)
    logger.info("QUERY 6: USER JOURNEY - SAMPLE OF PURCHASING USERS")
    logger.info("=" * 80)
    logger.info("Business Question: What's the path to purchase?")
    logger.info("Strategy: Show event sequence for users who purchased")
    logger.info("")
    
    try:
        result = spark.sql("""
            WITH purchasers AS (
                SELECT DISTINCT user_id
                FROM events
                WHERE event_type = 'purchase'
            )
            SELECT 
                e.user_id,
                e.event_id,
                e.event_type,
                e.product_id,
                e.amount,
                e.timestamp
            FROM events e
            JOIN purchasers p ON e.user_id = p.user_id
            ORDER BY e.user_id, e.timestamp
            LIMIT 20
        """)
        
        logger.info("Results (first 20 events from purchasing users):")
        result.show(truncate=False)
        logger.info(f"Query 6 completed successfully")
        return result
        
    except Exception as e:
        logger.error(f"Query 6 failed: {str(e)}", exc_info=True)
        return None


def query_weekday_vs_weekend(spark, logger):
    """Query 7: Weekday vs Weekend Behavior"""
    logger.info("")
    logger.info("=" * 80)
    logger.info("QUERY 7: WEEKDAY VS WEEKEND BEHAVIOR")
    logger.info("=" * 80)
    logger.info("Business Question: Do shopping patterns differ by day?")
    logger.info("Strategy: Compare metrics across weekday/weekend")
    logger.info("")
    
    try:
        result = spark.sql("""
            SELECT 
                CASE 
                    WHEN t.is_weekend = 1 THEN 'Weekend'
                    ELSE 'Weekday'
                END as day_type,
                COUNT(DISTINCT e.user_id) as total_users,
                COUNT(*) as total_events,
                ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT e.user_id), 2) as avg_events_per_user,
                COUNT(CASE WHEN e.event_type = 'product_view' THEN 1 END) as product_views,
                COUNT(CASE WHEN e.event_type = 'add_to_cart' THEN 1 END) as add_to_carts,
                COUNT(CASE WHEN e.event_type = 'purchase' THEN 1 END) as purchases,
                ROUND(SUM(CASE WHEN e.event_type = 'purchase' THEN e.amount ELSE 0 END), 2) as revenue,
                ROUND(SUM(CASE WHEN e.event_type = 'purchase' THEN e.amount ELSE 0 END) / COUNT(DISTINCT e.user_id), 2) as revenue_per_user
            FROM events e
            JOIN time_dim t ON e.timestamp = t.timestamp
            GROUP BY t.is_weekend
        """)
        
        logger.info("Results:")
        result.show(truncate=False)
        logger.info(f"Query 7 completed successfully")
        return result
        
    except Exception as e:
        logger.error(f"Query 7 failed: {str(e)}", exc_info=True)
        return None


def main():
    logger = setup_logger("analytics")
    
    try:
        logger.info("=" * 80)
        logger.info("GOLD LAYER ANALYTICAL QUERIES")
        logger.info("=" * 80)
        
        spark = get_spark(app_name="gold_layer_analysis")
        gold_path = str(project_root / "data" / "gold")
        
        logger.info(f"Gold path: {gold_path}")
        logger.info("")
        
        # Setup tables
        setup_tables(spark, gold_path, logger)
        
        # Run all queries
        results = {}
        try:
            results['query_1'] = query_top_products(spark, logger)
            results['query_2'] = query_conversion_funnel(spark, logger)
            results['query_3'] = query_daily_active_users(spark, logger)
            results['query_4'] = query_user_segmentation(spark, logger)
            results['query_5'] = query_purchase_metrics(spark, logger)
            results['query_6'] = query_user_journey(spark, logger)
            results['query_7'] = query_weekday_vs_weekend(spark, logger)
            
            logger.info("")
            logger.info("=" * 80)
            logger.info("ALL 7 QUERIES COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"Error running queries: {str(e)}", exc_info=True)
            raise
    
    except Exception as e:
        logger.error("=" * 80)
        logger.error("ANALYTICS JOB FAILED")
        logger.error("=" * 80)
        logger.error(f"Error: {str(e)}", exc_info=True)
        raise
    
    finally:
        logger.info("Stopping Spark session...")
        spark.stop()
        logger.info("Done!")


if __name__ == "__main__":
    main()
