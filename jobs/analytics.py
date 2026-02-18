from pathlib import Path
import sys
from pyspark.sql import functions as F

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from utils.spark_session import get_spark


def setup_tables(spark, gold_path):
    """Register gold layer tables as temporary views for SQL queries."""
    
    print("Loading gold layer tables...")
    
    # Read fact and dimensions
    events = spark.read.parquet(f"{gold_path}/events")
    users = spark.read.parquet(f"{gold_path}/users")
    products = spark.read.parquet(f"{gold_path}/products")
    event_types = spark.read.parquet(f"{gold_path}/event_types")
    time_dim = spark.read.parquet(f"{gold_path}/time")
    
    # Register as temp views for SQL
    events.createOrReplaceTempView("events")
    users.createOrReplaceTempView("users")
    products.createOrReplaceTempView("products")
    event_types.createOrReplaceTempView("event_types")
    time_dim.createOrReplaceTempView("time_dim")
    
    return events, users, products, event_types, time_dim


def query_top_products(spark):
    """
    Query 1: Which products get the most views?
    
    Business question: "What are our bestselling products?"
    Answer: Look at product_view events, count by product
    """
    print("\n" + "="*70)
    print("QUERY 1: TOP 10 MOST VIEWED PRODUCTS")
    print("="*70)
    
    result = spark.sql("""
        SELECT 
            e.product_id,
            COUNT(*) as view_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct_of_all_views,
            COUNT(CASE WHEN e.event_type = 'add_to_cart' THEN 1 END) as add_to_cart_count,
            COUNT(CASE WHEN e.event_type = 'purchase' THEN 1 END) as purchase_count
        FROM events e
        WHERE e.event_type = 'product_view'
        GROUP BY e.product_id
        ORDER BY view_count DESC
        LIMIT 10
    """)
    
    result.show(truncate=False)
    return result


def query_conversion_funnel(spark):
    """
    Query 2: What's the conversion funnel?
    
    Business question: "How many users complete the purchase journey?"
    Answer: Track users through view → cart → purchase
    
    This is critical for ecommerce - shows where you're losing customers.
    """
    print("\n" + "="*70)
    print("QUERY 2: CONVERSION FUNNEL (View → Cart → Purchase)")
    print("="*70)
    
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
    
    result.show(truncate=False)
    return result


def query_daily_active_users(spark):
    """
    Query 3: How many users are active each day?
    
    Business question: "Is traffic growing or shrinking?"
    Answer: Count unique users per day
    
    Why use time_dim? To quickly filter weekdays vs weekends without
    recalculating day-of-week from timestamp.
    """
    print("\n" + "="*70)
    print("QUERY 3: DAILY ACTIVE USERS & TOTAL EVENTS")
    print("="*70)
    
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
        LIMIT 14  -- Last 2 weeks
    """)
    
    result.show(truncate=False)
    return result


def query_user_segmentation(spark):
    """
    Query 4: Segment users by activity level
    
    Business question: "Who are our loyal users vs one-time visitors?"
    Answer: Classify users by number of events
    """
    print("\n" + "="*70)
    print("QUERY 4: USER SEGMENTATION BY ACTIVITY")
    print("="*70)
    
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
        )
        SELECT 
            CASE 
                WHEN event_count = 0 THEN 'Never Active'
                WHEN event_count <= 5 THEN 'Low Activity (1-5 events)'
                WHEN event_count <= 20 THEN 'Medium Activity (6-20 events)'
                ELSE 'High Activity (20+ events)'
            END as user_segment,
            COUNT(*) as user_count,
            ROUND(COUNT(*) * 100.0 / COUNT(*) OVER (), 2) as pct_of_users,
            ROUND(AVG(event_count), 2) as avg_events_per_user,
            ROUND(AVG(purchases), 2) as avg_purchases,
            ROUND(AVG(total_spent), 2) as avg_spent
        FROM user_activity
        GROUP BY user_segment
        ORDER BY user_count DESC
    """)
    
    result.show(truncate=False)
    return result


def query_purchase_metrics(spark):
    """
    Query 5: Key purchase metrics
    
    Business question: "How much revenue are we making and from whom?"
    Answer: Revenue, AOV (average order value), repeat purchasers
    """
    print("\n" + "="*70)
    print("QUERY 5: REVENUE & PURCHASE METRICS")
    print("="*70)
    
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
    
    result.show(truncate=False)
    return result


def query_user_journey(spark):
    """
    Query 6: User journey - which product are users viewing before purchasing?
    
    Business question: "What's the path users take to purchase?"
    Answer: Show sequence of events for users who made a purchase
    
    This is more complex - shows ordering/sequencing in your data.
    """
    print("\n" + "="*70)
    print("QUERY 6: USER JOURNEY SAMPLE (Users who purchased)")
    print("="*70)
    
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
        LIMIT 20  -- Show first 20 events from purchasing users
    """)
    
    result.show(truncate=False)
    return result


def query_weekday_vs_weekend(spark):
    """
    Query 7: Behavioral differences between weekdays and weekends
    
    Business question: "Are there different patterns on weekends?"
    Answer: Compare event counts, conversion, AOV by day type
    """
    print("\n" + "="*70)
    print("QUERY 7: WEEKDAY VS WEEKEND BEHAVIOR")
    print("="*70)
    
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
    
    result.show(truncate=False)
    return result


def main():
    """Run all analytical queries."""
    
    print("\n")
    print("█"*70)
    print("█" + " "*68 + "█")
    print("█" + "  GOLD LAYER ANALYTICAL QUERIES - CLICKSTREAM ANALYSIS".center(68) + "█")
    print("█" + " "*68 + "█")
    print("█"*70)
    
    spark = get_spark(app_name="gold_layer_analysis")
    
    # Define gold path
    gold_path = str(project_root / "data" / "gold")
    
    # Setup tables
    setup_tables(spark, gold_path)
    
    # Run all queries
    try:
        query_top_products(spark)
        query_conversion_funnel(spark)
        query_daily_active_users(spark)
        query_user_segmentation(spark)
        query_purchase_metrics(spark)
        query_user_journey(spark)
        query_weekday_vs_weekend(spark)
        
        print("\n" + "█"*70)
        print("█ ALL QUERIES COMPLETED SUCCESSFULLY".ljust(69) + "█")
        print("█"*70 + "\n")
        
    except Exception as e:
        print(f"\n❌ ERROR running queries: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()




