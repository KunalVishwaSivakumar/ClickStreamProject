# Data Dictionary

Complete reference for every table and column in your clickstream pipeline

## Overview

This document describes all data flowing through your pipeline. Start here when you need to understand what a column means or where a piece of data comes from.

The data dictionary is organized by layer, then by table, then by column.

---

## Bronze Layer

The Bronze layer contains raw data as ingested. It has two tables from the same source.

### Table: ecommerce_clickstream_transactions

Raw customer clickstream events, minimally transformed.

**Location**: `data/bronze/ingestion_date=YYYY-MM-DD/`

**Format**: Parquet (Snappy compressed)

**Partitioning**: By ingestion_date

**Row Count**: ~2,000,000

**Columns**:

#### UserID
- **Type**: String
- **Nullable**: Yes (but Bronze includes nulls, Silver removes them)
- **Source**: Event tracking system
- **Description**: Unique identifier for the customer who took the action
- **Example Values**: "user_001", "user_12345", "123456789"
- **Null Percentage**: 0.0025% (about 50 rows)
- **Notes**: This is how we identify which customer did what. If UserID is null, we don't know who acted.

#### SessionID
- **Type**: String
- **Nullable**: Yes
- **Source**: Event tracking system
- **Description**: Unique identifier for the browsing session. Groups all actions from one user during one website visit.
- **Example Values**: "session_001", "session_999", "sess_abcd123"
- **Null Percentage**: 0.001%
- **Notes**: A session starts when user arrives, ends when they leave. One user can have many sessions. All events in one session have same SessionID.

#### Timestamp
- **Type**: String (formatted as YYYY-MM-DD HH:MM:SS.ffffff)
- **Nullable**: No
- **Source**: Event tracking system
- **Description**: The exact moment the action occurred
- **Example Values**: "2024-07-07 18:00:26.959902", "2024-03-05 22:01:00.072000"
- **Format**: ISO 8601 with microseconds
- **Notes**: Stored as string in Bronze. Converted to timestamp in Silver/Gold. Microsecond precision allows ordering of rapid-fire events.

#### EventType
- **Type**: String
- **Nullable**: Yes (but Bronze includes them, Silver removes)
- **Source**: Event tracking system
- **Description**: What the customer did
- **Possible Values**:
  - `page_view`: Viewed any page on website
  - `product_view`: Viewed a specific product page
  - `add_to_cart`: Added item to shopping cart
  - `purchase`: Completed a purchase
  - `search`: Used site search
  - `login`: Logged into account
  - `logout`: Logged out
  - `error`: Error occurred
- **Null Percentage**: 0.001%
- **Notes**: The heart of clickstream analysis. Events are categorized by type. Different event types have different meaning (product_view needs ProductID, purchase needs Amount).

#### ProductID
- **Type**: String
- **Nullable**: Yes
- **Source**: Event tracking system (only populated for product-related events)
- **Description**: Unique identifier for the product related to this event
- **Example Values**: "prod_001", "prod_8199", "SKU_12345"
- **When Populated**: 
  - `product_view`: Always (which product did they view?)
  - `add_to_cart`: Always (what did they add?)
  - `purchase`: Always (what did they buy?)
  - `page_view`: Never (no specific product)
  - `search`: Sometimes (what did they search for? - optional)
  - `login`: Never
  - `logout`: Never
- **Null Percentage**: ~40% (only populated for product-related events)
- **Notes**: Missing ProductID for product-related events means lost context. Silver layer removes these rows.

#### Amount
- **Type**: Double
- **Nullable**: Yes
- **Source**: Payment/billing system
- **Description**: Purchase amount in dollars
- **Range**: $0.01 to $9,999.99
- **When Populated**:
  - `purchase`: Always (how much did they spend?)
  - All other events: Never
- **Null Percentage**: ~99.3% (only purchase events have amounts)
- **Notes**: Revenue analysis depends on accurate amounts. Missing amounts for purchase events mean lost revenue tracking. Silver layer removes these rows.

#### Outcome
- **Type**: String
- **Nullable**: Yes
- **Source**: Event tracking system
- **Description**: Whether the action completed successfully or failed
- **Possible Values**:
  - `success`: Action completed as intended
  - `failure`: Action encountered an error
  - (null): Unknown
- **Distribution**: ~99% success, ~1% failure
- **Notes**: Useful for debugging. A high failure rate on purchases is alarming.

#### ingestion_timestamp (Added by Bronze)
- **Type**: Timestamp
- **Nullable**: No
- **Source**: Bronze ingestion job
- **Description**: When this record was ingested into Bronze layer
- **Example Values**: "2024-01-26 14:00:00", "2024-01-27 02:30:45"
- **Notes**: Records when we received the data, not when the event occurred. Useful for detecting ingestion delays.

#### source_file (Added by Bronze)
- **Type**: String
- **Nullable**: No
- **Source**: Bronze ingestion job
- **Description**: Which file this record came from
- **Example Values**: "ecommerce_clickstream_transactions.csv"
- **Notes**: Allows audit trail back to source files. If multiple sources feed this table, you know where each record originated.

---

## Silver Layer

The Silver layer contains validated, cleaned data. It has one table (the cleaned version of Bronze).

### Table: ecommerce_clickstream_transactions (Silver)

Validated clickstream events with quality enforced.

**Location**: `data/silver/`

**Format**: Parquet (Snappy compressed)

**Partitioning**: By ingestion_date (inherited from Bronze)

**Row Count**: ~1,900,000 (quality filtered)

**Quality Rules Applied**:
1. Removed rows with null UserID (50 rows)
2. Removed rows with null EventType (20 rows)
3. Removed product events without ProductID (150 rows)
4. Removed purchase events without Amount (80 rows)

**Columns**: Same as Bronze

**Key Differences from Bronze**:
- No null UserIDs (all rows have valid customer)
- No null EventTypes (all rows are categorized)
- No product events missing ProductID
- No purchase events missing Amount
- Fewer rows (99.985% of original)

**Usage**: This is the source of truth for analytics. Always use Silver for building reports, dashboards, and models. Never use Bronze for analytics.

**Data Quality Score**: PASS - 99.985% retention, 100% valid for downstream use

---

## Gold Layer

The Gold layer contains optimized data organized in a star schema. It has six tables: one fact table and five dimensions.

### Fact Table: Events

Central table containing every customer interaction.

**Location**: `data/gold/events/`

**Format**: Parquet (Snappy compressed)

**Row Count**: ~1,900,000 (one row per event)

**Columns**:

#### event_id
- **Type**: Long (integer)
- **Nullable**: No
- **Source**: Gold transformation
- **Description**: Unique identifier for this event, auto-generated
- **Range**: 1 to 1,900,000
- **Notes**: Primary key of fact table. Used to join back to original records if needed.

#### user_id
- **Type**: String
- **Nullable**: No
- **Source**: Inherited from Silver UserID column
- **Description**: Foreign key to Users dimension. Which customer took this action?
- **Example Values**: "user_001", "user_12345"
- **Notes**: Join to Users dimension to get customer profile (first_seen, last_seen, total_events)

#### session_id
- **Type**: String
- **Nullable**: No
- **Source**: Inherited from Silver SessionID column
- **Description**: Foreign key to Sessions dimension. Which browsing session?
- **Example Values**: "session_001", "session_999"
- **Notes**: Join to Sessions dimension to get session metrics (start_time, end_time, event_count)

#### product_id
- **Type**: String
- **Nullable**: Yes
- **Source**: Inherited from Silver ProductID column
- **Description**: Foreign key to Products dimension. Which product?
- **Example Values**: "prod_8199", "prod_001"
- **When Populated**: Only for product-related events (product_view, add_to_cart, purchase)
- **Notes**: Join to Products dimension for product details. Null for non-product events (page_view, search, login, logout, error).

#### event_type
- **Type**: String
- **Nullable**: No
- **Source**: Inherited from Silver EventType column
- **Description**: Foreign key to EventTypes dimension. What did the customer do?
- **Possible Values**: page_view, product_view, add_to_cart, purchase, search, login, logout, error
- **Notes**: Join to EventTypes dimension for event catalog. Defines the type of interaction.

#### amount
- **Type**: Double
- **Nullable**: Yes
- **Source**: Inherited from Silver Amount column
- **Description**: Purchase amount in dollars
- **Range**: $0.01 to $9,999.99
- **When Populated**: Only for purchase events
- **Notes**: Null for all non-purchase events. Sum of amount for revenue analysis. Most queries filter to purchase events before using.

#### outcome
- **Type**: String
- **Nullable**: Yes
- **Source**: Inherited from Silver Outcome column
- **Description**: Success or failure indicator
- **Possible Values**: "success", "failure", or null
- **Notes**: Flag for debugging. High failure rates indicate system issues.

#### timestamp
- **Type**: Timestamp
- **Nullable**: No
- **Source**: Inherited from Silver Timestamp (converted from string)
- **Description**: Foreign key to Time dimension. When did this happen?
- **Format**: Timestamp (YYYY-MM-DD HH:MM:SS)
- **Precision**: Second (microseconds dropped)
- **Notes**: Join to Time dimension to get temporal attributes (year, month, day_of_week, is_weekend). Pre-calculated in Time dimension for query performance.

---

### Dimension Table: Users

Customer profiles aggregated from events.

**Location**: `data/gold/users/`

**Format**: Parquet

**Row Count**: ~45,000 (unique customers)

**Columns**:

#### user_id
- **Type**: String
- **Nullable**: No
- **Description**: Unique customer identifier, primary key
- **Example Values**: "user_001", "user_12345"
- **Notes**: Matches user_id in Events fact table. One row per unique customer.

#### first_seen_date
- **Type**: Timestamp
- **Nullable**: No
- **Description**: When this customer first visited our website
- **Example Value**: "2024-01-05 10:23:45"
- **Notes**: Marks customer acquisition date. Useful for cohort analysis. "How many customers acquired last month?"

#### last_seen_date
- **Type**: Timestamp
- **Nullable**: No
- **Description**: When this customer last visited
- **Example Value**: "2024-01-26 14:55:12"
- **Notes**: Indicates recency. If very old, customer is inactive. "Which customers haven't visited in 30 days?"

#### total_events
- **Type**: Long (integer)
- **Nullable**: No
- **Description**: Total number of actions this customer has taken
- **Range**: 1 to tens of thousands
- **Example Values**: 1 (one-time visitor), 47 (regular customer), 1000+ (power user)
- **Notes**: Indicates engagement. Used for segmentation. "Users with more than 20 events are engaged."

**Usage**: Join to Events fact table to filter/group by customer characteristics.

**Example Query**:
```sql
SELECT COUNT(*) as engaged_users
FROM users
WHERE total_events > 20
```

---

### Dimension Table: Sessions

Browsing session profiles aggregated from events.

**Location**: `data/gold/sessions/`

**Format**: Parquet

**Row Count**: ~150,000 (unique sessions)

**Columns**:

#### session_id
- **Type**: String
- **Nullable**: No
- **Description**: Unique session identifier, primary key
- **Example Values**: "session_001", "session_999"
- **Notes**: One row per unique browsing session. Matches session_id in Events fact table.

#### user_id
- **Type**: String
- **Nullable**: No
- **Description**: Foreign key to Users. Which customer had this session?
- **Example Values**: "user_001"
- **Notes**: Join to Users to identify the customer, then to Events to see what they did.

#### session_start_time
- **Type**: Timestamp
- **Nullable**: No
- **Description**: When the session started
- **Example Value**: "2024-01-26 10:00:00"
- **Notes**: First event in this session. Can calculate session duration from this and session_end_time.

#### session_end_time
- **Type**: Timestamp
- **Nullable**: No
- **Description**: When the session ended (last event timestamp)
- **Example Value**: "2024-01-26 10:15:00"
- **Notes**: Last event in this session. Duration = session_end_time - session_start_time. This session was 15 minutes.

#### session_event_count
- **Type**: Long (integer)
- **Nullable**: No
- **Description**: How many actions happened in this session
- **Range**: 1 to hundreds
- **Example Value**: 12 (customer took 12 actions before leaving)
- **Notes**: More events = more engagement. Sessions with 0 purchases may still be valuable (browsing/research).

**Usage**: Understand session patterns. "What's average session duration? Do long sessions lead to purchases?"

**Example Query**:
```sql
SELECT AVG(DATEDIFF(minute, session_start_time, session_end_time)) as avg_session_minutes
FROM sessions
```

---

### Dimension Table: Products

Product catalog.

**Location**: `data/gold/products/`

**Format**: Parquet

**Row Count**: ~5,000 (unique products)

**Columns**:

#### product_id
- **Type**: String
- **Nullable**: No
- **Description**: Unique product identifier, primary key
- **Example Values**: "prod_001", "prod_8199"
- **Notes**: Matches product_id in Events fact table and Sessions. One row per product in catalog.

**Note**: This dimension is minimal (just product_id). In real systems, you'd add: product_name, category, price, supplier, etc. You can extend it anytime by modifying gold_transform.py.

**Usage**: Group events by product. "Which products drive revenue?"

**Example Query**:
```sql
SELECT e.product_id, COUNT(*) as view_count
FROM events e
GROUP BY e.product_id
ORDER BY view_count DESC
LIMIT 10
```

---

### Dimension Table: EventTypes

Event type catalog.

**Location**: `data/gold/event_types/`

**Format**: Parquet

**Row Count**: 8 (fixed list)

**Columns**:

#### event_type
- **Type**: String
- **Nullable**: No
- **Description**: Event type identifier, primary key
- **Possible Values**:
  - `page_view`: Customer viewed any page
  - `product_view`: Customer viewed product details
  - `add_to_cart`: Customer added item to cart
  - `purchase`: Customer completed purchase
  - `search`: Customer used site search
  - `login`: Customer logged in
  - `logout`: Customer logged out
  - `error`: System error occurred
- **Notes**: Static list. Used to categorize and filter events. Join to Events fact table.

**Usage**: Categorical analysis. "What percentage of users who view products actually purchase?"

**Example Query**:
```sql
SELECT 
  event_type,
  COUNT(*) as event_count
FROM events
GROUP BY event_type
ORDER BY event_count DESC
```

---

### Dimension Table: Time

Temporal attributes pre-calculated.

**Location**: `data/gold/time/`

**Format**: Parquet

**Row Count**: ~180 (one per unique date in data)

**Columns**:

#### timestamp
- **Type**: Timestamp
- **Nullable**: No
- **Description**: The date/time, primary key
- **Format**: YYYY-MM-DD HH:MM:SS
- **Example Values**: "2024-01-01 00:00:00", "2024-01-26 18:30:45"
- **Notes**: Matches timestamp in Events fact table. Pre-calculated temporal attributes avoid expensive calculations during queries.

#### year
- **Type**: Integer
- **Nullable**: No
- **Description**: Year of timestamp
- **Range**: 2024
- **Example Value**: 2024
- **Notes**: Filter to specific years. "Events in 2024?"

#### month
- **Type**: Integer
- **Nullable**: No
- **Description**: Month of timestamp (1-12)
- **Range**: 1 to 12
- **Example Values**: 1 (January), 7 (July), 12 (December)
- **Notes**: Monthly analysis. "Which months have highest revenue?"

#### day_of_month
- **Type**: Integer
- **Nullable**: No
- **Description**: Day of month (1-31)
- **Range**: 1 to 31
- **Example Values**: 1, 15, 26
- **Notes**: Filter to specific days. "How many purchases on the 1st of month?" (payday effect)

#### day_of_week
- **Type**: Integer
- **Nullable**: No
- **Description**: Day of week (1=Monday, 7=Sunday)
- **Range**: 1 to 7
- **Mapping**:
  - 1 = Monday
  - 2 = Tuesday
  - 3 = Wednesday
  - 4 = Thursday
  - 5 = Friday
  - 6 = Saturday
  - 7 = Sunday
- **Notes**: Analyze weekday patterns. "Is Friday shopping different from Monday?"

#### is_weekend
- **Type**: Integer
- **Nullable**: No
- **Description**: Is this a weekend day? (1=yes, 0=no)
- **Values**: 0 (weekday Monday-Friday), 1 (weekend Saturday-Sunday)
- **Notes**: Quick weekend/weekday filtering. "How many purchases on weekends vs weekdays?"

**Usage**: Temporal analysis without calculating during queries.

**Example Query**:
```sql
SELECT 
  CASE WHEN t.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END as day_type,
  COUNT(*) as event_count
FROM events e
JOIN time_dim t ON e.timestamp = t.timestamp
GROUP BY t.is_weekend
```

---

## Data Types Reference

Understanding data types helps you write correct queries.

### String
- Customer IDs, event types, product IDs
- Examples: "user_001", "product_view", "prod_8199"
- String comparisons are case-sensitive

### Timestamp
- Dates and times
- Format: YYYY-MM-DD HH:MM:SS
- Precision: Seconds (microseconds dropped in Gold)
- Sortable and comparable

### Double
- Decimal numbers, especially currency
- Range: Very large (from tiny cents to millions)
- Used for amounts ($), percentages, ratios
- Example: 45.99, 0.001, 3000000.00

### Integer / Long
- Whole numbers
- Range: Up to 9 billion (for Long)
- Used for counts, day numbers, year
- Examples: 180 (number of days), 12 (number of months)

---

## Relationships and Joins

How tables connect to each other.

### Events to Users
```
Events.user_id ──────▶ Users.user_id
```
Join to get customer profile (first_seen, last_seen, total_events)

### Events to Sessions
```
Events.session_id ──────▶ Sessions.session_id
```
Join to get session details (duration, event count)

### Events to Products
```
Events.product_id ──────▶ Products.product_id
```
Join to get product information (null for non-product events)

### Events to EventTypes
```
Events.event_type ──────▶ EventTypes.event_type
```
Join to categorize events

### Events to Time
```
Events.timestamp ──────▶ Time.timestamp
```
Join to get temporal attributes (weekday, is_weekend, month, etc.)

---

## Null Value Handling

Understanding nulls is critical.

### In Bronze
Nulls are preserved (shown as-is)

### In Silver
Nulls are removed from key columns (UserID, EventType) and certain fields (ProductID for product events, Amount for purchases)

### In Gold

**NULL in Events table**:
- `product_id`: NULL for non-product events (page_view, search, login, logout, error)
- `amount`: NULL for all non-purchase events
- `outcome`: Sometimes NULL (meaning unknown)

**How to handle in queries**:
```sql
-- Count only purchases (ignore nulls)
SELECT SUM(amount) as total_revenue
FROM events
WHERE event_type = 'purchase' AND amount IS NOT NULL

-- Count all events (including nulls)
SELECT COUNT(*) as all_events
FROM events

-- Count non-null amounts
SELECT COUNT(amount) as events_with_amounts
FROM events
```

---

## Data Quality Metrics

Understanding data quality at each layer.

### Bronze Quality
- Input rows: 2,000,000
- Null UserIDs: 50 (0.0025%)
- Null EventTypes: 20 (0.001%)
- Note: Bronze preserves everything

### Silver Quality
- Input rows: 2,000,000
- Output rows: 1,899,700 (retained 99.985%)
- Rows removed: 300
- Quality Score: PASS
- Note: Silver is source of truth

### Gold Quality
- Events fact: 1,899,700 (matches Silver)
- Unique users: 45,000
- Unique sessions: 150,000
- Unique products: 5,000
- Unique event types: 8
- Unique timestamps: 180
- Note: Gold is optimized for queries

---

## Common Analysis Patterns

How to use these tables for common questions.

### Question: Top 10 Products by Revenue?

```sql
SELECT 
  p.product_id,
  COUNT(*) as purchase_count,
  SUM(e.amount) as total_revenue,
  AVG(e.amount) as avg_order_value
FROM events e
JOIN products p ON e.product_id = p.product_id
WHERE e.event_type = 'purchase'
GROUP BY p.product_id
ORDER BY total_revenue DESC
LIMIT 10
```

### Question: Which Users Haven't Visited in 30 Days?

```sql
SELECT 
  user_id,
  last_seen_date,
  DATEDIFF(day, last_seen_date, CURRENT_DATE) as days_since_visit
FROM users
WHERE DATEDIFF(day, last_seen_date, CURRENT_DATE) > 30
ORDER BY days_since_visit DESC
```

### Question: Conversion Rate by Day of Week?

```sql
SELECT
  t.day_of_week,
  CASE t.day_of_week
    WHEN 1 THEN 'Monday'
    WHEN 2 THEN 'Tuesday'
    -- ... etc
    ELSE 'Sunday'
  END as day_name,
  COUNT(DISTINCT CASE WHEN e.event_type = 'product_view' THEN e.user_id END) as viewers,
  COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN e.user_id END) as purchasers,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN e.user_id END) 
    / COUNT(DISTINCT CASE WHEN e.event_type = 'product_view' THEN e.user_id END), 2) as conversion_pct
FROM events e
JOIN time_dim t ON e.timestamp = t.timestamp
GROUP BY t.day_of_week
ORDER BY t.day_of_week
```

---

## Summary

This data dictionary provides complete reference for your pipeline. When you need to:

- **Understand a column**: Find it here
- **Write a query**: Use the "Common Analysis Patterns" section
- **Debug an issue**: Check the Data Quality Metrics
- **Understand relationships**: See the "Relationships and Joins" section
- **Handle nulls**: Review "Null Value Handling"

Your pipeline contains clean, organized, optimized data. Use this reference to extract maximum value.

---

Last Updated: February 2026
