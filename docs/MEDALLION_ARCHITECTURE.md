# Understanding the Medallion Architecture

A deep dive into why your pipeline uses Bronze, Silver, and Gold layers

## What Is The Medallion Architecture?

The medallion architecture is an organizational design for data lakes developed by Databricks and adopted by Netflix, Amazon, Google, and most enterprise data teams. It separates data processing into three distinct layers, each serving a specific purpose.

You can think of it as a quality control system. Raw materials arrive. They're inspected. They're refined. Finally, they're optimized for use.

In manufacturing, this looks like:

```
Raw materials warehouse → Quality inspection → Finished goods
      (Bronze)                (Silver)              (Gold)
```

In data engineering, it looks like:

```
Raw customer events → Data validation → Analytics optimized
      (Bronze)           (Silver)              (Gold)
```

Each layer is independent but connected. Each can be understood in isolation. Together, they create a system that's auditable, flexible, and scalable.

---

## Bronze Layer: The Safety Net

### Purpose

The Bronze layer is your safety net. It captures data exactly as it arrives from the source, with absolutely minimal transformation. Its job is to preserve the truth about what happened.

Think of Bronze as your audit trail. If someone asks, "What did the raw data actually say?", you point to Bronze and provide an unquestionable answer.

### Design Principles

**Immutability**: Bronze data never changes. Once written, it stays as-is forever. This gives you an auditable record of what came in.

**Minimal Processing**: We add only metadata (ingestion timestamp, source file), nothing else. No cleaning. No filtering. No business logic.

**Partitioning**: We partition by ingestion_date so new data is isolated from old data. This enables:
- Incremental processing (only process new data)
- Data retention (archive old data independently)
- Query optimization (skip irrelevant date ranges)

**Format**: We use Parquet (columnar, compressed) instead of CSV because:
- Smaller file sizes (500MB CSV → 50MB Parquet)
- Faster reads (only read needed columns)
- Schema enforcement (catches type issues)

### What Bronze Answers

If anything goes wrong downstream, Bronze answers:
- What was the raw data exactly?
- When did we receive it?
- Which file did it come from?
- How many rows arrived?
- What nulls existed?

This traceability is invaluable.

### When You Use Bronze

Bronze is not for analytics. It's for debugging, auditing, and replay. Use Bronze when:
- You need to understand what the original data said
- A calculation downstream seems wrong (replay from Bronze)
- You're investigating data quality issues
- You're building new analytics (they need clean source)

### Example: Bronze in Action

Data arrives at 2PM. Your ingestion system produces a CSV with 2,000,000 rows.

```
Raw CSV
├─ File size: 500 MB
├─ Rows: 2,000,000
├─ Columns: 7 (UserID, SessionID, Timestamp, EventType, ProductID, Amount, Outcome)
└─ Quality: Mixed (some nulls, some odd values)

Bronze Ingestion Job Runs
├─ Reads CSV (no schema validation yet)
├─ Adds ingestion_timestamp = 2024-01-26 14:00:00
├─ Adds source_file = "ecommerce_clickstream_transactions.csv"
└─ Writes to data/bronze/ingestion_date=2024-01-26/

Bronze Output
├─ File size: 50 MB (10x compression)
├─ Rows: 2,000,000 (unchanged)
├─ Columns: 9 (original 7 + 2 metadata)
└─ Quality: Same as input (no validation applied)

Six months later, you discover a bug in calculations
├─ You go to Bronze
├─ Re-process from that date
├─ Recalculate everything with the fixed logic
├─ Problem solved without loss of historical data
```

---

## Silver Layer: The Quality Gate

### Purpose

The Silver layer is your quality gate. It's where you enforce the rules about what constitutes good data for your business.

Silver says: "For data to flow downstream to analytics, it must satisfy these business rules."

This prevents garbage data from contaminating all downstream systems.

### Design Principles

**Validation**: We enforce business rules. Missing required fields are removed. Invalid combinations are flagged. Data type mismatches are fixed.

**Consistency**: Rules are documented and enforced consistently. Every row either passes all rules or is removed.

**Traceability**: We track what failed validation so you know data quality issues.

**Non-Destructive of Source**: While we remove bad rows, the source (Bronze) is unchanged. If rules change, you can re-process from Bronze.

### Your Validation Rules

Your Silver layer enforces four rules:

**Rule 1: Every row must have a UserID**

Why? You can't analyze customer behavior without knowing which customer. Rows without UserID are either data entry errors or incomplete tracking.

```
Remove if UserID is null
└─ These 50 rows are useless (which user?)
```

**Rule 2: Every row must have an EventType**

Why? You can't categorize what customers did without knowing what they did. 

```
Remove if EventType is null
└─ These 20 rows tell us nothing
```

**Rule 3: Product-related events must reference a Product**

Why? If a customer "added to cart", there must be a product added. If we don't know which product, we can't analyze product performance.

```
Events with EventType in [product_view, add_to_cart, purchase]
│
├─ Must have ProductID
├─ If ProductID is null
│  └─ Remove (which product were they looking at?)
│
└─ These 150 rows fail the rule
```

**Rule 4: Purchase events must have an Amount**

Why? Revenue analysis requires knowing how much customers spent. A purchase without an amount is useless for revenue reporting.

```
Events with EventType = purchase
│
├─ Must have Amount > 0
├─ If Amount is null or 0
│  └─ Remove (how much did they spend?)
│
└─ These 80 rows fail the rule
```

### Data Quality Metrics

After applying all rules, you see:

```
Input:  2,000,000 rows
├─ Null UserID: 50 rows (removed)
├─ Null EventType: 20 rows (removed)
├─ Product event without ProductID: 150 rows (removed)
└─ Purchase without Amount: 80 rows (removed)

Total removed: 300 rows
Output: 1,999,700 rows (99.985% quality)

Quality Score: PASS
```

### When You Use Silver

Silver is the source of truth for analytics. Use Silver when:
- Building analytics dashboards
- Creating machine learning datasets
- Feeding data warehouses
- Anything requiring consistent, validated data

Never use Bronze for analytics. Always use Silver.

### When Rules Change

Real companies change rules constantly. Your business department might decide:

"We want to include page_view events even if they don't have a product" - Silver logic changes, Gold is rebuilt.

"We're starting to track new event types" - Add validation rules, re-process Silver.

"Marketing wants nulls in Amount filled with $0" - Adjust rule, re-run.

The medallion architecture makes this change natural:

```
Old Rule:
Remove all events without ProductID

New Rule:
Keep page_view events even without ProductID

How to implement:
1. Update silver_transform.py logic
2. Run silver_transform.py (consumes Bronze, produces new Silver)
3. Run gold_transform.py (consumes new Silver, produces new Gold)
4. Run analytics.py (uses new Gold, produces new results)

Bronze never changes. Changes cascade from Silver forward.
```

---

## Gold Layer: The Performance Foundation

### Purpose

The Gold layer is optimized for analytics. It reorganizes validated Silver data into a star schema that makes queries fast and simple.

Think of Gold as a specialized library. Books are organized by subject for browsing. Each book has an index for quick lookup. You can find what you need instantly.

Raw Silver data is like a pile of 1.9 million unorganized event logs. Your query needs to search through all of them linearly. Gold is like an organized library where you can find what you need in seconds.

### Design Principles

**Denormalization**: In a traditional database, you'd normalize everything to eliminate data repetition. Gold does the opposite. It duplicates carefully to enable fast queries.

**Dimensional Modeling**: Gold uses the star schema pattern with dimensions and a central fact table.

**Pre-Calculation**: Expensive calculations (min/max dates, distinct counts) are pre-computed once during loading, not during queries.

**Aggregation-Friendly**: The structure naturally supports GROUP BY queries that business analysts need.

### Your Star Schema

Your Gold schema has six tables:

**The Fact Table: Events**

The Events table is the center of the star. Every customer interaction is a row.

```
Events Table (1,899,700 rows)
├─ event_id: Unique identifier for this event (1 to 1,899,700)
├─ user_id: Which user did this (FK to Users)
├─ session_id: Which session (FK to Sessions)
├─ product_id: Which product (FK to Products, may be null)
├─ event_type: What they did (FK to EventTypes)
├─ amount: Money (null unless purchase)
├─ outcome: Success or failure
└─ timestamp: When it happened (FK to Time)

This is where all the raw interactions live.
It's also the smallest table (just IDs and amounts, no long strings).
```

**Dimension 1: Users**

The Users dimension profiles customers.

```
Users Dimension (45,000 rows)
├─ user_id: Unique customer ID
├─ first_seen_date: First interaction ever
├─ last_seen_date: Most recent interaction
└─ total_events: How many actions they took

Example:
user_id=123
├─ first_seen = 2024-01-15 (when they first visited)
├─ last_seen = 2024-01-26 (when they last visited)
└─ total_events = 47 (they took 47 actions total)
```

Business use: "Get me all users who visited more than 10 times"

```
SELECT user_id
FROM users
WHERE total_events > 10
```

That's instant. No scanning 1.9M rows. Just check 45K profiles.

**Dimension 2: Sessions**

The Sessions dimension profiles browsing sessions.

```
Sessions Dimension (150,000 rows)
├─ session_id: Unique session identifier
├─ user_id: Which user (FK to Users)
├─ session_start_time: When they started
├─ session_end_time: When they finished
└─ session_event_count: Actions in this session

Example:
session_id=5001
├─ user_id=123
├─ session_start_time=2024-01-26 10:00:00
├─ session_end_time=2024-01-26 10:15:00 (15 minute visit)
└─ session_event_count=12 (they took 12 actions)
```

Business use: "What's the average session length?" 

```
SELECT AVG(session_end_time - session_start_time) as avg_session_minutes
FROM sessions
```

Pre-calculated in the dimension, not computed from 1.9M events.

**Dimension 3: Products**

The Products dimension catalogs your inventory.

```
Products Dimension (5,000 rows)
├─ product_id: Unique product identifier
└─ (additional columns you might add: product_name, category, price)

This is small and easy to join.
```

Business use: "Which of our 5,000 products get the most views?"

```
SELECT e.product_id, COUNT(*) as view_count
FROM events e
JOIN products p ON e.product_id = p.product_id
WHERE e.event_type = 'product_view'
GROUP BY e.product_id
ORDER BY view_count DESC
```

5,000 products, not 1.9M events.

**Dimension 4: EventTypes**

The EventTypes dimension is your event catalog.

```
EventTypes Dimension (8 rows)
├─ event_type: page_view
├─ event_type: product_view
├─ event_type: add_to_cart
├─ event_type: purchase
├─ event_type: search
├─ event_type: login
├─ event_type: logout
└─ event_type: error
```

Business use: "Count events by type"

```
SELECT e.event_type, COUNT(*) as count
FROM events e
GROUP BY e.event_type
```

You're grouping by 8 event types, not scanning 1.9M rows.

**Dimension 5: Time**

The Time dimension captures temporal attributes.

```
Time Dimension (180 rows - one per day)
├─ timestamp: 2024-01-01 00:00:00
├─ year: 2024
├─ month: 1
├─ day_of_month: 1
├─ day_of_week: 1 (Monday)
└─ is_weekend: 0
```

Why pre-calculate this?

In raw data, asking "how many purchases on weekends?" requires:
1. Parse timestamp
2. Calculate day of week
3. Filter to weekends
4. Count
5. Repeat for 1.9M rows

With Time dimension:

```
SELECT COUNT(*)
FROM events e
JOIN time_dim t ON e.timestamp = t.timestamp
WHERE t.is_weekend = 1 AND e.event_type = 'purchase'
```

The calculation is pre-done. You just filter and count.

### Why This Design Matters

Let's compare three approaches to: "How many weekend purchases?"

**Approach 1: Raw CSV (Bad)**
```sql
SELECT COUNT(*)
FROM raw_events
WHERE EventType = 'purchase'
AND DAYOFWEEK(Timestamp) IN (1, 7)
```

Problems:
- Parse timestamp on 1.9M rows
- Calculate day of week on 1.9M rows
- Filter then count
- Takes seconds

**Approach 2: Silver Table (Better)**
```sql
SELECT COUNT(*)
FROM silver_events
WHERE EventType = 'purchase'
AND DAYOFWEEK(Timestamp) IN (1, 7)
```

Same query, but Silver has validation. Still slow because you're calculating day_of_week on 1.9M rows.

**Approach 3: Gold Star Schema (Best)**
```sql
SELECT COUNT(*)
FROM events e
JOIN time_dim t ON e.timestamp = t.timestamp
WHERE e.event_type = 'purchase'
AND t.is_weekend = 1
```

Why faster:
- No calculation (is_weekend already computed)
- time_dim is 180 rows (vs 1.9M)
- Join is trivial
- Returns in milliseconds

The star schema pre-calculates expensive operations so queries are fast.

---

## The Three Layers in Context: A Real Example

Let's trace a real business question through all three layers.

**Business Question**: "What's our conversion rate on weekends vs weekdays?"

**Step 1: Why This Question Matters**

Marketing wants to know if they should run weekend campaigns. If weekend conversion is much lower, they should focus on weekdays.

**Step 2: Navigate to Gold**

This question requires accurate data (that's why Silver validated it) and fast answers (that's why Gold optimized it).

If someone asks about raw Bronze data: "These are the raw events - you'll have to clean them yourself."

**Step 3: Write the Query**

```sql
WITH weekend_purchases AS (
  SELECT 
    CASE WHEN t.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END as day_type,
    COUNT(DISTINCT CASE WHEN e.event_type = 'product_view' THEN e.user_id END) as viewers,
    COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN e.user_id END) as purchasers
  FROM events e
  JOIN time_dim t ON e.timestamp = t.timestamp
  GROUP BY t.is_weekend
)
SELECT 
  day_type,
  viewers,
  purchasers,
  ROUND(100.0 * purchasers / viewers, 2) as conversion_rate_pct
FROM weekend_purchases
```

**Step 4: Results**

```
Day Type   Viewers   Purchasers   Conversion Rate
Weekday    100,000   30,000       30%
Weekend    60,000    21,000       35%
```

**What This Tells You**:

Weekend conversion is actually higher (35% vs 30%). Marketing should absolutely run weekend campaigns.

**How We Got Here**:

- Bronze: Preserved the raw data exactly
- Silver: Validated it was good enough to use
- Gold: Pre-organized it so we could answer in milliseconds

Each layer did its job.

---

## Comparison: With and Without Medallion Architecture

### Without Medallion (Single Layer, Naive Approach)

```
Raw Data → [Everything: Ingest, Clean, Transform, Optimize, Analyze]
                             ↓
                       One Big Job

Problems:
├─ Can't debug (where did it break?)
├─ Can't replay (what was the original data?)
├─ Can't change rules (everything breaks)
├─ Can't reuse data (built for one purpose)
└─ Not scalable (one job doing everything)

Example: You notice purchases are missing amounts.
├─ Was it in the raw data? (check raw files... no logs)
├─ Was it removed in cleaning? (check code... commented out)
├─ Was it lost in transformation? (check SQL... unclear)
└─ Result: Spend 2 hours debugging instead of checking Bronze
```

### With Medallion (Three Layers, Organized)

```
Raw Data → [Bronze: Ingest] → [Silver: Clean] → [Gold: Optimize] → [Analyze]

Benefits:
├─ Debug easily (check each layer's output)
├─ Replay easily (re-run from Bronze forward)
├─ Change rules easily (update Silver, rebuild Gold)
├─ Reuse data (different analytics paths from Gold)
└─ Scale easily (each layer can be parallelized)

Example: You notice purchases are missing amounts.
├─ Check Silver output: "50 rows removed due to missing Amount"
├─ Check Bronze original data: "Amount column exists, has nulls"
├─ Root cause found in 30 seconds (null handling rule)
└─ Result: Fix the rule, re-run Silver, done
```

---

## When to Use Each Layer

### Use Bronze When

- Debugging: Something seems wrong - check raw data first
- Auditing: "Prove this data really arrived"
- Replaying: Business logic changed - recalculate from source
- Creating new analytics: Build from validated source
- Investigating quality issues: What did the original say?

Bronze is your safety net.

### Use Silver When

- Building dashboards: Use validated data
- Training models: Use clean, consistent data
- Feeding warehouses: Send high-quality data only
- General analytics: Source of truth for clean data

Silver is your operational layer.

### Use Gold When

- Running analytical queries: It's optimized for this
- Creating reports: Data is organized intuitively
- Building BI dashboards: Star schema is natural
- Answering business questions: Design is built for this

Gold is your analytical layer.

---

## Key Advantages of Medallion

### 1. Auditability

You can trace any result back to source.

```
Result: "Product X has 1,000 views"
├─ Check Gold: "1,000 rows for this product"
├─ Check Silver: "1,000 rows passed validation"
├─ Check Bronze: "1,100 rows in original (100 failed validation)"
├─ Conclusion: Result is correct, 100 rows were rejected for cause
```

### 2. Flexibility

Business rules change. You're never locked in.

```
Old rule: Remove all events without ProductID
├─ Problem: Losing data for page_view and search events
├─ New rule: Only remove nulls for product-specific events

Solution: Update Silver logic, re-run from Bronze
└─ One day to change globally
```

### 3. Reusability

Different analytics can share Gold layer.

```
Fact Table:
├─ Marketing dashboard uses Events fact + Time dimension
├─ Revenue team uses Events fact + Products dimension
├─ Operations uses Sessions dimension
└─ All reading from same Gold layer

Single source of truth.
```

### 4. Scalability

Each layer processes independently. Can parallelize.

```
Day 1: Run Bronze ingestion for 10 new days of data
Day 2: Run Silver transformation (1 hour)
Day 3: Run Gold transformation (1 hour)
Day 4: Run analytics (30 seconds)

If scaled to billion-row dataset:
├─ Bronze: 1 hour (read CSV, write Parquet)
├─ Silver: 2 hours (validation on huge dataset)
├─ Gold: 3 hours (dimensional building)
└─ Analytics: 1 minute (queries are pre-optimized)

Each layer is independently scalable.
```

### 5. Cost Efficiency

Cloud resources (AWS, GCP, Azure) become cheaper with medallion.

```
Without medallion: Re-process everything daily
├─ Read raw files (expensive)
├─ Process everything again
├─ Result: High AWS bill

With medallion: Incremental processing
├─ Read only new data
├─ Append to Bronze
├─ Process incremental changes Silver/Gold
└─ Result: 70% lower bill
```

---

## How Medallion Works in the Cloud

Your local medallion architecture scales identically in the cloud:

```
Local:                          AWS:
──────                          ────

CSV → Bronze                    S3 → Kinesis → Bronze (S3)
      ↓                                              ↓
    Silver    ──────────→                        AWS Glue (Silver)
      ↓                                              ↓
     Gold     ──────────→                    Amazon Redshift (Gold)
      ↓                                              ↓
  Analytics   ──────────→                  Athena + QuickSight
```

The medallion pattern is universal. The implementation changes. The architecture stays the same.

---

## Summary

The medallion architecture is three layers, each with a purpose:

**Bronze**: Preserve. Keep raw data unchanged for audit and replay.

**Silver**: Validate. Enforce business rules so only good data flows downstream.

**Gold**: Optimize. Reorganize validated data for fast, simple analytics.

This simple pattern enables:
- Auditable data systems
- Flexible rule changes
- Reusable datasets
- Scalable pipelines
- Cost-efficient operations

It's why Netflix uses it. Why Amazon uses it. Why Databricks made it their architecture.

And it's why you should use it.

---

Next, read DATA_DICTIONARY.md to understand exactly what data lives in each table.
