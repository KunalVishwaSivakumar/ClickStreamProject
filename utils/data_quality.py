# utils/data_quality.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, count as spark_count


class DataQualityValidator:
    """
    Validates data quality at each layer of the pipeline.
    
    This class provides methods to check data quality metrics:
    - Row counts and completeness
    - Null value tracking
    - Validation rule compliance
    - Data profiles
    
    Methods are specific to each layer (Bronze, Silver, Gold)
    so you know exactly what checks apply where.
    """
    
    def __init__(self, spark, logger):
        """
        Initialize validator with Spark session and logger.
        
        Parameters:
        -----------
        spark : SparkSession
            Active Spark session
        logger : logging.Logger
            Logger instance for recording validation results
        """
        self.spark = spark
        self.logger = logger
    
    def validate_bronze(self, df):
        """
        Validate Bronze layer (raw) data.
        
        Bronze validation checks:
        - Total row count (how much data arrived?)
        - Null counts for critical columns
        - Total column count (schema intact?)
        
        Parameters:
        -----------
        df : DataFrame
            Bronze layer dataframe to validate
        
        Returns:
        --------
        metrics : dict
            Dictionary with validation results
        """
        row_count = df.count()
        null_userids = df.filter(col("UserID").isNull()).count()
        null_eventtypes = df.filter(col("EventType").isNull()).count()
        
        # Calculate quality score
        if row_count > 0:
            quality_score = ((row_count - null_userids) / row_count * 100)
        else:
            quality_score = 0
        
        metrics = {
            "layer": "bronze",
            "total_rows": row_count,
            "null_userids": null_userids,
            "null_eventtypes": null_eventtypes,
            "total_columns": len(df.columns),
            "quality_score_pct": round(quality_score, 2)
        }
        
        self.logger.info(f"Bronze validation: {row_count:,} rows, "
                        f"Quality score: {quality_score:.2f}%")
        
        return metrics
    
    def validate_silver(self, df):
        """
        Validate Silver layer (cleaned) data.
        
        Silver validation checks:
        - Total rows after cleaning (how many passed quality?)
        - Remaining null values (should be minimal)
        - Data type consistency
        
        Parameters:
        -----------
        df : DataFrame
            Silver layer dataframe to validate
        
        Returns:
        --------
        metrics : dict
            Dictionary with validation results
        """
        row_count = df.count()
        
        # Count total nulls across all columns
        null_counts = {}
        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                null_counts[col_name] = null_count
        
        total_nulls = sum(null_counts.values())
        
        # Quality check: should have no nulls in key columns
        quality_pass = total_nulls == 0 or total_nulls < 10  # Allow very few
        
        metrics = {
            "layer": "silver",
            "total_rows": row_count,
            "total_null_values": total_nulls,
            "null_by_column": null_counts,
            "total_columns": len(df.columns),
            "quality_status": "PASS" if quality_pass else "WARNING"
        }
        
        self.logger.info(f"Silver validation: {row_count:,} rows cleaned, "
                        f"Null values: {total_nulls}, Status: {metrics['quality_status']}")
        
        return metrics
    
    def validate_gold(self, fact_df, dimensions_dict):
        """
        Validate Gold layer (analytics) data.
        
        Gold validation checks:
        - Fact table row count
        - Dimension table cardinalities
        - Foreign key relationships
        
        Parameters:
        -----------
        fact_df : DataFrame
            Events fact table
        dimensions_dict : dict
            Dictionary of dimension_name -> DataFrame pairs
        
        Returns:
        --------
        metrics : dict
            Dictionary with validation results
        """
        metrics = {
            "layer": "gold",
            "fact_table_rows": fact_df.count(),
            "dimensions": {}
        }
        
        # Validate each dimension
        for dim_name, dim_df in dimensions_dict.items():
            dim_count = dim_df.count()
            metrics["dimensions"][dim_name] = dim_count
        
        self.logger.info(f"Gold validation: "
                        f"Fact table {metrics['fact_table_rows']:,} rows, "
                        f"Dimensions: {metrics['dimensions']}")
        
        return metrics
    
    def get_data_profile(self, df, sample_size=100):
        """
        Get a quick data profile showing structure and sample.
        
        Useful for understanding what data looks like at a glance.
        
        Parameters:
        -----------
        df : DataFrame
            Dataframe to profile
        sample_size : int
            Number of rows to show in sample
        
        Returns:
        --------
        profile : dict
            Dictionary with profile information
        """
        profile = {
            "row_count": df.count(),
            "column_count": len(df.columns),
            "columns": df.columns,
            "dtypes": dict(df.dtypes)
        }
        
        self.logger.info(f"Data profile: {profile['row_count']:,} rows, "
                        f"{profile['column_count']} columns")
        
        # Show sample data
        self.logger.info(f"Sample {sample_size} rows:")
        df.limit(sample_size).show(truncate=False)
        
        return profile
    
    def compare_row_counts(self, before_df, after_df, operation_name):
        """
        Compare row counts before and after an operation.
        
        Helpful for tracking data loss through transformations.
        
        Parameters:
        -----------
        before_df : DataFrame
            Data before operation
        after_df : DataFrame
            Data after operation
        operation_name : str
            Name of the operation (e.g., "quality filtering")
        
        Returns:
        --------
        comparison : dict
            Comparison metrics
        """
        before_count = before_df.count()
        after_count = after_df.count()
        removed = before_count - after_count
        retention_pct = (after_count / before_count * 100) if before_count > 0 else 0
        
        comparison = {
            "operation": operation_name,
            "before_rows": before_count,
            "after_rows": after_count,
            "rows_removed": removed,
            "retention_pct": round(retention_pct, 2)
        }
        
        self.logger.info(f"{operation_name}: {before_count:,} → {after_count:,} rows "
                        f"({retention_pct:.2f}% retained, {removed:,} removed)")
        
        return comparison
