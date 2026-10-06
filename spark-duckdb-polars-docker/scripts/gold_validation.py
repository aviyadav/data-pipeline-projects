"""Gold Layer: Business Logic Quality"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, min as spark_min, expr
import json


def create_customer_segments(spark: SparkSession):
    """Create customer segments for marketing (Gold layer)"""
    
    print("Reading from silver.customers_with_spending...")
    silver_df = spark.read.parquet("/opt/spark/work-dir/silver/customers_with_spending")
    
    print("Applying segmentation logic...")
    gold_df = silver_df.select(
        col("customer_id"),
        col("total_spending"),
        col("order_count"),
        expr("""
            CASE 
                WHEN total_spending > 10000 THEN 'premium'
                WHEN total_spending > 1000 THEN 'standard'
                ELSE 'basic'
            END
        """).alias("segment")
    )
    
    return gold_df


def validate_business_rules(gold_df, table_name="gold.customer_segments"):
    """Validate business rules for Gold layer"""
    print(f"\nValidating business rules for {table_name}...")
    
    failures = []
    
    # Rule 1: Premium customers should have at least 5 orders
    premium_min_orders = gold_df.filter(col("segment") == "premium") \
                                .agg(spark_min("order_count")) \
                                .collect()[0][0]
    
    rule1_passed = premium_min_orders >= 5 if premium_min_orders is not None else False
    if not rule1_passed:
        failures.append({
            "rule": "premium_has_min_order_count",
            "message": f"Premium customers should have at least 5 orders. Found min: {premium_min_orders}"
        })
    
    # Rule 2: Premium segment should be < 20% of customer base
    total_customers = gold_df.count()
    premium_customers = gold_df.filter(col("segment") == "premium").count()
    premium_ratio = premium_customers / total_customers if total_customers > 0 else 0
    
    rule2_passed = premium_ratio < 0.2
    if not rule2_passed:
        failures.append({
            "rule": "segment_distribution_reasonable",
            "message": f"Premium segment should be < 20%. Current: {premium_ratio*100:.1f}%"
        })
    
    # Rule 3: No null segments
    null_segments = gold_df.filter(col("segment").isNull()).count()
    
    rule3_passed = null_segments == 0
    if not rule3_passed:
        failures.append({
            "rule": "no_null_segments",
            "message": f"Found {null_segments} customers with null segments"
        })
    
    print(f"  premium_has_min_order_count: {'PASSED' if rule1_passed else 'FAILED'}")
    print(f"  segment_distribution_reasonable: {'PASSED' if rule2_passed else 'FAILED'}")
    print(f"  no_null_segments: {'PASSED' if rule3_passed else 'FAILED'}")
    
    if failures:
        print(f"\nFAILURES DETECTED:")
        for failure in failures:
            print(f"  - {failure['rule']}: {failure['message']}")
    
    return len(failures) == 0, failures