"""Main Pipeline: Orchestrates Bronze -> Silver -> Gold with quality checks"""

from pyspark.sql import SparkSession
import sys
import os

sys.path.append("/opt/spark/work-dir/scripts")

from bronze_validator import SourceQualityValidator
from silver_transformation import transform_bronze_to_silver, validate_silver_quality
from gold_validation import create_customer_segments, validate_business_rules


def create_sample_data(spark):
    """Create sample customer data for testing"""
    
    print("Creating sample customer data...")
    
    data = [
        ("C001", "john@example.com", "NA", "2024-01-15 10:30:00", 15000, 8),
        ("C002", "jane@example.com", "EU", "2024-02-20 14:45:00", 5000, 3),
        ("C003", "bob@example.com", "APAC", "2024-03-10 09:15:00", 500, 1),
        ("C004", "alice@example.com", "NA", "2024-04-05 16:20:00", 25000, 12),
        ("C005", None, "EU", "2024-05-12 11:00:00", 8000, 5),
        ("C006", "charlie@example.com", "UNKNOWN_REGION", "2024-06-18 13:30:00", 12000, 6),
        ("C007", "david@example.com", "NA", "2024-07-22 08:45:00", 3000, 2),
        ("C008", "eve@example.com", "APAC", "2024-08-30 15:10:00", 18000, 9),
        (None, "frank@example.com", "EU", "2024-09-14 12:00:00", 7000, 4),
        ("C010", "grace@example.com", "NA", "2024-10-01 10:00:00", 2000, 1),
    ]
    
    columns = ["customer_id", "email", "region", "created_at", "total_spending", "order_count"]
    
    bronze_df = spark.createDataFrame(data, columns)
    bronze_df.write.mode("overwrite").parquet("/opt/spark/work-dir/bronze/customers_raw")
    
    silver_input_data = [
        ("C001", 15000, 8),
        ("C002", 5000, 3),
        ("C003", 500, 1),
        ("C004", 25000, 12),
        ("C005", 8000, 5),
        ("C006", 12000, 6),
        ("C007", 3000, 2),
        ("C008", 18000, 9),
        ("C010", 2000, 1),
    ]
    
    silver_columns = ["customer_id", "total_spending", "order_count"]
    silver_df = spark.createDataFrame(silver_input_data, silver_columns)
    silver_df.write.mode("overwrite").parquet("/opt/spark/work-dir/silver/customers_with_spending")
    
    print(f"Created {bronze_df.count()} sample records")
    return bronze_df


def run_bronze_layer(spark, df):
    """Run Bronze layer validation"""
    
    print("\n" + "="*60)
    print("BRONZE LAYER: Source Quality Validation")
    print("="*60)
    
    validator = SourceQualityValidator(
        table_name="bronze.customers_raw",
        rules={
            "expected_columns": ["customer_id", "email", "created_at", "region"],
            "min_row_count": 1,
            "max_row_count": 10000000,
            "non_nullable_columns": ["customer_id"]
        }
    )
    
    passed = validator.run_all_checks(df)
    
    if not passed:
        print("\nBronze validation FAILED. Stopping pipeline.")
        return False
    
    print("\nBronze validation PASSED")
    return True


def run_silver_layer(spark):
    """Run Silver layer transformation and validation"""
    
    print("\n" + "="*60)
    print("SILVER LAYER: Transformation Quality")
    print("="*60)
    
    silver_df = transform_bronze_to_silver(spark)
    passed, checks = validate_silver_quality(silver_df)
    
    if not passed:
        print("\nSilver validation FAILED. Stopping pipeline.")
        return False
    
    print("\nSilver validation PASSED")
    return True


def run_gold_layer(spark):
    """Run Gold layer business logic validation"""
    
    print("\n" + "="*60)
    print("GOLD LAYER: Business Logic Quality")
    print("="*60)
    
    gold_df = create_customer_segments(spark)
    passed, failures = validate_business_rules(gold_df)
    
    if not passed:
        print("\nGold validation FAILED. Alerting but not writing.")
        return False
    
    print("\nWriting to gold.customer_segments...")
    gold_df.write.mode("overwrite").parquet("/opt/spark/work-dir/gold/customer_segments")
    
    print("\nGold validation PASSED. Data written successfully.")
    return True


def main():
    """Main pipeline execution"""
    
    print("Starting Data Quality Pipeline...")
    print("="*60)
    
    spark = SparkSession.builder \
        .appName("DataQualityPipeline") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        bronze_df = create_sample_data(spark)
        
        if not run_bronze_layer(spark, bronze_df):
            sys.exit(1)
        
        if not run_silver_layer(spark):
            sys.exit(1)
        
        if not run_gold_layer(spark):
            sys.exit(1)
        
        print("\n" + "="*60)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print("="*60)
        
    except Exception as e:
        print(f"\nPipeline failed with error: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()