"""Test script to validate the data quality framework"""

from pyspark.sql import SparkSession
import sys
import os

sys.path.append("scripts")

from bronze_validator import SourceQualityValidator


def test_bronze_validation():
    """Test Bronze layer validation"""
    
    print("="*60)
    print("Testing Bronze Layer Validation")
    print("="*60)
    
    spark = SparkSession.builder \
        .appName("BronzeValidationTest") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        # Test 1: Valid data
        print("\n--- Test 1: Valid Data ---")
        valid_data = [
            ("C001", "john@example.com", "NA", "2024-01-15"),
            ("C002", "jane@example.com", "EU", "2024-02-20"),
            ("C003", "bob@example.com", "APAC", "2024-03-10"),
        ]
        
        valid_df = spark.createDataFrame(valid_data, ["customer_id", "email", "region", "created_at"])
        
        validator = SourceQualityValidator(
            table_name="test.valid_customers",
            rules={
                "expected_columns": ["customer_id", "email", "region", "created_at"],
                "min_row_count": 1,
                "max_row_count": 100,
                "non_nullable_columns": ["customer_id"]
            }
        )
        
        result = validator.run_all_checks(valid_df)
        print(f"Result: {'PASSED' if result else 'FAILED'}\n")
        
        # Test 2: Missing columns
        print("--- Test 2: Missing Columns ---")
        missing_col_data = [
            ("C001", "john@example.com", "NA"),
        ]
        
        missing_col_df = spark.createDataFrame(missing_col_data, ["customer_id", "email", "region"])
        
        validator2 = SourceQualityValidator(
            table_name="test.missing_cols",
            rules={
                "expected_columns": ["customer_id", "email", "region", "created_at"],
                "min_row_count": 1,
                "max_row_count": 100,
                "non_nullable_columns": ["customer_id"]
            }
        )
        
        result2 = validator2.run_all_checks(missing_col_df)
        print(f"Result: {'PASSED' if result2 else 'FAILED'}\n")
        
        # Test 3: Null values in critical column
        print("--- Test 3: Null Values in Critical Column ---")
        null_data = [
            (None, "john@example.com", "NA", "2024-01-15"),
            ("C002", "jane@example.com", "EU", "2024-02-20"),
        ]
        
        null_df = spark.createDataFrame(null_data, ["customer_id", "email", "region", "created_at"])
        
        validator3 = SourceQualityValidator(
            table_name="test.null_values",
            rules={
                "expected_columns": ["customer_id", "email", "region", "created_at"],
                "min_row_count": 1,
                "max_row_count": 100,
                "non_nullable_columns": ["customer_id"]
            }
        )
        
        result3 = validator3.run_all_checks(null_df)
        print(f"Result: {'PASSED' if result3 else 'FAILED'}\n")
        
        # Test 4: Row count out of range
        print("--- Test 4: Row Count Out of Range ---")
        small_data = [
            ("C001", "john@example.com", "NA", "2024-01-15"),
        ]
        
        small_df = spark.createDataFrame(small_data, ["customer_id", "email", "region", "created_at"])
        
        validator4 = SourceQualityValidator(
            table_name="test.row_count",
            rules={
                "expected_columns": ["customer_id", "email", "region", "created_at"],
                "min_row_count": 10,
                "max_row_count": 100,
                "non_nullable_columns": ["customer_id"]
            }
        )
        
        result4 = validator4.run_all_checks(small_df)
        print(f"Result: {'PASSED' if result4 else 'FAILED'}\n")
        
        print("="*60)
        print("All tests completed!")
        print("="*60)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    test_bronze_validation()