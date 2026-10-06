"""Silver Layer: Transformation Quality"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, count, when


def transform_bronze_to_silver(spark: SparkSession):
    """Transform bronze data to silver with inline quality checks"""
    
    print("Reading from bronze.customers_raw...")
    bronze_df = spark.read.parquet("/opt/spark/work-dir/bronze/customers_raw")
    
    print("Applying transformations...")
    silver_df = bronze_df.select(
        col("customer_id"),
        col("email"),
        col("region"),
        col("created_at").cast("date").alias("created_date"),
        when(col("region").isin("NA", "EU", "APAC"), col("region"))
            .otherwise("UNKNOWN")
            .alias("region_clean")
    ).filter(col("customer_id").isNotNull())
    
    print("Writing to silver.customers...")
    silver_df.write.mode("overwrite").parquet("/opt/spark/work-dir/silver/customers")
    
    return silver_df


def validate_silver_quality(silver_df, table_name="silver.customers"):
    """Validate silver layer data quality"""
    print(f"\nValidating {table_name}...")
    
    checks = []
    
    # Check 1: Duplicate IDs
    total_count = silver_df.count()
    distinct_count = silver_df.select(countDistinct("customer_id")).collect()[0][0]
    duplicate_count = total_count - distinct_count
    
    check1 = {
        "check_type": "duplicate_ids",
        "passed": duplicate_count == 0,
        "details": {
            "total_rows": total_count,
            "distinct_ids": distinct_count,
            "duplicates": duplicate_count
        }
    }
    checks.append(check1)
    
    # Check 2: Invalid regions
    invalid_regions = silver_df.filter(col("region_clean") == "UNKNOWN").count()
    
    check2 = {
        "check_type": "invalid_regions",
        "passed": invalid_regions == 0,
        "details": {
            "invalid_region_count": invalid_regions
        }
    }
    checks.append(check2)
    
    for check in checks:
        status = "PASSED" if check["passed"] else "FAILED"
        print(f"  {check['check_type']}: {status}")
        if not check["passed"]:
            print(f"    Details: {check['details']}")
    
    all_passed = all(check["passed"] for check in checks)
    return all_passed, checks