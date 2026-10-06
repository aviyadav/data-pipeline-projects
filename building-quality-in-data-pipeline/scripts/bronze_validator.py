from pyspark.sql import DataFrame
import json
from datetime import datetime


class SourceQualityValidator:
    """Validates incoming data at the Bronze layer"""
    
    def __init__(self, table_name: str, rules: dict):
        self.table_name = table_name
        self.rules = rules
        self.checks_run = []
    
    def validate_schema(self, df: DataFrame) -> dict:
        """Ensure expected columns exist"""
        expected_cols = set(self.rules.get("expected_columns", []))
        actual_cols = set(df.columns)
        missing = expected_cols - actual_cols
        extra = actual_cols - expected_cols
        
        check = {
            "check_type": "schema",
            "passed": len(missing) == 0 and len(extra) == 0,
            "details": {
                "missing_columns": list(missing),
                "extra_columns": list(extra)
            }
        }
        
        self.checks_run.append(check)
        return check
    
    def validate_row_count(self, df: DataFrame) -> dict:
        """Ensure row count is within bounds"""
        count = df.count()
        min_rows = self.rules.get("min_row_count", 0)
        max_rows = self.rules.get("max_row_count", float('inf'))
        
        check = {
            "check_type": "row_count",
            "passed": min_rows <= count <= max_rows,
            "details": {
                "actual_count": count,
                "min_expected": min_rows,
                "max_expected": max_rows
            }
        }
        
        self.checks_run.append(check)
        return check
    
    def validate_nulls(self, df: DataFrame) -> dict:
        """Check critical columns for nulls"""
        critical_cols = self.rules.get("non_nullable_columns", [])
        null_counts = {col: int(df.filter(df[col].isNull()).count()) 
                      for col in critical_cols}
        
        check = {
            "check_type": "null_validation",
            "passed": all(count == 0 for count in null_counts.values()),
            "details": null_counts
        }
        
        self.checks_run.append(check)
        return check
    
    def run_all_checks(self, df: DataFrame) -> bool:
        """Run all validations and return pass/fail"""
        self.validate_schema(df)
        self.validate_row_count(df)
        self.validate_nulls(df)
        
        all_passed = all(check["passed"] for check in self.checks_run)
        
        print(f"Quality checks for {self.table_name}:")
        for check in self.checks_run:
            status = "PASSED" if check["passed"] else "FAILED"
            print(f"  {check['check_type']}: {status}")
            if not check["passed"]:
                print(f"    Details: {json.dumps(check['details'], indent=4)}")
        
        return all_passed