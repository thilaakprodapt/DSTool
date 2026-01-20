"""
Test Script for SQL Template System and DAG Generation

This script demonstrates the template system with dummy data.
Run this file directly to test without needing BigQuery.

Usage:
    python tests/test_dag_generation.py
"""

import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.utils.sql_templates import SQLTemplateEngine
from app.utils.dag_generator import DAGGenerator


# =============================================================================
# EXAMPLE 1: Dummy Customer Data
# =============================================================================

DUMMY_CUSTOMER_DATA = """
| customer_id | name          | age | income    | gender | signup_date | category | purchase_count |
|-------------|---------------|-----|-----------|--------|-------------|----------|----------------|
| 1           | John Doe      | 35  | 75000.00  | Male   | 2023-01-15  | Premium  | 12             |
| 2           | Jane Smith    | 28  | 52000.00  | Female | 2023-03-22  | Standard | 5              |
| 3           | Bob Johnson   | 45  | 120000.00 | Male   | 2022-11-10  | Premium  | 25             |
| 4           | Alice Brown   | NULL| 48000.00  | Female | 2023-06-01  | Basic    | 2              |
| 5           | Charlie Wilson| 62  | 95000.00  | Male   | 2021-08-20  | Premium  | 45             |
| 6           | Diana Lee     | 31  | NULL      | Female | 2023-09-05  | Standard | 8              |
| 7           | Edward Kim    | 29  | 68000.00  | Male   | 2023-02-14  | Standard | 15             |
| 8           | Fiona Garcia  | 55  | 250000.00 | Female | 2020-04-30  | Premium  | 100            |
"""


def test_sql_templates():
    """Test individual SQL template rendering."""
    print("=" * 70)
    print("TEST 1: SQL Template Rendering")
    print("=" * 70)
    
    engine = SQLTemplateEngine(
        project_id="cloud-practice-dev-2",
        dataset_id="test_dataset",
        source_table="customers"
    )
    
    # Test various templates
    templates_to_test = [
        ("standardization", {"column": "age"}),
        ("log_transformation", {"column": "income"}),
        ("label_encoding", {"column": "category"}),
        ("impute_mean", {"column": "age"}),
        ("missing_indicator", {"column": "income"}),
        ("binning", {"column": "purchase_count", "num_bins": 4}),
    ]
    
    for template_name, params in templates_to_test:
        try:
            sql = engine.render(template_name, **params)
            print(f"\n✅ {template_name}:")
            print(f"   Params: {params}")
            print(f"   SQL: {sql.strip()}")
        except Exception as e:
            print(f"\n❌ {template_name}: {e}")
    
    print("\n" + "=" * 70)
    print(f"Available templates ({len(engine.get_available_templates())}):")
    print(", ".join(sorted(set(engine.get_available_templates()))))


def test_dag_generation():
    """Test DAG generation with template system."""
    print("\n" + "=" * 70)
    print("TEST 2: DAG Generation")
    print("=" * 70)
    
    # Example input (same format as API request)
    input_data = {
        "project_id": "cloud-practice-dev-2",
        "dataset_id": "test_dataset",
        "source_table": "customers",
        "target_dataset": "transformed_data",
        "transformation": [
            {"column_name": "age", "fe_method": "standardization"},
            {"column_name": "income", "fe_method": "log_transformation"},
            {"column_name": "category", "fe_method": "label_encoding"},
            {"column_name": "age", "fe_method": "impute_mean"},
            {"column_name": "income", "fe_method": "missing_indicator"},
            {"column_name": "signup_date", "fe_method": "extract_date_parts"},
        ]
    }
    
    print("\n📥 INPUT:")
    print("-" * 40)
    import json
    print(json.dumps(input_data, indent=2))
    
    # Generate DAG
    generator = DAGGenerator(
        project_id=input_data["project_id"],
        dataset_id=input_data["dataset_id"],
        source_table=input_data["source_table"],
        target_dataset=input_data["target_dataset"]
    )
    
    result = generator.generate(input_data["transformation"])
    
    print("\n📤 OUTPUT:")
    print("-" * 40)
    print(f"Status: {result['status']}")
    print(f"DAG Name: {result['dag_name']}")
    print(f"Target Table: {result['target_table_name']}")
    print(f"Transformations Applied: {len(result['transformations_applied'])}")
    print(f"Transformations Skipped: {len(result.get('transformations_skipped', []))}")
    
    print("\n📜 GENERATED DAG CODE (first 80 lines):")
    print("-" * 40)
    for i, line in enumerate(result['dag_code_lines'][:80]):
        print(f"{i+1:3}: {line}")
    
    if len(result['dag_code_lines']) > 80:
        print(f"\n... and {len(result['dag_code_lines']) - 80} more lines ...")
    
    return result


def test_select_statement():
    """Test generating a SELECT statement for preview."""
    print("\n" + "=" * 70)
    print("TEST 3: SQL SELECT Statement Preview")
    print("=" * 70)
    
    engine = SQLTemplateEngine(
        project_id="cloud-practice-dev-2",
        dataset_id="test_dataset",
        source_table="customers"
    )
    
    transformations = [
        {"column_name": "age", "fe_method": "standardization"},
        {"column_name": "income", "fe_method": "log_transformation"},
        {"column_name": "category", "fe_method": "label_encoding"},
    ]
    
    sql = engine.render_create_table_as(
        target_dataset="transformed_data",
        target_table="customers_transformed",
        transformations=transformations
    )
    
    print("\n📜 Generated SQL:")
    print("-" * 40)
    print(sql)


def test_api_curl_example():
    """Print curl command for testing the API."""
    print("\n" + "=" * 70)
    print("TEST 4: API Test Commands")
    print("=" * 70)
    
    api_input = {
        "project_id": "cloud-practice-dev-2",
        "dataset_id": "test_dataset",
        "source_table": "customers",
        "target_dataset": "transformed_data",
        "transformation": [
            {"column_name": "age", "fe_method": "standardization"},
            {"column_name": "income", "fe_method": "log_transformation"},
            {"column_name": "category", "fe_method": "label_encoding"}
        ]
    }
    
    import json
    json_str = json.dumps(api_input)
    
    print("\n🔗 Test with curl:")
    print("-" * 40)
    print(f'''curl -X POST "http://localhost:8000/Transformation/generate_dag" \\
  -H "Content-Type: application/json" \\
  -d '{json_str}'
''')
    
    print("\n🔗 Test with PowerShell:")
    print("-" * 40)
    print(f'''$body = @'
{json.dumps(api_input, indent=2)}
'@

Invoke-RestMethod -Uri "http://localhost:8000/Transformation/generate_dag" `
  -Method POST `
  -ContentType "application/json" `
  -Body $body
''')


if __name__ == "__main__":
    print("\n" + "🧪" * 35)
    print("    SQL TEMPLATE SYSTEM - TEST SUITE")
    print("🧪" * 35)
    print(f"\nDummy Data Used:\n{DUMMY_CUSTOMER_DATA}")
    
    test_sql_templates()
    test_dag_generation()
    test_select_statement()
    test_api_curl_example()
    
    print("\n" + "=" * 70)
    print("✅ All tests completed!")
    print("=" * 70)
