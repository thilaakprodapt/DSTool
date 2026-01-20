# Complete Pipeline Test Guide

## Testing the Template-Based DAG Generation with Employee Table

### Prerequisites
- Server running: `uvicorn app.main:app --reload`
- BigQuery table: `cloud-practice-dev-2.EDADataset.Employee`

---

## Step 1: Check Server is Running

Open http://localhost:8000/docs in your browser. You should see the Swagger UI.

---

## Step 2: First, Check the Employee Table Schema

Run this in BigQuery Console to see columns:

```sql
SELECT column_name, data_type 
FROM `cloud-practice-dev-2.EDADataset.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'Employee';
```

Or use the Dashboard endpoint to get columns:

**PowerShell:**
```powershell
Invoke-RestMethod -Uri "http://localhost:8000/EDA/column_list?project_id=cloud-practice-dev-2&dataset_id=EDADataset&table_name=Employee" -Method GET
```

---

## Step 3: Test DAG Generation (Template-Based)

**PowerShell Command:**
```powershell
$body = @'
{
  "project_id": "cloud-practice-dev-2",
  "dataset_id": "EDADataset",
  "source_table": "Employee",
  "target_dataset": "EDADataset",
  "transformation": [
    {"column_name": "salary", "fe_method": "standardization"},
    {"column_name": "salary", "fe_method": "log_transformation"},
    {"column_name": "age", "fe_method": "binning", "num_bins": 4},
    {"column_name": "department", "fe_method": "label_encoding"}
  ]
}
'@

Invoke-RestMethod -Uri "http://localhost:8000/Transformation/generate_dag" `
  -Method POST `
  -ContentType "application/json" `
  -Body $body | ConvertTo-Json -Depth 10
```

---

## Step 4: Expected Response

```json
{
  "status": "success",
  "dag_code": "...(Airflow DAG Python code)...",
  "dag_name": "fe_dag_Employee_abc12345",
  "target_table_name": "Employee_transformed",
  "target_dataset": "EDADataset",
  "generation_method": "template"  // <-- Should be "template", NOT "ai_fallback"
}
```

---

## Step 5: Verify the Generated SQL

Copy the SQL from the DAG code response and run it manually in BigQuery Console to verify it works:

The SQL will look like:
```sql
CREATE OR REPLACE TABLE `cloud-practice-dev-2.EDADataset.Employee_transformed` AS
SELECT
    *,
    (salary - AVG(salary) OVER()) / NULLIF(STDDEV(salary) OVER(), 0) AS salary_standardized,
    LOG(salary + 1) AS salary_log,
    NTILE(4) OVER (ORDER BY age) AS age_bin,
    DENSE_RANK() OVER (ORDER BY department) - 1 AS department_encoded
FROM `cloud-practice-dev-2.EDADataset.Employee`
```

---

## Step 6: Check the Transformed Table

After running the SQL, verify in BigQuery:

```sql
SELECT * FROM `cloud-practice-dev-2.EDADataset.Employee_transformed` LIMIT 10;
```

---

## Alternative: One-Line Test

Quick test with minimal transformations:

```powershell
$body = '{"project_id":"cloud-practice-dev-2","dataset_id":"EDADataset","source_table":"Employee","target_dataset":"EDADataset","transformation":[{"column_name":"salary","fe_method":"standardization"}]}'

Invoke-RestMethod -Uri "http://localhost:8000/Transformation/generate_dag" -Method POST -ContentType "application/json" -Body $body | Select-Object status, dag_name, generation_method
```

---

## Validation Checklist

| Check | Expected | Status |
|-------|----------|--------|
| Server running | http://localhost:8000/docs loads | ☐ |
| Employee table exists | Query returns data | ☐ |
| generate_dag returns success | status = "success" | ☐ |
| Uses templates | generation_method = "template" | ☐ |
| Unique DAG name | dag_name like "fe_dag_Employee_xxxx" | ☐ |
| SQL runs in BigQuery | No errors | ☐ |
| Transformed table created | Employee_transformed exists | ☐ |
