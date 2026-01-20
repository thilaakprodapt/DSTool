# SQL Template System for Feature Engineering DAGs

## Overview

This document explains the SQL Template System implemented in the Data Science Assistant Tool. The system replaces AI-generated SQL with pre-validated, deterministic SQL templates for feature engineering transformations.

## Why Templates Instead of AI-Generated SQL?

| Issue with AI-Generated SQL | Solution with Templates |
|-----------------------------|-------------------------|
| Non-deterministic (same input = different output) | Deterministic (same input = same output) |
| Potential SQL syntax errors | Pre-validated, tested SQL |
| Risk of SQL injection | Sanitized column names |
| Hardcoded DAG names (overwrites) | Unique DAG IDs generated |
| Inconsistent transformation logic | Standardized transformations |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        SQL Template System                               │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
    ┌───────────────────────────────┼───────────────────────────────┐
    │                               ▼                               │
    │  app/utils/sql_templates.py                                   │
    │  └── SQLTemplateEngine                                        │
    │      ├── Pre-defined SQL templates for 25+ transformations   │
    │      ├── Template rendering with parameter substitution      │
    │      └── Column name validation and sanitization             │
    │                               │                               │
    │                               ▼                               │
    │  app/utils/dag_generator.py                                   │
    │  └── DAGGenerator                                             │
    │      ├── Generates Airflow DAG Python code                   │
    │      ├── Creates unique DAG IDs                              │
    │      └── Single-query or multi-task DAG modes                │
    │                               │                               │
    │                               ▼                               │
    │  app/api/services/dag_service.py                              │
    │  └── generate_dag_service()                                   │
    │      ├── Uses templates by default                           │
    │      └── Falls back to AI if template fails                  │
    └───────────────────────────────────────────────────────────────┘
```

---

## File Structure

```
app/
├── utils/
│   ├── sql_templates.py    # SQL template definitions
│   └── dag_generator.py    # DAG code generator
├── api/
│   └── services/
│       └── dag_service.py  # API service (updated)
```

---

## Available Transformations

### Numerical Transformations

| Template Name | Description | Example SQL |
|---------------|-------------|-------------|
| `standardization` | Z-score normalization | `(x - mean) / stddev` |
| `normalization` | Min-Max scaling (0-1) | `(x - min) / (max - min)` |
| `log_transformation` | Log transform | `LOG(x + 1)` |
| `sqrt_transformation` | Square root | `SQRT(ABS(x))` |
| `binning` | Quantile binning | `NTILE(4) OVER (ORDER BY x)` |
| `robust_scaling` | Median-IQR scaling | `(x - median) / IQR` |

### Categorical Transformations

| Template Name | Description | Example SQL |
|---------------|-------------|-------------|
| `label_encoding` | Integer encoding | `DENSE_RANK() OVER (ORDER BY x) - 1` |
| `frequency_encoding` | Frequency counts | `COUNT(*) OVER (PARTITION BY x)` |
| `target_encoding` | Mean target by category | `AVG(target) OVER (PARTITION BY x)` |

### Missing Value Handling

| Template Name | Description |
|---------------|-------------|
| `impute_mean` | Replace NULL with mean |
| `impute_median` | Replace NULL with median |
| `impute_mode` | Replace NULL with mode |
| `impute_constant` | Replace NULL with constant value |
| `missing_indicator` | Create binary indicator column |

### Outlier Handling

| Template Name | Description |
|---------------|-------------|
| `clip_outliers` | Clip to specified bounds |
| `clip_iqr` | Clip using 1.5×IQR rule |
| `winsorize` | Cap at percentiles |

### Feature Engineering

| Template Name | Description |
|---------------|-------------|
| `polynomial_features` | Create squared terms |
| `interaction` | Multiply two columns |
| `ratio` | Divide two columns |

### Date/Time Features

| Template Name | Description |
|---------------|-------------|
| `extract_date_parts` | Extract year, month, day, dayofweek |
| `date_diff` | Days from current date |

---

## Usage Examples

### 1. Using SQLTemplateEngine Directly

```python
from app.utils.sql_templates import SQLTemplateEngine

# Initialize engine
engine = SQLTemplateEngine(
    project_id="my-project",
    dataset_id="my_dataset",
    source_table="customers"
)

# Render a single transformation
sql = engine.render("standardization", column="age")
print(sql)
# Output: (age - AVG(age) OVER()) / NULLIF(STDDEV(age) OVER(), 0) AS age_standardized

# Render multiple transformations as SELECT statement
transformations = [
    {"column_name": "age", "fe_method": "standardization"},
    {"column_name": "income", "fe_method": "log_transformation"},
    {"column_name": "category", "fe_method": "label_encoding"}
]
sql = engine.render_select_statement(transformations)
print(sql)
```

### 2. Using DAGGenerator

```python
from app.utils.dag_generator import DAGGenerator

generator = DAGGenerator(
    project_id="my-project",
    dataset_id="my_dataset",
    source_table="customers",
    target_dataset="transformed_data"
)

transformations = [
    {"column_name": "age", "fe_method": "standardization"},
    {"column_name": "income", "fe_method": "log_transformation"}
]

result = generator.generate(transformations)
print(result["dag_code"])  # Complete Airflow DAG Python code
print(result["dag_name"])  # Unique DAG ID like "fe_dag_customers_a1b2c3d4"
```

### 3. API Usage

```bash
# POST /Transformation/generate_dag
curl -X POST "http://localhost:8000/Transformation/generate_dag" \
  -H "Content-Type: application/json" \
  -d '{
    "project_id": "my-project",
    "dataset_id": "my_dataset",
    "source_table": "customers",
    "target_dataset": "transformed_data",
    "transformation": [
      {"column_name": "age", "fe_method": "standardization"},
      {"column_name": "income", "fe_method": "log_transformation"}
    ]
  }'
```

Response includes:
- `dag_code`: Complete Airflow DAG Python code
- `dag_name`: Unique DAG ID
- `generation_method`: "template" or "ai_fallback"
- `transformations_applied`: List of applied transformations
- `transformations_skipped`: List of skipped transformations (with reasons)

---

## How It Works

### Step 1: Input Validation

When a transformation request comes in, the system:
1. Validates required fields (project_id, dataset_id, etc.)
2. Sanitizes column names to prevent SQL injection
3. Checks if template exists for each `fe_method`

### Step 2: SQL Template Rendering

For each transformation, the `SQLTemplateEngine`:
1. Looks up the template by name (case-insensitive, handles spaces/hyphens)
2. Substitutes parameters (column name, etc.)
3. Returns the rendered SQL fragment

Example template:
```python
STANDARDIZATION = """
(${column} - AVG(${column}) OVER()) / NULLIF(STDDEV(${column}) OVER(), 0) AS ${column}_standardized
"""
```

After rendering with `column="age"`:
```sql
(age - AVG(age) OVER()) / NULLIF(STDDEV(age) OVER(), 0) AS age_standardized
```

### Step 3: DAG Code Generation

The `DAGGenerator` creates a complete Airflow DAG with:
- Unique DAG ID: `fe_dag_{source_table}_{uuid}`
- `BigQueryInsertJobOperator` for executing SQL
- Proper imports and configuration
- Task dependencies

### Step 4: Fallback to AI

If template-based generation fails (e.g., unknown transformation method), the system:
1. Logs a warning
2. Falls back to AI-based generation (Gemini)
3. Marks the response with `generation_method: "ai_fallback"`

---

## Adding New Templates

To add a new transformation template:

1. Open `app/utils/sql_templates.py`

2. Add the template string:
```python
MY_NEW_TRANSFORM = """
-- Description of transformation
SOME_SQL_FUNCTION(${column}) AS ${column}_transformed
"""
```

3. Register in `TEMPLATES` dictionary:
```python
TEMPLATES: Dict[str, str] = {
    # ... existing templates ...
    "my_new_transform": MY_NEW_TRANSFORM,
    "my-new-transform": MY_NEW_TRANSFORM,  # alias with hyphens
}
```

4. (Optional) Add default parameters:
```python
DEFAULT_PARAMS: Dict[str, Dict[str, Any]] = {
    # ... existing defaults ...
    "my_new_transform": {"some_param": 10},
}
```

---

## Security Considerations

### Column Name Validation

```python
def validate_column_name(column: str) -> bool:
    """Only allow alphanumeric and underscore."""
    return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', column))
```

Invalid column names are sanitized:
- `user-name` → `user_name`
- `123column` → `_123column`
- `col@name` → `col_name`

### Template Substitution

Uses Python's `string.Template` with `substitute()` instead of f-strings:
- Prevents code injection
- Raises `KeyError` for missing parameters
- No eval() or exec()

---

## Performance Benefits

| Metric | AI-Generated | Template-Based |
|--------|--------------|----------------|
| Latency | 5-15 seconds (API call) | < 100ms |
| Consistency | Variable | 100% consistent |
| Cost | API charges per request | Free |
| Reliability | Depends on AI model | Always works |

---

## Troubleshooting

### Template Not Found

```
ValueError: Unknown template: 'my_transform'. Available templates: [...]
```

**Solution**: Check available templates using:
```python
engine = SQLTemplateEngine("", "", "")
print(engine.get_available_templates())
```

### Missing Parameter

```
ValueError: Missing required parameter: 'column'
```

**Solution**: Ensure all required parameters are provided:
```python
engine.render("binning", column="age", num_bins=5)
```

### Column Name Rejected

Look for warning in logs:
```
Warning: Sanitized column name 'user-name' to 'user_name'
```

This is automatic - no action needed, but review the sanitized name.

---

## Future Improvements

1. **BigQuery ML Integration**: Add templates for BQML functions
2. **Composite Transformations**: Chain multiple templates
3. **Custom Template Upload**: Allow users to define custom templates
4. **Template Versioning**: Track template changes
5. **Dry-Run Mode**: Validate SQL with EXPLAIN before execution
