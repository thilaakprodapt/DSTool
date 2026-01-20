# Data Science Assistant Tool

A FastAPI-based backend for automating data science workflows with Google Cloud Platform integration.

## Features

- **EDA (Exploratory Data Analysis)** - Automated analysis with Gemini AI
- **Feature Engineering** - AI-powered recommendations with template-based DAG generation
- **Data Balancing** - Handle imbalanced datasets
- **Leakage Detection** - Identify data leakage risks
- **Airflow DAG Generation** - Template-based SQL transformations

## Quick Start

```bash
# Clone
git clone https://github.com/thilaakprodapt/DSTool.git
cd DSTool

# Create venv
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Add service account key (not in repo)
# Place your GCP service account JSON in the project root

# Run
uvicorn app.main:app --reload
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/Dashboard/check_bigquery_connection` | POST | Test BQ connection |
| `/EDA/analyze` | POST | Run EDA analysis |
| `/Feature Engineering/FE_analyze` | POST | Get FE recommendations |
| `/Transformation/generate_dag` | POST | Generate Airflow DAG |
| `/Transformation/save_dag` | POST | Save & trigger DAG |

## Template-Based DAG Generation

The system uses pre-validated SQL templates instead of AI-generated SQL for reliable transformations:

```json
POST /Transformation/generate_dag
{
  "project_id": "your-project",
  "dataset_id": "your_dataset",
  "source_table": "your_table",
  "target_dataset": "output_dataset",
  "transformation": [
    {"column_name": "salary", "fe_method": "standardization"},
    {"column_name": "category", "fe_method": "label_encoding"}
  ]
}
```

### Supported Transformations

| Category | Methods |
|----------|---------|
| Numerical | standardization, normalization, log_transformation, binning |
| Categorical | label_encoding, frequency_encoding, one_hot_encoding |
| Missing Values | impute_mean, impute_median, impute_mode |
| Outliers | clip_outliers, clip_iqr, winsorize |

See [docs/sql_template_system.md](docs/sql_template_system.md) for full documentation.

## Project Structure

```
app/
├── api/
│   ├── routes/         # API endpoints
│   └── services/       # Business logic
├── core/
│   └── config.py       # GCP configuration
└── utils/
    ├── sql_templates.py   # SQL template engine
    └── dag_generator.py   # DAG code generator
```

## Requirements

- Python 3.10+
- Google Cloud service account with BigQuery & GCS access
- Vertex AI API enabled

## License

Internal use only.
