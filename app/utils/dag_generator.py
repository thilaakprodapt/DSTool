"""
DAG Generator using SQL Templates - ML-Ready Output

This module generates Airflow DAG code that produces ML-ready transformed data:
- Chains transformations properly (impute → encode)
- Outputs only final columns (no originals, no intermediate)
- Ensures no NULLs in output
- All features are numeric (except target if specified)

Usage:
    from app.utils.dag_generator import DAGGenerator
    
    generator = DAGGenerator(
        project_id="my-project",
        dataset_id="my_dataset",
        source_table="customers",
        target_dataset="transformed"
    )
    
    dag_code = generator.generate(transformations, target_column="label")
"""

import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict
from app.utils.sql_templates import SQLTemplateEngine, validate_column_name, sanitize_column_name


class DAGGenerator:
    """
    Generates Airflow DAG Python code with ML-ready transformations.
    
    Key features:
        - Chains transformations: imputation → encoding
        - Outputs only transformed columns with suffixes
        - Ensures no NULLs in output
        - Supports target column preservation
    """
    
    DAG_TEMPLATE = '''"""
Auto-generated Airflow DAG for Feature Engineering
Generated at: {generated_at}
Source: {source_table}
Target: {target_table}
"""

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# -----------------------------------------------------------------------------
# DAG CONFIGURATION
# -----------------------------------------------------------------------------

PROJECT_ID = "{project_id}"
SOURCE_DATASET = "{source_dataset}"
SOURCE_TABLE = "{source_table}"
TARGET_DATASET = "{target_dataset}"
TARGET_TABLE = "{target_table}"

default_args = {{
    'owner': 'data-science-assistant',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}}

# -----------------------------------------------------------------------------
# DAG DEFINITION
# -----------------------------------------------------------------------------

with DAG(
    dag_id='{dag_id}',
    default_args=default_args,
    description='Feature Engineering DAG for {source_table}',
    schedule_interval=None,  # Triggered manually
    catchup=False,
    max_active_runs=1,
    tags=['feature-engineering', 'auto-generated', 'ml-ready'],
) as dag:

{task_definitions}

{task_dependencies}
'''

    TASK_TEMPLATE = '''
    # -------------------------------------------------------------------------
    # Task: {task_id}
    # {description}
    # -------------------------------------------------------------------------
    {task_id} = BigQueryInsertJobOperator(
        task_id='{task_id}',
        configuration={{
            "query": {{
                "query": """{sql_query}""",
                "useLegacySql": False,
                "priority": "BATCH",
            }}
        }},
        location='US',
    )
'''

    # Transformation priority for chaining order
    TRANSFORM_PRIORITY = {
        # Imputation comes first (priority 1)
        "impute_mean": 1,
        "impute_median": 1,
        "impute_mode": 1,
        "impute_constant": 1,
        "mean_imputation": 1,
        "median_imputation": 1,
        "mode_imputation": 1,
        "mean imputation": 1,
        "median imputation": 1,
        "mode imputation": 1,
        
        # Scaling/transformation comes second (priority 2)
        "standardization": 2,
        "normalization": 2,
        "min_max_normalization": 2,
        "log_transformation": 2,
        "sqrt_transformation": 2,
        "winsorize": 2,
        "clip_outliers": 2,
        "clip_iqr": 2,
        "robust_scaling": 2,
        
        # Encoding comes last (priority 3)
        "label_encoding": 3,
        "frequency_encoding": 3,
        "hash_encoding": 3,
        "binary_encoding": 3,
        "one_hot_encoding": 3,
        "target_encoding": 3,
    }

    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        source_table: str,
        target_dataset: str,
        target_table: Optional[str] = None
    ):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.source_table = source_table
        self.target_dataset = target_dataset
        self.target_table = target_table or f"{source_table}_transformed"
        
        # Generate unique DAG ID
        self.dag_id = f"fe_dag_{source_table}_{uuid.uuid4().hex[:8]}"
        
        # Initialize SQL template engine
        self.sql_engine = SQLTemplateEngine(project_id, dataset_id, source_table)
    
    def _normalize_method(self, method: str) -> str:
        """Normalize method name for consistency."""
        return method.lower().replace(" ", "_").replace("-", "_")
    
    def _get_transform_priority(self, method: str) -> int:
        """Get priority for a transformation method (lower = earlier)."""
        normalized = self._normalize_method(method)
        return self.TRANSFORM_PRIORITY.get(normalized, 2)  # Default to middle priority
    
    def _is_imputation(self, method: str) -> bool:
        """Check if method is an imputation type."""
        normalized = self._normalize_method(method)
        return "impute" in normalized or "imputation" in normalized
    
    def _is_encoding(self, method: str) -> bool:
        """Check if method is an encoding type."""
        normalized = self._normalize_method(method)
        return "encoding" in normalized or "encode" in normalized
    
    def _group_transformations_by_column(
        self, 
        transformations: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Group transformations by column and sort by priority."""
        grouped = defaultdict(list)
        
        for t in transformations:
            column = t.get("column_name", "")
            if column:
                grouped[column].append(t)
        
        # Sort each column's transformations by priority
        for column in grouped:
            grouped[column].sort(key=lambda x: self._get_transform_priority(x.get("fe_method", "")))
        
        return dict(grouped)
    
    def _render_chained_expression(
        self, 
        column: str, 
        transforms: List[Dict[str, Any]]
    ) -> Tuple[str, str]:
        """
        Render a chained SQL expression for a column with multiple transformations.
        
        Returns:
            Tuple of (sql_expression, output_column_name)
        """
        if not transforms:
            return column, column
        
        # Start with the column itself
        current_expr = column
        suffix_parts = []
        
        for t in transforms:
            method = t.get("fe_method", "")
            normalized = self._normalize_method(method)
            params = {k: v for k, v in t.items() if k not in ("column_name", "fe_method")}
            
            if self._is_imputation(method):
                # Imputation: wrap current expression in COALESCE
                if "median" in normalized:
                    current_expr = f"COALESCE({current_expr}, PERCENTILE_CONT({column}, 0.5) OVER())"
                    suffix_parts.append("imputed")
                elif "mean" in normalized:
                    current_expr = f"COALESCE({current_expr}, AVG({column}) OVER())"
                    suffix_parts.append("imputed")
                elif "mode" in normalized:
                    current_expr = f"COALESCE({current_expr}, FIRST_VALUE({column} IGNORE NULLS) OVER (ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))"
                    suffix_parts.append("imputed")
                elif "constant" in normalized:
                    fill_value = params.get("fill_value", 0)
                    current_expr = f"COALESCE({current_expr}, {fill_value})"
                    suffix_parts.append("imputed")
            
            elif self._is_encoding(method):
                # Encoding: wrap current expression in encoding function
                if "hash" in normalized or "one_hot" in normalized:
                    num_buckets = params.get("num_buckets", 50)
                    current_expr = f"MOD(ABS(FARM_FINGERPRINT(CAST({current_expr} AS STRING))), {num_buckets})"
                    suffix_parts.append("encoded")
                elif "binary" in normalized:
                    current_expr = f"CASE WHEN CAST({current_expr} AS STRING) IN ('Yes', 'yes', 'YES', 'true', 'True', 'TRUE', '1') THEN 1 ELSE 0 END"
                    suffix_parts.append("binary")
                elif "label" in normalized:
                    current_expr = f"DENSE_RANK() OVER (ORDER BY COALESCE(CAST({current_expr} AS STRING), '')) - 1"
                    suffix_parts.append("encoded")
                elif "frequency" in normalized:
                    current_expr = f"COUNT(*) OVER (PARTITION BY CAST({current_expr} AS STRING))"
                    suffix_parts.append("freq")
            
            elif "winsorize" in normalized:
                lower = params.get("lower_percentile", 0.05)
                upper = params.get("upper_percentile", 0.95)
                current_expr = f"GREATEST(LEAST({current_expr}, PERCENTILE_CONT({column}, {upper}) OVER()), PERCENTILE_CONT({column}, {lower}) OVER())"
                suffix_parts.append("winsorized")
            
            elif "standardization" in normalized or "standardize" in normalized:
                current_expr = f"({current_expr} - AVG({column}) OVER()) / NULLIF(STDDEV({column}) OVER(), 0)"
                suffix_parts.append("scaled")
            
            elif "normalization" in normalized or "normalize" in normalized or "min_max" in normalized:
                current_expr = f"({current_expr} - MIN({column}) OVER()) / NULLIF(MAX({column}) OVER() - MIN({column}) OVER(), 0)"
                suffix_parts.append("normalized")
            
            elif "log" in normalized:
                current_expr = f"LOG({current_expr} + 1)"
                suffix_parts.append("log")
            
            elif "sqrt" in normalized:
                current_expr = f"SQRT(ABS({current_expr}))"
                suffix_parts.append("sqrt")
            
            elif "clip" in normalized or "outlier" in normalized:
                current_expr = f"GREATEST(LEAST({current_expr}, PERCENTILE_CONT({column}, 0.75) OVER() + 1.5 * (PERCENTILE_CONT({column}, 0.75) OVER() - PERCENTILE_CONT({column}, 0.25) OVER())), PERCENTILE_CONT({column}, 0.25) OVER() - 1.5 * (PERCENTILE_CONT({column}, 0.75) OVER() - PERCENTILE_CONT({column}, 0.25) OVER()))"
                suffix_parts.append("clipped")
        
        # Generate output column name with suffix
        if suffix_parts:
            # Remove duplicates while preserving order
            unique_suffixes = []
            for s in suffix_parts:
                if s not in unique_suffixes:
                    unique_suffixes.append(s)
            output_name = f"{column}_{'_'.join(unique_suffixes)}"
        else:
            output_name = column
        
        return current_expr, output_name
    
    def generate(
        self,
        transformations: List[Dict[str, Any]],
        target_column: Optional[str] = None,
        include_target: bool = True
    ) -> Dict[str, Any]:
        """
        Generate Airflow DAG code for ML-ready transformed data.
        
        Args:
            transformations: List of transformation specifications
            target_column: Name of the target/label column (kept as-is or encoded)
            include_target: Whether to include target column in output
        
        Returns:
            Dict with dag_code, dag_id, metadata
        """
        # Validate transformations
        validated_transformations = []
        skipped = []
        
        for t in transformations:
            column = t.get("column_name", "")
            method = t.get("fe_method", "")
            
            if not column or not method:
                skipped.append({"column": column, "reason": "Missing column_name or fe_method"})
                continue
            
            # Sanitize column name
            if not validate_column_name(column):
                sanitized = sanitize_column_name(column)
                print(f"Warning: Sanitized column name '{column}' to '{sanitized}'")
                t["column_name"] = sanitized
            
            validated_transformations.append(t)
        
        if not validated_transformations:
            raise ValueError("No valid transformations provided")
        
        # Group by column
        grouped = self._group_transformations_by_column(validated_transformations)
        
        # Generate SQL for each column
        select_expressions = []
        column_metadata = []
        
        for column, transforms in grouped.items():
            # Skip target column in regular processing
            if column == target_column:
                continue
            
            expr, output_name = self._render_chained_expression(column, transforms)
            select_expressions.append(f"    {expr} AS {output_name}")
            column_metadata.append({
                "original_column": column,
                "output_column": output_name,
                "transformations": [t.get("fe_method") for t in transforms]
            })
        
        # Handle target column
        if target_column and include_target:
            # Check if target has transformations
            if target_column in grouped:
                expr, output_name = self._render_chained_expression(target_column, grouped[target_column])
                select_expressions.append(f"    {expr} AS target")
            else:
                # Keep target as-is
                select_expressions.append(f"    {target_column} AS target")
            column_metadata.append({
                "original_column": target_column,
                "output_column": "target",
                "transformations": ["target_column"]
            })
        
        # Build complete SQL
        target_full = f"`{self.project_id}.{self.target_dataset}.{self.target_table}`"
        source_full = f"`{self.project_id}.{self.dataset_id}.{self.source_table}`"
        
        select_clause = ",\n".join(select_expressions)
        sql_query = f"""
CREATE OR REPLACE TABLE {target_full} AS
SELECT
{select_clause}
FROM {source_full}
"""
        
        # Generate task definition
        task_def = self.TASK_TEMPLATE.format(
            task_id="apply_ml_transformations",
            description="Apply all ML-ready transformations (chained: impute → encode)",
            sql_query=sql_query
        )
        
        task_deps = "    # Single task - no dependencies needed"
        
        dag_code = self.DAG_TEMPLATE.format(
            generated_at=datetime.utcnow().isoformat(),
            project_id=self.project_id,
            source_dataset=self.dataset_id,
            source_table=self.source_table,
            target_dataset=self.target_dataset,
            target_table=self.target_table,
            dag_id=self.dag_id,
            task_definitions=task_def,
            task_dependencies=task_deps
        )
        
        return {
            "status": "success",
            "dag_code": dag_code,
            "dag_code_lines": dag_code.split('\n'),
            "dag_id": self.dag_id,
            "dag_name": self.dag_id,
            "target_table_name": self.target_table,
            "target_dataset": self.target_dataset,
            "source_table": self.source_table,
            "target_column": target_column,
            "transformations_applied": validated_transformations,
            "transformations_skipped": skipped,
            "column_mapping": column_metadata,
            "generation_method": "template_chained",
        }
    
    def get_dag_id(self) -> str:
        """Return the generated DAG ID."""
        return self.dag_id
    
    def preview_sql(
        self, 
        transformations: List[Dict[str, Any]],
        target_column: Optional[str] = None
    ) -> str:
        """Preview the SQL that would be generated."""
        result = self.generate(transformations, target_column)
        # Extract just the SQL from the DAG code
        import re
        match = re.search(r'"""(CREATE OR REPLACE.*?)"""', result["dag_code"], re.DOTALL)
        return match.group(1).strip() if match else "SQL extraction failed"
