"""
DAG Generator using SQL Templates

This module generates Airflow DAG code using pre-validated SQL templates
instead of AI-generated SQL, providing safer and more reliable transformations.

Usage:
    from app.utils.dag_generator import DAGGenerator
    
    generator = DAGGenerator(
        project_id="my-project",
        dataset_id="my_dataset",
        source_table="customers",
        target_dataset="transformed"
    )
    
    dag_code = generator.generate(transformations)
"""

import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional
from app.utils.sql_templates import SQLTemplateEngine, validate_column_name, sanitize_column_name


class DAGGenerator:
    """
    Generates Airflow DAG Python code using SQL templates.
    
    This generator creates production-ready DAG code with:
        - Unique DAG names to prevent overwrites
        - Pre-validated SQL transformations
        - Proper error handling
        - Clean temporary table management
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
    tags=['feature-engineering', 'auto-generated'],
) as dag:

{task_definitions}

{task_dependencies}
'''

    TASK_TEMPLATE = '''
    # -------------------------------------------------------------------------
    # Task: {task_id}
    # Transformation: {transformation_name} on column '{column_name}'
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

    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        source_table: str,
        target_dataset: str,
        target_table: Optional[str] = None
    ):
        """
        Initialize the DAG generator.
        
        Args:
            project_id: GCP project ID
            dataset_id: Source dataset ID
            source_table: Source table name
            target_dataset: Target dataset for transformed table
            target_table: Target table name (default: {source_table}_transformed)
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.source_table = source_table
        self.target_dataset = target_dataset
        self.target_table = target_table or f"{source_table}_transformed"
        
        # Generate unique DAG ID
        self.dag_id = f"fe_dag_{source_table}_{uuid.uuid4().hex[:8]}"
        
        # Initialize SQL template engine
        self.sql_engine = SQLTemplateEngine(project_id, dataset_id, source_table)
    
    def generate(
        self,
        transformations: List[Dict[str, Any]],
        use_single_query: bool = True
    ) -> Dict[str, Any]:
        """
        Generate Airflow DAG code for the given transformations.
        
        Args:
            transformations: List of transformation specifications
                Each item should have:
                    - column_name: Name of the column to transform
                    - fe_method: Feature engineering method to apply
                    - (optional) Additional parameters for the template
            use_single_query: If True, combines all transformations into one query
                              If False, creates separate tasks for each
        
        Returns:
            Dict with:
                - dag_code: Complete Python DAG code
                - dag_id: Generated DAG ID
                - target_table: Target table name
                - transformations_applied: List of applied transformations
        """
        # Validate and sanitize transformations
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
            
            # Check if template exists
            method_normalized = method.lower().replace(" ", "_").replace("-", "_")
            if method_normalized not in self.sql_engine.TEMPLATES:
                skipped.append({
                    "column": column, 
                    "method": method,
                    "reason": f"Unknown method. Available: {self.sql_engine.get_available_templates()}"
                })
                continue
            
            validated_transformations.append(t)
        
        if not validated_transformations:
            raise ValueError("No valid transformations provided")
        
        # Generate DAG code
        if use_single_query:
            dag_code = self._generate_single_query_dag(validated_transformations)
        else:
            dag_code = self._generate_multi_task_dag(validated_transformations)
        
        return {
            "status": "success",
            "dag_code": dag_code,
            "dag_code_lines": dag_code.split('\n'),
            "dag_id": self.dag_id,
            "dag_name": self.dag_id,
            "target_table_name": self.target_table,
            "target_dataset": self.target_dataset,
            "source_table": self.source_table,
            "transformations_applied": validated_transformations,
            "transformations_skipped": skipped,
        }
    
    def _generate_single_query_dag(self, transformations: List[Dict[str, Any]]) -> str:
        """Generate a DAG with a single query that applies all transformations."""
        
        # Generate the complete CREATE TABLE AS SELECT statement
        sql_query = self.sql_engine.render_create_table_as(
            target_dataset=self.target_dataset,
            target_table=self.target_table,
            transformations=transformations,
            include_original=True
        )
        
        # Generate single task
        task_def = self.TASK_TEMPLATE.format(
            task_id="apply_all_transformations",
            transformation_name="All feature engineering transformations",
            column_name="multiple",
            sql_query=sql_query
        )
        
        # No dependencies needed for single task
        task_deps = "    # Single task - no dependencies needed"
        
        return self.DAG_TEMPLATE.format(
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
    
    def _generate_multi_task_dag(self, transformations: List[Dict[str, Any]]) -> str:
        """Generate a DAG with separate tasks for each transformation."""
        
        task_definitions = []
        task_ids = []
        
        # Create intermediate table name
        temp_table = f"{self.source_table}_temp_{uuid.uuid4().hex[:6]}"
        current_source = f"`{self.project_id}.{self.dataset_id}.{self.source_table}`"
        
        for i, t in enumerate(transformations):
            column = t.get("column_name")
            method = t.get("fe_method")
            params = {k: v for k, v in t.items() if k not in ("column_name", "fe_method")}
            params["column"] = column
            
            task_id = f"transform_{sanitize_column_name(column)}_{i}"
            task_ids.append(task_id)
            
            # Render the transformation
            transform_sql = self.sql_engine.render(method, **params)
            
            # Determine target for this step
            if i == len(transformations) - 1:
                # Last transformation - write to final table
                target = f"`{self.project_id}.{self.target_dataset}.{self.target_table}`"
            else:
                # Intermediate transformation - write to temp table
                target = f"`{self.project_id}.{self.dataset_id}.{temp_table}_{i}`"
            
            sql_query = f"""
CREATE OR REPLACE TABLE {target} AS
SELECT
    *,
    {transform_sql}
FROM {current_source}
"""
            
            # Update source for next iteration
            current_source = target
            
            task_def = self.TASK_TEMPLATE.format(
                task_id=task_id,
                transformation_name=method,
                column_name=column,
                sql_query=sql_query
            )
            task_definitions.append(task_def)
        
        # Generate task dependencies
        if len(task_ids) > 1:
            deps = " >> ".join(task_ids)
            task_deps = f"    # Task dependencies\n    {deps}"
        else:
            task_deps = "    # Single task - no dependencies needed"
        
        return self.DAG_TEMPLATE.format(
            generated_at=datetime.utcnow().isoformat(),
            project_id=self.project_id,
            source_dataset=self.dataset_id,
            source_table=self.source_table,
            target_dataset=self.target_dataset,
            target_table=self.target_table,
            dag_id=self.dag_id,
            task_definitions="\n".join(task_definitions),
            task_dependencies=task_deps
        )
    
    def get_dag_id(self) -> str:
        """Return the generated DAG ID."""
        return self.dag_id
    
    def preview_sql(self, transformations: List[Dict[str, Any]]) -> str:
        """
        Preview the SQL that would be generated without creating the full DAG.
        
        Useful for debugging and validation.
        """
        return self.sql_engine.render_create_table_as(
            target_dataset=self.target_dataset,
            target_table=self.target_table,
            transformations=transformations,
            include_original=True
        )
