import os
import re
import json
import time
import uuid
from datetime import datetime
from typing import Optional
from pydantic import BaseModel
from fastapi import HTTPException
from app.core.config import bq_client  # your BigQuery client
from vertexai.preview.generative_models import GenerativeModel  # your LLM wrapper
from app.utils.dag_trigger import wait_for_dag_and_trigger
from app.utils.status_updater import set_phase_status
from app.utils.dag_generator import DAGGenerator
from app.utils.sql_templates import SQLTemplateEngine


class SaveDAGRequest(BaseModel):
    dag_code: str
    dag_name: str
    target_table_name: str
    target_dataset: str
    source_table: str
    input_data: dict


# =============================================================================
# TEMPLATE-BASED DAG GENERATION (RECOMMENDED - Safer & More Reliable)
# =============================================================================

async def generate_dag_service(input: dict):
    """
    Generate Airflow DAG using pre-validated SQL templates.
    
    This is the recommended approach as it:
    - Uses pre-validated SQL templates (no AI hallucinations)
    - Generates unique DAG IDs (no overwrites)
    - Validates column names (no SQL injection)
    - Is deterministic (same input = same output)
    
    Falls back to AI generation if template-based generation fails.
    """
    table = input.get("source_table")
    
    try:
        set_phase_status(bq_client, table, "DAG_status", "Inprogress")
        
        # Extract required parameters
        project_id = input.get("project_id")
        dataset_id = input.get("dataset_id")
        source_table = input.get("source_table")
        target_dataset = input.get("target_dataset")
        transformations = input.get("transformation", [])
        
        # Validate required fields
        if not all([project_id, dataset_id, source_table, target_dataset, transformations]):
            raise ValueError("Missing required fields: project_id, dataset_id, source_table, target_dataset, transformation")
        
        # Try template-based generation first
        try:
            result = _generate_dag_with_templates(
                project_id=project_id,
                dataset_id=dataset_id,
                source_table=source_table,
                target_dataset=target_dataset,
                transformations=transformations
            )
            result["generation_method"] = "template"
            result["input_data"] = input
            
            print(f"✅ Template-based DAG generated: {result['dag_name']}")
            return result
            
        except Exception as template_error:
            print(f"⚠️ Template generation failed: {template_error}")
            print("⚠️ Falling back to AI-based generation...")
            
            # Fall back to AI generation
            result = await _generate_dag_with_ai(input)
            result["generation_method"] = "ai_fallback"
            result["template_error"] = str(template_error)
            return result
    
    except Exception as e:
        set_phase_status(bq_client, table, "DAG_status", "failed")
        print(f"Error during DAG generation: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }


def _generate_dag_with_templates(
    project_id: str,
    dataset_id: str,
    source_table: str,
    target_dataset: str,
    transformations: list
) -> dict:
    """
    Generate DAG using the SQL template system.
    
    This function uses pre-validated SQL templates for common transformations.
    """
    # Initialize the DAG generator
    generator = DAGGenerator(
        project_id=project_id,
        dataset_id=dataset_id,
        source_table=source_table,
        target_dataset=target_dataset
    )
    
    # Generate the DAG
    result = generator.generate(
        transformations=transformations,
        use_single_query=True  # Combine all transformations into one efficient query
    )
    
    if result.get("status") != "success":
        raise ValueError(f"DAG generation failed: {result.get('message', 'Unknown error')}")
    
    return result


async def _generate_dag_with_ai(input: dict) -> dict:
    """
    Generate DAG using AI (Gemini) - FALLBACK METHOD.
    
    This is used only when template-based generation fails.
    Note: AI-generated SQL may contain errors and should be reviewed.
    """
    table = input.get("source_table")
    
    model = GenerativeModel("gemini-2.5-pro")
    prompt = f"""
        You are an expert Airflow DAG developer.

             You will be given a JSON {input} with the following keys:
             - project_id
             - dataset_id
             - source_table
             - target_dataset
             - transformation : a list, where each item contains:
                 - column_name
                 - fe_method

             Your tasks:

             1. Read and parse the JSON EXACTLY as given. Do not modify any key names.
             2. Generate a complete Airflow DAG in Python.
             3. Generate a UNIQUE DAG name using the source_table name.
             4. Create one Airflow task per transformation.
             5. Every transformation task must:
                 - Use BigQueryInsertJobOperator
                 - Contain a valid SQL query that applies the fe_method on the column_name
                 - Fully qualify all tables as: project_id.dataset_id.table
             6. After all FE tasks, create ONE final task that:
                 - Creates (IF NOT EXISTS) and loads a final BigQuery table
                 - Target table name must be exactly: <source_table>_transformed
                 - Full path: project_id.target_dataset.<source_table>_transformed
                 - Uses BigQueryInsertJobOperator for both CREATE TABLE and INSERT INTO
             7. All SQL must be syntactically correct.
             8. Use the Airflow connection to authenticate (service account).
             9. Do NOT create a new dataset. Assume it already exists.
             
             Return ONLY the Python code, no explanations.
    """
    
    response = model.generate_content(prompt)
    response_text = response.text

    print("LLM response:", response_text[:500])

    # Extract DAG code
    dag_code_match = re.search(r'```(?:python|dag)?\s*(.*?)\s*```', response_text, re.DOTALL)
    
    if dag_code_match:
        dag_code = dag_code_match.group(1).strip()
    else:
        # Try to use the entire response as code
        dag_code = response_text.strip()
    
    # Generate unique DAG name
    dag_name = f"fe_dag_{table}_{uuid.uuid4().hex[:8]}"
    target_table_name = f"{table}_transformed"
    target_dataset = input.get("target_dataset", "")

    return {
        "status": "success",
        "dag_code": dag_code,
        "dag_code_lines": dag_code.split('\n'),
        "dag_name": dag_name,
        "target_table_name": target_table_name,
        "target_dataset": target_dataset,
        "source_table": table,
        "input_data": input,
        "warning": "AI-generated SQL - please review before production use"
    }


# =============================================================================
# HELPER: Get Available Transformations
# =============================================================================

def get_available_transformations() -> dict:
    """
    Return a list of all available SQL transformations.
    
    Useful for frontend display and validation.
    """
    engine = SQLTemplateEngine("", "", "")
    templates = engine.get_available_templates()
    
    # Group by category
    categories = {
        "numerical": [
            "standardization", "normalization", "log_transformation",
            "sqrt_transformation", "binning", "robust_scaling"
        ],
        "categorical": [
            "label_encoding", "frequency_encoding", "target_encoding"
        ],
        "missing_values": [
            "impute_mean", "impute_median", "impute_mode",
            "impute_constant", "missing_indicator"
        ],
        "outliers": [
            "clip_outliers", "clip_iqr", "winsorize"
        ],
        "feature_engineering": [
            "polynomial_features", "interaction", "ratio"
        ],
        "datetime": [
            "extract_date_parts", "date_diff"
        ]
    }
    
    return {
        "available_templates": templates,
        "categories": categories
    }



def save_dag_service(request: SaveDAGRequest):
    dag_name = request.dag_name
    dag_code = request.dag_code
    target_table_name = request.target_table_name
    target_dataset = request.target_dataset
    input_data = request.input_data
    source_table = request.source_table

    try:
        # Save DAG file
        file_path = f"/home/airflow/dags/{dag_name}.py"
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as f:
            f.write(dag_code)
        print(f"DAG saved at {file_path}")

        # Trigger DAG via Docker
        triggered = wait_for_dag_and_trigger(dag_name)
        if not triggered:
            print(f"⚠️ Failed to trigger DAG {dag_name}")
            return {
                "status": "error",
                "message": f"Failed to trigger DAG {dag_name}"
            }

        # Log DAG details in BigQuery
        store_fe_details(input_data=input_data, dag_name=dag_name,
                         target_table_name=target_table_name,
                         target_dataset=target_dataset)

        set_phase_status(bq_client, source_table, "DAG_status", "Completed")
        
        return {
            "status": "success",
            "dag_name": dag_name,
            "target_table_name": target_table_name,
            "target_dataset": target_dataset,
            "file_path": file_path,
            "message": f"DAG created and triggered successfully"
        }

    except Exception as e:
        set_phase_status(bq_client, source_table, "DAG_status", "failed")
        print(f"Error during DAG save/trigger: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }


def store_fe_details(input_data: dict, dag_name: str, target_table_name: str, target_dataset: str):
    """
    Store the feature engineering details in BigQuery DAG_details table
    """
    try:
        from google.cloud import bigquery
        import json
        from datetime import datetime
        import uuid
        
        
        # Generate a unique ID
        unique_id = str(uuid.uuid4())
        
        # Prepare the row to insert
        row = {
            "id": unique_id,
            "dag_name": dag_name,
            "target_table_name": target_table_name,
            "target_dataset": target_dataset,
            "FE_done": json.dumps(input_data),  # Store input as JSON string
            "created_at": datetime.utcnow(),
            "status": "completed"
        }
        
        # Insert row into DAG_details table
        # Replace 'your_project' and 'your_dataset' with your actual project and dataset
        project_id = "cloud-practice-dev-2"
        table_id = f"{project_id}.DS_details.DAG_details"
        
        errors = bq_client.insert_rows_json(table_id, [row])
        
        if errors:
            print(f"Error inserting row into DAG_details table: {errors}")
        else:
            print(f"Successfully logged DAG details to {table_id}")
            
    except Exception as e:
        print(f"Error storing DAG details: {str(e)}")


def fe_chat_check_service(input: str):
    model = GenerativeModel("gemini-2.0-flash")

    prompt = f"""
      You are an expert in machine learning feature engineering.

        Input: "{input}"

        Task:
        1. If the input EXACTLY matches a recognized ML feature engineering technique (e.g., "one-hot encoding", "standardization", "PCA"), set "valid": true and empty suggestion.
        2. If the input is generic, partially correct, or broad (like "encode", "scale", "transform"):
        - Set "valid": false.
        - Suggest only the techniques that belong to that category:
            - encode → one-hot encoding, label encoding, target encoding, ordinal encoding, frequency encoding
            - scale → standardization, min-max scaling, robust scaling, normalization
            - transform → log transformation, polynomial features, interaction features
            - reduce → PCA, SVD, feature hashing, embeddings
            - handle_missing → missing value imputation
            - handle_outlier → outlier clipping, bucketing
            - balance → SMOTE
        3. If the input is random text unrelated to ML feature engineering, set "valid": false and suggest: "Check that the input is a valid ML feature engineering technique."

        Output STRICTLY as JSON:

        {{
        "valid": true/false,
        "suggestion": "string"
        }}

    """

    response = model.generate_content(prompt)
    raw = response.text.strip()
    print("RAW LLM OUTPUT:", raw)

    # ---------------------------------------
    # 1️⃣ Remove ```json or ``` wrapping
    # ---------------------------------------
    raw = raw.replace("```json", "").replace("```", "").strip()

    # ---------------------------------------
    # 2️⃣ Extract JSON substring safely
    # ---------------------------------------
    json_match = re.search(r"\{.*\}", raw, re.DOTALL)
    if not json_match:
        raise HTTPException(status_code=500, detail="No JSON object found in LLM output")

    json_str = json_match.group(0)

    # ---------------------------------------
    # 3️⃣ Final JSON parse
    # ---------------------------------------
    try:
        parsed = json.loads(json_str)
    except Exception as e:
        print("FAILED JSON PARSE:", json_str)
        raise HTTPException(status_code=500, detail="LLM returned invalid JSON structure")

    return parsed


