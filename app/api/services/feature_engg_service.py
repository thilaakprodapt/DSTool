import json
import re
from fastapi import HTTPException
from pydantic import BaseModel
from google.cloud import bigquery
from vertexai.generative_models import GenerativeModel
from app.utils.status_updater import set_phase_status  # if exists
from app.core.config import (
    bq_client,
    PROJECT_ID,
    FE_TABLE_ID,
)


class FERequest(BaseModel):
    eda_output: dict  # Full EDA output from /analyze or /eda_result
    target_column : str
    chat: str | None = None



def save_fe_to_bq(bq_client, dataset: str, table: str, data: dict):
    """
    Saves FE Gemini output to FE_details table using DML instead of streaming.
    """

    # Normalize FE items
    if isinstance(data, dict) and "output" in data:
        features = data["output"] if isinstance(data["output"], list) else [data["output"]]
    elif isinstance(data, list):
        features = data
    else:
        features = [data]

    if not features:
        print("Warning: No valid FE features to save")
        return None

    for item in features:
        feature_name = item.get("feature_name") or "generic_feature"

        sql = f"""
        INSERT INTO `{FE_TABLE_ID}` (
            id, dataset, table_name, feature_name, eda_insights,
            selected_inputs, recommendations, full_output, updated_at
        )
        VALUES(
            CAST(RAND() * 9223372036854775807 AS INT64),
            @dataset,
            @table_name,
            @feature_name,
            TO_JSON(@eda_insights),
            TO_JSON(@selected_inputs),
            TO_JSON(@recommendations),
            TO_JSON(@full_output),
            CURRENT_TIMESTAMP()
        )
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("dataset", "STRING", dataset),
                bigquery.ScalarQueryParameter("table_name", "STRING", table),
                bigquery.ScalarQueryParameter("feature_name", "STRING", feature_name),
                bigquery.ScalarQueryParameter("eda_insights", "JSON", item.get("eda_insights", {})),
                bigquery.ScalarQueryParameter("selected_inputs", "JSON", item.get("selected_inputs", [])),
                bigquery.ScalarQueryParameter("recommendations", "JSON", item.get("recommendations", [])),
                bigquery.ScalarQueryParameter("full_output", "JSON", data),
            ]
        )

        bq_client.query(sql, job_config=job_config).result()

    print("Inserted FE records using DML successfully")
    return True

async def FE_analyze_service(request: FERequest):
    """
    Generate Feature Engineering recommendations based on EDA output.
    
    Args:
        request.eda_output: Full EDA output dictionary (from /analyze or /eda_result)
        request.chat: Optional custom instructions
    """
    eda_output = request.eda_output
    if not eda_output:
        raise HTTPException(status_code=400, detail="eda_output is required")
    
    # Extract analysis_id from the EDA output
    analysis_id = eda_output.get("analysis_id")
    if not analysis_id:
        raise HTTPException(status_code=400, detail="analysis_id not found in eda_output")
    
    # Query BigQuery to get dataset and table from analysis_id
    query = f"""
    SELECT source_dataset, source_table
    FROM `cloud-practice-dev-2.DS_details.EDA_Details`
    WHERE analysis_id = @analysis_id
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("analysis_id", "STRING", analysis_id)
        ]
    )

    query_job = bq_client.query(query, job_config=job_config)
    result = query_job.result()
    row = list(result)
    if not row:
        raise HTTPException(status_code=404, detail=f"No EDA details found for analysis_id {analysis_id}")

    dataset = row[0].source_dataset
    table = row[0].source_table
    
    # Use the analysis data from the input (not from DB)
    input_data = eda_output.get("analysis", {})
    print(f"Processing FE for dataset: {dataset}, table: {table}")
    # Initialize Gemini model
    set_phase_status(bq_client, table, "FE_status", "inprogress")
    model = GenerativeModel("gemini-2.0-flash")
    

    prompt = f"""
You are a senior data scientist.

You are given an EDA report for a dataset:
{input_data}
TARGET COLUMN (FIXED):
The target column is "{request.target_column}".
This column MUST be used for imbalance analysis.
Do NOT change or infer another target column.
Your task has FOUR parts:

1. FEATURE ENGINEERING (per feature)
- Handle missing values
- Scaling or transformation
- Encoding for categorical features
- Feature creation
- Feature selection or dropping
- Keep explanations short

2. IMBALANCE ANALYSIS (dataset level)
- Identify the target column
- Detect if class imbalance exists
- Calculate imbalance severity
- Explain reasoning briefly

3. BIAS DETECTION (dataset level)
- Identify sensitive features (e.g. gender, region, country, age)
- Detect representation or outcome bias
- Assign risk level with short evidence

4. BALANCING RECOMMENDATIONS (dataset level)
- If target is categorical → recommend Python Random Oversampling
- If target is continuous → recommend SMOTER
- Do NOT generate code
- Only recommend methods with pros & cons

TARGET COLUMN (IMPORTANT):
- The target column is FIXED and PROVIDED by the user.
- Target column name: "{request.target_column}"
- You MUST use this exact column for imbalance analysis.
- Do NOT infer or change the target column.


STRICT RULES:
- imbalance_analysis MUST always contain a valid object.
- bias_detection MUST always contain at least one feature OR a note saying "no significant bias detected".
- balancing_recommendations MUST always be present.



IMPORTANT:
- Return ONLY valid JSON
- Follow EXACTLY this output structure:
CRITICAL RULES:

1. Identify majority class = class with MAX count
2. Identify minority class = class with MIN count
3. Calculate:
   imbalance_ratio = majority_class_count / minority_class_count

4. If majority_count == minority_count:
   imbalance_ratio = 1.0
   is_imbalanced = false

5. imbalance_ratio MUST be >= 1 by definition

{{
  "feature_engineering": [
    {{
      "feature_name": "",
      "eda_insights": {{
        "distribution": [],
        "missing_values": [],
        "correlations": [],
        "outliers": []
      }},
      "selected_inputs": [],
      "recommendations": [
        {{
          "technique": "",
          "reason": ""
        }}
      ]
    }}
  ],
  "imbalance_analysis": {{
    "target_column": "",
    "target_type": "",
    "class_distribution": [],
    "imbalance_ratio": ,
    "is_imbalanced": ,
    "severity": "",
    "reasoning": ""
  }},
  "bias_detection": [
    {{
      "sensitive_feature": "",
      "bias_type": "",
      "risk_level": "",
      "evidence": ""
    }}
  ],
  "balancing_recommendations": {{
    "recommended": true,
    "techniques": [
      {{
        "method": "",
        "applicable_when": "",
        "pros": "",
        "cons": ""
      }}
    ],
    "user_action_required": "Do you want to proceed with a balanced dataset?"
  }}
}}

Do not add explanations outside JSON.
"""

    response = model.generate_content(prompt)

    response_text = ""

    if response.candidates:
        for part in response.candidates[0].content.parts:
            if hasattr(part, "text") and part.text:
                response_text += part.text

    print("RAW GEMINI RESPONSE:", repr(response_text[:1000]))
 # temp debug

    json_str = None

# 1️⃣ Extract JSON from ```json ... ``` (handles newlines & trailing spaces)
    code_fence_match = re.search(
        r"```(?:json)?\s*([\s\S]*?)\s*```",
        response_text,
        re.IGNORECASE
    )

    if code_fence_match:
        json_str = code_fence_match.group(1).strip()

    # 2️⃣ Fallback: extract first JSON object anywhere in text
    if not json_str:
        fallback_match = re.search(r"\{[\s\S]*\}", response_text)
        if fallback_match:
            json_str = fallback_match.group(0).strip()

    # 3️⃣ Final safety check
    if not json_str:
        set_phase_status(bq_client, table, "FE_status", "failed")
        print("❌ No JSON found in LLM response")
        return {"feature_engineering_recommendations": {}}


    try:
        data = json.loads(json_str)
        required_sections = [
        "feature_engineering",
        "imbalance_analysis",
        "bias_detection",
        "balancing_recommendations",
    ]
        for section in required_sections:
            if section not in data:
                set_phase_status(bq_client, table, "FE_status", "failed")
                print(f"Missing section in LLM output: {section}")
                return {"feature_engineering_recommendations": {}}
        save_fe_to_bq(bq_client, dataset, table, data)
        set_phase_status(bq_client, table, "FE_status", "completed")
        return {"feature_engineering_recommendations": data}
    
    except json.JSONDecodeError as e:
        set_phase_status(bq_client, table, "FE_status", "failed")
        print("JSON decode error:", e)
        print("json_str snippet:", json_str[:400])
        return {"feature_engineering_recommendations": {}}
    
async def get_fe_result_service(dataset: str, table: str):
    """
    Fetch the most recent Feature Engineering results for a given dataset and table.
    
    Args:
        dataset: Source dataset name
        table: Source table name
        
    Returns:
        Latest feature engineering recommendations from the FE_details table
    """
    try:
        # Step 1: Get the most recent FE analysis timestamp
        timestamp_query = f"""
            SELECT MAX(updated_at) as latest_timestamp
            FROM `{PROJECT_ID}.DS_details.FE_details`
            WHERE dataset = @dataset
              AND table_name = @table
        """
        
        timestamp_job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("dataset", "STRING", dataset),
                bigquery.ScalarQueryParameter("table", "STRING", table),
            ]
        )
        
        timestamp_result = list(bq_client.query(timestamp_query, job_config=timestamp_job_config).result())
        
        if not timestamp_result or not timestamp_result[0]["latest_timestamp"]:
            raise HTTPException(
                status_code=404,
                detail=f"No Feature Engineering results found for '{dataset}.{table}'. Please run /FE_analyze first."
            )
        
        latest_timestamp = timestamp_result[0]["latest_timestamp"]
        
        # Step 2: Get only FE results from the latest analysis (same timestamp)
        fe_query = f"""
            SELECT full_output, feature_name, updated_at
            FROM `{PROJECT_ID}.DS_details.FE_details`
            WHERE dataset = @dataset
              AND table_name = @table
              AND updated_at = @latest_timestamp
            ORDER BY id DESC
        """
        
        fe_job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("dataset", "STRING", dataset),
                bigquery.ScalarQueryParameter("table", "STRING", table),
                bigquery.ScalarQueryParameter("latest_timestamp", "TIMESTAMP", latest_timestamp),
            ]
        )
        
        fe_results = list(bq_client.query(fe_query, job_config=fe_job_config).result())
        
        if not fe_results:
            raise HTTPException(
                status_code=404,
                detail=f"No Feature Engineering results found for '{dataset}.{table}'. Please run /FE_analyze first."
            )
        
        # Step 3: Collect all FE recommendations
        fe_recommendations = []
        for row in fe_results:
            full_output = row["full_output"]
            
            # Parse JSON if it's a string
            if isinstance(full_output, str):
                try:
                    full_output = json.loads(full_output)
                except json.JSONDecodeError:
                    print(f"Warning: Could not parse full_output for feature: {row.get('feature_name', 'unknown')}")
                    continue
            
            fe_recommendations.append(full_output)
        
        return {
            "feature_engineering_recommendations": fe_recommendations
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching FE results: {str(e)}")

