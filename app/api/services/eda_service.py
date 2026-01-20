import json
import re
from datetime import datetime
from pydantic import BaseModel
import pandas as pd
from fastapi import HTTPException
from google.cloud import bigquery
from vertexai.preview.generative_models import GenerativeModel

from app.utils.eda_stats import (
    compute_data_overview,
    compute_missing_values_bq,
    compute_outliers_bq,
    compute_cardinality,
    compute_univariate_numerical_bq,
    compute_univariate_categorical,
    compute_correlations_bq,
    compute_chi2_scores,
)

from app.utils.status_updater import set_phase_status
from app.utils.target_finder import find_target_column
from app.utils.charts import (
    univariate_numerical,
    univariate_categorical,
    numeric_target_analysis,
    categorical_target_analysis,
)
from app.utils.gcs_utils import refresh_signed_urls_in_data
from app.core.config import PROJECT_ID, bq_client

class EDARequest(BaseModel):
    dataset: str
    table: str
    chat: str | None = None


def save_analysis_to_bigquery(dataset_id: str, table_name: str, analysis_data: dict, df: pd.DataFrame):
    """
    Save analysis results to BigQuery table using batch load (not streaming insert)
    
    Args:
        dataset_id: Source dataset ID
        table_name: Source table name
        analysis_data: Analysis results dictionary
        df: Original DataFrame (to extract column list)
    """
    try:
        output_table_id = "cloud-practice-dev-2.DS_details.EDA_Details"
        
        # Generate unique analysis ID
        analysis_id = f"{dataset_id}_{table_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        # Extract data overview shape
        data_overview = analysis_data.get("Data Overview", {})
        shape = data_overview.get("shape", {})
        
        # Get column list from DataFrame with data types
        column_info = {
            "columns": df.columns.tolist(),
            "data_types": df.dtypes.astype(str).to_dict()
        }
        
        # Prepare the row to insert
        row = {
            "analysis_id": analysis_id,
            "analysis_timestamp": datetime.utcnow().isoformat(),
            "source_dataset": dataset_id,
            "source_table": table_name,
            "table_columns": json.dumps(column_info),  # Save as JSON
            "rows_count": shape.get("rows", 0),
            "columns_count": shape.get("columns", 0),
            "analysis_results": json.dumps(analysis_data),
            "data_overview": json.dumps(data_overview),
            "data_quality": json.dumps(analysis_data.get("Data quality", {})),
            "univariate_analysis": json.dumps(analysis_data.get("Univariate Analysis", {})),
            "bivariate_analysis": json.dumps(analysis_data.get("Bivariate Analysis", {})),
            "summary": analysis_data.get("Summary", {}).get("summary", ""),
            "full_eda_report": json.dumps(analysis_data)
        }
        
        # Use batch load instead of streaming insert
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=False,
        )
        
        load_job = bq_client.load_table_from_json(
            json_rows=[row],
            destination=output_table_id,
            job_config=job_config
        )
        
        load_job.result()  # Wait for batch job to complete
        
        print(f"✅ Successfully saved analysis to {output_table_id}")
        print(f"   Analysis ID: {analysis_id}")
        print(f"   Batch Job ID: {load_job.job_id}")
        print(f"   Columns saved: {len(column_info['columns'])}")
        
        return analysis_id
            
    except Exception as e:
        print(f"❌ Error saving to BigQuery: {str(e)}")
        raise

def get_column_list(project_id: str, dataset_id: str, table_name: str):
    BQ_EDA_TABLE = "cloud-practice-dev-2.DS_details.EDA_Details"

    query = f"""
        SELECT table_columns
        FROM `{BQ_EDA_TABLE}`
        WHERE source_dataset = @dataset
          AND source_table = @table
        ORDER BY analysis_timestamp DESC
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("dataset", "STRING", dataset_id),
            bigquery.ScalarQueryParameter("table", "STRING", table_name),
        ]
    )

    result = list(bq_client.query(query, job_config=job_config))

    if not result:
        raise HTTPException(404, "No EDA data found for this table.")

    # BigQuery returned STRING instead of dict → convert to dict
    raw_table_columns = result[0]["table_columns"]

    try:
        table_columns_json = (
            raw_table_columns
            if isinstance(raw_table_columns, dict)
            else json.loads(raw_table_columns)  # <-- FIX
        )
    except Exception as e:
        raise HTTPException(500, f"Invalid JSON format in table_columns: {e}")

    columns = table_columns_json.get("columns", [])

    return {
        "project_id": project_id,
        "dataset_id": dataset_id,
        "table_name": table_name,
        "columns": columns
    }
async def analyze_service(request: EDARequest):
    """
    Analyze data from BigQuery using Gemini and save results
    """
    try:
        # -------------------------------------------------
        # 1. Pull Data from BigQuery
        # -------------------------------------------------
        query = f"""
        SELECT *
        FROM `{request.dataset}.{request.table}` LIMIT 1500;
        """
        set_phase_status(bq_client, request.table, "status", "inprogress")
        set_phase_status(bq_client, request.table, "EDA_status", "inprogress")
        df = bq_client.query(query).to_dataframe()
        print(f"Data loaded: {df.shape[0]} rows, {df.shape[1]} columns\n")

        # -------------------------------------------------
        # 2. Auto-detect Column Types
        # -------------------------------------------------
        num_cols = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
        cat_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()

        print(f"Numerical columns: {num_cols}")
        print(f"Categorical columns: {cat_cols}\n")

        # -------------------------------------------------
        # 3. Compute Statistics using BQ-optimized functions
        # -------------------------------------------------
        print("Computing statistics (BigQuery optimized)...")
        data_overview = compute_data_overview(bq_client, PROJECT_ID, request.dataset, request.table, df)
        missing_values_dict = compute_missing_values_bq(bq_client, PROJECT_ID, request.dataset, request.table)
        outliers_dict = compute_outliers_bq(bq_client, PROJECT_ID, request.dataset, request.table, num_cols)
        cardinality_dict = compute_cardinality(df)
        uni_num = compute_univariate_numerical_bq(bq_client, PROJECT_ID, request.dataset, request.table, num_cols)
        uni_cat = compute_univariate_categorical(df, cat_cols)
        
        target = find_target_column(request.dataset, request.table)
        print("Target:", target)

        # Initialize bivariate stats dict
        bivar_stats = {}

        # -------------------------------------------------
        # 4. Generate Charts
        # -------------------------------------------------
        print("Generating charts...")
        
        # Define placeholders
        PLACEHOLDER_UNI_NUM = "__UNIVARIATE_NUM_URL__"
        PLACEHOLDER_UNI_CAT = "__UNIVARIATE_CAT_URL__"
        PLACEHOLDER_NUM_VS_TARGET = "__NUM_VS_TARGET_URL__"
        PLACEHOLDER_CAT_VS_TARGET = "__CAT_VS_TARGET_URL__"
        
        url_map = {}

        if num_cols:
            real_uni_num = univariate_numerical(df, num_cols)
            url_map[PLACEHOLDER_UNI_NUM] = real_uni_num
            univariate_num_url = PLACEHOLDER_UNI_NUM
        else:
            univariate_num_url = None

        if cat_cols:
            real_uni_cat = univariate_categorical(df, cat_cols)
            url_map[PLACEHOLDER_UNI_CAT] = real_uni_cat
            univariate_cat_url = PLACEHOLDER_UNI_CAT
        else:
            univariate_cat_url = None
        
        # Determine target type and generate appropriate bivariate charts
        num_vs_target_url = None
        cat_vs_target_url = None
        
        if target in num_cols:
            print(f"Target '{target}' is Numerical. Running numeric target analysis...")
            target_charts = numeric_target_analysis(df, target, num_cols, cat_cols)
            
            real_num_vs_target = target_charts.get("Correlation with Target (Bar Chart)")
            real_cat_vs_target = target_charts.get("Categorical vs Target (Mean Plot Grid)")
            
            url_map[PLACEHOLDER_NUM_VS_TARGET] = real_num_vs_target
            url_map[PLACEHOLDER_CAT_VS_TARGET] = real_cat_vs_target
            
            num_vs_target_url = PLACEHOLDER_NUM_VS_TARGET
            cat_vs_target_url = PLACEHOLDER_CAT_VS_TARGET
            
            # Compute correlation stats
            corr_stats = compute_correlations_bq(bq_client, PROJECT_ID, request.dataset, request.table, target, num_cols)
            bivar_stats.update(corr_stats)
            
        elif target in cat_cols:
            print(f"Target '{target}' is Categorical. Running categorical target analysis...")
            target_charts = categorical_target_analysis(df, target, num_cols, cat_cols)
            
            real_num_vs_target = target_charts.get("Numerical vs Target (Boxplot Grid)")
            real_cat_vs_target = target_charts.get("Categorical Importance (Chi-square Bar Chart)")
            
            url_map[PLACEHOLDER_NUM_VS_TARGET] = real_num_vs_target
            url_map[PLACEHOLDER_CAT_VS_TARGET] = real_cat_vs_target
            
            num_vs_target_url = PLACEHOLDER_NUM_VS_TARGET
            cat_vs_target_url = PLACEHOLDER_CAT_VS_TARGET
            
            # Compute chi-square stats
            chi2_stats = compute_chi2_scores(df, target, cat_cols)
            bivar_stats.update(chi2_stats)
        else:
            print(f"Target '{target}' type unknown or not in columns.")

        print("Charts generated successfully!")

        # -------------------------------------------------
        # 5. Initialize Gemini Model
        # -------------------------------------------------
        model = GenerativeModel("gemini-2.0-flash")

        # -------------------------------------------------
        # 6. Create Prompt with Computed Statistics
        # -------------------------------------------------
        prompt = f"""
            You are an expert data scientist.
            I have computed the following statistics for the dataset:

            Data Overview: {json.dumps(data_overview)}
            Data Quality - Missing Values: {json.dumps(missing_values_dict)}
            Data Quality - Outliers: {json.dumps(outliers_dict)}
            Data Quality - Cardinality: {json.dumps(cardinality_dict)}
            Univariate Numerical: {json.dumps(uni_num)}
            Univariate Categorical: {json.dumps(uni_cat)}
            Bivariate Stats: {json.dumps(bivar_stats)}

            Chart URLs:
            Univariate Numerical: {univariate_num_url}
            Univariate Categorical: {univariate_cat_url}
            Numerical vs Target: {num_vs_target_url}
            Categorical vs Target: {cat_vs_target_url}

            Your task is to generate a JSON response with the EXACT structure provided below.
            Fill in the values using the statistics provided above.
            Write detailed summaries for the analysis sections based on the stats and the charts.

            Output JSON Structure:
            {{
             "Data Overview": {{
                 "shape": {{"rows": <number>, "columns": <number>}},
                 "feature_types": {{
                     "numerical": {json.dumps(num_cols)},
                     "categorical": {json.dumps(cat_cols)},
                     "boolean": [],
                     "datetime": [],
                     "text": [],
                     "high_cardinality": []
                 }},
                 "sample_data": {json.dumps(data_overview.get("sample_data", []))}
             }},
                        "Data quality":{{
             "missing_values": {json.dumps(missing_values_dict["missing_values"])},
                        "outliers": {json.dumps(outliers_dict["outliers"])},
                        "Cardinality Check": {json.dumps(cardinality_dict["Cardinality Check"])}
                        }},
             "Univariate Analysis": {{
                 "numerical": {json.dumps(uni_num)},
                 "img_url": "{univariate_num_url}",
                                "summary": "Give a detailed summary of the numerical analysis and the histogram visualization",
                 "categorical": {json.dumps(uni_cat)},
                 "img_url_cat": "{univariate_cat_url}",
                                "summary_cat": "Give a detailed summary of the categorical analysis and the bar chart visualization"
             }},
        
             "Bivariate Analysis": {{
                 "Target_column":"Based on column metadata and dataset patterns, {target} has been selected as the target column.",
                 "Numerical vs Target": {{
                 "img_url": "{num_vs_target_url}",
                 "summary": "<detailed summary of relationships with target>"
                 }},
                 "Categorical vs Target": {{
                 "img_url": "{cat_vs_target_url}",
                 "summary": "<detailed summary of categorical features with target>"
                 }}
             }},
             "Summary": {{
                 "summary":"From the whole analysis give a comprehensive summary on the data, analysis and insights"
             }}
             }}

            IMPORTANT INSTRUCTIONS:
            1. Make sure the output JSON follows the structure exactly as shown
            2. Return ONLY valid JSON - no markdown, no code blocks, no preamble
            3. For img_url fields: Use the EXACT PLACEHOLDERS provided above - do not modify them
            4. Ensure all numbers are actual numbers (not strings)
            5. All arrays must contain objects as shown in the structure
            6. Provide comprehensive summaries for all visualization sections
"""

        # -------------------------------------------------
        # 7. Generate Content from Gemini
        # -------------------------------------------------
        response = model.generate_content(prompt)
        response_text = response.text
        print("Response from Gemini:", response_text[:200])
        
        # -------------------------------------------------
        # 8. Parse JSON Response
        # -------------------------------------------------
        try:
            columns_count = len(df.columns.tolist())
            # Try multiple patterns
            json_match = re.search(r'```json\s*(\{[\s\S]*?\})\s*```', response_text)
            
            if json_match:
                json_str = json_match.group(1)
            else:
                json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
                if json_match:
                    json_str = json_match.group(0)
                else:
                    json_str = None
            
            if json_str:
                data = json.loads(json_str)
                
                # Helper to replace placeholders with actual URLs
                def replace_values_recursive(obj, mapping):
                    if isinstance(obj, dict):
                        return {k: replace_values_recursive(v, mapping) for k, v in obj.items()}
                    elif isinstance(obj, list):
                        return [replace_values_recursive(item, mapping) for item in obj]
                    elif isinstance(obj, str):
                        return mapping.get(obj, obj)
                    return obj

                # Restore the actual Signed URLs
                if url_map:
                    data = replace_values_recursive(data, url_map)
                    
            else:
                print("No JSON found in response")
                data = {}
                
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            print(f"Attempted to parse: {json_str[:200] if json_str else 'None'}...")
            data = {}

        # -------------------------------------------------
        # 9. Save to BigQuery
        # -------------------------------------------------
        if data:
            analysis_id = save_analysis_to_bigquery(request.dataset, request.table, data, df)
            bq_client.query(
                f"""
                UPDATE `{PROJECT_ID}.DS_details.Workspace_details`
                SET EDA_status = @status,
                    EDA_analysis_id = @aid,
                    Last_updated = CURRENT_DATETIME()
                WHERE table_name = @table
                """,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("status", "STRING", "completed"),
                        bigquery.ScalarQueryParameter("aid", "STRING", analysis_id),
                        bigquery.ScalarQueryParameter("table", "STRING", request.table),
                    ]
                ),
            ).result()
            set_phase_status(bq_client, request.table, "status", "completed")
            set_phase_status(bq_client, request.table, "EDA_status", "completed")
            return {
                "analysis": data,
                "saved_to_bigquery": True,
                "analysis_id": analysis_id,
                "columns_count": columns_count
            }
        else:
            set_phase_status(bq_client, request.table, "status", "failed")
            set_phase_status(bq_client, request.table, "EDA_status", "failed")
            return {
                "analysis": data,
                "saved_to_bigquery": False,
                "error": "No valid analysis data to save"
            }
    
    except Exception as e:
        set_phase_status(bq_client, request.table, "status", "failed")
        set_phase_status(bq_client, request.table, "EDA_status", "failed")
        print(f"Error in analyze: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

async def get_eda_result_service(dataset: str, table: str):
    """
    Returns ONLY the full_eda_report (complete raw Gemini JSON) 
    for the given dataset.table if EDA is completed.
    """
    # Step 1: Check status in Workspace_details
    ws_query = f"""
        SELECT EDA_status, EDA_analysis_id
        FROM `{PROJECT_ID}.DS_details.Workspace_details`
        WHERE table_name = @table
        LIMIT 1
    """
    ws_job = bq_client.query(
        ws_query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("table", "STRING", table)]
        )
    )
    ws_rows = list(ws_job.result())

    if not ws_rows:
        raise HTTPException(status_code=404, detail=f"Table '{table}' not found in workspace.")

    row = ws_rows[0]
    eda_status = row["EDA_status"]
    analysis_id = row["EDA_analysis_id"]

    if eda_status != "completed":
        raise HTTPException(
            status_code=400,
            detail=f"EDA not completed for '{table}'. Current status: {eda_status}"
        )

    if not analysis_id:
        raise HTTPException(
            status_code=500,
            detail=f"EDA completed but EDA_analysis_id is missing for '{table}'."
        )

    # Step 2: Fetch ONLY full_eda_report from EDA_Details
    eda_query = f"""
        SELECT full_eda_report, columns_count
        FROM `{PROJECT_ID}.DS_details.EDA_Details`
        WHERE analysis_id = @analysis_id
        AND source_dataset = @dataset
        AND source_table = @table
        LIMIT 1
    """
    eda_job = bq_client.query(
        eda_query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("analysis_id", "STRING", analysis_id),
                bigquery.ScalarQueryParameter("dataset", "STRING", dataset),
                bigquery.ScalarQueryParameter("table", "STRING", table),
            ]
        )
    )

    eda_rows = list(eda_job.result())

    if not eda_rows:
        raise HTTPException(status_code=404, detail=f"Full EDA report not found for analysis_id: {analysis_id}")

    full_report_json = eda_rows[0]["full_eda_report"]
    columns_count = eda_rows[0]["columns_count"]   # ✅ Get it here

    # Parse JSON if needed
    if isinstance(full_report_json, str):
        try:
            full_report_json = json.loads(full_report_json)
        except json.JSONDecodeError:
            raise HTTPException(status_code=500, detail="Failed to parse full_eda_report JSON")

    # ✅ REFRESH SIGNED URLs - Regenerate any expired GCS signed URLs
    print(f"🔄 Refreshing signed URLs for analysis_id: {analysis_id}")
    full_report_json = refresh_signed_urls_in_data(full_report_json, expiration_hours=24)

    # Return final response
    return {
        "analysis": full_report_json,
        "saved_to_bigquery": True,
        "analysis_id": analysis_id,
        "columns_count": columns_count   # ✅ Added here
    }