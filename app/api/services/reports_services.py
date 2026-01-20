import json, re
from pydantic import BaseModel
from app.utils.biqguery_utils import get_static_bq_client
from vertexai.preview.generative_models import GenerativeModel
from app.api.schemas.schemas import Col_report_schema
from app.utils.status_updater import set_phase_status
from google.cloud import bigquery
from app.core.config import bq_client, PROJECT_ID

# Use bq_client and PROJECT_ID from config.py (already imported above)
project_id = PROJECT_ID

class TableRequest(BaseModel):
    table_name: str   # user input
TABLES = {
    "EDA_Details": {
        "table": "DS_details.EDA_Details",
        "match_column": "source_table",
        "output_column": "full_eda_report",
        "suffix": ""
    },
    "FE_details": {
        "table": "DS_details.FE_details",
        "match_column": "table_name",
        "output_column": "full_output",
        "suffix": ""
    },
    "Leakage_Detection_details": {
        "table": "DS_details.Leakage_Detection_details",
        "match_column": "table_name",
        "output_column": "ld_full_output",
        "suffix": "_transformed"
    },
    "DAG_details": {
        "table": "DS_details.DAG_details",
        "match_column": "target_table_name",
        "output_column": "FE_done",
        "suffix": "_transformed"
    }
}

def fetch_from_bigquery(dataset_table, match_column, match_value, output_column):
    query = f"""
        SELECT {output_column}
        FROM `{project_id}.{dataset_table}`
        WHERE {match_column} = @match_value
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("match_value", "STRING", match_value)
        ]
    )

    result = bq_client.query(query, job_config=job_config).result()

    for row in result:
        value = row.get(output_column)
        try:
            return json.loads(value)
        except:
            return value

    return None


#aggregated
def column_report_service(request: TableRequest):
    input_data = get_table_data_service(request)
    print("response:",input_data)
    model = GenerativeModel("gemini-2.5-pro")

    prompt = f"""
    You are given input: {input_data}

    Return a STRICT JSON LIST.
    FORMAT EXACTLY:

    [
      {{
        "column_name": "",
        "eda_details": "<1 word only>",
        "fe_details": "<technique only or none>",
        "leakage_details": "<1 word>",
        "dag_details": "<value or null>",
        "data": {{
            "eda": {{ }},
            "fe": {{ }},
            "leakage": {{ }},
            "dag": {{ }}
        }}
      }}
    ]

    IMPORTANT RULES:
    - Make sure you follow the **JSON STRUCTURE** {Col_report_schema} as it is eveytime.
    - Output ONLY a JSON LIST ([]).
    - NO text outside JSON.
    - NO comments, NO explanations.
    """

    response = model.generate_content(prompt)
    response_text = response.text.strip()

    # ---- TRY TO EXTRACT JSON LIST SAFELY ----
    try:
        # First try: extract code block with json
        json_match = re.search(r'```(?:json)?\s*(\[[\s\S]*?\])\s*```', response_text)
        
        if json_match:
            json_str = json_match.group(1)
        else:
            # Second try: find first JSON list
            json_match = re.search(r'\[[\s\S]*\]', response_text)
            json_str = json_match.group(0) if json_match else None

        if json_str:
            data = json.loads(json_str)
        else:
            print("❌ No JSON list detected in model output.")
            data = []

    except json.JSONDecodeError as e:
        print(f"❌ JSON decode error: {e}")
        print("MODEL OUTPUT START ===")
        print(response_text[:800])
        print("MODEL OUTPUT END ===")
        data = []

    return data

# ------------------------------------------------
# GET FULL TABLE DATA (EDA, FE, LEAKAGE, DAG)
#consolidated
# ------------------------------------------------
def get_table_data_service(request: TableRequest):
    table_name = request.table_name
    response = {}
    set_phase_status(bq_client, table_name, "report_status", "inprogress")

    for key, config in TABLES.items():

        suffix = config.get("suffix", "")
        match_value = table_name + suffix  # transformed names

        output = fetch_from_bigquery(
            dataset_table=config["table"],
            match_column=config["match_column"],
            match_value=match_value,
            output_column=config["output_column"]
        )

        response[key] = output
    set_phase_status(bq_client, table_name, "report_status", "completed")
    return {
        "input_table_name": table_name,
        "result": response
    }