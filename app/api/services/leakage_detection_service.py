import json
import re
from datetime import datetime, timezone
from random import getrandbits

from fastapi import HTTPException
from google.cloud import bigquery
from vertexai.generative_models import GenerativeModel

from app.core.config import (
    bq_client,
    LEAK_TABLE_ID,
)
from app.utils.status_updater import set_phase_status


def save_leakage_to_bq(bq_client, dataset: str, table: str, data: dict, full_output: str):
    """
    Saves leakage analysis to BigQuery using batch load (not streaming insert)
    """
    summary = data.get("summary", {}) or {}

    def norm_list(key):
        arr = data.get(key, []) or []
        return [
            {
                "feature": x.get("feature", ""),
                "reason": x.get("reason", "")
            }
            for x in arr if isinstance(x, dict)
        ]

    row = {
        "id": int(getrandbits(63)),
        "dataset": dataset,
        "table_name": table,
        "summary": {
            "high_risk": int(summary.get("high_risk", 0) or 0),
            "medium_risk": int(summary.get("medium_risk", 0) or 0),
            "low_risk": int(summary.get("low_risk", 0) or 0),
        },
        "high_risk_features": norm_list("high_risk_features"),
        "medium_risk_features": norm_list("medium_risk_features"),
        "low_risk_features": norm_list("low_risk_features"),
        "ld_full_output": full_output,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    # Convert to JSON string for batch load
    json_data = json.dumps([row])
    
    # Use load_table_from_json (batch job) instead of insert_rows_json (streaming)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False,
    )
    
    load_job = bq_client.load_table_from_json(
        json_rows=[row],
        destination=LEAK_TABLE_ID,
        job_config=job_config
    )
    
    load_job.result()  # Wait for batch job to complete
    print(f"Leakage batch job {load_job.job_id} completed")
    
    return row["id"]

def leakage_detection_service(dataset: str, table_name: str):
    table = table_name.replace("_transformed", "")
    print(table)
    try:
        # ---------------------------------------------------
        # 1. Mark Status: IN PROGRESS
        # ---------------------------------------------------
        set_phase_status(bq_client,table, "Leakage_status", "inprogress")

        full_table_id = f"cloud-practice-dev-2.{dataset}.{table_name}"
        query = f"SELECT * FROM `{full_table_id}`"
        df = bq_client.query(query).to_dataframe()

        # Use only 200 rows to keep model prompt small
        sample_json = df.head(200).to_json(orient="records")

        # ---------------------------------------------------
        # 2. LLM Prompt
        # ---------------------------------------------------
        model = GenerativeModel("gemini-2.0-flash")
        prompt = f"""
        You are a **machine learning data leakage detection expert**.
        You are given a Dataset sample (max 200 rows):
        {sample_json}

        Your task:
        Analyze this dataset for potential **target leakage**.

        You MUST return results **strictly in JSON** with this structure:

        {{
        "summary": {{
            "high_risk": <number>,
            "medium_risk": <number>,
            "low_risk": <number>
        }},
        "high_risk_features": [
            {{
            "feature": "<column_name>",
            "reason": "<reason>"
            }},
            ...
        ],
        "medium_risk_features": [
            {{
            "feature": "<column_name>",
            "reason": "<reason>"
            }},
            ...
        ],
        "low_risk_features": [
            {{
            "feature": "<column_name>",
            "reason": "<reason>"
            }},
            ...
        ]
        }}

        Rules:
        - Only output valid JSON.
        - No extra explanation outside JSON.
        - Every feature listed must exist in the dataset.
        - High-risk = directly reveals target or comes from future data.
        - Medium-risk = strong proxy to target or correlated patterns.
        - Low-risk = minor risk or needs review.
        """

        response = model.generate_content(prompt)
        response_text = response.text

        print("LLM Output:", response_text)

        # ---------------------------------------------------
        # 3. Extract JSON from LLM Response
        # ---------------------------------------------------
        json_str = None

        # Prefer JSON inside ```json … ```
        json_match = re.search(
            r'```(?:json)?\s*([\s\S]*?)\s*```',
            response_text,
            re.DOTALL
        )

        if json_match:
            json_str = json_match.group(1).strip()
        else:
            # Try to find a raw JSON object { ... }
            json_match = re.search(r'(\{.*\})', response_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
        full_output = json_str
        # If no JSON found → fail safely
        if not json_str:
            set_phase_status(bq_client, table, "Leakage_status", "failed")
            return {"leakage_detection": {}, "error": "No valid JSON returned"}

        # ---------------------------------------------------
        # 4. Parse JSON
        # ---------------------------------------------------
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError:
            set_phase_status(bq_client, table, "Leakage_status", "failed")
            return {
                "leakage_detection": {},
                "error": "JSON parsing failed",
                "raw_output": response_text
            }
            
        # ---------------------------------------------------
        # 5. Save to BigQuery (parsed JSON + full output)
        # ---------------------------------------------------
        _id = save_leakage_to_bq(
            bq_client,
            dataset,
            table,
            data,
            full_output   # ⭐ NEW FIELD
        )

        # ---------------------------------------------------
        # 6. Mark Completed
        # ---------------------------------------------------
        set_phase_status(bq_client, table, "Leakage_status", "completed")

        return {
            "status": "success",
            "id": _id,
            "leakage_detection": data
        }

    except Exception as e:
        set_phase_status(bq_client, table, "Leakage_status", "failed")
        return {
            "status": "error",
            "message": str(e)
        }