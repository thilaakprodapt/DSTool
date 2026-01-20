from fastapi import HTTPException
from google.cloud import bigquery
import pandas as pd
import numpy as np
import json
import re
from vertexai.generative_models import GenerativeModel
from app.utils.bias_imbalance_detection import generate_balanced_data
from pydantic import BaseModel
# Import BigQuery client from config (uses service account credentials)
from app.core.config import bq_client

class BalanceRequest(BaseModel):
    dataset: str
    table: str
    target_column: str
    target_type: str
    balancing_method: str
    user_preference: str
    rare_value_threshold: float
    random_seed: int
    num_bins: int


def df_to_json_safe(df: pd.DataFrame):
    return json.loads(
        df.replace({np.nan: None}).to_json(orient="records")
    )


async def balance_data_service(request: BalanceRequest):

    # 1️⃣ Fetch data
    query = f"SELECT * FROM `{request.dataset}.{request.table}`"
    df = bq_client.query(query).to_dataframe()

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for {request.dataset}.{request.table}"
        )

    target_col = request.target_column
    if target_col not in df.columns:
        raise HTTPException(
            status_code=400,
            detail=f"Target column '{target_col}' not found in table"
        )
    target_type = request.target_type
    method = request.balancing_method
    rare_thresh = request.rare_value_threshold
    seed = request.random_seed
    user_pref = request.user_preference.lower()

    np.random.seed(seed)

    # 2️⃣ Pre-balance analysis
    if target_type == "categorical":
        class_counts = df[target_col].value_counts(dropna=False)
        imbalance_ratio_before = round(class_counts.max() / class_counts.min(), 2)
        rare_values_handled = int((class_counts / len(df) < rare_thresh).sum())
        original_dist = [{"class": str(k), "count": int(v)} for k, v in class_counts.items()]
    else:
        bins = pd.qcut(df[target_col], q=4, duplicates="drop")
        class_counts = bins.value_counts()
        imbalance_ratio_before = round(class_counts.max() / class_counts.min(), 2)
        rare_values_handled = int((class_counts / len(df) < rare_thresh).sum())
        original_dist = [{"class": str(k), "count": int(v)} for k, v in class_counts.items()]

    # 3️⃣ Python-based balancing
    if user_pref == "use original dataset":
        balanced_df = df.copy()
    else:
        balanced_df = generate_balanced_data(df, target_col, target_type)

    # 4️⃣ LLM input rows
    # data_rows = balanced_df.to_dict(orient="records")

    model = GenerativeModel("gemini-2.0-flash")

    prompt = f"""
You are a data analysis assistant.

Pre-balance:
- Target: {target_col}
- Type: {target_type}
- Distribution: {original_dist}
- Imbalance ratio: {imbalance_ratio_before}

Post-balance:
- Balancing method: {method}
- User preference: {user_pref}

Return STRICT JSON ONLY:
{{
  "summary": "",
  "notes": ""
}}
"""


    response = model.generate_content(prompt)
    raw_text = response.text.strip()

    match = re.search(r"\{[\s\S]*\}", raw_text)
    if not match:
        raise HTTPException(status_code=500, detail="LLM did not return valid JSON")

    json_str = match.group()

    try:
        result = json.loads(json_str)
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Invalid JSON from LLM")

    # if "balanced_data" not in result or not isinstance(result["balanced_data"], list):
    #     raise HTTPException(
    #         status_code=500,
    #         detail="LLM JSON missing 'balanced_data' array"
    #     )

    # balanced_df = pd.DataFrame(result["balanced_data"])

    if "summary" not in result or "notes" not in result:
        raise HTTPException(
            status_code=500,
            detail="LLM JSON missing summary or notes"
        )

    # 5️⃣ Post-balance analysis
    if target_type == "categorical":
        class_counts_after = balanced_df[target_col].value_counts()
        imbalance_ratio_after = round(
            class_counts_after.max() / max(class_counts_after.min(), 1), 2
        )

        analysis = {
            "target_column": target_col,
            "target_type": target_type,
            "class_distribution": {
                str(k): int(v) for k, v in class_counts_after.items()
            },
            "imbalance_ratio_after": float(imbalance_ratio_after),
            "balance_status": "Balanced" if imbalance_ratio_after <= 1.2 else "Imbalanced"
        }
    else:
        bins = pd.qcut(balanced_df[target_col], 4, duplicates="drop")
        class_counts_after = bins.value_counts()
        imbalance_ratio_after = round(class_counts_after.max() / class_counts_after.min(), 2)

        analysis = {
            "target_column": target_col,
            "target_type": target_type,
            "class_distribution": {str(k): int(v) for k, v in class_counts_after.items()},
            "imbalance_ratio_after": float(imbalance_ratio_after),
            "balance_status": "Balanced" if imbalance_ratio_after <= 1.2 else "Imbalanced"
        }

    # result["post_balance_analysis"] = analysis

    missing_cols = [c for c in df.columns if c not in balanced_df.columns]
    if missing_cols:
        raise HTTPException(
            status_code=500,
            detail=f"Balanced data missing columns: {missing_cols}"
        )

    return {
    "balanced_data_preview": df_to_json_safe(balanced_df.head(100)),
    "pre_balance_analysis": {
        "distribution": original_dist,
        "imbalance_ratio": float(imbalance_ratio_before)
    },
    "post_balance_analysis": analysis,
    "llm_summary": result
}

