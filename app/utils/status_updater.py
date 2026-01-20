from google.cloud import bigquery
from app.core.config import bq_client, PROJECT_ID

WORKSPACE_details = f"{PROJECT_ID}.DS_details.Workspace_details"

def set_phase_status(bq_client: bigquery.Client, table_name: str, phase_col: str, status: str):
    sql = f"""
    UPDATE `{WORKSPACE_details}`
    SET {phase_col} = @status, Last_updated = CURRENT_DATETIME()
    WHERE table_name = @table_name
    """
    job = bq_client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("status", "STRING", status),
                bigquery.ScalarQueryParameter("table_name", "STRING", table_name),
            ]
        ),
    )
    job.result()