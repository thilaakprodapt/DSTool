import json
import uuid
import tempfile
import os
from datetime import datetime, timezone

from fastapi import UploadFile, File, Form
from google.cloud import bigquery

from app.utils.biqguery_utils import (
    get_static_bq_client,
    get_bq_client_from_sa_file
)

# -------------------------------------------------------------------
# Constants & Clients
# -------------------------------------------------------------------

# Import from config.py (uses service account credentials)
from app.core.config import PROJECT_ID, bq_client

# Static BigQuery client (used for metadata tables)
bq_read_client = bq_client

# -------------------------------------------------------------------
# Services
# -------------------------------------------------------------------

async def check_bigquery_connection_service(
    service_account_file: UploadFile = File(...),
    project_id: str = Form(...),
):
    tmp_path = None
    try:
        # Save uploaded service account file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp_file:
            contents = await service_account_file.read()
            tmp_file.write(contents)
            tmp_path = tmp_file.name

        # Validate connection
        client = get_bq_client_from_sa_file(tmp_path, project_id)
        
        # Test connection by listing datasets
        list(client.list_datasets())  #review

        return {
            "status": "success",
            "details": "BigQuery connection successful."
        }

    except Exception as e:
        return {
            "status": "failed",
            "details": f"BigQuery connection failed: {str(e)}"
        }

    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)



# -------------------------------------------------------------------

async def save_connection_service(
    service_account_file: UploadFile = File(...),
    connection_name: str = Form(...),
    project_id: str = Form(...),
):
    """
    Save a BigQuery connection with service account credentials.
    This endpoint validates the connection and stores it in the Connection_details table.
    """
    tmp_path = None
    try:
        # Save uploaded service account file temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp_file:
            contents = await service_account_file.read()
            tmp_file.write(contents)
            tmp_path = tmp_file.name

        # Parse JSON for validation
        service_account_content = json.loads(contents.decode("utf-8"))

        # Create BigQuery client to validate connection
        client = get_bq_client_from_sa_file(tmp_path, project_id)

        # Validate connection by listing datasets
        datasets = list(client.list_datasets())   #review2
        schema_names = [dataset.dataset_id for dataset in datasets]  #review2

        # ✅ Target table in DS_details dataset for storing connection details
        table_id = f"{PROJECT_ID}.DS_details.Connection_details"

        # Generate unique ID
        unique_id = str(uuid.uuid4())

        # Prepare data row
        row = {
            "ID": unique_id,
            "connection_name": connection_name,
            "project_id": project_id,
            "service_account": json.dumps(service_account_content),
            "schema_list": json.dumps(schema_names),
        }

        # Use batch load instead of streaming insert
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=False,
        )

        load_job = bq_read_client.load_table_from_json(
            json_rows=[row],
            destination=table_id,
            job_config=job_config
        )

        load_job.result()  # Wait for batch job to complete

        return {
            "status": "success",
            "details": f"Connection '{connection_name}' saved successfully.",
            "connection_name": connection_name,
            "project_id": project_id,
            "schema_count": len(schema_names),
            "batch_job_id": load_job.job_id
        }

    except Exception as e:
        return {
            "status": "failed",
            "details": str(e)
        }

    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)



# -------------------------------------------------------------------

async def get_schemas_service(connection_name: str):
    """
    Fetch the list of schemas (datasets) for a saved connection.
    
    Args:
        connection_name: The name of the saved connection
        
    Returns:
        List of schema names available in the connection
    """
    try:
        # Query to get schema_list from Connection_details
        query = f"""
            SELECT schema_list, project_id
            FROM `{PROJECT_ID}.DS_details.Connection_details`
            WHERE connection_name = @connection_name
            LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("connection_name", "STRING", connection_name)
            ]
        )
        
        query_job = bq_read_client.query(query, job_config=job_config)
        result = list(query_job.result())

        if not result:
            return {
                "status": "failed",
                "details": f"No connection found with name '{connection_name}'."
            }

        schema_list = json.loads(result[0]["schema_list"])
        project_id = result[0]["project_id"]

        return {
            "status": "success",
            "details": f"Found {len(schema_list)} schemas for connection '{connection_name}'.",
            "connection_name": connection_name,
            "project_id": project_id,
            "schemas": schema_list
        }

    except Exception as e:
        return {
            "status": "failed",
            "details": str(e)
        }

# -------------------------------------------------------------------

async def save_connection_and_get_schemas_service(
    service_account_file: UploadFile = File(...),
    connection_name: str = Form(...),
    project_id: str = Form(...),
):
    tmp_path = None
    try:
        # Save uploaded service account file temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp_file:
            contents = await service_account_file.read()
            tmp_file.write(contents)
            tmp_path = tmp_file.name

        # Parse JSON for validation
        service_account_content = json.loads(contents.decode("utf-8"))

        # Create BigQuery client
        client = get_bq_client_from_sa_file(tmp_path, project_id)

        # ✅ Get list of all datasets (schemas) from the project
        datasets = list(client.list_datasets())
        schema_names = [dataset.dataset_id for dataset in datasets]

        # ✅ Target table in DS_details dataset for storing connection details
        table_id = f"{project_id}.DS_details.Connection_details"

        # Generate unique ID
        unique_id = str(uuid.uuid4())

        # Prepare data row
        schema_list_str = json.dumps(schema_names)
        
        row = {
            "ID": unique_id,
            "connection_name": connection_name,
            "project_id": project_id,
            "service_account": json.dumps(service_account_content),
            "schema_list": schema_list_str,
        }

        # Use batch load instead of streaming insert
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=False,
        )

        load_job = bq_read_client.load_table_from_json(
            json_rows=[row],
            destination=table_id,
            job_config=job_config
        )

        load_job.result()  # Wait for batch job to complete

        return {
            "status": "success",
            "details": f"Connection saved successfully with {len(schema_names)} schemas.",
            "schemas": schema_names,
            "batch_job_id": load_job.job_id
        }

    except Exception as e:
        return {
            "status": "failed",
            "details": str(e)
        }

    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)

# -------------------------------------------------------------------

async def get_tables_and_save_schema_service(
    connection_name: str = Form(...),
    schema_id: str = Form(...),
):
    """
    Fetch the table list for a given connection_name and schema_id,
    save it to Table_details table, and return the list.
    """
    try:
        # ✅ Query to get service account details and project_id
        query = f"""
            SELECT service_account, project_id, schema_list
            FROM `{PROJECT_ID}.DS_details.Connection_details`
            WHERE connection_name = @connection_name
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("connection_name", "STRING", connection_name)
            ]
        )
        query_job = bq_read_client.query(query, job_config=job_config)
        result = list(query_job.result())

        if not result:
            return {
                "status": "failed",
                "details": f"No connection found with name '{connection_name}'."
            }

        service_account_json = json.loads(result[0]["service_account"])
        project_id = result[0]["project_id"]
        schema_list = json.loads(result[0]["schema_list"])

        # ✅ Verify schema_id exists
        if schema_id not in schema_list:
            return {
                "status": "failed",
                "details": f"Schema '{schema_id}' not found in connection '{connection_name}'."
            }

        # Write service account to temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode="w") as tmp_file:
            json.dump(service_account_json, tmp_file)
            tmp_path = tmp_file.name

        try:
            client = get_bq_client_from_sa_file(tmp_path, project_id)
            
            # ✅ List all tables in the specified schema/dataset
            tables = client.list_tables(schema_id)
            table_names = [table.table_id for table in tables]

            # ✅ Save to Table_details table
            table_details_table = f"{project_id}.DS_details.Table_details"
            
            table_row = {
                "id": str(uuid.uuid4()),
                "connection_name": connection_name,
                "schema_id": schema_id,
                "table_list": json.dumps(table_names),
            }
            
            errors = bq_read_client.insert_rows_json(table_details_table, [table_row])
            if errors:
                return {
                    "status": "failed",
                    "details": f"Failed to save table list: {errors}"
                }

            return {
                "status": "success",
                "details": f"Found {len(table_names)} tables in schema '{schema_id}'.",
                "tables": table_names
            }
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    except Exception as e:
        return {
            "status": "failed",
            "details": str(e)
        }

# -------------------------------------------------------------------

async def get_all_connections_service():
    """
    Fetch all connection names from the Connection_details table
    using the static service account credentials.
    """
    try:
        query = f"""
            SELECT connection_name 
            FROM `{PROJECT_ID}.DS_details.Connection_details`
        """
        query_job = bq_read_client.query(query)
        results = [row["connection_name"] for row in query_job.result()]

        return {
            "status": "success",
            "details": f"Found {len(results)} connections.",
            "connections": results
        }

    except Exception as e:
        return {"status": "failed", "details": str(e)}

# -------------------------------------------------------------------

async def add_workspace_service(table_names: str):
    """
    Insert one or more workspace entries into Workspace_details table
    using static service account credentials.
    Uses LOAD JOB instead of STREAMING INSERT (fixes update/merge issues).
    """

    try:
        # BigQuery table reference
        table_id = f"{PROJECT_ID}.DS_details.Workspace_details"

        # Split comma-separated input
        table_list = [t.strip() for t in table_names.split(",") if t.strip()]

        if not table_list:
            return {"status": "failed", "details": "No valid table names provided."}

        # Fetch existing table names
        query = f"SELECT DISTINCT table_name FROM `{table_id}`"
        existing = {row.table_name for row in bq_read_client.query(query)}

        new_tables = []
        duplicate_tables = []

        for name in table_list:
            if name in existing:
                duplicate_tables.append(name)
            else:
                new_tables.append(name)

        # Prepare rows for load job
        rows_to_insert = [
            {
                "id": str(uuid.uuid4()),
                "table_name": name,
                "status": "pending",
                "Last_updated": datetime.utcnow().isoformat(),
                "EDA_status": "pending",
                "FE_status": "pending",
                "DAG_status": "pending",
                "Leakage_status": "pending",
                "report_status": "pending"
            }
            for name in new_tables
        ]

        # ---- IMPORTANT FIX ----
        # Use load_table_from_json → NOT streaming insert_rows_json
        # ------------------------
        errors = None
        if rows_to_insert:
            load_job = bq_read_client.load_table_from_json(
                rows_to_insert,
                table_id
            )
            load_job.result()  # wait until load completes (no streaming buffer)

        # Prepare response
        response = {"status": "success"}

        if new_tables:
            response["new_tables_added"] = new_tables
            response["count_added"] = len(new_tables)
            response["message"] = f"Successfully added {len(new_tables)} new table(s)."

        if duplicate_tables:
            response["duplicate_tables"] = duplicate_tables
            response["count_duplicates"] = len(duplicate_tables)
            response["message"] = (
                f"{response.get('message', '')} "
                f"{len(duplicate_tables)} table(s) already exist."
            )

        if not new_tables and duplicate_tables:
            response["status"] = "no_action"
            response["message"] = "All tables already exist in workspace."

        return response

    except Exception as e:
        return {"status": "failed", "details": str(e)}


# -------------------------------------------------------------------

async def get_all_workspaces_service():
    """
    Fetch all table names, status, and last updated timestamps
    from the Workspace_details table with human-readable time.
    """
    try:
        table_id = f"{PROJECT_ID}.DS_details.Workspace_details"

        query = f"""
            SELECT table_name, status, Last_updated
            FROM `{table_id}`
            ORDER BY Last_updated DESC
        """

        query_job = bq_read_client.query(query)
        results = list(query_job.result())

        if not results:
            return {
                "status": "failed",
                "details": "No workspace entries found."
            }

        def time_ago(updated_time):
            if not updated_time:
                return None

            # Convert to UTC-aware datetime if not already
            if updated_time.tzinfo is None:
                updated_time = updated_time.replace(tzinfo=timezone.utc)

            now = datetime.now(timezone.utc)
            diff = now - updated_time
            seconds = diff.total_seconds()

            if seconds < 60:
                return f"{int(seconds)} seconds ago"
            elif seconds < 3600:
                minutes = int(seconds // 60)
                return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
            elif seconds < 86400:
                hours = int(seconds // 3600)
                return f"{hours} hour{'s' if hours != 1 else ''} ago"
            elif seconds < 604800:
                days = int(seconds // 86400)
                return f"{days} day{'s' if days != 1 else ''} ago"
            else:
                return updated_time.strftime("%Y-%m-%d")

        workspace_list = [
            {
                "table_name": row["table_name"],
                "status": row["status"],
                "last_updated": time_ago(row["Last_updated"])
            }
            for row in results
        ]

        return {
            "status": "success",
            "details": f"Found {len(workspace_list)} workspace entries.",
            "workspaces": workspace_list
        }

    except Exception as e:
        return {"status": "failed", "details": str(e)}
    


# -------------------------------------------------------------------

async def get_workspace_status_service(table_name: str):
    """
    Fetch the EDA, FE, DAG, Leakage, and Report statuses 
    for a given table_name from the Workspace_details table.
    """
    try:
        # ✅ BigQuery table reference
        table_id = f"{PROJECT_ID}.DS_details.Workspace_details"

        # ✅ Query to get the status fields
        query = f"""
            SELECT 
                EDA_status,
                FE_status,
                DAG_status,
                Leakage_status,
                report_status
            FROM `{table_id}`
            WHERE table_name = @table_name
            LIMIT 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("table_name", "STRING", table_name)
            ]
        )

        query_job = bq_read_client.query(query, job_config=job_config)
        results = list(query_job.result())

        # ✅ If no matching table found
        if not results:
            return {
                "status": "failed",
                "details": f"No workspace entry found for table '{table_name}'."
            }

        row = results[0]

        # ✅ Prepare response
        data = {
            "EDA_status": row["EDA_status"],
            "FE_status": row["FE_status"],
            "DAG_status": row["DAG_status"],
            "Leakage_status": row["Leakage_status"],
            "report_status": row["report_status"]
        }

        return {
            "status": "success",
            "details": f"Fetched workflow statuses for '{table_name}'.",
            "data": data
        }

    except Exception as e:
        return {"status": "failed", "details": str(e)}