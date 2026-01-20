from fastapi import APIRouter, UploadFile, File, Form
from app.api.services.dashboard_services import (
    check_bigquery_connection_service,
    save_connection_service,
    get_schemas_service,
    save_connection_and_get_schemas_service,
    get_tables_and_save_schema_service,
    get_all_connections_service,
    add_workspace_service,
    get_all_workspaces_service,
    get_workspace_status_service,
)

router = APIRouter(
    prefix="",
    tags=["Dashboard"]
)

# 1️⃣ Check BigQuery
@router.post("/check_bigquery/")
async def check_bigquery_connection(
    service_account_file: UploadFile = File(...),
    project_id: str = Form(...)
):
    return await check_bigquery_connection_service(service_account_file, project_id)


# 2️⃣ Save Connection
@router.post("/save_connection/")
async def save_connection(
    service_account_file: UploadFile = File(...),
    connection_name: str = Form(...),
    project_id: str = Form(...)
):
    return await save_connection_service(
        service_account_file,
        connection_name,
        project_id
    )


# 3️⃣ Get Schemas
@router.get("/get_schemas/")
async def get_schemas(connection_name: str):
    return await get_schemas_service(connection_name)


# 4️⃣ Save Connection & Get Schemas
@router.post("/save_connection_and_get_schemas/")
async def save_connection_and_get_schemas(
    service_account_file: UploadFile = File(...),
    connection_name: str = Form(...),
    project_id: str = Form(...)
):
    return await save_connection_and_get_schemas_service(
        service_account_file,
        connection_name,
        project_id
    )


# 5️⃣ Get Tables and Save Schema
@router.post("/get_tables_and_save_schema/")
async def get_tables_and_save_schema(
    connection_name: str = Form(...),
    schema_id: str = Form(...)
):
    return await get_tables_and_save_schema_service(
        connection_name,
        schema_id
    )


# 6️⃣ Get Connections
@router.get("/get_connections/")
async def get_all_connections():
    return await get_all_connections_service()


# 7️⃣ Add Workspace
@router.post("/add_workspace/")
async def add_workspace(table_names: str = Form(...)):
    return await add_workspace_service(table_names)


# 8️⃣ Get All Workspaces
@router.get("/get_all_workspaces/")
async def get_all_workspaces():
    return await get_all_workspaces_service()


# 9️⃣ Get Workspace Status
@router.get("/get_workspace_status/")
async def get_workspace_status(table_name: str):
    return await get_workspace_status_service(table_name)
