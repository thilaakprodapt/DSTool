from fastapi import APIRouter
from app.api.services.reports_services import (
    column_report_service,
    get_table_data_service,
    TableRequest
)

router = APIRouter(
    prefix="",
    tags=["Reports"]
)

@router.post("/consolidated_report")
async def consolidated_report(request: TableRequest):
    return get_table_data_service(request)

@router.post("/Aggregated_column_report")
async def aggregated_column_report(request: TableRequest):
    return column_report_service(request)
