from fastapi import APIRouter
from app.api.services.eda_service import (
    analyze_service,
    get_eda_result_service,
    get_column_list,
    EDARequest
)

router = APIRouter(
    prefix="",
    tags=["EDA"]
)

@router.get("/column_list")
async def column_list(
    project_id: str,
    dataset_id: str,
    table_name: str
):
    return await get_column_list(project_id, dataset_id, table_name)


@router.post("/analyze")
async def analyze(request: EDARequest):
    return await analyze_service(request)

@router.get("/eda_result")
async def eda_result(request: EDARequest):
    return await get_eda_result_service(request)
