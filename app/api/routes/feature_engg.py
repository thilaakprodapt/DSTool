from fastapi import APIRouter

from app.api.services.feature_engg_service import (
    save_fe_to_bq,
    FE_analyze_service,
    get_fe_result_service,
    FERequest
)

router = APIRouter(
    prefix="",
    tags=["Feature Engineering"]
)

@router.post("/FE_analyze")
async def fe_analyze(request: FERequest):
    return await FE_analyze_service(request)

@router.get("/fe_result")
async def get_fe_result(request: FERequest):
    return await get_fe_result_service(request)
