from fastapi import APIRouter
from app.api.services.dag_service import (
    generate_dag_service,
    save_dag_service,
    fe_chat_check_service,
    SaveDAGRequest
)

router = APIRouter(
    prefix="",
    tags=["Transformation"]
)

@router.post("/generate_dag")
async def generate_dag(input: dict):
    return await generate_dag_service(input)

@router.post("/save_dag")
async def save_dag(request: SaveDAGRequest):
    # save_dag_service is sync, don't await it
    return save_dag_service(request)

@router.post("/fe_chat_check")
async def fe_chat_check(input: str):
    # fe_chat_check_service is sync, don't await it
    return fe_chat_check_service(input)
