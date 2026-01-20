from fastapi import APIRouter
from app.api.services.leakage_detection_service import leakage_detection_service

router = APIRouter(
    prefix="",
    tags=["Leakage Detection"]
)

@router.post("/leakage_detection")
async def leakage_detection(dataset: str, table_name: str):
    return leakage_detection_service(dataset, table_name)
