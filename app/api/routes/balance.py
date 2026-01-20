from fastapi import APIRouter, HTTPException
from app.api.services.balance_service import balance_data_service, BalanceRequest

router = APIRouter(
    prefix="",
    tags=["Data Balancing"]
)

@router.post("/balance_data")
async def balance_data(request: BalanceRequest):
    return await balance_data_service(request)
