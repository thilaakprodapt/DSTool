from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes.dashboard import router as dashboard_router
from app.api.routes.eda import router as eda_router
from app.api.routes.feature_engg import router as fe_router
from app.api.routes.leakage_detection import router as leakage_router
from app.api.routes.reports import router as reports_router
from app.api.routes.dag import router as dag_router
from app.api.routes.balance import router as balance_router

app = FastAPI(
    title="Data Science Assistant Tool"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(dashboard_router, prefix="/Dashboard")
app.include_router(eda_router, prefix="/EDA")
app.include_router(fe_router, prefix="/Feature Engineering")
app.include_router(balance_router, prefix="/DataBalancing")
app.include_router(leakage_router, prefix="/Leakage Detection")
app.include_router(reports_router, prefix="/Reports")
app.include_router(dag_router, prefix="/Transformation")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app,host="0.0.0.0", port=8001)