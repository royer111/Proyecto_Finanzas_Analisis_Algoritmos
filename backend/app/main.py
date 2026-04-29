"""
backend/app/main.py
Dashboard API — Requerimiento 4

Ejecutar con:
    uvicorn backend.app.main:app --reload --port 8000

O directamente como script:
    python -m uvicorn backend.app.main:app --reload --port 8000
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pathlib import Path

app = FastAPI(
    title="Dashboard Bursatil — Analisis de Algoritmos",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Importar routers — se hace aqui despues de crear 'app' para evitar
# importaciones circulares cuando los routers importan desde services.
# ---------------------------------------------------------------------------
from backend.app.routes.routes_dashboard import router as dashboard_router
from backend.app.routes.routes_export    import router as export_router

app.include_router(dashboard_router, prefix="/api/dashboard", tags=["dashboard"])
app.include_router(export_router,    prefix="/api/export",    tags=["export"])


@app.get("/api/health", tags=["health"])
def health():
    """Verifica que la API esta corriendo."""
    return {"status": "ok", "version": "1.0.0"}


# ---------------------------------------------------------------------------
# Servir el frontend estatico (opcional)
# Coloca index.html en frontend/ a la misma altura que backend/
# y accede desde http://localhost:8000/
# ---------------------------------------------------------------------------
_frontend = Path(__file__).resolve().parents[2] / "frontend"
if _frontend.exists():
    app.mount("/", StaticFiles(directory=str(_frontend), html=True), name="frontend")


# ---------------------------------------------------------------------------
# Punto de entrada para ejecucion directa (python backend/app/main.py)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.app.main:app", host="127.0.0.1", port=8000, reload=True)