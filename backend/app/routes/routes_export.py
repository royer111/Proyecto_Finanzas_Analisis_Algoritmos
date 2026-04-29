"""
backend/app/routes/routes_export.py
Endpoint para exportar el reporte PDF consolidado del Req. 4.
"""

import os
import tempfile
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

# El router DEBE llamarse 'router' para que main.py pueda importarlo.
router = APIRouter()


class PDFRequest(BaseModel):
    tickers: Optional[List[str]] = ["NVDA", "TSLA", "VOO", "GLD"]


@router.post("/pdf")
def export_pdf(body: PDFRequest):
    """
    Genera y descarga el reporte tecnico PDF consolidado.
    Incluye: portada, heatmap de correlacion, risk ranking,
    frecuencia de patrones, scatter de volatilidad y candlesticks.
    """
    try:
        from backend.app.services.pdf_service import generate_pdf_report

        # Archivo temporal para el PDF
        tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
        tmp.close()

        tickers = body.tickers or ["NVDA", "TSLA", "VOO", "GLD"]

        output_path = generate_pdf_report(
            output_path=tmp.name,
            candlestick_tickers=tickers[:4],   # maximo 4 tickers
        )

        return FileResponse(
            path=output_path,
            media_type="application/pdf",
            filename="reporte_tecnico_bursatil.pdf",
            headers={
                "Content-Disposition": 'attachment; filename="reporte_tecnico_bursatil.pdf"'
            },
        )

    except FileNotFoundError as e:
        raise HTTPException(
            status_code=404,
            detail=f"Archivo de datos no encontrado: {str(e)}. "
                   "Asegurate de haber ejecutado el ETL y el Req. 3 primero."
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error generando PDF: {str(e)}"
        )