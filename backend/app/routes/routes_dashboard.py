"""
backend/app/routes/dashboard.py
Endpoints REST para el dashboard del Req. 4.
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from backend.app.services.dashboard_service import (
    get_correlation_matrix,
    get_candlestick_data,
    get_risk_ranking,
    get_patterns_summary,
    get_available_tickers,
    get_volatility_metrics,
)

router = APIRouter(tags=["dashboard"])


@router.get("/tickers")
def tickers():
    """Lista de todos los tickers disponibles."""
    return get_available_tickers()


@router.get("/correlation")
def correlation():
    """Matriz de correlacion n×n (Pearson sobre retornos log)."""
    try:
        return get_correlation_matrix()
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/candlestick/{ticker}")
def candlestick(
        ticker: str,
        sma_windows: str = Query("20,50,200", description="Ventanas SMA separadas por coma"),
        last_n: Optional[int] = Query(None, description="Ultimos N dias (None = todos)")
):
    """
    Datos OHLCV + medias moviles simples para un activo.
    sma_windows: '20,50,200'  — ventanas separadas por coma.
    """
    try:
        windows = [int(w.strip()) for w in sma_windows.split(",") if w.strip()]
        data = get_candlestick_data(ticker.upper(), sma_windows=windows)
        if last_n:
            for key in ("dates","opens","highs","lows","closes","volumes"):
                data[key] = data[key][-last_n:]
            for k in data["sma"]:
                data["sma"][k] = data["sma"][k][-last_n:]
        return data
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/risk-ranking")
def risk_ranking():
    """Listado de activos ordenado por volatilidad (del Req. 3)."""
    return get_risk_ranking()


@router.get("/patterns")
def patterns():
    """Frecuencia de patrones detectados por activo (del Req. 3)."""
    return get_patterns_summary()


@router.get("/volatility")
def volatility():
    """Metricas de volatilidad historica por activo (del Req. 3)."""
    return get_volatility_metrics()