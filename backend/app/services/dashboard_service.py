"""
backend/app/services/dashboard_service.py
Calcula todos los datos necesarios para el dashboard del Req. 4.

Reutiliza las clases ya implementadas en el proyecto:
  - PearsonCorrelation  → matriz de correlacion (Req. similitud)
  - compute_log_returns → retornos logaritmicos  (Req. 3 volatility_service)
  - compute_annualized_volatility → volatilidad  (Req. 3 volatility_service)

Algoritmos propios de este servicio:
  - SMA con suma deslizante O(n)
  - Carga y parsing de CSVs
"""

import csv
import math
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# ─── Ruta base del proyecto ───────────────────────────────────────────────
# dashboard_service.py esta en: Proyecto/backend/app/services/dashboard_service.py
# parents[0] = backend/app/services
# parents[1] = backend/app
# parents[2] = backend
# parents[3] = Proyecto_Finanzas  <-- raiz del proyecto
BASE = Path(__file__).resolve().parents[3]

MERGED_PRICES = BASE / "data" / "merged" / "merged_prices.csv"
MERGED_LONG   = BASE / "data" / "merged" / "merged_long_format.csv"
REQ3_OUTPUT   = BASE / "backend" / "app" / "model" / "output" / "requerimiento_3"

# ─── Reutilizar clases ya implementadas ──────────────────────────────────
# PearsonCorrelation del Req. de similitud
from backend.app.algorithms.similarity.pearson_correlation import PearsonCorrelation

# compute_log_returns y compute_annualized_volatility del Req. 3
from backend.app.algorithms.volatility.volatility_service import (
    compute_log_returns,
    compute_annualized_volatility,
)

_pearson_calculator = PearsonCorrelation()


# ===========================================================================
# CARGA DE DATOS
# ===========================================================================

def _load_prices():
    """
    Lee merged_prices.csv (wide: Date + columnas de tickers).
    Retorna (dates, tickers, series) ordenados cronologicamente.
    """
    if not MERGED_PRICES.exists():
        raise FileNotFoundError(
            f"No se encontro: {MERGED_PRICES}\n"
            f"BASE detectada como: {BASE}\n"
            "Verifica que data/merged/merged_prices.csv exista en la raiz del proyecto."
        )

    rows = []
    with open(MERGED_PRICES, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        tickers = [
            h.strip() for h in reader.fieldnames
            if h.strip().lower() not in ("date", "fecha")
        ]
        for row in reader:
            raw = (row.get("Date") or row.get("date") or "").strip()
            try:
                dt = datetime.strptime(raw, "%Y-%m-%d").date()
            except ValueError:
                continue
            rec = {"date": dt}
            for t in tickers:
                try:
                    rec[t] = float(row[t])
                except (ValueError, TypeError):
                    rec[t] = None
            rows.append(rec)

    rows.sort(key=lambda r: r["date"])
    dates  = [r["date"].isoformat() for r in rows]
    series = {t: [r[t] for r in rows] for t in tickers}
    return dates, tickers, series


def _load_long_format():
    """
    Lee merged_long_format.csv (long: Date, Ticker, Open, High, Low, Close, Volume).
    Retorna {ticker: {dates, opens, highs, lows, closes, volumes}}.
    """
    if not MERGED_LONG.exists():
        raise FileNotFoundError(
            f"No se encontro: {MERGED_LONG}\n"
            "Ejecuta merge_long_format() en el ETL Pipeline primero."
        )

    by_ticker = defaultdict(list)
    with open(MERGED_LONG, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = (row.get("Ticker") or "").strip()
            raw    = (row.get("Date")   or "").strip()
            if not ticker or not raw:
                continue
            try:
                dt = datetime.strptime(raw, "%Y-%m-%d").date()
            except ValueError:
                continue

            def _flt(k):
                try:
                    return float(row.get(k) or "")
                except (ValueError, TypeError):
                    return None

            close = _flt("Close")
            if close is None:
                continue

            by_ticker[ticker].append({
                "date":   dt,
                "open":   _flt("Open"),
                "high":   _flt("High"),
                "low":    _flt("Low"),
                "close":  close,
                "volume": _flt("Volume"),
            })

    result = {}
    for ticker, rows in by_ticker.items():
        rows.sort(key=lambda r: r["date"])
        result[ticker] = {
            "dates":   [r["date"].isoformat() for r in rows],
            "opens":   [r["open"]   for r in rows],
            "highs":   [r["high"]   for r in rows],
            "lows":    [r["low"]    for r in rows],
            "closes":  [r["close"]  for r in rows],
            "volumes": [r["volume"] for r in rows],
        }
    return result


# ===========================================================================
# MATRIZ DE CORRELACION
# Reutiliza PearsonCorrelation y compute_log_returns ya implementados
# ===========================================================================

def get_correlation_matrix():
    """
    Construye la matriz de correlacion n×n usando:
      - compute_log_returns() del Req. 3 (retornos logaritmicos)
      - PearsonCorrelation.calculate() del Req. de similitud

    Complejidad: O(n^2 * T) donde n = activos, T = dias.
    Retorna: { tickers: [...], matrix: [[...], ...] }
    """
    dates, tickers, series = _load_prices()

    # Calcular retornos logaritmicos reutilizando compute_log_returns del Req. 3
    returns = {}
    for t in tickers:
        closes_validos = [v for v in series[t] if v is not None]
        returns[t] = compute_log_returns(closes_validos)

    # Alinear longitudes al minimo comun
    min_len = min(len(v) for v in returns.values())
    for t in tickers:
        returns[t] = returns[t][:min_len]

    # Construir matriz usando PearsonCorrelation del Req. de similitud
    matrix = []
    for ti in tickers:
        row = []
        for tj in tickers:
            try:
                # Reutiliza la clase PearsonCorrelation ya implementada
                val = _pearson_calculator.calculate(returns[ti], returns[tj])
                row.append(round(val, 4))
            except (ValueError, ZeroDivisionError):
                row.append(0.0)
        matrix.append(row)

    return {"tickers": tickers, "matrix": matrix}


# ===========================================================================
# SMA CON SUMA DESLIZANTE — algoritmo propio del dashboard
# (no existia en otras clases; los otros reqs no calculaban SMA)
# ===========================================================================

def _sma(closes, window):
    """
    Media movil simple con suma deslizante O(n).
    Retorna lista de len(closes); None donde no hay suficientes datos.
    """
    n      = len(closes)
    result = [None] * n

    if sum(1 for c in closes if c is not None) < window:
        return result

    win_sum = sum(closes[i] for i in range(window) if closes[i] is not None)
    count   = sum(1 for i in range(window) if closes[i] is not None)

    if count == window:
        result[window - 1] = round(win_sum / window, 4)

    for i in range(window, n):
        old = closes[i - window]
        new = closes[i]
        if old is not None:
            win_sum -= old
            count   -= 1
        if new is not None:
            win_sum += new
            count   += 1
        if count == window:
            result[i] = round(win_sum / window, 4)

    return result


def get_candlestick_data(ticker, sma_windows=None):
    """
    Retorna datos OHLCV + medias moviles simples para un ticker.
    sma_windows: lista de enteros, por defecto [20, 50, 200].
    Complejidad: O(n * len(sma_windows)).
    """
    if sma_windows is None:
        sma_windows = [20, 50, 200]

    long_data = _load_long_format()
    if ticker not in long_data:
        raise ValueError(
            f"Ticker '{ticker}' no encontrado en merged_long_format.csv. "
            f"Disponibles: {sorted(long_data.keys())}"
        )

    s      = long_data[ticker]
    closes = s["closes"]

    sma_data = {f"sma{w}": _sma(closes, w) for w in sma_windows}

    return {
        "ticker":  ticker,
        "dates":   s["dates"],
        "opens":   s["opens"],
        "highs":   s["highs"],
        "lows":    s["lows"],
        "closes":  closes,
        "volumes": s["volumes"],
        "sma":     sma_data,
    }


# ===========================================================================
# LECTURA DE RESULTADOS DEL REQ. 3
# ===========================================================================

def _read_req3_csv(filename, row_parser):
    """Helper: lee un CSV del Req. 3 y parsea cada fila con row_parser."""
    path = REQ3_OUTPUT / filename
    if not path.exists():
        return {
            "error": f"{filename} no encontrado en {REQ3_OUTPUT}. Ejecuta el Req. 3 primero.",
            "data":  []
        }
    data = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                data.append(row_parser(row))
            except (ValueError, KeyError):
                continue
    return {"data": data}


def get_risk_ranking():
    """Lee risk_ranking.csv generado por el Req. 3."""
    return _read_req3_csv("risk_ranking.csv", lambda row: {
        "position":   int(row.get("Posicion", 0)),
        "ticker":     row.get("Ticker", ""),
        "volatility": float(row.get("Volatilidad Anualizada (%)", 0)),
        "category":   row.get("Categoria de Riesgo", ""),
        "risk_score": int(row.get("Risk Score", 0)),
    })


def get_patterns_summary():
    """Lee patterns_summary.csv generado por el Req. 3."""
    return _read_req3_csv("patterns_summary.csv", lambda row: {
        "ticker":      row.get("Ticker", ""),
        "pattern":     row.get("Patron", ""),
        "window":      row.get("Ventana", ""),
        "occurrences": int(row.get("Ocurrencias", 0)),
        "frequency":   float(row.get("Frecuencia", 0)),
    })


def get_volatility_metrics():
    """Lee volatility_metrics.csv generado por el Req. 3."""
    return _read_req3_csv("volatility_metrics.csv", lambda row: {
        "ticker":     row.get("Ticker", ""),
        "daily_std":  float(row.get("Desv. Estandar Diaria", 0)),
        "annual_vol": float(row.get("Volatilidad Anualizada", 0)),
        "n_returns":  int(row.get("N Retornos", 0)),
    })


def get_available_tickers():
    """Retorna la lista de tickers disponibles en merged_prices.csv."""
    try:
        _, tickers, _ = _load_prices()
        return {"tickers": tickers}
    except Exception as e:
        return {"tickers": [], "error": str(e)}


# ===========================================================================
# SIMILITUD ENTRE DOS ACTIVOS — llama a ServiceOrchestrator ya existente
# Exporta CSV en REQ2_OUTPUT / similarity_{a}_{b}_{type}.csv
# ===========================================================================

# Ruta de salida para los CSVs de similitud (Req. 2 output)
REQ2_OUTPUT = BASE / "backend" / "app" / "model" / "output" / "requerimiento_2"


def get_similarity(asset_a: str, asset_b: str, series_type: str = "prices") -> dict:
    """
    Compara dos activos usando los 4 algoritmos de similitud del proyecto:
      Euclidean, Pearson, Cosine y DTW.

    Delega completamente en ServiceOrchestrator.compare_assets() que ya
    esta implementado y probado. Esta funcion solo:
      1. Instancia el orquestador y llama a compare_assets().
      2. Exporta el resultado a un CSV en REQ2_OUTPUT.
      3. Retorna el JSON enriquecido con csv_path.

    Parametros:
        asset_a:     ticker del primer activo  (ej: "VOO")
        asset_b:     ticker del segundo activo (ej: "NVDA")
        series_type: "prices" (precios de cierre) o "returns" (retornos log)

    Retorna:
        {
          "asset_a":     "VOO",
          "asset_b":     "NVDA",
          "series_type": "prices",
          "dates":       ["2019-01-02", ...],   # fechas alineadas
          "series_a":    [229.99, ...],          # valores activo A
          "series_b":    [3.40, ...],            # valores activo B
          "metrics": {
              "euclidean": 1842.3,    # distancia — menor = mas similar
              "pearson":   0.8821,    # similitud  — mas cercano a 1 = mas similar
              "cosine":    0.9934,    # similitud  — mas cercano a 1 = mas similar
              "dtw":       38.21,     # distancia  — menor = mas similar
          },
          "metric_types": {
              "euclidean": "distance",
              "pearson":   "similarity",
              "cosine":    "similarity",
              "dtw":       "distance",
          },
          "csv_path": "backend/app/model/output/requerimiento_2/similarity_VOO_NVDA_prices.csv"
        }

    Lanza:
        ValueError      si un ticker no existe en las series alineadas.
        FileNotFoundError si merged_prices.csv no existe.
        Exception       si ServiceOrchestrator falla por cualquier razon.
    """
    # 1. Llamar a ServiceOrchestrator — reutiliza todo el codigo existente
    #    ServiceOrchestrator → SimilarityService → TimeSeries → algoritmos
    from backend.app.services.service_orchestrator import ServiceOrchestrator

    orchestrator = ServiceOrchestrator()
    result = orchestrator.compare_assets(asset_a, asset_b, series_type=series_type)
    # result tiene: { dates, series_a, series_b, metrics }

    dates    = result.get("dates",    [])
    series_a = result.get("series_a", [])
    series_b = result.get("series_b", [])
    metrics  = result.get("metrics",  {})

    if not dates:
        raise ValueError(
            f"No se encontraron fechas alineadas para {asset_a} y {asset_b}. "
            "Verifica que ambos activos existan en merged_prices.csv."
        )

    # 2. Exportar CSV con fechas, ambas series y las 4 metricas como columnas
    csv_path = _export_similarity_csv(
        asset_a, asset_b, series_type,
        dates, series_a, series_b, metrics
    )

    # 3. Tipos de cada metrica (distancia o similitud) para que el front
    #    pueda interpretar correctamente si mayor/menor es mejor
    metric_types = {
        "euclidean": "distance",
        "pearson":   "similarity",
        "cosine":    "similarity",
        "dtw":       "distance",
    }

    return {
        "asset_a":      asset_a,
        "asset_b":      asset_b,
        "series_type":  series_type,
        "dates":        [str(d) for d in dates],
        "series_a":     series_a,
        "series_b":     series_b,
        "metrics":      metrics,
        "metric_types": metric_types,
        "csv_path":     str(csv_path),
    }


def _export_similarity_csv(
        asset_a: str, asset_b: str, series_type: str,
        dates, series_a, series_b, metrics: dict
) -> str:
    """
    Exporta los resultados de similitud a un CSV en REQ2_OUTPUT.

    Estructura del CSV:
        Date, {asset_a}, {asset_b}, Euclidean, Pearson, Cosine, DTW

    Las metricas se repiten en todas las filas porque son valores globales
    del par de series completas — esto facilita leerlos con cualquier
    herramienta sin necesidad de parseo especial.

    Nombre del archivo: similarity_{asset_a}_{asset_b}_{series_type}.csv
    Ejemplo:            similarity_VOO_NVDA_prices.csv

    Retorna la ruta del archivo creado como string.
    """
    REQ2_OUTPUT.mkdir(parents=True, exist_ok=True)

    filename = f"similarity_{asset_a}_{asset_b}_{series_type}.csv"
    filepath = REQ2_OUTPUT / filename

    euclidean = round(metrics.get("euclidean", 0), 6)
    pearson   = round(metrics.get("pearson",   0), 6)
    cosine    = round(metrics.get("cosine",    0), 6)
    dtw       = round(metrics.get("dtw",       0), 6)

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        # Encabezado
        writer.writerow([
            "Date",
            asset_a,
            asset_b,
            "Euclidean",
            "Pearson",
            "Cosine",
            "DTW",
        ])

        # Una fila por fecha con los valores de ambas series
        # Las metricas se incluyen solo en la primera fila; las demas quedan vacias
        # para no inflar el archivo con valores repetidos
        for i, (date, val_a, val_b) in enumerate(zip(dates, series_a, series_b)):
            if i == 0:
                writer.writerow([
                    str(date),
                    round(val_a, 6) if val_a is not None else "",
                    round(val_b, 6) if val_b is not None else "",
                    euclidean, pearson, cosine, dtw,
                ])
            else:
                writer.writerow([
                    str(date),
                    round(val_a, 6) if val_a is not None else "",
                    round(val_b, 6) if val_b is not None else "",
                    "", "", "", "",
                ])

    return str(filepath)
