"""
data_loader.py
--------------
Carga y organiza los datos desde merged_long_format.csv
agrupandolos por activo para su uso en el Requerimiento 3.

Se usa merged_long_format.csv porque:
  - Ya contiene todas las columnas OHLCV por activo en formato long.
  - Una fila por (fecha, activo), lo que facilita agrupar por ticker.
  - No requiere transformaciones adicionales de formato wide a long.
"""

import csv
import os
from collections import defaultdict
from datetime import datetime


def load_asset_series(merged_long_path):
    """
    Lee merged_long_format.csv y devuelve un dict con las series
    temporales de cada activo, ordenadas cronologicamente.

    Parametros:
        merged_long_path: ruta absoluta al archivo merged_long_format.csv

    Retorna:
        {
          "VOO": {
            "dates":  ["2019-01-02", "2019-01-03", ...],
            "opens":  [228.5, 224.0, ...],
            "highs":  [231.0, 225.1, ...],
            "lows":   [227.8, 222.5, ...],
            "closes": [229.99, 224.52, ...],
            "volumes":[3500000, 4200000, ...],
          },
          "AAPL": { ... },
          ...
        }
    """
    if not os.path.exists(merged_long_path):
        raise FileNotFoundError(
            "Archivo no encontrado: " + merged_long_path + "\n"
                                                           "Ejecuta el ETL Pipeline con merge_long_format() primero."
        )

    # Agrupar filas por ticker
    by_ticker = defaultdict(list)

    with open(merged_long_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker   = (row.get("Ticker") or "").strip()
            raw_date = (row.get("Date")   or "").strip()
            if not ticker or not raw_date:
                continue
            try:
                dt = datetime.strptime(raw_date, "%Y-%m-%d")
            except ValueError:
                continue
            try:
                close = float(row.get("Close") or "")
            except (ValueError, TypeError):
                continue

            def _float(val):
                try: return float(val)
                except: return None

            by_ticker[ticker].append({
                "date":   dt,
                "open":   _float(row.get("Open")),
                "high":   _float(row.get("High")),
                "low":    _float(row.get("Low")),
                "close":  close,
                "volume": _float(row.get("Volume")),
            })

    # Ordenar cada ticker por fecha y extraer listas separadas
    asset_series = {}
    for ticker, rows in by_ticker.items():
        rows.sort(key=lambda r: r["date"])
        asset_series[ticker] = {
            "dates":   [r["date"].strftime("%Y-%m-%d") for r in rows],
            "opens":   [r["open"]   for r in rows],
            "highs":   [r["high"]   for r in rows],
            "lows":    [r["low"]    for r in rows],
            "closes":  [r["close"]  for r in rows],
            "volumes": [r["volume"] for r in rows],
        }

    return asset_series