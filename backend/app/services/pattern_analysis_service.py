"""
pattern_analysis_service.py
----------------------------
Servicio de orquestacion para el Requerimiento 3.

Responsabilidades:
  - Cargar series por activo desde merged_long_format.csv
  - Ejecutar los dos detectores de patrones (sliding window)
  - Calcular metricas de dispersion y volatilidad historica
  - Clasificar activos por nivel de riesgo (conservador/moderado/agresivo)
  - Retornar el listado ordenado por volatilidad ascendente

Sigue el mismo patron de diseno que SimilarityService:
  - Un metodo publico principal (analyze)
  - Metodos privados para cada responsabilidad
  - Sin dependencias de librerias de alto nivel (numpy, pandas, etc.)
"""

import os
from backend.app.model.sliding_window import detect_all_patterns
from backend.app.algorithms.volatility.volatility_service import (
    compute_annualized_volatility,
    classify_risk,
    sort_assets_by_risk,
)
from backend.app.model.data_loader import load_asset_series


class PatternAnalysisService:
    """
    Servicio principal del Requerimiento 3.

    Uso tipico:
        service = PatternAnalysisService(merged_long_path)
        result  = service.analyze()

        # Listado ordenado por riesgo
        for asset in result["risk_ranking"]:
            print(asset["ticker"], asset["risk_category"], asset["annualized_volatility"])
    """

    def __init__(self, merged_long_path,
                 window_up=3, window_squeeze=5, lookback_squeeze=20):
        """
        Parametros:
            merged_long_path:  ruta a merged_long_format.csv
            window_up:         ventana para patron de alza consecutiva (dias)
            window_squeeze:    ventana para patron de squeeze (dias consecutivos)
            lookback_squeeze:  dias de lookback para calcular rango promedio de referencia
        """
        self.merged_long_path  = merged_long_path
        self.window_up         = window_up
        self.window_squeeze    = window_squeeze
        self.lookback_squeeze  = lookback_squeeze

    # ------------------------------------------------------------------
    # METODO PUBLICO PRINCIPAL
    # ------------------------------------------------------------------

    def analyze(self):
        """
        Ejecuta el analisis completo del Requerimiento 3.

        Retorna:
        {
          "assets_analyzed": 17,
          "patterns": {
            "VOO": {
              "consecutive_up":     { occurrences, frequency, matches },
              "volatility_squeeze": { occurrences, frequency, matches }
            },
            ...
          },
          "volatility": {
            "VOO": { daily_std, annualized_volatility, n_returns },
            ...
          },
          "risk_classification": {
            "thresholds": { conservative_max, moderate_max },
            "classified":  { "VOO": { annualized_volatility, risk_category, risk_score }, ... }
          },
          "risk_ranking": [
            { ticker, annualized_volatility, risk_category, risk_score },
            ...  # ordenado ascendente por volatilidad (HeapSort)
          ]
        }
        """
        # 1. Cargar series por activo
        asset_series = load_asset_series(self.merged_long_path)

        # 2. Detectar patrones y calcular volatilidad por activo
        patterns_result  = {}
        volatility_map   = {}

        for ticker, series in asset_series.items():
            closes = series["closes"]
            highs  = [h if h is not None else c
                      for h, c in zip(series["highs"],  series["closes"])]
            lows   = [l if l is not None else c
                      for l, c in zip(series["lows"],   series["closes"])]
            dates  = series["dates"]

            # Patrones de sliding window
            patterns_result[ticker] = detect_all_patterns(
                closes, highs, lows, dates,
                window_up        = self.window_up,
                window_squeeze   = self.window_squeeze,
                lookback_squeeze = self.lookback_squeeze,
            )

            # Metricas de volatilidad
            vol_metrics = compute_annualized_volatility(closes)
            volatility_map[ticker] = vol_metrics["annualized_volatility"]

        # 3. Clasificar por riesgo
        risk_result = classify_risk(volatility_map)

        # 4. Ordenar por volatilidad ascendente (HeapSort explicito)
        risk_ranking = sort_assets_by_risk(risk_result["classified"])

        return {
            "assets_analyzed":    len(asset_series),
            "patterns":           patterns_result,
            "volatility":         {
                ticker: compute_annualized_volatility(asset_series[ticker]["closes"])
                for ticker in asset_series
            },
            "risk_classification": risk_result,
            "risk_ranking":        risk_ranking,
        }

    # ------------------------------------------------------------------
    # METODO DE CONVENIENCIA: solo riesgo (sin patrones)
    # ------------------------------------------------------------------

    def analyze_risk_only(self):
        """
        Calcula solo las metricas de volatilidad y clasificacion de riesgo,
        sin ejecutar los detectores de patrones. Util cuando se quiere
        solo el ranking de riesgo de forma rapida.
        """
        asset_series = load_asset_series(self.merged_long_path)
        volatility_map = {}
        for ticker, series in asset_series.items():
            vol = compute_annualized_volatility(series["closes"])
            volatility_map[ticker] = vol["annualized_volatility"]

        risk_result  = classify_risk(volatility_map)
        risk_ranking = sort_assets_by_risk(risk_result["classified"])

        return {
            "assets_analyzed":    len(asset_series),
            "risk_classification": risk_result,
            "risk_ranking":        risk_ranking,
        }

    # ------------------------------------------------------------------
    # METODO DE CONVENIENCIA: analizar un solo activo
    # ------------------------------------------------------------------

    def analyze_asset(self, ticker):
        """
        Ejecuta el analisis completo para un unico activo.
        Util para la capa de API/visualizacion cuando el usuario
        selecciona un activo especifico.

        Retorna:
        {
          "ticker":    "NVDA",
          "patterns":  { consecutive_up: {...}, volatility_squeeze: {...} },
          "volatility": { daily_std, annualized_volatility, n_returns },
          "risk":      { risk_category, risk_score }
        }
        """
        asset_series = load_asset_series(self.merged_long_path)

        if ticker not in asset_series:
            raise ValueError(
                "Activo '" + ticker + "' no encontrado en merged_long_format.csv.\n"
                                      "Activos disponibles: " + str(list(asset_series.keys()))
            )

        series = asset_series[ticker]
        closes = series["closes"]
        highs  = [h if h is not None else c for h, c in zip(series["highs"],  closes)]
        lows   = [l if l is not None else c for l, c in zip(series["lows"],   closes)]
        dates  = series["dates"]

        patterns = detect_all_patterns(
            closes, highs, lows, dates,
            window_up        = self.window_up,
            window_squeeze   = self.window_squeeze,
            lookback_squeeze = self.lookback_squeeze,
        )
        vol_metrics = compute_annualized_volatility(closes)

        # Para clasificar el riesgo necesitamos el contexto del portfolio
        all_vols = {}
        for t, s in asset_series.items():
            v = compute_annualized_volatility(s["closes"])
            all_vols[t] = v["annualized_volatility"]

        risk_result = classify_risk(all_vols)
        asset_risk  = risk_result["classified"].get(ticker, {})

        return {
            "ticker":    ticker,
            "patterns":  patterns,
            "volatility": vol_metrics,
            "risk": {
                "risk_category": asset_risk.get("risk_category", "unknown"),
                "risk_score":    asset_risk.get("risk_score",    0),
            },
        }