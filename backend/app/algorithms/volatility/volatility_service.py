"""
volatility_service.py
---------------------
Calculo de metricas de dispersion y clasificacion de riesgo
para cada activo financiero del portfolio.

Metricas calculadas:
  - Retornos diarios logaritmicos: r_i = ln(close_i / close_{i-1})
  - Desviacion estandar de retornos: sigma = std(r_1, ..., r_{n-1})
  - Volatilidad historica anualizada: sigma_anual = sigma * sqrt(252)

Clasificacion de riesgo (por cuartiles de la distribucion del portfolio):
  - Conservador:  volatilidad en el primer tercio del portfolio
  - Moderado:     volatilidad en el segundo tercio
  - Agresivo:     volatilidad en el tercio superior

El ordenamiento del listado final es estrictamente algoritmico:
se usa HeapSort sobre los valores de volatilidad anualizada.
"""

import math
from typing import List, Dict, Any


# ===========================================================================
# CALCULO DE RETORNOS Y METRICAS DE DISPERSION
# ===========================================================================

def compute_log_returns(closes):
    """
    Calcula los retornos logaritmicos diarios de una serie de precios.

    Definicion:
        r_i = ln(close_i / close_{i-1})  para i en [1, n-1]

    Por que logaritmicos y no aritmeticos (close_i/close_{i-1} - 1):
        Los retornos logaritmicos son aditivos en el tiempo (la suma de
        retornos log diarios es el retorno log del periodo completo) y
        tienen distribucion mas cercana a la normal, lo que hace que la
        desviacion estandar sea una medida de dispersion mas robusta.

    Complejidad: O(n)
    """
    returns = []
    for i in range(1, len(closes)):
        if closes[i - 1] > 0 and closes[i] > 0:
            returns.append(math.log(closes[i] / closes[i - 1]))
    return returns


def compute_std(values):
    """
    Calcula la desviacion estandar muestral (denominador n-1, formula de Bessel).

    Definicion:
        mean  = (1/n) * sum(values)
        std   = sqrt( (1/(n-1)) * sum((x - mean)^2) )

    Se usa n-1 (no n) porque los retornos son una muestra de la
    distribucion subyacente del activo, no la poblacion completa.

    Complejidad: O(n)
    """
    n = len(values)
    if n < 2:
        return 0.0
    mean = sum(values) / n
    variance = sum((x - mean) ** 2 for x in values) / (n - 1)
    return math.sqrt(variance)


def compute_annualized_volatility(closes, trading_days=252):
    """
    Calcula la volatilidad historica anualizada de un activo.

    Formula:
        sigma_diaria  = std(retornos_logaritmicos)
        sigma_anual   = sigma_diaria * sqrt(trading_days)

    El factor sqrt(252) anualiza la volatilidad diaria asumiendo
    252 dias de negociacion por ano (estandar de mercados bursatiles).
    Para mercados con diferente calendario se puede ajustar el parametro.

    Retorna:
        {
          "daily_std":            0.0182,   # desv. estandar diaria
          "annualized_volatility": 0.2889,  # volatilidad anualizada (29%)
          "n_returns":            1231,     # numero de retornos calculados
        }

    Complejidad: O(n)
    """
    returns = compute_log_returns(closes)
    if len(returns) < 2:
        return {"daily_std": 0.0, "annualized_volatility": 0.0, "n_returns": 0}

    daily_std    = compute_std(returns)
    annualized   = daily_std * math.sqrt(trading_days)

    return {
        "daily_std":            round(daily_std, 6),
        "annualized_volatility": round(annualized, 6),
        "n_returns":            len(returns),
    }


# ===========================================================================
# CLASIFICACION DE RIESGO POR TERCILES
# ===========================================================================

def classify_risk(assets_volatility):
    """
    Clasifica cada activo del portfolio en una categoria de riesgo
    basada en terciles de la distribucion de volatilidades del portfolio.

    Algoritmo:
        1. Ordenar las volatilidades de todos los activos (HeapSort, O(n log n)).
        2. Calcular los umbrales en los percentiles 33% y 66%.
        3. Asignar categoria a cada activo segun su volatilidad:
             - Conservador: vol <= percentil_33
             - Moderado:    percentil_33 < vol <= percentil_66
             - Agresivo:    vol > percentil_66

    Justificacion de terciles vs cuartiles:
        Con 17 activos, los cuartiles producen grupos de ~4 activos.
        Los terciles producen grupos de ~5-6 activos, mas representativos
        para un portfolio de este tamano.

    Parametros:
        assets_volatility: dict {ticker: annualized_volatility_float}

    Retorna:
        {
          "thresholds": {"conservative_max": 0.20, "moderate_max": 0.30},
          "classified": {
            "ticker": {
              "annualized_volatility": 0.25,
              "risk_category": "moderate",
              "risk_score": 2           # 1=conservador, 2=moderado, 3=agresivo
            }, ...
          }
        }
    """
    if not assets_volatility:
        return {"thresholds": {}, "classified": {}}

    # Ordenar volatilidades con HeapSort para calcular terciles
    vols_sorted = _heap_sort(list(assets_volatility.values()))

    n = len(vols_sorted)
    # Indices de terciles (floor para el primer tercil, ceil para el segundo)
    idx_33 = max(0, int(n * 0.33) - 1)
    idx_66 = max(0, int(n * 0.66) - 1)

    threshold_conservative = vols_sorted[idx_33]
    threshold_moderate     = vols_sorted[idx_66]

    classified = {}
    for ticker, vol in assets_volatility.items():
        if vol <= threshold_conservative:
            category   = "conservador"
            risk_score = 1
        elif vol <= threshold_moderate:
            category   = "moderado"
            risk_score = 2
        else:
            category   = "agresivo"
            risk_score = 3

        classified[ticker] = {
            "annualized_volatility": round(vol, 6),
            "risk_category":         category,
            "risk_score":            risk_score,
        }

    return {
        "thresholds": {
            "conservative_max": round(threshold_conservative, 6),
            "moderate_max":     round(threshold_moderate, 6),
        },
        "classified": classified,
    }


# ===========================================================================
# ORDENAMIENTO POR RIESGO (HeapSort explicito)
# ===========================================================================

def sort_assets_by_risk(classified):
    """
    Ordena los activos por volatilidad anualizada de forma ascendente
    usando HeapSort implementado explicitamente (sin sorted() ni .sort()).

    El ordenamiento es estrictamente algoritmico tal como exige el enunciado.

    Retorna lista de dicts ordenada:
        [
          {"ticker": "GLD",  "annualized_volatility": 0.12, "risk_category": "conservador", "risk_score": 1},
          {"ticker": "TLT",  "annualized_volatility": 0.14, "risk_category": "conservador", "risk_score": 1},
          ...
          {"ticker": "NVDA", "annualized_volatility": 0.58, "risk_category": "agresivo",     "risk_score": 3},
        ]

    Complejidad: O(n log n) — HeapSort garantizado en todos los casos.
    """
    items = [
        {
            "ticker":               ticker,
            "annualized_volatility": data["annualized_volatility"],
            "risk_category":         data["risk_category"],
            "risk_score":            data["risk_score"],
        }
        for ticker, data in classified.items()
    ]

    # HeapSort ascendente por volatilidad anualizada
    return _heap_sort_dicts(items, key=lambda x: x["annualized_volatility"])


# ===========================================================================
# HeapSort explicito (sin funciones de alto nivel)
# ===========================================================================

def _heapify(arr, n, i, key):
    largest = i
    left    = 2 * i + 1
    right   = 2 * i + 2
    if left  < n and key(arr[left])  > key(arr[largest]): largest = left
    if right < n and key(arr[right]) > key(arr[largest]): largest = right
    if largest != i:
        arr[i], arr[largest] = arr[largest], arr[i]
        _heapify(arr, n, largest, key)


def _heap_sort_dicts(arr, key):
    """HeapSort ascendente sobre lista de dicts con clave personalizada."""
    data = list(arr)
    n    = len(data)
    for i in range(n // 2 - 1, -1, -1):
        _heapify(data, n, i, key)
    for i in range(n - 1, 0, -1):
        data[0], data[i] = data[i], data[0]
        _heapify(data, i, 0, key)
    return data


def _heap_sort(arr):
    """HeapSort ascendente sobre lista de escalares."""
    return _heap_sort_dicts(arr, key=lambda x: x)