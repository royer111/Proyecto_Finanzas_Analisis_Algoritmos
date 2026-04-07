"""
sliding_window.py
-----------------
Implementacion del algoritmo de ventana deslizante (sliding window)
para deteccion de patrones en series de precios financieros.

Patrones implementados:
  1. Patron de alza consecutiva (consecutive_up):
       N dias seguidos donde close[i] > close[i-1]

  2. Patron de compresion de volatilidad (volatility_squeeze):
       N dias donde el rango diario (High - Low) es menor que
       el rango promedio de los ultimos M dias. Indica acumulacion
       antes de un movimiento fuerte (breakout).

Complejidad general del algoritmo de ventana deslizante:
  - Temporal: O(n) — un unico recorrido con suma deslizante.
  - Espacial: O(n) — se almacenan los rangos diarios; la ventana activa es O(w).
"""

import math
from typing import List, Dict, Any


# ===========================================================================
# PATRON 1 - Alza consecutiva (Consecutive Up Days)
# ===========================================================================

def detect_consecutive_up(closes, dates, window=3):
    """
    Detecta secuencias de 'window' dias consecutivos de alza en el precio
    de cierre usando una ventana deslizante.

    Definicion formal del patron:
        P_up(i, w) = True  si  close[j] < close[j+1]  para todo j en [i-w+1, i-1]
        Es decir: los ultimos 'w' precios de cierre forman una secuencia
        estrictamente creciente.

    Algoritmo (sliding window):
        - Se mantiene un contador 'streak' de dias consecutivos al alza.
        - En cada posicion i:
            * Si close[i] > close[i-1]: streak += 1
            * Si no: streak = 0 (se rompe la racha)
            * Si streak >= window: se registra una ocurrencia del patron.
        - Complejidad: O(n) - un unico recorrido lineal.

    Por que este patron es relevante en finanzas:
        Secuencias de N dias consecutivos al alza indican momentum positivo
        sostenido. En analisis tecnico, 3 dias seguidos al alza es el umbral
        minimo para confirmar una tendencia emergente. Este patron es la base
        del indicador 'Three White Soldiers' en analisis de velas japonesas.

    Parametros:
        closes: lista de precios de cierre en orden cronologico.
        dates:  lista de fechas correspondientes (misma longitud).
        window: numero minimo de dias consecutivos al alza para el patron.

    Retorna dict con occurrences, frequency y lista de matches.
    """
    n = len(closes)
    if n < 2 or window < 2:
        return _empty_result("consecutive_up", window)

    streak  = 0
    matches = []

    for i in range(1, n):
        if closes[i] > closes[i - 1]:
            streak += 1
            if streak >= window:
                matches.append({
                    "start_date": dates[i - window + 1],
                    "end_date":   dates[i],
                    "streak":     streak,
                })
        else:
            streak = 0

    occurrences = len(matches)
    denominator = max(1, n - window + 1)
    frequency   = round(occurrences / denominator, 6)

    return {
        "pattern":     "consecutive_up",
        "window":      window,
        "occurrences": occurrences,
        "frequency":   frequency,
        "matches":     matches,
    }


# ===========================================================================
# PATRON 2 - Compresion de volatilidad (Volatility Squeeze)
# ===========================================================================

def detect_volatility_squeeze(highs, lows, dates, window=5, lookback=20):
    """
    Detecta ventanas de 'window' dias consecutivos donde el rango diario
    (High - Low) es menor que el rango promedio de los 'lookback' dias
    previos. Indica compresion de volatilidad (squeeze).

    Definicion formal del patron:
        range_i     = High_i - Low_i
        avg_range_i = mean(range_{i-lookback}, ..., range_{i-1})

        P_squeeze(i, w, lb) = True  si  range_j < avg_range_j  para todo j en [i-w+1, i]

        Es decir: los ultimos 'w' rangos diarios son todos menores que
        el rango promedio de los 'lb' dias anteriores a cada uno.

    Por que este patron es relevante en finanzas:
        La volatilidad es mean-reverting: periodos de baja volatilidad
        tienden a preceder movimientos bruscos de precio (breakouts).
        Un squeeze sostenido durante 'w' dias indica acumulacion o
        distribucion silenciosa antes de una expansion de volatilidad.
        Es la base del indicador Bollinger Bands Squeeze ampliamente
        usado en trading cuantitativo.

    Algoritmo (sliding window con suma deslizante):
        - Se pre-computan los rangos diarios: range[i] = high[i] - low[i].
        - Se mantiene una suma deslizante de los ultimos 'lookback' rangos
          para calcular el promedio en O(1) en lugar de O(lookback).
        - En cada posicion i se verifica si range[i] < promedio_referencia.
        - Se mantiene un contador 'squeeze_streak' similar al patron 1.
        - Complejidad: O(n) con suma deslizante.

    Parametros:
        highs:    lista de precios maximos diarios.
        lows:     lista de precios minimos diarios.
        dates:    lista de fechas (misma longitud).
        window:   dias consecutivos de compresion para activar el patron.
        lookback: dias previos para calcular el rango promedio de referencia.

    Retorna dict con occurrences, frequency y lista de matches.
    """
    n = len(highs)
    if n < lookback + window:
        return _empty_result("volatility_squeeze", window)

    # Pre-calcular rangos diarios
    daily_ranges = [highs[i] - lows[i] for i in range(n)]

    # Inicializar suma deslizante con los primeros 'lookback' rangos
    window_sum     = sum(daily_ranges[:lookback])
    squeeze_streak = 0
    matches        = []

    for i in range(lookback, n):
        avg_range = window_sum / lookback

        # Actualizar suma deslizante O(1)
        window_sum += daily_ranges[i] - daily_ranges[i - lookback]

        if daily_ranges[i] < avg_range:
            squeeze_streak += 1
            if squeeze_streak >= window:
                matches.append({
                    "start_date":   dates[i - window + 1],
                    "end_date":     dates[i],
                    "avg_range":    round(avg_range, 4),
                    "squeeze_days": squeeze_streak,
                })
        else:
            squeeze_streak = 0

    occurrences = len(matches)
    denominator = max(1, n - lookback - window + 1)
    frequency   = round(occurrences / denominator, 6)

    return {
        "pattern":     "volatility_squeeze",
        "window":      window,
        "lookback":    lookback,
        "occurrences": occurrences,
        "frequency":   frequency,
        "matches":     matches,
    }


# ===========================================================================
# DISPATCHER
# ===========================================================================

def detect_all_patterns(closes, highs, lows, dates,
                        window_up=3, window_squeeze=5, lookback_squeeze=20):
    """
    Ejecuta los dos detectores de patrones sobre las series de un activo
    y retorna un diccionario consolidado con ambos resultados.
    """
    return {
        "consecutive_up":     detect_consecutive_up(closes, dates, window=window_up),
        "volatility_squeeze": detect_volatility_squeeze(
            highs, lows, dates, window=window_squeeze, lookback=lookback_squeeze
        ),
    }


def _empty_result(pattern, window):
    return {"pattern": pattern, "window": window,
            "occurrences": 0, "frequency": 0.0, "matches": []}