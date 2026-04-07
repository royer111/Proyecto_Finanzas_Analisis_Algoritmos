"""
requerimiento_3.py
------------------
Script principal del Requerimiento 3.

Ejecuta:
  1. Deteccion de patrones con ventana deslizante por activo.
  2. Calculo de metricas de dispersion y volatilidad historica.
  3. Clasificacion de activos por nivel de riesgo.
  4. Generacion del listado ordenado por volatilidad (HeapSort).
  5. Exportacion de resultados a CSV.
  6. Generacion de graficos.

Uso:
    python requerimiento_3.py
"""

import os
import csv
import math
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

from backend.app.services.pattern_analysis_service import PatternAnalysisService

# ===========================================================================
# CONFIGURACION
# ===========================================================================

MERGED_LONG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "..", "data", "merged", "merged_long_format.csv"
)

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", "requerimiento_3")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Parametros de los patrones (ajustables)
WINDOW_UP        = 3    # dias consecutivos al alza para el patron 1
WINDOW_SQUEEZE   = 5    # dias de compresion para el patron 2
LOOKBACK_SQUEEZE = 20   # dias de referencia para el patron 2

# Colores por categoria de riesgo
RISK_COLORS = {
    "conservador": "#2ecc71",
    "moderado":    "#f39c12",
    "agresivo":    "#e74c3c",
}


# ===========================================================================
# EXPORTACION DE RESULTADOS
# ===========================================================================

def export_risk_ranking(risk_ranking, output_dir):
    """Exporta el listado de activos ordenado por volatilidad a CSV."""
    path = os.path.join(output_dir, "risk_ranking.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Posicion", "Ticker", "Volatilidad Anualizada (%)",
                         "Categoria de Riesgo", "Risk Score"])
        for i, asset in enumerate(risk_ranking, 1):
            writer.writerow([
                i,
                asset["ticker"],
                str(round(asset["annualized_volatility"] * 100, 2)),
                asset["risk_category"],
                asset["risk_score"],
            ])
    print("[OK] Risk ranking -> " + path)
    return path


def export_patterns(patterns_result, output_dir):
    """Exporta el resumen de patrones detectados por activo a CSV."""
    path = os.path.join(output_dir, "patterns_summary.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Ticker",
            "Patron", "Ventana", "Ocurrencias", "Frecuencia"
        ])
        for ticker, pats in patterns_result.items():
            for pat_name, pat_data in pats.items():
                writer.writerow([
                    ticker,
                    pat_name,
                    pat_data.get("window", ""),
                    pat_data.get("occurrences", 0),
                    pat_data.get("frequency", 0.0),
                ])
    print("[OK] Patterns summary -> " + path)
    return path


def export_volatility(volatility_result, output_dir):
    """Exporta las metricas de volatilidad por activo a CSV."""
    path = os.path.join(output_dir, "volatility_metrics.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Ticker", "Desv. Estandar Diaria",
                         "Volatilidad Anualizada", "N Retornos"])
        for ticker, metrics in volatility_result.items():
            writer.writerow([
                ticker,
                metrics.get("daily_std", ""),
                metrics.get("annualized_volatility", ""),
                metrics.get("n_returns", ""),
            ])
    print("[OK] Volatility metrics -> " + path)
    return path


# ===========================================================================
# GRAFICOS
# ===========================================================================

def plot_risk_ranking(risk_ranking, output_dir):
    """
    Diagrama de barras horizontal con la volatilidad anualizada
    de cada activo, coloreado por categoria de riesgo.
    Ordenado de menor a mayor volatilidad (el mas conservador arriba).
    """
    tickers = [a["ticker"]                        for a in risk_ranking]
    vols    = [a["annualized_volatility"] * 100   for a in risk_ranking]
    colors  = [RISK_COLORS.get(a["risk_category"], "#95a5a6") for a in risk_ranking]

    fig, ax = plt.subplots(figsize=(12, max(6, len(tickers) * 0.55)))
    bars = ax.barh(tickers, vols, color=colors, edgecolor="white", linewidth=0.6)

    max_v = max(vols) if vols else 1
    for bar, v, asset in zip(bars, vols, risk_ranking):
        ax.text(
            v + max_v * 0.005,
            bar.get_y() + bar.get_height() / 2,
            str(round(v, 1)) + "%  [" + asset["risk_category"] + "]",
            va="center", ha="left", fontsize=8, color="#333333"
        )

    # Leyenda de colores
    from matplotlib.patches import Patch
    legend = [
        Patch(color=RISK_COLORS["conservador"], label="Conservador"),
        Patch(color=RISK_COLORS["moderado"],    label="Moderado"),
        Patch(color=RISK_COLORS["agresivo"],    label="Agresivo"),
    ]
    ax.legend(handles=legend, loc="lower right", fontsize=9)

    ax.invert_yaxis()
    ax.set_xlabel("Volatilidad Historica Anualizada (%)", fontsize=11)
    ax.set_title("Clasificacion de Activos por Nivel de Riesgo\n(Volatilidad Historica Anualizada, ordenado ascendente)",
                 fontsize=12, fontweight="bold", pad=12)
    ax.grid(axis="x", linestyle="--", alpha=0.4)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: str(round(x, 1)) + "%"))

    plt.tight_layout()
    path = os.path.join(output_dir, "risk_ranking.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print("[OK] Grafico risk ranking -> " + path)


def plot_pattern_frequency(patterns_result, output_dir):
    """
    Doble diagrama de barras: frecuencia de cada patron por activo.
    Panel izquierdo: consecutive_up. Panel derecho: volatility_squeeze.
    """
    tickers  = list(patterns_result.keys())
    freq_up  = [patterns_result[t]["consecutive_up"]["frequency"]     for t in tickers]
    freq_sq  = [patterns_result[t]["volatility_squeeze"]["frequency"] for t in tickers]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, max(5, len(tickers) * 0.5)))

    # Patron 1 - consecutive_up
    ax1.barh(tickers, freq_up, color="#3498db", edgecolor="white", linewidth=0.6)
    ax1.invert_yaxis()
    ax1.set_xlabel("Frecuencia relativa", fontsize=10)
    ax1.set_title("Patron: Alza Consecutiva\n(" + str(WINDOW_UP) + " dias)", fontsize=11, fontweight="bold")
    ax1.grid(axis="x", linestyle="--", alpha=0.4)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    # Patron 2 - volatility_squeeze
    ax2.barh(tickers, freq_sq, color="#9b59b6", edgecolor="white", linewidth=0.6)
    ax2.invert_yaxis()
    ax2.set_xlabel("Frecuencia relativa", fontsize=10)
    ax2.set_title("Patron: Compresion de Volatilidad\n(" + str(WINDOW_SQUEEZE) + " dias, lookback " + str(LOOKBACK_SQUEEZE) + ")", fontsize=11, fontweight="bold")
    ax2.grid(axis="x", linestyle="--", alpha=0.4)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    plt.suptitle("Frecuencia de Patrones por Activo", fontsize=13, fontweight="bold", y=1.02)
    plt.tight_layout()
    path = os.path.join(output_dir, "pattern_frequency.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("[OK] Grafico frecuencia de patrones -> " + path)


def plot_volatility_distribution(risk_ranking, risk_classification, output_dir):
    """
    Grafico de dispersion (scatter) con la volatilidad anualizada de cada activo
    y lineas verticales que muestran los umbrales de clasificacion de riesgo.
    """
    thresholds = risk_classification.get("thresholds", {})
    t_cons = thresholds.get("conservative_max", 0) * 100
    t_mod  = thresholds.get("moderate_max",     0) * 100

    tickers = [a["ticker"]                      for a in risk_ranking]
    vols    = [a["annualized_volatility"] * 100  for a in risk_ranking]
    colors  = [RISK_COLORS.get(a["risk_category"], "#95a5a6") for a in risk_ranking]

    fig, ax = plt.subplots(figsize=(13, 5))
    ax.scatter(range(len(tickers)), vols, c=colors, s=90, zorder=3, edgecolors="white", linewidths=0.8)

    for i, (t, v) in enumerate(zip(tickers, vols)):
        ax.annotate(t, (i, v), textcoords="offset points",
                    xytext=(0, 7), ha="center", fontsize=7.5, color="#333333")

    # Umbrales de riesgo como lineas horizontales
    ax.axhline(t_cons, color=RISK_COLORS["conservador"], linestyle="--", linewidth=1.2,
               label="Umbral conservador (" + str(round(t_cons, 1)) + "%)")
    ax.axhline(t_mod,  color=RISK_COLORS["moderado"],    linestyle="--", linewidth=1.2,
               label="Umbral moderado ("    + str(round(t_mod,  1)) + "%)")

    ax.set_xticks(range(len(tickers)))
    ax.set_xticklabels(tickers, rotation=35, ha="right", fontsize=8)
    ax.set_ylabel("Volatilidad Anualizada (%)", fontsize=10)
    ax.set_title("Distribucion de Volatilidad por Activo con Umbrales de Clasificacion",
                 fontsize=11, fontweight="bold", pad=10)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: str(round(x, 1)) + "%"))
    ax.legend(fontsize=9, loc="upper left")
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()
    path = os.path.join(output_dir, "volatility_distribution.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print("[OK] Grafico distribucion volatilidad -> " + path)


# ===========================================================================
# IMPRESION EN CONSOLA
# ===========================================================================

def print_risk_ranking(risk_ranking, risk_classification):
    """Imprime el listado ordenado por riesgo en consola."""
    thresholds = risk_classification.get("thresholds", {})
    sep = "=" * 72
    print("\n" + sep)
    print("  CLASIFICACION DE ACTIVOS POR NIVEL DE RIESGO (ascendente)")
    print(sep)
    print("  Umbrales — Conservador: vol <= " +
          str(round(thresholds.get("conservative_max", 0) * 100, 1)) + "%  |  " +
          "Moderado: vol <= " +
          str(round(thresholds.get("moderate_max", 0) * 100, 1)) + "%")
    print(sep)
    print("  " + "#".rjust(3) + "  " + "Ticker".ljust(10) +
          "  " + "Vol. Anual (%)".rjust(14) +
          "  " + "Categoria".ljust(14) + "  Score")
    print("  " + "-"*3 + "  " + "-"*10 + "  " + "-"*14 + "  " + "-"*14 + "  " + "-"*5)
    for i, asset in enumerate(risk_ranking, 1):
        print("  " + str(i).rjust(3) + "  " +
              asset["ticker"].ljust(10) + "  " +
              (str(round(asset["annualized_volatility"] * 100, 2)) + "%").rjust(14) + "  " +
              asset["risk_category"].ljust(14) + "  " +
              str(asset["risk_score"]))


def print_pattern_summary(patterns_result):
    """Imprime el resumen de patrones detectados por activo."""
    sep = "=" * 72
    print("\n" + sep)
    print("  RESUMEN DE PATRONES DETECTADOS (ventana deslizante)")
    print(sep)
    print("  " + "Ticker".ljust(8) + "  " +
          "Alza Consec. (occ/freq)".ljust(26) + "  " +
          "Vol. Squeeze (occ/freq)".ljust(26))
    print("  " + "-"*8 + "  " + "-"*26 + "  " + "-"*26)
    for ticker, pats in sorted(patterns_result.items()):
        up  = pats["consecutive_up"]
        sq  = pats["volatility_squeeze"]
        up_str = str(up["occurrences"]) + " / " + str(up["frequency"])
        sq_str = str(sq["occurrences"]) + " / " + str(sq["frequency"])
        print("  " + ticker.ljust(8) + "  " + up_str.ljust(26) + "  " + sq_str.ljust(26))


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    sep = "=" * 65
    print(sep)
    print("  REQUERIMIENTO 3 - Patrones y Volatilidad")
    print(sep)

    print("\n[1/4] Inicializando servicio de analisis...")
    service = PatternAnalysisService(
        merged_long_path  = MERGED_LONG_PATH,
        window_up         = WINDOW_UP,
        window_squeeze    = WINDOW_SQUEEZE,
        lookback_squeeze  = LOOKBACK_SQUEEZE,
    )

    print("[2/4] Ejecutando analisis completo (patrones + volatilidad + riesgo)...")
    result = service.analyze()

    print("      Activos analizados: " + str(result["assets_analyzed"]))

    # Consola
    print_pattern_summary(result["patterns"])
    print_risk_ranking(result["risk_ranking"], result["risk_classification"])

    # Exportar CSVs
    print("\n[3/4] Exportando resultados a CSV...")
    export_risk_ranking(result["risk_ranking"], OUTPUT_DIR)
    export_patterns(result["patterns"], OUTPUT_DIR)
    export_volatility(result["volatility"], OUTPUT_DIR)

    # Graficos
    print("\n[4/4] Generando graficos...")
    plot_risk_ranking(result["risk_ranking"], OUTPUT_DIR)
    plot_pattern_frequency(result["patterns"], OUTPUT_DIR)
    plot_volatility_distribution(
        result["risk_ranking"],
        result["risk_classification"],
        OUTPUT_DIR,
    )

    print("\n" + sep)
    print("  Completado. Resultados en: " + OUTPUT_DIR)
    print(sep)


if __name__ == "__main__":
    main()