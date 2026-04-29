"""
backend/app/services/pdf_service.py
Genera el reporte tecnico PDF consolidado del Req. 4.
Usa matplotlib para generar figuras y las consolida en un PDF
multi-pagina con matplotlib.backends.backend_pdf.PdfPages.
Sin dependencias de reportlab ni weasyprint.
"""

import io
import math
import os
import csv
from pathlib import Path
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.ticker as mticker
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.patches import Patch
import numpy as np  # solo para la matriz de correlacion (masking)

from backend.app.services.dashboard_service import (
    get_correlation_matrix,
    get_candlestick_data,
    get_risk_ranking,
    get_patterns_summary,
    get_volatility_metrics,
    _log_returns,
    _load_prices,
)

BASE       = Path(__file__).resolve().parents[4]
REQ3_OUT   = BASE / "backend" / "app" / "model" / "output" / "requerimiento_3"
REQ2_OUT   = BASE / "backend" / "Seguimiento" / "First" / "output"

RISK_COLORS = {
    "conservador": "#2ecc71",
    "moderado":    "#f39c12",
    "agresivo":    "#e74c3c",
}

STYLE = {
    "bg":      "#0d1117",
    "fg":      "#e6edf3",
    "grid":    "#21262d",
    "accent":  "#58a6ff",
    "green":   "#3fb950",
    "red":     "#f85149",
    "orange":  "#d29922",
}


def _set_dark_style(fig, axes_list):
    fig.patch.set_facecolor(STYLE["bg"])
    for ax in axes_list:
        if ax is None: continue
        ax.set_facecolor(STYLE["bg"])
        ax.tick_params(colors=STYLE["fg"])
        ax.xaxis.label.set_color(STYLE["fg"])
        ax.yaxis.label.set_color(STYLE["fg"])
        ax.title.set_color(STYLE["fg"])
        for spine in ax.spines.values():
            spine.set_edgecolor(STYLE["grid"])


# ─── Pagina 1: Portada ────────────────────────────────────────────────────

def _page_cover(pdf):
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(STYLE["bg"])
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_facecolor(STYLE["bg"])
    ax.axis("off")

    # Linea decorativa superior
    ax.axhline(0.93, xmin=0.05, xmax=0.95, color=STYLE["accent"], linewidth=2)

    ax.text(0.5, 0.82, "REPORTE TECNICO BURSATIL",
            ha="center", va="center", fontsize=28, fontweight="bold",
            color=STYLE["accent"], transform=ax.transAxes, fontfamily="monospace")

    ax.text(0.5, 0.72, "Analisis de Algoritmos — Proyecto Financiero",
            ha="center", va="center", fontsize=16,
            color=STYLE["fg"], transform=ax.transAxes)

    ax.text(0.5, 0.62, "Universidad del Quindio\nPrograma de Ingenieria de Sistemas y Computacion",
            ha="center", va="center", fontsize=13, linespacing=1.8,
            color="#8b949e", transform=ax.transAxes)

    # Contenido del reporte
    items = [
        "Matriz de Correlacion entre Activos (Heatmap)",
        "Graficos Candlestick con Medias Moviles (SMA 20/50/200)",
        "Clasificacion de Activos por Nivel de Riesgo",
        "Frecuencia de Patrones de Ventana Deslizante",
        "Metricas de Volatilidad Historica Anualizada",
    ]
    ax.text(0.5, 0.48, "Contenido:", ha="center", fontsize=12,
            fontweight="bold", color=STYLE["fg"], transform=ax.transAxes)
    for i, item in enumerate(items):
        ax.text(0.5, 0.42 - i * 0.055, f"  {i+1}.  {item}",
                ha="center", fontsize=11, color="#8b949e", transform=ax.transAxes)

    ax.axhline(0.07, xmin=0.05, xmax=0.95, color=STYLE["grid"], linewidth=1)
    ax.text(0.5, 0.04, f"Generado el {datetime.now().strftime('%d/%m/%Y %H:%M')}",
            ha="center", fontsize=10, color="#8b949e", transform=ax.transAxes)

    pdf.savefig(fig, facecolor=fig.get_facecolor())
    plt.close(fig)


# ─── Pagina 2: Heatmap de correlacion ────────────────────────────────────

def _page_heatmap(pdf):
    corr_data = get_correlation_matrix()
    tickers = corr_data["tickers"]
    matrix  = corr_data["matrix"]
    n = len(tickers)

    fig, ax = plt.subplots(figsize=(11, 8.5))
    _set_dark_style(fig, [ax])

    # Convertir a lista de listas para plotting sin numpy fancy indexing
    import numpy as np_local
    data = np_local.array(matrix)

    cmap = plt.cm.RdYlGn
    im = ax.imshow(data, cmap=cmap, vmin=-1, vmax=1, aspect="auto")

    # Anotaciones de valores
    for i in range(n):
        for j in range(n):
            val = matrix[i][j]
            color = "black" if abs(val) > 0.5 else STYLE["fg"]
            ax.text(j, i, f"{val:.2f}", ha="center", va="center",
                    fontsize=7 if n > 12 else 8, color=color, fontweight="bold")

    ax.set_xticks(range(n))
    ax.set_yticks(range(n))
    ax.set_xticklabels(tickers, rotation=45, ha="right", fontsize=9, color=STYLE["fg"])
    ax.set_yticklabels(tickers, fontsize=9, color=STYLE["fg"])
    ax.set_title("Matriz de Correlacion de Activos\n(Retornos Logaritmicos Diarios — Pearson)",
                 fontsize=13, fontweight="bold", color=STYLE["fg"], pad=15)

    cbar = plt.colorbar(im, ax=ax, fraction=0.03, pad=0.02)
    cbar.ax.tick_params(colors=STYLE["fg"])
    cbar.set_label("Coeficiente de Correlacion", color=STYLE["fg"])

    plt.tight_layout()
    pdf.savefig(fig, facecolor=fig.get_facecolor())
    plt.close(fig)


# ─── Pagina 3+: Candlestick + SMA por activo ────────────────────────────

def _page_candlestick(pdf, ticker, last_n=252):
    """Genera una pagina de candlestick para un ticker. last_n = ~1 ano."""
    try:
        data = get_candlestick_data(ticker, sma_windows=[20, 50, 200])
    except Exception:
        return

    dates   = data["dates"][-last_n:]
    opens   = data["opens"][-last_n:]
    highs   = data["highs"][-last_n:]
    lows    = data["lows"][-last_n:]
    closes  = data["closes"][-last_n:]
    volumes = data["volumes"][-last_n:]
    sma20   = data["sma"]["sma20"][-last_n:]
    sma50   = data["sma"]["sma50"][-last_n:]
    sma200  = data["sma"]["sma200"][-last_n:]

    n = len(dates)
    x = list(range(n))

    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(STYLE["bg"])
    gs  = gridspec.GridSpec(2, 1, height_ratios=[3, 1], hspace=0.05)
    ax1 = fig.add_subplot(gs[0])
    ax2 = fig.add_subplot(gs[1], sharex=ax1)
    _set_dark_style(fig, [ax1, ax2])

    # Velas
    for i in range(n):
        o, h, l, c = opens[i], highs[i], lows[i], closes[i]
        if None in (o, h, l, c):
            continue
        color = STYLE["green"] if c >= o else STYLE["red"]
        # Mecha
        ax1.plot([i, i], [l, h], color=color, linewidth=0.8, zorder=2)
        # Cuerpo
        body_h = max(abs(c - o), 0.01)
        ax1.bar(i, body_h, bottom=min(o, c), color=color,
                width=0.6, linewidth=0, zorder=3)

    # SMAs
    sma_cfg = [(sma20, "#f1c40f", "SMA 20"), (sma50, "#3498db", "SMA 50"), (sma200, "#e67e22", "SMA 200")]
    for sma_vals, color, label in sma_cfg:
        xs_valid = [i for i, v in enumerate(sma_vals) if v is not None]
        ys_valid = [v for v in sma_vals if v is not None]
        if xs_valid:
            ax1.plot(xs_valid, ys_valid, color=color, linewidth=1.2, label=label, zorder=4)

    ax1.legend(loc="upper left", fontsize=9, facecolor=STYLE["bg"],
               labelcolor=STYLE["fg"], edgecolor=STYLE["grid"])
    ax1.set_ylabel("Precio (USD)", color=STYLE["fg"])
    ax1.set_title(f"{ticker} — Candlestick con Medias Moviles (ultimo año aprox.)",
                  fontsize=13, fontweight="bold", color=STYLE["fg"], pad=10)
    ax1.grid(axis="y", color=STYLE["grid"], linewidth=0.5, alpha=0.6)
    plt.setp(ax1.get_xticklabels(), visible=False)

    # Volumen
    for i in range(n):
        o, c, v = opens[i], closes[i], volumes[i]
        if None in (o, c, v):
            continue
        color = STYLE["green"] if c >= o else STYLE["red"]
        ax2.bar(i, v, color=color, width=0.6, alpha=0.7, linewidth=0)

    ax2.set_ylabel("Volumen", color=STYLE["fg"])
    ax2.grid(axis="y", color=STYLE["grid"], linewidth=0.5, alpha=0.4)
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda x, _: f"{x/1e6:.1f}M" if x >= 1e6 else f"{x/1e3:.0f}K"
    ))

    # Eje X: fechas representativas
    step = max(1, n // 10)
    ax2.set_xticks(range(0, n, step))
    ax2.set_xticklabels([dates[i] for i in range(0, n, step)],
                        rotation=30, ha="right", fontsize=8, color=STYLE["fg"])

    plt.tight_layout()
    pdf.savefig(fig, facecolor=fig.get_facecolor())
    plt.close(fig)


# ─── Pagina: Risk Ranking ─────────────────────────────────────────────────

def _page_risk_ranking(pdf):
    rr = get_risk_ranking()
    if "error" in rr:
        return
    assets = rr["data"]

    tickers = [a["ticker"]   for a in assets]
    vols    = [a["volatility"] for a in assets]
    colors  = [RISK_COLORS.get(a["category"], "#95a5a6") for a in assets]

    fig, ax = plt.subplots(figsize=(11, 8.5))
    _set_dark_style(fig, [ax])

    bars = ax.barh(tickers, vols, color=colors, edgecolor=STYLE["bg"], linewidth=0.5)
    max_v = max(vols) if vols else 1
    for bar, v, a in zip(bars, vols, assets):
        ax.text(v + max_v * 0.005, bar.get_y() + bar.get_height() / 2,
                f"{v:.1f}%  [{a['category']}]", va="center", ha="left",
                fontsize=8.5, color=STYLE["fg"])

    legend = [
        Patch(color=RISK_COLORS["conservador"], label="Conservador"),
        Patch(color=RISK_COLORS["moderado"],    label="Moderado"),
        Patch(color=RISK_COLORS["agresivo"],    label="Agresivo"),
    ]
    ax.legend(handles=legend, loc="lower right", fontsize=10,
              facecolor=STYLE["bg"], labelcolor=STYLE["fg"], edgecolor=STYLE["grid"])
    ax.invert_yaxis()
    ax.set_xlabel("Volatilidad Historica Anualizada (%)", color=STYLE["fg"])
    ax.set_title("Clasificacion de Activos por Nivel de Riesgo\n(Volatilidad Historica Anualizada — HeapSort ascendente)",
                 fontsize=13, fontweight="bold", color=STYLE["fg"], pad=12)
    ax.grid(axis="x", color=STYLE["grid"], linewidth=0.5, alpha=0.5)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))

    plt.tight_layout()
    pdf.savefig(fig, facecolor=fig.get_facecolor())
    plt.close(fig)


# ─── Pagina: Patron frequency ─────────────────────────────────────────────

def _page_patterns(pdf):
    ps = get_patterns_summary()
    if "error" in ps:
        return
    rows = ps["data"]

    # Separar por patron
    by_ticker_up  = {r["ticker"]: r["frequency"] for r in rows if "consecutive" in r["pattern"]}
    by_ticker_sq  = {r["ticker"]: r["frequency"] for r in rows if "squeeze" in r["pattern"]}
    tickers = sorted(by_ticker_up.keys())

    freq_up = [by_ticker_up.get(t, 0) for t in tickers]
    freq_sq = [by_ticker_sq.get(t, 0) for t in tickers]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 8.5))
    _set_dark_style(fig, [ax1, ax2])

    ax1.barh(tickers, freq_up, color=STYLE["accent"], edgecolor=STYLE["bg"], linewidth=0.4)
    ax1.invert_yaxis()
    ax1.set_xlabel("Frecuencia relativa", color=STYLE["fg"])
    ax1.set_title("Alza Consecutiva\n(3 dias)", fontsize=11, fontweight="bold", color=STYLE["fg"])
    ax1.grid(axis="x", color=STYLE["grid"], linewidth=0.5, alpha=0.5)

    ax2.barh(tickers, freq_sq, color="#9b59b6", edgecolor=STYLE["bg"], linewidth=0.4)
    ax2.invert_yaxis()
    ax2.set_xlabel("Frecuencia relativa", color=STYLE["fg"])
    ax2.set_title("Compresion de Volatilidad\n(5 dias, lookback 20)", fontsize=11, fontweight="bold", color=STYLE["fg"])
    ax2.grid(axis="x", color=STYLE["grid"], linewidth=0.5, alpha=0.5)

    fig.suptitle("Frecuencia de Patrones por Activo (Ventana Deslizante)",
                 fontsize=13, fontweight="bold", color=STYLE["fg"], y=1.01)
    plt.tight_layout()
    pdf.savefig(fig, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close(fig)


# ─── Pagina: Volatility distribution scatter ─────────────────────────────

def _page_vol_scatter(pdf):
    rr = get_risk_ranking()
    vm = get_volatility_metrics()
    if "error" in rr or "error" in vm:
        return

    assets  = rr["data"]
    tickers = [a["ticker"]     for a in assets]
    vols    = [a["volatility"] for a in assets]
    colors  = [RISK_COLORS.get(a["category"], "#95a5a6") for a in assets]

    # Umbrales dinamicos
    sorted_vols = sorted(vols)
    n = len(sorted_vols)
    t_cons = sorted_vols[max(0, int(n * 0.33) - 1)]
    t_mod  = sorted_vols[max(0, int(n * 0.66) - 1)]

    fig, ax = plt.subplots(figsize=(11, 8.5))
    _set_dark_style(fig, [ax])

    ax.scatter(range(len(tickers)), vols, c=colors, s=100,
               zorder=3, edgecolors=STYLE["bg"], linewidths=1)

    for i, (t, v) in enumerate(zip(tickers, vols)):
        ax.annotate(t, (i, v), textcoords="offset points",
                    xytext=(0, 8), ha="center", fontsize=8, color=STYLE["fg"])

    ax.axhline(t_cons, color=RISK_COLORS["conservador"], linestyle="--", linewidth=1.3,
               label=f"Umbral conservador ({t_cons:.1f}%)")
    ax.axhline(t_mod,  color=RISK_COLORS["moderado"],    linestyle="--", linewidth=1.3,
               label=f"Umbral moderado ({t_mod:.1f}%)")

    ax.set_xticks(range(len(tickers)))
    ax.set_xticklabels(tickers, rotation=35, ha="right", fontsize=8, color=STYLE["fg"])
    ax.set_ylabel("Volatilidad Anualizada (%)", color=STYLE["fg"])
    ax.set_title("Distribucion de Volatilidad con Umbrales de Clasificacion de Riesgo",
                 fontsize=12, fontweight="bold", color=STYLE["fg"], pad=10)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.1f}%"))
    ax.legend(fontsize=9, facecolor=STYLE["bg"], labelcolor=STYLE["fg"],
              edgecolor=STYLE["grid"], loc="upper left")
    ax.grid(axis="y", color=STYLE["grid"], linewidth=0.5, alpha=0.4)

    plt.tight_layout()
    pdf.savefig(fig, facecolor=fig.get_facecolor())
    plt.close(fig)


# ─── Exportacion completa ─────────────────────────────────────────────────

def generate_pdf_report(output_path: str, candlestick_tickers=None):
    """
    Genera el reporte PDF completo con todas las secciones.
    candlestick_tickers: lista de tickers para graficar (por defecto los primeros 4).
    """
    if candlestick_tickers is None:
        candlestick_tickers = ["NVDA", "TSLA", "VOO", "GLD"]

    with PdfPages(output_path) as pdf:
        # Metadatos del PDF
        d = pdf.infodict()
        d["Title"]   = "Reporte Tecnico Bursatil — Analisis de Algoritmos"
        d["Author"]  = "Sistema de Analisis Financiero"
        d["Subject"] = "Dashboard Bursatil — Requerimiento 4"
        d["CreationDate"] = datetime.now()

        _page_cover(pdf)
        _page_heatmap(pdf)
        _page_risk_ranking(pdf)
        _page_patterns(pdf)
        _page_vol_scatter(pdf)
        for ticker in candlestick_tickers:
            _page_candlestick(pdf, ticker)

    return output_path