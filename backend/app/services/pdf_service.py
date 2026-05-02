"""
backend/app/services/pdf_service.py
Genera el reporte tecnico PDF consolidado del Req. 4.

Estructura del PDF:
  Pagina 1  — Portada con resumen ejecutivo
  Pagina 2  — Indice de contenidos con tabla de activos
  Pagina 3  — Matriz de correlacion (heatmap)
  Pagina 4  — Tabla completa de correlaciones (valores numericos)
  Pagina 5  — Clasificacion de riesgo (grafico de barras horizontal)
  Pagina 6  — Tabla de risk ranking con todas las metricas
  Pagina 7  — Frecuencia de patrones (doble panel)
  Pagina 8  — Tabla de patrones con ocurrencias y frecuencias
  Pagina 9  — Scatter de volatilidad con umbrales
  Pagina 10+ — Candlestick + SMA + volumen por cada ticker seleccionado
"""

import csv
import math
import os
from pathlib import Path
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.ticker as mticker
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.patches import Patch, FancyBboxPatch
import numpy as np

from backend.app.services.dashboard_service import (
    get_correlation_matrix,
    get_candlestick_data,
    get_risk_ranking,
    get_patterns_summary,
    get_volatility_metrics,
)

# ─── Rutas ────────────────────────────────────────────────────────────────
BASE     = Path(__file__).resolve().parents[3]
REQ3_OUT = BASE / "backend" / "app" / "model" / "output" / "requerimiento_3"

# ─── Paleta de colores ───────────────────────────────────────────────────
RISK_COLORS = {
    "conservador": "#2ecc71",
    "moderado":    "#f39c12",
    "agresivo":    "#e74c3c",
}

S = {
    "bg":      "#0d1117",
    "bg2":     "#161b22",
    "surface": "#21262d",
    "fg":      "#e6edf3",
    "fg2":     "#8b949e",
    "grid":    "#30363d",
    "accent":  "#58a6ff",
    "green":   "#3fb950",
    "red":     "#f85149",
    "orange":  "#d29922",
    "purple":  "#bc8cff",
}

PAGE_W, PAGE_H = 11, 8.5   # landscape


# ===========================================================================
# HELPERS
# ===========================================================================

def _apply_dark(fig, *axes):
    fig.patch.set_facecolor(S["bg"])
    for ax in axes:
        if ax is None:
            continue
        ax.set_facecolor(S["bg"])
        ax.tick_params(colors=S["fg2"], labelsize=8)
        ax.xaxis.label.set_color(S["fg2"])
        ax.yaxis.label.set_color(S["fg2"])
        ax.title.set_color(S["fg"])
        for spine in ax.spines.values():
            spine.set_edgecolor(S["grid"])
            spine.set_linewidth(0.5)


def _header_bar(ax, title, subtitle="", y_pos=0.97, color=S["accent"]):
    """Dibuja una barra de titulo en la parte superior de un axes de texto."""
    ax.plot([0, 1], [y_pos, y_pos], color=color, linewidth=1.5, transform=ax.transAxes, clip_on=False)
    ax.text(0.0, y_pos - 0.03, title, transform=ax.transAxes,
            fontsize=14, fontweight="bold", color=color,
            fontfamily="monospace", va="top")
    if subtitle:
        ax.text(0.0, y_pos - 0.08, subtitle, transform=ax.transAxes,
                fontsize=9, color=S["fg2"], va="top")


def _table_on_axes(ax, headers, rows, col_widths, row_colors=None,
                   header_color=S["accent"], font_size=8):
    """
    Dibuja una tabla usando ax.table() con estilo oscuro.
    col_widths: lista de anchos relativos (deben sumar 1.0).
    """
    ax.axis("off")
    table = ax.table(
        cellText=rows,
        colLabels=headers,
        colWidths=col_widths,
        loc="upper center",
        cellLoc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(font_size)

    n_rows = len(rows)
    n_cols = len(headers)

    for (r, c), cell in table.get_celld().items():
        cell.set_edgecolor(S["grid"])
        cell.set_linewidth(0.4)
        if r == 0:
            # Header
            cell.set_facecolor("#1c2a3a")
            cell.set_text_props(color=header_color, fontweight="bold",
                                fontsize=font_size, fontfamily="monospace")
        else:
            # Alternating rows
            if row_colors and (r - 1) < len(row_colors):
                cell.set_facecolor(row_colors[r - 1])
            else:
                cell.set_facecolor(S["bg2"] if r % 2 == 0 else S["surface"])
            cell.set_text_props(color=S["fg"], fontsize=font_size)

    table.scale(1, 1.6)
    return table


def _page_number_text(fig, page_n, total_pages=None):
    label = f"Pag. {page_n}" + (f" / {total_pages}" if total_pages else "")
    fig.text(0.98, 0.02, label, ha="right", va="bottom",
             fontsize=7, color=S["fg2"], fontfamily="monospace")
    fig.text(0.02, 0.02, "Dashboard Bursatil — Requerimiento 4",
             ha="left", va="bottom", fontsize=7, color=S["fg2"], fontfamily="monospace")


# ===========================================================================
# PAGINA 1 — PORTADA
# ===========================================================================

def _page_cover(pdf, risk_data, vol_data):
    fig = plt.figure(figsize=(PAGE_W, PAGE_H))
    fig.patch.set_facecolor(S["bg"])
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_facecolor(S["bg"])
    ax.axis("off")

    # Lineas decorativas
    ax.plot([0.04, 0.96], [0.96, 0.96], color=S["accent"], linewidth=2, transform=ax.transAxes, clip_on=False)
    ax.plot([0.04, 0.96], [0.95, 0.95], color=S["accent"], linewidth=0.4, alpha=0.4, transform=ax.transAxes, clip_on=False)

    ax.text(0.5, 0.88, "REPORTE TECNICO BURSATIL",
            ha="center", fontsize=30, fontweight="bold",
            color=S["accent"], transform=ax.transAxes, fontfamily="monospace")
    ax.text(0.5, 0.80, "Analisis de Algoritmos  •  Proyecto Financiero",
            ha="center", fontsize=14, color=S["fg"], transform=ax.transAxes)
    ax.text(0.5, 0.74, "Universidad del Quindio  |  Programa de Ingenieria de Sistemas",
            ha="center", fontsize=11, color=S["fg2"], transform=ax.transAxes)

    # Resumen ejecutivo (mini stats)
    if risk_data:
        cons_count = sum(1 for a in risk_data if a["category"] == "conservador")
        mod_count  = sum(1 for a in risk_data if a["category"] == "moderado")
        agg_count  = sum(1 for a in risk_data if a["category"] == "agresivo")
        min_vol = risk_data[0]
        max_vol = risk_data[-1]
        avg_vol = sum(a["volatility"] for a in risk_data) / len(risk_data)

        stats = [
            ("Activos analizados", str(len(risk_data))),
            ("Conservadores",      f"{cons_count}  (vol ≤ {risk_data[cons_count-1]['volatility']:.1f}%)"),
            ("Moderados",          f"{mod_count}"),
            ("Agresivos",          f"{agg_count}  (hasta {max_vol['volatility']:.1f}%)"),
            ("Menor volatilidad",  f"{min_vol['ticker']}  {min_vol['volatility']:.1f}%"),
            ("Mayor volatilidad",  f"{max_vol['ticker']}  {max_vol['volatility']:.1f}%"),
            ("Volatilidad media",  f"{avg_vol:.1f}%"),
        ]

        col_x = [0.10, 0.40, 0.60, 0.85]
        ax.text(0.5, 0.64, "RESUMEN EJECUTIVO", ha="center", fontsize=10,
                fontweight="bold", color=S["accent"], transform=ax.transAxes,
                fontfamily="monospace")

        for i, (label, value) in enumerate(stats):
            row, col = divmod(i, 2)
            x = 0.12 + col * 0.50
            y = 0.58 - row * 0.075
            ax.text(x, y, label + ":", fontsize=9, color=S["fg2"],
                    transform=ax.transAxes, fontfamily="monospace")
            ax.text(x + 0.22, y, value, fontsize=9, color=S["fg"],
                    transform=ax.transAxes, fontweight="bold")

    # Contenido del PDF
    ax.text(0.5, 0.29, "CONTENIDO", ha="center", fontsize=10,
            fontweight="bold", color=S["accent"], transform=ax.transAxes,
            fontfamily="monospace")
    sections = [
        "1.  Indice de activos del portfolio",
        "2.  Matriz de correlacion  (heatmap + tabla numerica)",
        "3.  Clasificacion de riesgo  (grafico + tabla completa)",
        "4.  Frecuencia de patrones  (graficos + tabla)",
        "5.  Distribucion de volatilidad",
        "6.  Graficos candlestick con SMA 20 / 50 / 200",
    ]
    for i, s in enumerate(sections):
        ax.text(0.5, 0.23 - i * 0.045, s, ha="center", fontsize=9,
                color=S["fg2"], transform=ax.transAxes)

    # Footer
    ax.plot([0.04, 0.96], [0.05, 0.05], color=S["grid"], linewidth=0.6, transform=ax.transAxes, clip_on=False)
    ax.text(0.5, 0.025, f"Generado el {datetime.now().strftime('%d/%m/%Y a las %H:%M')}",
            ha="center", fontsize=8, color=S["fg2"], transform=ax.transAxes)

    _page_number_text(fig, 1)
    pdf.savefig(fig, facecolor=S["bg"])
    plt.close(fig)


# ===========================================================================
# PAGINA 2 — INDICE DE ACTIVOS
# ===========================================================================

def _page_index(pdf, risk_data, vol_data):
    fig = plt.figure(figsize=(PAGE_W, PAGE_H))
    fig.patch.set_facecolor(S["bg"])

    # Layout: titulo + tabla
    ax_title = fig.add_axes([0.04, 0.88, 0.92, 0.08])
    ax_table = fig.add_axes([0.04, 0.06, 0.92, 0.80])
    for ax in (ax_title, ax_table):
        ax.set_facecolor(S["bg"])
        ax.axis("off")

    ax_title.text(0, 0.7, "PORTFOLIO DE ACTIVOS ANALIZADOS",
                  fontsize=14, fontweight="bold", color=S["accent"],
                  fontfamily="monospace")
    ax_title.text(0, 0.1, f"{len(risk_data)} activos  •  Horizonte: 5 anos  •  Fuente: merged_prices.csv / merged_long_format.csv",
                  fontsize=9, color=S["fg2"])

    # Construir tabla completa
    vol_map = {v["ticker"]: v for v in (vol_data.get("data") or [])}
    headers = ["#", "Ticker", "Categoria", "Volatilidad (%)", "Desv. Est. Diaria", "N Retornos", "Score"]
    rows = []
    row_colors = []
    for a in risk_data:
        vm = vol_map.get(a["ticker"], {})
        rows.append([
            str(a["position"]),
            a["ticker"],
            a["category"].upper(),
            f"{a['volatility']:.2f}",
            f"{vm.get('daily_std', 0):.6f}",
            str(vm.get("n_returns", "—")),
            str(a["risk_score"]),
        ])
        # Color de fila segun categoria
        c_map = {"conservador": "#0d2318", "moderado": "#261a00", "agresivo": "#1f0000"}
        row_colors.append(c_map.get(a["category"], S["bg2"]))

    _table_on_axes(ax_table, headers, rows,
                   col_widths=[0.05, 0.12, 0.16, 0.16, 0.18, 0.16, 0.09],
                   row_colors=row_colors, font_size=9)

    # Leyenda de categorias
    legend_items = [
        Patch(color=RISK_COLORS["conservador"], label="Conservador"),
        Patch(color=RISK_COLORS["moderado"],    label="Moderado"),
        Patch(color=RISK_COLORS["agresivo"],    label="Agresivo"),
    ]
    ax_table.legend(handles=legend_items, loc="lower right",
                    fontsize=8, facecolor=S["surface"],
                    labelcolor=S["fg"], edgecolor=S["grid"])

    _page_number_text(fig, 2)
    pdf.savefig(fig, facecolor=S["bg"])
    plt.close(fig)


# ===========================================================================
# PAGINA 3 — HEATMAP DE CORRELACION
# ===========================================================================

def _page_heatmap(pdf, corr_data):
    tickers = corr_data["tickers"]
    matrix  = corr_data["matrix"]
    n       = len(tickers)
    data    = np.array(matrix)

    fig, ax = plt.subplots(figsize=(PAGE_W, PAGE_H))
    _apply_dark(fig, ax)

    im = ax.imshow(data, cmap=plt.cm.RdYlGn, vmin=-1, vmax=1, aspect="auto")

    fs = 7 if n > 12 else 8
    for i in range(n):
        for j in range(n):
            val = matrix[i][j]
            color = "#000" if abs(val) > 0.5 else S["fg"]
            ax.text(j, i, f"{val:.2f}", ha="center", va="center",
                    fontsize=fs, color=color, fontweight="bold")

    ax.set_xticks(range(n))
    ax.set_yticks(range(n))
    ax.set_xticklabels(tickers, rotation=45, ha="right", fontsize=8, color=S["fg"])
    ax.set_yticklabels(tickers, fontsize=8, color=S["fg"])
    ax.set_title("MATRIZ DE CORRELACION  (Pearson sobre Retornos Logaritmicos Diarios)",
                 fontsize=12, fontweight="bold", color=S["accent"], pad=12,
                 fontfamily="monospace")

    cbar = plt.colorbar(im, ax=ax, fraction=0.025, pad=0.01)
    cbar.ax.tick_params(colors=S["fg2"], labelsize=8)
    cbar.set_label("r de Pearson", color=S["fg2"], fontsize=8)

    fig.text(0.5, 0.01,
             "Verde = correlacion positiva alta  |  Rojo = correlacion negativa  |  "
             "Diagonal = autocorrelacion = 1.00",
             ha="center", fontsize=8, color=S["fg2"])

    plt.tight_layout(rect=[0, 0.03, 1, 1])
    _page_number_text(fig, 3)
    pdf.savefig(fig, facecolor=S["bg"])
    plt.close(fig)


# ===========================================================================
# PAGINA 4 — TABLA NUMERICA DE CORRELACIONES
# ===========================================================================

def _page_corr_table(pdf, corr_data):
    tickers = corr_data["tickers"]
    matrix  = corr_data["matrix"]
    n       = len(tickers)

    fig = plt.figure(figsize=(PAGE_W, PAGE_H))
    fig.patch.set_facecolor(S["bg"])

    ax_title = fig.add_axes([0.02, 0.90, 0.96, 0.08])
    ax_table = fig.add_axes([0.02, 0.04, 0.96, 0.85])
    ax_title.set_facecolor(S["bg"]); ax_title.axis("off")
    ax_table.set_facecolor(S["bg"]); ax_table.axis("off")

    ax_title.text(0, 0.7, "TABLA DE CORRELACIONES — VALORES NUMERICOS",
                  fontsize=13, fontweight="bold", color=S["accent"],
                  fontfamily="monospace")
    ax_title.text(0, 0.05,
                  "Coeficiente de Pearson calculado sobre retornos logaritmicos diarios. "
                  "Rango [-1, 1]. Valores > 0.7 indican alta correlacion positiva.",
                  fontsize=8, color=S["fg2"])

    headers = ["Ticker"] + tickers
    rows = []
    row_colors = []
    for i, ti in enumerate(tickers):
        row = [ti] + [f"{matrix[i][j]:.3f}" for j in range(n)]
        rows.append(row)
        row_colors.append(S["bg2"] if i % 2 == 0 else S["surface"])

    col_w = [0.10] + [round(0.90 / n, 4)] * n
    ax_table.axis("off")
    table = ax_table.table(
        cellText=rows, colLabels=headers,
        colWidths=col_w, loc="upper center", cellLoc="center",
    )
    table.auto_set_font_size(False)
    fs = max(5, 9 - n // 4)
    table.set_fontsize(fs)

    for (r, c), cell in table.get_celld().items():
        cell.set_edgecolor(S["grid"])
        cell.set_linewidth(0.3)
        if r == 0 or c == 0:
            cell.set_facecolor("#1c2a3a")
            cell.set_text_props(color=S["accent"], fontweight="bold",
                                fontsize=fs, fontfamily="monospace")
        else:
            val = matrix[r - 1][c - 1] if r > 0 and c > 0 else 0
            # Color de fondo segun valor de correlacion
            if r == c:
                cell.set_facecolor("#0a2010")
            elif val > 0.7:
                cell.set_facecolor("#0d2318")
            elif val > 0.4:
                cell.set_facecolor("#181200")
            elif val < 0:
                cell.set_facecolor("#200808")
            else:
                cell.set_facecolor(S["bg2"] if r % 2 == 0 else S["surface"])
            cell.set_text_props(color=S["fg"], fontsize=fs)

    table.scale(1, 1.4)
    _page_number_text(fig, 4)
    pdf.savefig(fig, facecolor=S["bg"])
    plt.close(fig)


# ===========================================================================
# PAGINA 5 — CLASIFICACION DE RIESGO (grafico)
# ===========================================================================

def _page_risk_chart(pdf, risk_data):
    assets  = risk_data
    tickers = [a["ticker"]     for a in assets]
    vols    = [a["volatility"] for a in assets]
    colors  = [RISK_COLORS.get(a["category"], "#95a5a6") for a in assets]

    fig, ax = plt.subplots(figsize=(PAGE_W, PAGE_H))
    _apply_dark(fig, ax)

    bars = ax.barh(tickers, vols, color=colors, edgecolor=S["bg2"],
                   linewidth=0.5, height=0.65)
    max_v = max(vols) if vols else 1

    for bar, v, a in zip(bars, vols, assets):
        ax.text(v + max_v * 0.004, bar.get_y() + bar.get_height() / 2,
                f"  {v:.1f}%   {a['category'].upper()}",
                va="center", ha="left", fontsize=8, color=S["fg"],
                fontfamily="monospace")

    # Lineas de umbral
    sorted_vols = sorted(vols)
    n = len(sorted_vols)
    t_cons = sorted_vols[max(0, int(n * 0.33) - 1)]
    t_mod  = sorted_vols[max(0, int(n * 0.66) - 1)]
    ax.axvline(t_cons, color=RISK_COLORS["conservador"], linestyle="--",
               linewidth=1, alpha=0.7, label=f"Umbral conservador {t_cons:.1f}%")
    ax.axvline(t_mod,  color=RISK_COLORS["moderado"],    linestyle="--",
               linewidth=1, alpha=0.7, label=f"Umbral moderado {t_mod:.1f}%")

    legend_items = [
        Patch(color=RISK_COLORS["conservador"], label="Conservador"),
        Patch(color=RISK_COLORS["moderado"],    label="Moderado"),
        Patch(color=RISK_COLORS["agresivo"],    label="Agresivo"),
    ]
    ax.legend(handles=legend_items + [
        plt.Line2D([0],[0], color=RISK_COLORS["conservador"], linestyle="--", label=f"Umbral cons. {t_cons:.1f}%"),
        plt.Line2D([0],[0], color=RISK_COLORS["moderado"],    linestyle="--", label=f"Umbral mod.  {t_mod:.1f}%"),
    ], loc="lower right", fontsize=8, facecolor=S["surface"],
              labelcolor=S["fg"], edgecolor=S["grid"])

    ax.invert_yaxis()
    ax.set_xlabel("Volatilidad Historica Anualizada (%)", fontsize=9)
    ax.set_title("CLASIFICACION DE ACTIVOS POR NIVEL DE RIESGO\n"
                 "Volatilidad Historica Anualizada — Ordenado con HeapSort",
                 fontsize=12, fontweight="bold", color=S["accent"],
                 pad=12, fontfamily="monospace")
    ax.grid(axis="x", color=S["grid"], linewidth=0.5, alpha=0.5)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax.set_xlim(0, max_v * 1.22)

    plt.tight_layout()
    _page_number_text(fig, 5)
    pdf.savefig(fig, facecolor=S["bg"])
    plt.close(fig)


# ===========================================================================
# PAGINA 6 — TABLA DE RIESGO COMPLETA
# ===========================================================================

def _page_risk_table(pdf, risk_data, vol_data):
    fig = plt.figure(figsize=(PAGE_W, PAGE_H))
    fig.patch.set_facecolor(S["bg"])

    ax_title = fig.add_axes([0.03, 0.90, 0.94, 0.08])
    ax_table = fig.add_axes([0.03, 0.05, 0.94, 0.84])
    ax_title.set_facecolor(S["bg"]); ax_title.axis("off")
    ax_table.set_facecolor(S["bg"]); ax_table.axis("off")

    ax_title.text(0, 0.7, "TABLA DETALLADA DE CLASIFICACION DE RIESGO",
                  fontsize=13, fontweight="bold", color=S["accent"],
                  fontfamily="monospace")
    ax_title.text(0, 0.05,
                  "Clasificacion por terciles  |  "
                  "Conservador ≤ P33  |  Moderado ≤ P66  |  Agresivo > P66  |  "
                  "Ordenamiento: HeapSort O(n log n)",
                  fontsize=8, color=S["fg2"])

    vol_map = {v["ticker"]: v for v in (vol_data.get("data") or [])}
    headers = ["Pos", "Ticker", "Categoria", "Vol. Anual (%)",
               "Desv. Est. Diaria", "N Retornos", "Risk Score"]
    rows, row_colors = [], []
    c_map = {"conservador": "#0d2318", "moderado": "#261a00", "agresivo": "#1f0000"}
    for a in risk_data:
        vm = vol_map.get(a["ticker"], {})
        rows.append([
            str(a["position"]),
            a["ticker"],
            a["category"].upper(),
            f"{a['volatility']:.4f}",
            f"{vm.get('daily_std', 0):.6f}",
            str(vm.get("n_returns", "—")),
            "★" * a["risk_score"],
            ])
        row_colors.append(c_map.get(a["category"], S["bg2"]))

    _table_on_axes(ax_table, headers, rows,
                   col_widths=[0.06, 0.12, 0.17, 0.16, 0.17, 0.15, 0.12],
                   row_colors=row_colors, font_size=9)

    _page_number_text(fig, 6)
    pdf.savefig(fig, facecolor=S["bg"])
    plt.close(fig)


# ===========================================================================
# PAGINA 7 — FRECUENCIA DE PATRONES (graficos)
# ===========================================================================

def _page_patterns_chart(pdf, patterns_data):
    rows = patterns_data.get("data", [])
    by_up = {r["ticker"]: r["frequency"] for r in rows if "consecutive" in r["pattern"]}
    by_sq = {r["ticker"]: r["frequency"] for r in rows if "squeeze"     in r["pattern"]}
    tickers = sorted(by_up.keys())
    freq_up = [by_up.get(t, 0) for t in tickers]
    freq_sq = [by_sq.get(t, 0) for t in tickers]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(PAGE_W, PAGE_H))
    _apply_dark(fig, ax1, ax2)
    fig.suptitle("FRECUENCIA DE PATRONES POR ACTIVO  (Ventana Deslizante)",
                 fontsize=12, fontweight="bold", color=S["accent"],
                 fontfamily="monospace", y=0.98)

    def _bar_panel(ax, tickers, freqs, color, title, note):
        bars = ax.barh(tickers, freqs, color=color, edgecolor=S["bg2"],
                       linewidth=0.4, height=0.6)
        max_f = max(freqs) if freqs else 1
        for bar, v in zip(bars, freqs):
            ax.text(v + max_f * 0.01, bar.get_y() + bar.get_height() / 2,
                    f" {v*100:.1f}%", va="center", fontsize=7.5, color=S["fg"])
        ax.invert_yaxis()
        ax.set_xlabel("Frecuencia relativa", fontsize=8)
        ax.set_title(title + "\n" + note, fontsize=9, fontweight="bold",
                     color=S["fg"], pad=8)
        ax.grid(axis="x", color=S["grid"], linewidth=0.5, alpha=0.5)
        ax.set_xlim(0, max_f * 1.18)

    _bar_panel(ax1, tickers, freq_up, S["accent"],
               "Alza Consecutiva", "window = 3 dias")
    _bar_panel(ax2, tickers, freq_sq, S["purple"],
               "Compresion de Volatilidad", "window = 5 dias  |  lookback = 20 dias")

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    _page_number_text(fig, 7)
    pdf.savefig(fig, facecolor=S["bg"])
    plt.close(fig)


# ===========================================================================
# PAGINA 8 — TABLA DE PATRONES
# ===========================================================================

def _page_patterns_table(pdf, patterns_data):
    rows_raw = patterns_data.get("data", [])

    fig = plt.figure(figsize=(PAGE_W, PAGE_H))
    fig.patch.set_facecolor(S["bg"])

    ax_title = fig.add_axes([0.03, 0.90, 0.94, 0.08])
    ax_t1    = fig.add_axes([0.03, 0.05, 0.45, 0.84])
    ax_t2    = fig.add_axes([0.52, 0.05, 0.45, 0.84])
    for ax in (ax_title, ax_t1, ax_t2):
        ax.set_facecolor(S["bg"]); ax.axis("off")

    ax_title.text(0, 0.7, "TABLA DE PATRONES DETECTADOS (VENTANA DESLIZANTE)",
                  fontsize=12, fontweight="bold", color=S["accent"],
                  fontfamily="monospace")
    ax_title.text(0, 0.05,
                  "Patron 1: Alza Consecutiva — N dias seguidos con close[i] > close[i-1]  |  "
                  "Patron 2: Compresion de Volatilidad — range < avg_range del lookback",
                  fontsize=8, color=S["fg2"])

    up_rows  = sorted([r for r in rows_raw if "consecutive" in r["pattern"]],
                      key=lambda x: -x["frequency"])
    sq_rows  = sorted([r for r in rows_raw if "squeeze"     in r["pattern"]],
                      key=lambda x: -x["frequency"])

    headers = ["Ticker", "Ocurrencias", "Frecuencia", "Ventana"]

    def build_rows(data):
        return [[r["ticker"], str(r["occurrences"]),
                 f"{r['frequency']*100:.2f}%", str(r["window"])]
                for r in data]

    # Tabla 1: alza consecutiva
    ax_t1.text(0.5, 0.97, "ALZA CONSECUTIVA (3 dias)",
               ha="center", fontsize=9, fontweight="bold",
               color=S["accent"], transform=ax_t1.transAxes,
               fontfamily="monospace")
    _table_on_axes(ax_t1, headers, build_rows(up_rows),
                   col_widths=[0.30, 0.25, 0.25, 0.20],
                   header_color=S["accent"], font_size=9)

    # Tabla 2: compresion
    ax_t2.text(0.5, 0.97, "COMPRESION DE VOLATILIDAD (5 dias)",
               ha="center", fontsize=9, fontweight="bold",
               color=S["purple"], transform=ax_t2.transAxes,
               fontfamily="monospace")
    _table_on_axes(ax_t2, headers, build_rows(sq_rows),
                   col_widths=[0.30, 0.25, 0.25, 0.20],
                   header_color=S["purple"], font_size=9)

    _page_number_text(fig, 8)
    pdf.savefig(fig, facecolor=S["bg"])
    plt.close(fig)


# ===========================================================================
# PAGINA 9 — SCATTER DE VOLATILIDAD
# ===========================================================================

def _page_vol_scatter(pdf, risk_data):
    tickers = [a["ticker"]     for a in risk_data]
    vols    = [a["volatility"] for a in risk_data]
    colors  = [RISK_COLORS.get(a["category"], "#95a5a6") for a in risk_data]

    sorted_v = sorted(vols)
    n = len(sorted_v)
    t_cons = sorted_v[max(0, int(n * 0.33) - 1)]
    t_mod  = sorted_v[max(0, int(n * 0.66) - 1)]

    fig, ax = plt.subplots(figsize=(PAGE_W, PAGE_H))
    _apply_dark(fig, ax)

    ax.scatter(range(len(tickers)), vols, c=colors, s=120,
               zorder=3, edgecolors=S["bg2"], linewidths=1)

    for i, (t, v) in enumerate(zip(tickers, vols)):
        ax.annotate(t, (i, v), textcoords="offset points",
                    xytext=(0, 9), ha="center", fontsize=8, color=S["fg"],
                    fontfamily="monospace")

    ax.axhline(t_cons, color=RISK_COLORS["conservador"], linestyle="--",
               linewidth=1.2, label=f"Umbral conservador ({t_cons:.1f}%)")
    ax.axhline(t_mod,  color=RISK_COLORS["moderado"],    linestyle="--",
               linewidth=1.2, label=f"Umbral moderado ({t_mod:.1f}%)")

    # Franjas de fondo por zona
    ax.axhspan(0,      t_cons, alpha=0.04, color=RISK_COLORS["conservador"])
    ax.axhspan(t_cons, t_mod,  alpha=0.04, color=RISK_COLORS["moderado"])
    ax.axhspan(t_mod,  max(vols)*1.15, alpha=0.04, color=RISK_COLORS["agresivo"])

    ax.set_xticks(range(len(tickers)))
    ax.set_xticklabels(tickers, rotation=35, ha="right", fontsize=8, color=S["fg"])
    ax.set_ylabel("Volatilidad Anualizada (%)", fontsize=9)
    ax.set_title("DISTRIBUCION DE VOLATILIDAD CON UMBRALES DE CLASIFICACION",
                 fontsize=12, fontweight="bold", color=S["accent"],
                 pad=12, fontfamily="monospace")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.1f}%"))
    ax.legend(fontsize=8, facecolor=S["surface"], labelcolor=S["fg"],
              edgecolor=S["grid"], loc="upper left")
    ax.grid(axis="y", color=S["grid"], linewidth=0.5, alpha=0.4)

    plt.tight_layout()
    _page_number_text(fig, 9)
    pdf.savefig(fig, facecolor=S["bg"])
    plt.close(fig)


# ===========================================================================
# PAGINA 10+ — CANDLESTICK POR ACTIVO
# ===========================================================================

def _page_candlestick(pdf, ticker, page_n, last_n=252):
    try:
        data = get_candlestick_data(ticker, sma_windows=[20, 50, 200])
    except Exception as e:
        print(f"[WARN] Candlestick {ticker}: {e}")
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
    n       = len(dates)

    fig = plt.figure(figsize=(PAGE_W, PAGE_H))
    fig.patch.set_facecolor(S["bg"])
    gs  = gridspec.GridSpec(2, 1, height_ratios=[3.5, 1], hspace=0.04,
                            left=0.05, right=0.95, top=0.93, bottom=0.08)
    ax1 = fig.add_subplot(gs[0])
    ax2 = fig.add_subplot(gs[1], sharex=ax1)
    _apply_dark(fig, ax1, ax2)

    # Velas
    for i in range(n):
        o, h, l, c = opens[i], highs[i], lows[i], closes[i]
        if None in (o, h, l, c):
            continue
        color = S["green"] if c >= o else S["red"]
        ax1.plot([i, i], [l, h], color=color, linewidth=0.7, zorder=2)
        body_h = max(abs(c - o), 0.01 * (max(closes) - min(closes) + 0.01))
        ax1.bar(i, body_h, bottom=min(o, c), color=color,
                width=0.55, linewidth=0, zorder=3, alpha=0.85)

    # SMAs
    for sma_vals, color, label in [
        (sma20,  "#f1c40f", "SMA 20"),
        (sma50,  "#3498db", "SMA 50"),
        (sma200, "#e67e22", "SMA 200"),
    ]:
        xs = [i for i, v in enumerate(sma_vals) if v is not None]
        ys = [v for v in sma_vals if v is not None]
        if xs:
            ax1.plot(xs, ys, color=color, linewidth=1.3, label=label, zorder=4)

    ax1.legend(loc="upper left", fontsize=8, facecolor=S["bg2"],
               labelcolor=S["fg"], edgecolor=S["grid"], framealpha=0.8)
    ax1.set_ylabel("Precio (USD)", fontsize=8)
    ax1.set_title(f"{ticker}  —  Candlestick  |  SMA 20 / 50 / 200  |  Ultimo anno aprox.",
                  fontsize=11, fontweight="bold", color=S["accent"],
                  pad=8, fontfamily="monospace")
    ax1.grid(axis="y", color=S["grid"], linewidth=0.4, alpha=0.5)
    plt.setp(ax1.get_xticklabels(), visible=False)

    # Estadisticas en el grafico
    if closes:
        valid = [c for c in closes if c is not None]
        stats_txt = (f"Min: ${min(valid):.2f}  |  Max: ${max(valid):.2f}  |  "
                     f"Ultimo: ${valid[-1]:.2f}  |  Cambio: "
                     f"{((valid[-1]/valid[0])-1)*100:+.1f}%")
        ax1.text(0.5, 0.01, stats_txt, transform=ax1.transAxes,
                 ha="center", fontsize=7.5, color=S["fg2"], fontfamily="monospace")

    # Volumen
    for i in range(n):
        o, c, v = opens[i], closes[i], volumes[i]
        if None in (o, c, v):
            continue
        color = S["green"] if c >= o else S["red"]
        ax2.bar(i, v, color=color, width=0.55, alpha=0.6, linewidth=0)

    ax2.set_ylabel("Volumen", fontsize=7)
    ax2.grid(axis="y", color=S["grid"], linewidth=0.4, alpha=0.4)
    ax2.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M" if x >= 1e6 else f"{x/1e3:.0f}K")
    )

    step = max(1, n // 10)
    ax2.set_xticks(range(0, n, step))
    ax2.set_xticklabels([dates[i][:7] for i in range(0, n, step)],
                        rotation=30, ha="right", fontsize=7, color=S["fg2"])

    _page_number_text(fig, page_n)
    pdf.savefig(fig, facecolor=S["bg"])
    plt.close(fig)


# ===========================================================================
# PAGINA FINAL — RESUMEN DE METRICAS
# ===========================================================================

def _page_summary(pdf, risk_data, corr_data, page_n):
    tickers = corr_data["tickers"]
    matrix  = corr_data["matrix"]

    # Par con mayor correlacion positiva (distinto de diagonal)
    max_corr = max(
        ((matrix[i][j], tickers[i], tickers[j])
         for i in range(len(tickers)) for j in range(len(tickers)) if i != j),
        key=lambda x: x[0]
    )
    # Par con menor correlacion
    min_corr = min(
        ((matrix[i][j], tickers[i], tickers[j])
         for i in range(len(tickers)) for j in range(len(tickers)) if i != j),
        key=lambda x: x[0]
    )

    fig = plt.figure(figsize=(PAGE_W, PAGE_H))
    fig.patch.set_facecolor(S["bg"])
    ax = fig.add_axes([0.04, 0.04, 0.92, 0.92])
    ax.set_facecolor(S["bg"]); ax.axis("off")

    ax.text(0.5, 0.96, "RESUMEN EJECUTIVO DE METRICAS",
            ha="center", fontsize=14, fontweight="bold", color=S["accent"],
            transform=ax.transAxes, fontfamily="monospace")
    ax.plot([0, 1], [0.92, 0.92], color=S["accent"],
            linewidth=1, transform=ax.transAxes, clip_on=False)

    # ── Correlaciones destacadas
    highlights = [
        ("Par mas correlacionado",
         f"{max_corr[1]} & {max_corr[2]}  r = {max_corr[0]:.4f}"),
        ("Par menos correlacionado",
         f"{min_corr[1]} & {min_corr[2]}  r = {min_corr[0]:.4f}"),
        ("Activo mas conservador",
         f"{risk_data[0]['ticker']}  vol = {risk_data[0]['volatility']:.2f}%"),
        ("Activo mas agresivo",
         f"{risk_data[-1]['ticker']}  vol = {risk_data[-1]['volatility']:.2f}%"),
        ("Volatilidad media del portfolio",
         f"{sum(a['volatility'] for a in risk_data)/len(risk_data):.2f}%"),
    ]

    y = 0.84
    for label, value in highlights:
        ax.text(0.05, y, label + ":", fontsize=10, color=S["fg2"],
                transform=ax.transAxes, fontfamily="monospace")
        ax.text(0.45, y, value, fontsize=10, color=S["fg"],
                transform=ax.transAxes, fontweight="bold")
        y -= 0.07

    ax.plot([0, 1], [y + 0.02, y + 0.02], color=S["grid"],
            linewidth=0.5, transform=ax.transAxes, clip_on=False)

    # ── Tabla final de todos los activos con su score
    ax.text(0.5, y - 0.02, "RANKING FINAL",
            ha="center", fontsize=11, fontweight="bold", color=S["accent"],
            transform=ax.transAxes, fontfamily="monospace")

    ax_table = fig.add_axes([0.04, 0.03, 0.92, y - 0.06])
    ax_table.set_facecolor(S["bg"]); ax_table.axis("off")

    headers = ["Pos", "Ticker", "Categoria", "Vol. Anual (%)", "Risk Score"]
    rows = [[str(a["position"]), a["ticker"], a["category"].upper(),
             f"{a['volatility']:.2f}%", "★" * a["risk_score"]]
            for a in risk_data]
    c_map = {"conservador": "#0d2318", "moderado": "#261a00", "agresivo": "#1f0000"}
    row_colors = [c_map.get(a["category"], S["bg2"]) for a in risk_data]

    _table_on_axes(ax_table, headers, rows,
                   col_widths=[0.08, 0.15, 0.22, 0.22, 0.22],
                   row_colors=row_colors, font_size=8)

    _page_number_text(fig, page_n)
    pdf.savefig(fig, facecolor=S["bg"])
    plt.close(fig)


# ===========================================================================
# FUNCION PRINCIPAL DE EXPORTACION
# ===========================================================================

def generate_pdf_report(output_path: str, candlestick_tickers=None):
    """
    Genera el reporte PDF completo con todas las secciones.

    Paginas generadas:
      1  — Portada con resumen ejecutivo
      2  — Indice de activos del portfolio
      3  — Heatmap de correlacion
      4  — Tabla numerica de correlaciones
      5  — Grafico de clasificacion de riesgo
      6  — Tabla detallada de riesgo
      7  — Graficos de frecuencia de patrones
      8  — Tabla de patrones
      9  — Scatter de volatilidad
      10+— Candlestick por ticker (1 pagina cada uno)
      N  — Resumen ejecutivo final
    """
    if candlestick_tickers is None:
        candlestick_tickers = ["NVDA", "TSLA", "VOO", "GLD"]

    # Pre-cargar datos una sola vez para no repetir llamadas
    print("[PDF] Cargando datos...")
    risk_result     = get_risk_ranking()
    patterns_result = get_patterns_summary()
    vol_result      = get_volatility_metrics()
    corr_result     = get_correlation_matrix()

    risk_data = risk_result.get("data", [])

    print(f"[PDF] Generando reporte: {output_path}")
    with PdfPages(output_path) as pdf:
        d = pdf.infodict()
        d["Title"]        = "Reporte Tecnico Bursatil — Analisis de Algoritmos"
        d["Author"]       = "Sistema de Analisis Financiero — UQ"
        d["Subject"]      = "Dashboard Bursatil — Requerimiento 4"
        d["CreationDate"] = datetime.now()

        _page_cover(pdf, risk_data, vol_result)         # p1
        _page_index(pdf, risk_data, vol_result)          # p2
        _page_heatmap(pdf, corr_result)                  # p3
        _page_corr_table(pdf, corr_result)               # p4
        _page_risk_chart(pdf, risk_data)                 # p5
        _page_risk_table(pdf, risk_data, vol_result)     # p6
        _page_patterns_chart(pdf, patterns_result)       # p7
        _page_patterns_table(pdf, patterns_result)       # p8
        _page_vol_scatter(pdf, risk_data)                # p9

        page_n = 10
        for ticker in candlestick_tickers:
            print(f"[PDF]   Candlestick: {ticker}")
            _page_candlestick(pdf, ticker, page_n)
            page_n += 1

        _page_summary(pdf, risk_data, corr_result, page_n)  # ultima pagina

    total_pages = page_n
    print(f"[PDF] Listo — {total_pages} paginas en {output_path}")
    return output_path