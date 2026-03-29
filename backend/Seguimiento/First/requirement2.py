"""
requerimiento_2.py
------------------
Punto de entrada principal para el Requerimiento 2.

Flujo por cada CSV mergeado de precios encontrado:
  1. Detecta automaticamente todos los CSVs en data/merged/ que corresponden
     a precios (excluye volumenes y long_format).
  2. Por cada CSV: carga, construye registros, ejecuta 12 algoritmos,
     exporta Tabla 1, diagrama completo y diagrama sin Gnome/Selection Sort.
  3. Una sola vez: top-15 volumen via DataMerger + diagrama de barras horizontal.

CSVs procesados como precios:
  - merged_prices.csv        -> datos ordenados por fecha
  - merged_desorganized.csv  -> datos sin orden cronologico
  - merged_long_format.csv   -> formato long (Date, Ticker, Open, High, Low, Close, Volume)

Salidas en output/:
  Tabla 1:      tabla1_<nombre_csv>.csv          (una por CSV)
  Dataset ord.: dataset_ordenado_<nombre_csv>.csv (una por CSV)
  Diagrama 1:   diagrama_completo_<nombre_csv>.png (una por CSV, todos los alg.)
  Diagrama 2:   diagrama_sin_outliers_<nombre_csv>.png (una por CSV, sin Gnome/Selection)
  Top 15 vol.:  top15_mayor_volumen.csv           (una sola vez)
  Diagrama vol: diagrama_top15_volumen.png        (una sola vez)
"""

import csv
import time
import os
import math
from datetime import datetime

# ===========================================================================
# CONFIGURACION
# ===========================================================================

# Carpeta donde estan los CSVs mergeados
MERGED_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "..", "data", "merged"
)

# Carpeta de salida
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")

# CSVs a EXCLUIR del procesamiento de precios (no son datasets de precios wide/long)
EXCLUDE_FILES = {"merged_volumenes.csv"}

# Algoritmos excluidos del diagrama sin outliers
OUTLIER_ALGORITHMS = {"Selection Sort", "Gnome Sort"}

# ---------------------------------------------------------------------------
# Import de tu DataMerger — ajusta la ruta segun tu proyecto
# ---------------------------------------------------------------------------
try:
    from backend.app.etl.data_merger import DataMerger
    DATA_MERGER_AVAILABLE = True
except ImportError:
    DATA_MERGER_AVAILABLE = False
    print("[WARN] DataMerger no pudo importarse. El analisis de volumen sera omitido.")

# ===========================================================================
# IMPORTS INTERNOS
# ===========================================================================

from sorting_algorithms import (
    tim_sort, comb_sort, selection_sort, tree_sort,
    pigeonhole_sort, bucket_sort, quick_sort, heap_sort,
    bitonic_sort, gnome_sort, binary_insertion_sort, radix_sort,
)

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    print("[WARN] matplotlib no encontrado. Instala con: pip install matplotlib")

# ===========================================================================
# COMPLEJIDADES
# ===========================================================================

COMPLEXITIES = {
    "TimSort":               "O(n log n)",
    "Comb Sort":             "O(n log n)",
    "Selection Sort":        "O(n^2)",
    "Tree Sort":             "O(n log n)",
    "Pigeonhole Sort":       "O(n + range)",
    "Bucket Sort":           "O(n + k)",
    "QuickSort":             "O(n log n)",
    "HeapSort":              "O(n log n)",
    "Bitonic Sort":          "O(n log^2 n)",
    "Gnome Sort":            "O(n^2)",
    "Binary Insertion Sort": "O(n^2)",
    "Radix Sort":            "O(nk)",
}


# ===========================================================================
# DETECCION DE CSVs MERGEADOS
# ===========================================================================

def discover_merged_csvs(merged_dir):
    """
    Detecta todos los archivos CSV en merged_dir y los clasifica:
      - wide:      columnas Date + tickers (merged_prices, merged_desorganized)
      - long:      columnas Date, Ticker, Open, High, Low, Close, Volume
                   (merged_long_format)
      - excluded:  volumenes u otros que no son de precios

    Retorna lista de dicts:
      {"path": str, "name": str, "format": "wide"|"long"}
    ordenada para que merged_prices siempre sea el primero.
    """
    if not os.path.isdir(merged_dir):
        print("[WARN] Carpeta merged no encontrada: " + merged_dir)
        return []

    found = []
    for fname in sorted(os.listdir(merged_dir)):
        if not fname.endswith(".csv"):
            continue
        if fname in EXCLUDE_FILES:
            continue
        path = os.path.join(merged_dir, fname)
        fmt = _detect_csv_format(path)
        if fmt is None:
            continue
        found.append({"path": path, "name": fname.replace(".csv", ""), "format": fmt})

    # merged_prices primero si existe
    found.sort(key=lambda x: (0 if x["name"] == "merged_prices" else 1, x["name"]))
    return found


def _detect_csv_format(path):
    """
    Lee solo el header del CSV para determinar si es wide o long.
    Retorna "wide", "long" o None si no es procesable.
    """
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            headers = [h.strip().lower() for h in next(reader)]
    except Exception:
        return None

    long_cols = {"date", "ticker", "open", "high", "low", "close", "volume"}
    if long_cols.issubset(set(headers)):
        return "long"
    if "date" in headers and len(headers) > 2:
        return "wide"
    return None


# ===========================================================================
# CARGA DE CSVs
# ===========================================================================

def load_csv_wide(path):
    """
    Carga CSV de formato wide: Date + columnas de tickers con precio de cierre.
    Retorna (tickers, rows) donde rows es lista de dicts con 'date' y floats.
    """
    if not os.path.exists(path):
        raise FileNotFoundError("Archivo no encontrado: " + path)
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = [h.strip() for h in reader.fieldnames]
        tickers = [h for h in headers if h.lower() not in ("date", "fecha")]
        for row in reader:
            record = {}
            raw_date = (row.get("Date") or row.get("date") or "").strip()
            try:
                record["date"] = datetime.strptime(raw_date, "%Y-%m-%d").date()
            except ValueError:
                continue
            for t in tickers:
                val = (row.get(t) or "").strip()
                try:
                    record[t] = float(val)
                except (ValueError, TypeError):
                    record[t] = None
            rows.append(record)
    return tickers, rows


def load_csv_long(path):
    """
    Carga CSV de formato long: Date, Ticker, Open, High, Low, Close, Volume.
    Retorna (tickers, rows) con la misma estructura que load_csv_wide()
    para que el resto del pipeline sea identico.

    Estrategia: agrupa por fecha y construye un dict por fecha con
    {ticker: close_price}, equivalente al formato wide en memoria.
    """
    if not os.path.exists(path):
        raise FileNotFoundError("Archivo no encontrado: " + path)

    by_date = {}
    tickers_set = []

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_date = (row.get("Date") or row.get("date") or "").strip()
            try:
                dt = datetime.strptime(raw_date, "%Y-%m-%d").date()
            except ValueError:
                continue
            ticker = (row.get("Ticker") or row.get("ticker") or "").strip()
            if not ticker:
                continue
            try:
                close = float(row.get("Close") or row.get("close") or "")
            except (ValueError, TypeError):
                continue
            if dt not in by_date:
                by_date[dt] = {"date": dt}
            by_date[dt][ticker] = close
            if ticker not in tickers_set:
                tickers_set.append(ticker)

    tickers_set.sort()
    rows = list(by_date.values())
    return tickers_set, rows


def load_csv_auto(path, fmt):
    """Despacha al loader correcto segun el formato detectado."""
    if fmt == "long":
        return load_csv_long(path)
    return load_csv_wide(path)


# ===========================================================================
# CONSTRUCCION DE REGISTROS
# ===========================================================================

def build_sort_records(tickers, rows):
    """
    Convierte dataset wide (en memoria) a lista plana de tuplas
    (date, close_price, ticker). Un registro por cada par (fecha, activo).
    """
    records = []
    for row in rows:
        for ticker in tickers:
            price = row.get(ticker)
            if price is not None:
                records.append((row["date"], price, ticker))
    return records


def sort_key_date_close(record):
    """Clave compuesta: primario fecha, secundario precio de cierre."""
    return (record[0].toordinal(), record[1])


# ===========================================================================
# EJECUCION DE LOS 12 ALGORITMOS
# ===========================================================================

def _sample(records, n):
    step = max(1, len(records) // n)
    return records[::step][:n]


def _pigeonhole_wrapper(arr, key=lambda x: x):
    int_key = lambda r: r[0].toordinal()
    partial = pigeonhole_sort(arr, key=int_key)
    result = list(partial)
    i = 0
    while i < len(result):
        j = i
        while j < len(result) and result[j][0] == result[i][0]:
            j += 1
        for k in range(i + 1, j):
            tmp = result[k]
            m = k - 1
            while m >= i and result[m][1] > tmp[1]:
                result[m + 1] = result[m]
                m -= 1
            result[m + 1] = tmp
        i = j
    return result


def _radix_wrapper(arr, key=lambda x: x):
    int_key = lambda r: r[0].toordinal() * 10_000_000 + int(r[1] * 100)
    return radix_sort(arr, key=int_key)


def run_all_sorts(records):
    """
    Ejecuta los 12 algoritmos sobre records.
    Retorna lista de dicts: nombre, complejidad, tamano, tiempo_s, sorted_data.
    """
    n = len(records)
    bitonic_n = 1
    while bitonic_n < n:
        bitonic_n <<= 1

    algorithms = [
        ("TimSort",               tim_sort,               n),
        ("Comb Sort",             comb_sort,              n),
        ("Selection Sort",        selection_sort,         n),
        ("Tree Sort",             tree_sort,              n),
        ("Pigeonhole Sort",       _pigeonhole_wrapper,    n),
        ("Bucket Sort",           bucket_sort,            n),
        ("QuickSort",             quick_sort,             n),
        ("HeapSort",              heap_sort,              n),
        ("Bitonic Sort",          bitonic_sort,           bitonic_n),
        ("Gnome Sort",            gnome_sort,             n),
        ("Binary Insertion Sort", binary_insertion_sort,  n),
        ("Radix Sort",            _radix_wrapper,         n),
    ]

    results = []
    for name, func, sample_n in algorithms:
        subset = _sample(records, sample_n) if sample_n < len(records) else list(records)
        if sample_n > len(records):
            subset = list(records)
        print("  " + name.ljust(25) + " n=" + str(len(subset)).rjust(7) + " ...", end=" ", flush=True)
        t0 = time.perf_counter()
        sorted_data = func(subset, key=sort_key_date_close)
        elapsed = time.perf_counter() - t0
        print(str(round(elapsed, 6)) + " s")
        results.append({
            "nombre":      name,
            "complejidad": COMPLEXITIES[name],
            "tamano":      len(subset),
            "tiempo_s":    elapsed,
            "sorted_data": sorted_data,
        })
    return results


# ===========================================================================
# TABLA 1 — exportar a CSV
# ===========================================================================

def export_table1(results, output_dir, suffix=""):
    """
    Exporta la Tabla 1 con tiempos de todos los algoritmos.
    suffix: identificador del CSV fuente (ej: 'merged_prices').
    """
    os.makedirs(output_dir, exist_ok=True)
    fname = "tabla1_" + suffix + ".csv" if suffix else "tabla1_ordenamientos.csv"
    path = os.path.join(output_dir, fname)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Metodo de ordenamiento", "Complejidad", "Tamano", "Tiempo (s)"])
        for r in results:
            writer.writerow([r["nombre"], r["complejidad"], r["tamano"], str(round(r["tiempo_s"], 6))])
    print("[OK] Tabla 1 -> " + path)


# ===========================================================================
# DATASET ORDENADO COMPLETO
# ===========================================================================

def export_sorted_dataset(sorted_records, output_dir, suffix=""):
    """
    Exporta el dataset completo ordenado por (fecha, close) a CSV.
    suffix: identificador del CSV fuente.
    """
    os.makedirs(output_dir, exist_ok=True)
    fname = "dataset_ordenado_" + suffix + ".csv" if suffix else "dataset_ordenado.csv"
    path = os.path.join(output_dir, fname)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Fecha", "Close", "Ticker"])
        for dt, close, ticker in sorted_records:
            writer.writerow([str(dt), str(round(close, 6)), ticker])
    print("[OK] Dataset ordenado -> " + path)


# ===========================================================================
# DIAGRAMA DE BARRAS COMPLETO (todos los algoritmos)
# ===========================================================================

def plot_bar_chart(results, output_dir, suffix="", title_extra=""):
    """
    Diagrama de barras con los 12 algoritmos ordenados por tiempo ascendente.
    suffix:      identificador del CSV fuente para el nombre del archivo.
    title_extra: texto adicional en el titulo (ej: nombre del CSV).
    """
    if not MATPLOTLIB_AVAILABLE:
        print("[SKIP] matplotlib no disponible.")
        return

    sorted_r = sorted(results, key=lambda r: r["tiempo_s"])
    names = [r["nombre"]   for r in sorted_r]
    times = [r["tiempo_s"] for r in sorted_r]
    sizes = [r["tamano"]   for r in sorted_r]

    fig, ax = plt.subplots(figsize=(15, 7))
    colors = plt.cm.viridis([i / len(names) for i in range(len(names))])
    bars = ax.bar(range(len(names)), times, color=colors, edgecolor="white", linewidth=0.8)

    max_t = max(times)
    for bar, t, s in zip(bars, times, sizes):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max_t * 0.01,
            str(round(t, 4)) + "s\n(n=" + str(s) + ")",
            ha="center", va="bottom", fontsize=6.5, color="#333333"
        )

    ax.set_xticks(range(len(names)))
    ax.set_xticklabels(names, rotation=35, ha="right", fontsize=9)
    ax.set_ylabel("Tiempo de ejecucion (segundos)", fontsize=11)
    title = "Requerimiento 2 — Tiempos de ordenamiento (todos los algoritmos)"
    if title_extra:
        title += "\nFuente: " + title_extra
    ax.set_title(title, fontsize=12, fontweight="bold", pad=12)
    ax.set_xlabel("Algoritmo", fontsize=11)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()
    os.makedirs(output_dir, exist_ok=True)
    fname = "diagrama_completo_" + suffix + ".png" if suffix else "diagrama_completo.png"
    path = os.path.join(output_dir, fname)
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print("[OK] Diagrama completo -> " + path)


# ===========================================================================
# DIAGRAMA DE BARRAS SIN OUTLIERS (excluye Gnome Sort y Selection Sort)
# ===========================================================================

def plot_bar_chart_no_outliers(results, output_dir, suffix="", title_extra=""):
    """
    Diagrama de barras excluyendo Gnome Sort y Selection Sort.
    El eje Y se ajusta automaticamente al rango real de los algoritmos
    restantes, permitiendo apreciar las diferencias entre ellos.
    suffix:      identificador del CSV fuente.
    title_extra: texto adicional en el titulo.
    """
    if not MATPLOTLIB_AVAILABLE:
        print("[SKIP] matplotlib no disponible.")
        return

    filtered = [r for r in results if r["nombre"] not in OUTLIER_ALGORITHMS]
    sorted_r = sorted(filtered, key=lambda r: r["tiempo_s"])
    names = [r["nombre"]   for r in sorted_r]
    times = [r["tiempo_s"] for r in sorted_r]
    sizes = [r["tamano"]   for r in sorted_r]

    # Paleta diferenciada por complejidad
    complexity_colors = {
        "O(n + range)":  "#2ecc71",
        "O(n + k)":      "#27ae60",
        "O(n log n)":    "#3498db",
        "O(n log^2 n)":  "#9b59b6",
        "O(nk)":         "#e67e22",
        "O(n^2)":        "#e74c3c",
    }
    bar_colors = [complexity_colors.get(COMPLEXITIES.get(n, ""), "#95a5a6") for n in names]

    fig, ax = plt.subplots(figsize=(15, 7))
    bars = ax.bar(range(len(names)), times, color=bar_colors, edgecolor="white", linewidth=0.8)

    max_t = max(times) if times else 1
    for bar, t, s in zip(bars, times, sizes):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max_t * 0.015,
            str(round(t, 4)) + "s\n(n=" + str(s) + ")",
            ha="center", va="bottom", fontsize=7, color="#333333"
        )

    ax.set_xticks(range(len(names)))
    ax.set_xticklabels(names, rotation=35, ha="right", fontsize=9)

    # Eje Y: desde 0 hasta max + 15% de margen para las etiquetas
    ax.set_ylim(0, max_t * 1.25)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.4f s"))
    ax.set_ylabel("Tiempo de ejecucion (segundos)", fontsize=11)

    title = "Requerimiento 2 — Tiempos de ordenamiento (sin Gnome Sort / Selection Sort)"
    if title_extra:
        title += "\nFuente: " + title_extra
    ax.set_title(title, fontsize=12, fontweight="bold", pad=12)
    ax.set_xlabel("Algoritmo", fontsize=11)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Leyenda de colores por complejidad
    from matplotlib.patches import Patch
    legend_items = [
        Patch(color="#2ecc71", label="O(n + range)"),
        Patch(color="#27ae60", label="O(n + k)"),
        Patch(color="#3498db", label="O(n log n)"),
        Patch(color="#9b59b6", label="O(n log^2 n)"),
        Patch(color="#e67e22", label="O(nk)"),
    ]
    ax.legend(handles=legend_items, loc="upper left", fontsize=8,
              title="Complejidad", title_fontsize=8, framealpha=0.7)

    plt.tight_layout()
    os.makedirs(output_dir, exist_ok=True)
    fname = "diagrama_sin_outliers_" + suffix + ".png" if suffix else "diagrama_sin_outliers.png"
    path = os.path.join(output_dir, fname)
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print("[OK] Diagrama sin outliers -> " + path)


# ===========================================================================
# TOP-15 VOLUMEN
# ===========================================================================

def get_top15_volume(vol_records):
    """Retorna los 15 registros con mayor volumen, ordenados ascendente."""
    sorted_desc = heap_sort(vol_records, key=lambda r: r[0])
    sorted_desc.reverse()
    top15 = sorted_desc[:15]
    return heap_sort(top15, key=lambda r: r[0])


def print_top15_volume(top15):
    sep = "=" * 62
    print("\n" + sep)
    print("  Top 15 dias con mayor volumen de negociacion (ascendente)")
    print(sep)
    print("  " + "#".rjust(3) + "  " + "Ticker".ljust(10) + "  " +
          "Fecha".ljust(12) + "  " + "Volumen".rjust(18))
    print("  " + "-"*3 + "  " + "-"*10 + "  " + "-"*12 + "  " + "-"*18)
    for i, (vol, dt, ticker) in enumerate(top15, 1):
        print("  " + str(i).rjust(3) + "  " + ticker.ljust(10) + "  " +
              str(dt).ljust(12) + "  " + str(int(vol)).rjust(18))


def export_top15_volume(top15, output_dir):
    """Exporta el top-15 de volumen a CSV."""
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "top15_mayor_volumen.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Posicion", "Ticker", "Fecha", "Volumen"])
        for i, (vol, dt, ticker) in enumerate(top15, 1):
            writer.writerow([i, ticker, str(dt), int(vol)])
    print("[OK] Top 15 volumen -> " + path)


def plot_top15_volume(top15, output_dir):
    """
    Diagrama de barras horizontal para los 15 dias con mayor volumen.
    Barras horizontales para que los tickers y fechas sean legibles.
    """
    if not MATPLOTLIB_AVAILABLE:
        print("[SKIP] matplotlib no disponible.")
        return

    labels = [str(dt) + " — " + ticker for (vol, dt, ticker) in top15]
    volumes = [vol for (vol, dt, ticker) in top15]

    # Colores por magnitud de volumen
    max_v = max(volumes)
    bar_colors = [plt.cm.YlOrRd(0.3 + 0.7 * v / max_v) for v in volumes]

    fig, ax = plt.subplots(figsize=(12, 7))
    bars = ax.barh(range(len(labels)), volumes, color=bar_colors,
                   edgecolor="white", linewidth=0.6)

    for bar, v in zip(bars, volumes):
        ax.text(
            v + max_v * 0.005,
            bar.get_y() + bar.get_height() / 2,
            "{:,.0f}".format(v),
            va="center", ha="left", fontsize=8, color="#333333"
        )

    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=8.5)
    ax.invert_yaxis()
    ax.set_xlabel("Volumen de negociacion", fontsize=11)
    ax.set_title("Top 15 dias con mayor volumen de negociacion (orden ascendente)",
                 fontsize=12, fontweight="bold", pad=12)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(
        lambda x, _: "{:,.0f}".format(x)
    ))
    ax.grid(axis="x", linestyle="--", alpha=0.4)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "diagrama_top15_volumen.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print("[OK] Diagrama top15 volumen -> " + path)


# ===========================================================================
# PROCESAMIENTO DE UN CSV MERGEADO
# ===========================================================================

def process_csv(csv_info, output_dir):
    """
    Ejecuta el pipeline completo de ordenamiento para un CSV mergeado.
    csv_info: dict con keys 'path', 'name', 'format'.
    Genera: tabla1, dataset_ordenado, diagrama_completo, diagrama_sin_outliers.
    """
    name   = csv_info["name"]
    path   = csv_info["path"]
    fmt    = csv_info["format"]
    suffix = name

    print("\n" + "=" * 65)
    print("  CSV: " + name + "  [formato: " + fmt + "]")
    print("=" * 65)

    print("\n  [1/4] Cargando datos...")
    tickers, rows = load_csv_auto(path, fmt)
    print("        Activos : " + str(len(tickers)))
    print("        Fechas  : " + str(len(rows)))

    print("\n  [2/4] Construyendo registros (date, close, ticker)...")
    records = build_sort_records(tickers, rows)
    print("        Total registros: " + str(len(records)))

    print("\n  [3/4] Ejecutando 12 algoritmos de ordenamiento...")
    sort_results = run_all_sorts(records)

    print("\n        " + "-" * 60)
    print("        " + "Metodo".ljust(25) + " " + "Complejidad".ljust(18) +
          " " + "Tamano".rjust(7) + "  " + "Tiempo (s)".rjust(11))
    print("        " + "-" * 60)
    for r in sort_results:
        print("        " + r["nombre"].ljust(25) + " " +
              r["complejidad"].ljust(18) + " " +
              str(r["tamano"]).rjust(7) + "  " +
              str(round(r["tiempo_s"], 6)).rjust(11))

    print("\n  [4/4] Exportando resultados...")
    export_table1(sort_results, output_dir, suffix)
    timsort_result = next(r for r in sort_results if r["nombre"] == "TimSort")
    export_sorted_dataset(timsort_result["sorted_data"], output_dir, suffix)
    plot_bar_chart(sort_results, output_dir, suffix, title_extra=name)
    plot_bar_chart_no_outliers(sort_results, output_dir, suffix, title_extra=name)

    return sort_results


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    sep = "=" * 65
    print(sep)
    print("  REQUERIMIENTO 2 - Analisis de algoritmos de ordenamiento")
    print(sep)

    # 1. Descubrir todos los CSVs mergeados de precios
    print("\n[INFO] Buscando CSVs en: " + MERGED_DIR)
    csv_list = discover_merged_csvs(MERGED_DIR)

    if not csv_list:
        print("[ERROR] No se encontraron CSVs mergeados en: " + MERGED_DIR)
        return

    print("[INFO] CSVs detectados:")
    for c in csv_list:
        print("       - " + c["name"] + ".csv  (" + c["format"] + ")")

    # 2. Procesar cada CSV
    all_results = {}
    for csv_info in csv_list:
        results = process_csv(csv_info, OUTPUT_DIR)
        all_results[csv_info["name"]] = results

    # 3. Top-15 volumen (una sola vez, independiente del CSV de precios)
    print("\n" + sep)
    print("  ANALISIS DE VOLUMEN")
    print(sep)

    if DATA_MERGER_AVAILABLE:
        print("\n[VOL] Obteniendo registros de volumen via DataMerger...")
        merger = DataMerger()
        vol_records = merger.get_volume_records()
        print("      Registros de volumen: " + str(len(vol_records)))
        top15 = get_top15_volume(vol_records)
        print_top15_volume(top15)
        export_top15_volume(top15, OUTPUT_DIR)
        plot_top15_volume(top15, OUTPUT_DIR)
    else:
        print("[SKIP] DataMerger no disponible — analisis de volumen omitido.")

    # 4. Resumen de archivos generados
    print("\n" + sep)
    print("  RESUMEN DE SALIDAS GENERADAS")
    print(sep)
    for csv_info in csv_list:
        n = csv_info["name"]
        print("  [" + n + "]")
        print("    tabla1_" + n + ".csv")
        print("    dataset_ordenado_" + n + ".csv")
        print("    diagrama_completo_" + n + ".png")
        print("    diagrama_sin_outliers_" + n + ".png")
    if DATA_MERGER_AVAILABLE:
        print("  [volumen]")
        print("    top15_mayor_volumen.csv")
        print("    diagrama_top15_volumen.png")

    print("\n  Todos los archivos en: " + OUTPUT_DIR)
    print(sep)


if __name__ == "__main__":
    main()