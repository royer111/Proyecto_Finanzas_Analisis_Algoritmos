"""
requerimiento_2.py
------------------
Punto de entrada principal para el Requerimiento 2.

Flujo:
  1. Carga merged_prices.csv (producido por el Requerimiento 1).
  2. Convierte el dataset wide a una lista de registros (date, close, ticker).
  3. Ejecuta los 12 algoritmos de ordenamiento midiendo el tiempo de cada uno.
  4. Construye la Tabla 1 y la exporta a CSV.
  5. Genera el diagrama de barras con los tiempos (ascendente).
  6. Obtiene los 15 dias con mayor volumen usando DataMerger.get_volume_records()
     y los ordena de forma ascendente, exportando el resultado a CSV.

PASOS PARA CONFIGURAR:
  1. Ajusta CSV_CLOSE_PATH si tu merged_prices.csv esta en otra ruta.
  2. Cambia el import de DataMerger (busca el comentario <- CAMBIA).
"""

import csv
import time
import os
import math
from datetime import datetime

# ===========================================================================
# CONFIGURACION
# ===========================================================================

# CSV_CLOSE_PATH
CSV_CLOSE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "..", "data", "merged", "merged_prices.csv"
)

# OUTPUT_DIR original: os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
# Ahora apuntamos al directorio actual (Seguimiento/First) para que los archivos generados queden ahí.
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
try:
    from backend.app.etl.data_merger import DataMerger
    DATA_MERGER_AVAILABLE = True
except ImportError:
    DATA_MERGER_AVAILABLE = False
    print("[WARN] DataMerger no pudo importarse. El analisis de volumen sera omitido.")
    print("       Ajusta el import de DataMerger al inicio de requerimiento_2.py")

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
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    print("[WARN] matplotlib no encontrado. Instala con: pip install matplotlib")

# ===========================================================================
# COMPLEJIDADES DE REFERENCIA PARA LA TABLA 1
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

SAMPLE_SIZE_SLOW    = 5000
SAMPLE_SIZE_BITONIC = 8192


# ===========================================================================
# 1. CARGA DEL CSV
# ===========================================================================

def load_csv(path):
    if not os.path.exists(path):
        raise FileNotFoundError(
            "Archivo no encontrado: " + path + "\n"

        )
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = [h.strip() for h in reader.fieldnames]
        tickers = [h for h in headers if h.lower() not in ("date", "fecha")]
        for row in reader:
            record = {}
            raw_date = (row.get("Date") or row.get("date") or row.get("Fecha") or "").strip()
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


# ===========================================================================
# 2. CONSTRUCCION DE REGISTROS
# ===========================================================================

def build_sort_records(tickers, rows):
    records = []
    for row in rows:
        for ticker in tickers:
            price = row.get(ticker)
            if price is not None:
                records.append((row["date"], price, ticker))
    return records


def sort_key_date_close(record):
    """Primario: fecha. Secundario: precio de cierre."""
    return (record[0].toordinal(), record[1])


# ===========================================================================
# 3. EJECUCION DE LOS 12 ALGORITMOS
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
    algorithms = [
        ("TimSort",               tim_sort,               len(records)),
        ("Comb Sort",             comb_sort,              len(records)),
        ("Selection Sort",        selection_sort,         SAMPLE_SIZE_SLOW),
        ("Tree Sort",             tree_sort,              len(records)),
        ("Pigeonhole Sort",       _pigeonhole_wrapper,    len(records)),
        ("Bucket Sort",           bucket_sort,            len(records)),
        ("QuickSort",             quick_sort,             len(records)),
        ("HeapSort",              heap_sort,              len(records)),
        ("Bitonic Sort",          bitonic_sort,           SAMPLE_SIZE_BITONIC),
        ("Gnome Sort",            gnome_sort,             SAMPLE_SIZE_SLOW),
        ("Binary Insertion Sort", binary_insertion_sort,  SAMPLE_SIZE_SLOW),
        ("Radix Sort",            _radix_wrapper,         len(records)),
    ]

    results = []
    for name, func, sample_n in algorithms:
        subset = _sample(records, sample_n) if sample_n < len(records) else list(records)
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
# 4. TABLA 1
# ===========================================================================

def export_table1(results, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "tabla1_ordenamientos.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Metodo de ordenamiento", "Complejidad", "Tamano", "Tiempo (s)"])
        for r in results:
            writer.writerow([r["nombre"], r["complejidad"], r["tamano"], str(round(r["tiempo_s"], 6))])
    print("[OK] Tabla 1 -> " + path)


# ===========================================================================
# 5. DIAGRAMA DE BARRAS
# ===========================================================================

def plot_bar_chart(results, output_dir):
    if not MATPLOTLIB_AVAILABLE:
        print("[SKIP] matplotlib no disponible.")
        return

    sorted_r = sorted(results, key=lambda r: r["tiempo_s"])
    names = [r["nombre"]   for r in sorted_r]
    times = [r["tiempo_s"] for r in sorted_r]
    sizes = [r["tamano"]   for r in sorted_r]

    fig, ax = plt.subplots(figsize=(14, 7))
    colors = plt.cm.viridis([i / len(names) for i in range(len(names))])
    bars = ax.bar(range(len(names)), times, color=colors, edgecolor="white", linewidth=0.8)

    max_t = max(times)
    for bar, t, s in zip(bars, times, sizes):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max_t * 0.01,
            str(round(t, 4)) + "s\n(n=" + str(s) + ")",
            ha="center", va="bottom", fontsize=7, color="#333333"
        )

    ax.set_xticks(range(len(names)))
    ax.set_xticklabels(names, rotation=35, ha="right", fontsize=9)
    ax.set_ylabel("Tiempo de ejecucion (segundos)", fontsize=11)
    ax.set_title("Requerimiento 2 - Tiempos de ordenamiento (ascendente)",
                 fontsize=13, fontweight="bold", pad=15)
    ax.set_xlabel("Algoritmo de ordenamiento", fontsize=11)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "diagrama_barras_tiempos.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print("[OK] Diagrama de barras -> " + path)


# ===========================================================================
# 6. TOP-15 DIAS CON MAYOR VOLUMEN
# ===========================================================================

def get_top15_volume(vol_records):
    sorted_desc = heap_sort(vol_records, key=lambda r: r[0])
    sorted_desc.reverse()
    top15 = sorted_desc[:15]
    return heap_sort(top15, key=lambda r: r[0])


def print_top15_volume(top15):
    sep = "=" * 62
    print("\n" + sep)
    print("  Top 15 dias con mayor volumen de negociacion (ascendente)")
    print(sep)
    print("  " + "#".rjust(3) + "  " + "Ticker".ljust(10) + "  " + "Fecha".ljust(12) + "  " + "Volumen".rjust(18))
    print("  " + "-"*3 + "  " + "-"*10 + "  " + "-"*12 + "  " + "-"*18)
    for i, (vol, dt, ticker) in enumerate(top15, 1):
        print("  " + str(i).rjust(3) + "  " + ticker.ljust(10) + "  " + str(dt).ljust(12) + "  " + str(int(vol)).rjust(18))


def export_top15_volume(top15, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "top15_mayor_volumen.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Posicion", "Ticker", "Fecha", "Volumen"])
        for i, (vol, dt, ticker) in enumerate(top15, 1):
            writer.writerow([i, ticker, str(dt), int(vol)])
    print("[OK] Top 15 volumen -> " + path)


# ===========================================================================
# 7. DATASET ORDENADO COMPLETO
# ===========================================================================

def export_sorted_dataset(sorted_records, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "dataset_ordenado_fecha_close.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Fecha", "Close", "Ticker"])
        for dt, close, ticker in sorted_records:
            writer.writerow([str(dt), str(round(close, 6)), ticker])
    print("[OK] Dataset ordenado -> " + path)


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    sep = "=" * 65
    print(sep)
    print("  REQUERIMIENTO 2 - Analisis de algoritmos de ordenamiento")
    print(sep)

    print("\n[1/5] Cargando: " + CSV_CLOSE_PATH)
    tickers, rows = load_csv(CSV_CLOSE_PATH)
    print("      Activos : " + str(len(tickers)))
    print("      Fechas  : " + str(len(rows)))

    print("\n[2/5] Construyendo registros (date, close, ticker) ...")
    records = build_sort_records(tickers, rows)
    print("      Total de registros: " + str(len(records)))

    print("\n[3/5] Ejecutando los 12 algoritmos de ordenamiento ...")
    sort_results = run_all_sorts(records)

    print("\n  " + "-"*62)
    print("  " + "Metodo".ljust(25) + " " + "Complejidad".ljust(18) + " " + "Tamano".rjust(8) + "  " + "Tiempo (s)".rjust(12))
    print("  " + "-"*25 + " " + "-"*18 + " " + "-"*8 + "  " + "-"*12)
    for r in sort_results:
        print("  " + r["nombre"].ljust(25) + " " + r["complejidad"].ljust(18) + " " + str(r["tamano"]).rjust(8) + "  " + str(round(r["tiempo_s"], 6)).rjust(12))

    print("\n[4/5] Exportando resultados ...")
    export_table1(sort_results, OUTPUT_DIR)
    timsort_result = next(r for r in sort_results if r["nombre"] == "TimSort")
    export_sorted_dataset(timsort_result["sorted_data"], OUTPUT_DIR)
    plot_bar_chart(sort_results, OUTPUT_DIR)

    print("\n[5/5] Analisis de los 15 dias con mayor volumen ...")
    if DATA_MERGER_AVAILABLE:
        merger = DataMerger()
        vol_records = merger.get_volume_records()
        print("      Registros de volumen: " + str(len(vol_records)))
        top15 = get_top15_volume(vol_records)
        print_top15_volume(top15)
        export_top15_volume(top15, OUTPUT_DIR)
    else:
        print("      [SKIP] DataMerger no disponible.")

    print("\n" + sep)
    print("  Completado. Resultados en: " + OUTPUT_DIR)
    print(sep)


if __name__ == "__main__":
    main()