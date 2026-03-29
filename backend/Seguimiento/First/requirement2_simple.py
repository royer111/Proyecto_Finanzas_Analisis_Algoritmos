import time
import os
import csv
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from datetime import datetime

from sorting_algorithms import (
    tim_sort, comb_sort, selection_sort, tree_sort,
    pigeonhole_sort, bucket_sort, quick_sort, heap_sort,
    bitonic_sort, gnome_sort, binary_insertion_sort, radix_sort,
)

try:
    from backend.app.etl.data_merger import DataMerger
    DATA_MERGER_AVAILABLE = True
except ImportError:
    DATA_MERGER_AVAILABLE = False
    print("[WARN] DataMerger no disponible. El top-15 de volumen sera omitido.")

# ================================================================================================================================ #
# CONFIGURACION DE RUTAS
# ================================================================================================================================ #

MERGED_CSV_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "..", "data", "merged", "merged_prices.csv"
)

OUTPUT_DIR_SORTING = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", "sorting_results")
OUTPUT_DIR_VOLUME  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", "top15_volume")

os.makedirs(OUTPUT_DIR_SORTING, exist_ok=True)
os.makedirs(OUTPUT_DIR_VOLUME,  exist_ok=True)

# ================================================================================================================================ #
# CARGA DEL ARCHIVO UNIFICADO
# ================================================================================================================================ #

def load_records(path=MERGED_CSV_PATH):
    """
    Carga el archivo merged_prices.csv (formato wide: Date + columnas de tickers)
    y lo convierte a una lista plana de dicts con la estructura:
        {
            "date":   date(2019, 1, 2),
            "close":  229.99,
            "ticker": "VOO"
        }
    Cada fila del CSV (un dia) genera tantos registros como activos haya.
    Este es el dataset sobre el que se aplican los 12 algoritmos de ordenamiento.
    """
    if not os.path.exists(path):
        raise FileNotFoundError("Archivo no encontrado: " + path)

    records = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers   = [h.strip() for h in reader.fieldnames]
        tickers   = [h for h in headers if h.lower() not in ("date", "fecha")]
        for row in reader:
            raw_date = (row.get("Date") or row.get("date") or "").strip()
            try:
                dt = datetime.strptime(raw_date, "%Y-%m-%d").date()
            except ValueError:
                continue
            for ticker in tickers:
                val = (row.get(ticker) or "").strip()
                try:
                    close = float(val)
                except (ValueError, TypeError):
                    continue
                records.append({
                    "date":   dt,
                    "close":  close,
                    "ticker": ticker,
                })
    return records

# ================================================================================================================================ #
# FUNCION CLAVE DE ORDENAMIENTO
# ================================================================================================================================ #

def sort_key_record(record):
    """
    Clave para ordenar registros financieros:
    1) Fecha de cotizacion (primario)
    2) Precio de cierre   (secundario — desempate cuando dos activos comparten fecha)
    """
    return (record["date"].toordinal(), record["close"])

# ================================================================================================================================ #
# ORGANIZACION DE LOS 12 ALGORITMOS
# ================================================================================================================================ #

algorithms = {
    "TimSort":               tim_sort,
    "Comb Sort":             comb_sort,
    "Selection Sort":        selection_sort,
    "Tree Sort":             tree_sort,
    "Pigeonhole Sort":       pigeonhole_sort,
    "Bucket Sort":           bucket_sort,
    "QuickSort":             quick_sort,
    "HeapSort":              heap_sort,
    "Bitonic Sort":          bitonic_sort,
    "Gnome Sort":            gnome_sort,
    "Binary Insertion Sort": binary_insertion_sort,
    "Radix Sort":            radix_sort,
}

# ================================================================================================================================ #
# REQUERIMIENTO 1 — Ordenar registros por fecha y precio de cierre
# ================================================================================================================================ #

def sort_records():
    """
    Ordena de manera ascendente todos los registros del archivo unificado.
    Criterio primario  : fecha de cotizacion del activo.
    Criterio secundario: precio de cierre (desempate cuando la fecha es igual).

    Por cada algoritmo:
      - Mide el tiempo de ejecucion con time.perf_counter().
      - Guarda el resultado en output/sorting_results/<Algoritmo>.csv
      - Imprime el tiempo en consola.

    Retorna un dict con los resultados de todos los algoritmos:
      { "TimSort": {"tiempo": 0.045, "orden": [...]}, ... }
    """
    archivo_unificado = load_records(MERGED_CSV_PATH)
    print("\nRegistros cargados: " + str(len(archivo_unificado)))

    resultados = {}

    for nombre, algoritmo in algorithms.items():
        arr = archivo_unificado.copy()

        # Inicia el conteo del tiempo del algoritmo
        start = time.perf_counter()
        ordenados = algoritmo(arr, key=sort_key_record)
        # Finaliza el conteo del tiempo del algoritmo
        end = time.perf_counter()

        resultados[nombre] = {
            "tiempo": end - start,
            "orden":  ordenados,
        }

        # Guardar resultado en CSV
        file_path = os.path.join(OUTPUT_DIR_SORTING, nombre.replace(" ", "_") + ".csv")
        with open(file_path, mode="w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Fecha", "Close", "Ticker"])
            for rec in ordenados:
                writer.writerow([str(rec["date"]), str(round(rec["close"], 6)), rec["ticker"]])

        print("\n--- " + nombre + " ---")
        print("Tiempo: " + str(round(resultados[nombre]["tiempo"], 6)) + " segundos")
        print("Resultados guardados en " + file_path)

    return resultados

# ================================================================================================================================ #
# REQUERIMIENTO 2 — Diagrama de barras con los tiempos de los 12 algoritmos
# ================================================================================================================================ #

def graph_times(resultados):
    """
    Genera un diagrama de barras ascendente con los tiempos de ejecucion
    de los 12 algoritmos de ordenamiento aplicados sobre el dataset financiero.
    Guarda la imagen en output/sorting_results/tiempos_algoritmos.png
    """
    algoritmos = list(resultados.keys())
    tiempos    = [resultados[n]["tiempo"] for n in algoritmos]

    # Ordenar de menor a mayor tiempo
    pares_ordenados     = sorted(zip(algoritmos, tiempos), key=lambda x: x[1])
    algoritmos_ordenados, tiempos_ordenados = zip(*pares_ordenados)

    plt.figure(figsize=(12, 7))
    bars = plt.barh(algoritmos_ordenados, tiempos_ordenados, color="steelblue", edgecolor="white")

    # Etiquetas de tiempo sobre cada barra
    max_t = max(tiempos_ordenados)
    for bar, t in zip(bars, tiempos_ordenados):
        plt.text(
            t + max_t * 0.005,
            bar.get_y() + bar.get_height() / 2,
            str(round(t, 4)) + " s",
            va="center", ha="left", fontsize=8
        )

    plt.xlabel("Tiempo de ejecucion (segundos)")
    plt.ylabel("Algoritmo")
    plt.title("Comparacion de tiempos de algoritmos de ordenamiento\n(datos financieros — fecha + precio de cierre)")
    plt.grid(axis="x", linestyle="--", alpha=0.6)
    plt.tight_layout()

    out_path = os.path.join(OUTPUT_DIR_SORTING, "tiempos_algoritmos.png")
    plt.savefig(out_path, dpi=300)
    plt.close()
    print("\n[OK] Diagrama de tiempos guardado en " + out_path)

# ================================================================================================================================ #
# REQUERIMIENTO 3 — Top 15 dias con mayor volumen de negociacion
# ================================================================================================================================ #

def sort_key_volume(record):
    """
    Clave para ordenar registros de volumen:
    1) Volumen de negociacion (primario)
    2) Fecha                  (secundario — desempate)
    """
    return (record[0], record[1])


def get_top15_volume():
    """
    Obtiene los 15 dias con mayor volumen de negociacion de todos los activos,
    los ordena de forma ascendente y exporta los resultados.

    Los datos de volumen se leen directamente desde los archivos
    {symbol}_clean.csv del Requerimiento 1 via DataMerger.get_volume_records(),
    que retorna una lista de tuplas (volume, date, ticker).

    El archivo merged_prices.csv solo contiene precios de cierre, por eso
    el volumen se obtiene desde la fuente original.
    """
    if not DATA_MERGER_AVAILABLE:
        print("[SKIP] DataMerger no disponible. No se puede calcular el top-15 de volumen.")
        return []

    merger      = DataMerger()
    vol_records = merger.get_volume_records()   # lista de (volume, date, ticker)

    print("\nRegistros de volumen cargados: " + str(len(vol_records)))

    # Ordenar descendente para obtener los 15 mayores
    sorted_desc = heap_sort(vol_records, key=lambda r: r[0])
    sorted_desc.reverse()
    top15_desc  = sorted_desc[:15]

    # Ordenar el top-15 de forma ascendente (como pide el enunciado)
    top15_asc = heap_sort(top15_desc, key=sort_key_volume)

    # Guardar en CSV
    file_path = os.path.join(OUTPUT_DIR_VOLUME, "top15_mayor_volumen.csv")
    with open(file_path, mode="w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Posicion", "Ticker", "Fecha", "Volumen"])
        for i, (vol, dt, ticker) in enumerate(top15_asc, 1):
            writer.writerow([i, ticker, str(dt), int(vol)])

    print("[OK] Top 15 volumen guardado en " + file_path)

    # Imprimir tabla en consola
    sep = "=" * 62
    print("\n" + sep)
    print("  Top 15 dias con mayor volumen de negociacion (ascendente)")
    print(sep)
    print("  " + "#".rjust(3) + "  " + "Ticker".ljust(10) +
          "  " + "Fecha".ljust(12) + "  " + "Volumen".rjust(18))
    print("  " + "-"*3 + "  " + "-"*10 + "  " + "-"*12 + "  " + "-"*18)
    for i, (vol, dt, ticker) in enumerate(top15_asc, 1):
        print("  " + str(i).rjust(3) + "  " + ticker.ljust(10) +
              "  " + str(dt).ljust(12) + "  " + str(int(vol)).rjust(18))

    return top15_asc


def graph_top15_volume(top15):
    """
    Genera un diagrama de barras horizontal con los 15 dias de mayor volumen,
    ordenados de forma ascendente (el de menor volumen del top queda arriba).
    Guarda la imagen en output/top15_volume/top15_volumen.png
    """
    if not top15:
        print("[SKIP] No hay datos de volumen para graficar.")
        return

    labels  = [str(dt) + " — " + ticker for (vol, dt, ticker) in top15]
    volumes = [vol for (vol, dt, ticker) in top15]

    plt.figure(figsize=(12, 7))
    bars = plt.barh(labels, volumes, color="coral", edgecolor="white")

    max_v = max(volumes)
    for bar, v in zip(bars, volumes):
        plt.text(
            v + max_v * 0.005,
            bar.get_y() + bar.get_height() / 2,
            "{:,.0f}".format(v),
            va="center", ha="left", fontsize=8
        )

    plt.xlabel("Volumen de negociacion")
    plt.ylabel("Fecha — Activo")
    plt.title("Top 15 dias con mayor volumen de negociacion (ascendente)")
    plt.gca().invert_yaxis()
    plt.grid(axis="x", linestyle="--", alpha=0.6)
    plt.tight_layout()

    out_path = os.path.join(OUTPUT_DIR_VOLUME, "top15_volumen.png")
    plt.savefig(out_path, dpi=300)
    plt.close()
    print("[OK] Diagrama top-15 volumen guardado en " + out_path)

# ================================================================================================================================ #
# MAIN
# ================================================================================================================================ #

if __name__ == "__main__":

    # Requerimiento 1 --------------------------------------------------------- #
    # Ordenar todos los registros del archivo unificado por fecha y precio cierre
    resultados = sort_records()

    # Requerimiento 2 --------------------------------------------------------- #
    # Diagrama de barras ascendente con los tiempos de los 12 algoritmos
    graph_times(resultados)

    # Requerimiento 3 --------------------------------------------------------- #
    # Obtener, ordenar y graficar los 15 dias con mayor volumen de negociacion
    top15 = get_top15_volume()
    graph_top15_volume(top15)