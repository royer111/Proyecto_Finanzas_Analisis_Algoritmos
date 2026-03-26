import csv
import random
from collections import defaultdict
from backend.app.config import settings


class DataMerger:
    """
    Encargado de:
    - Leer datos limpios desde data/processed/
    - Alinear activos por fecha
    - Construir matriz de precios combinada
    - Construir matriz de volumenes combinada
    - Guardar datasets consolidados
    """

    def __init__(self):
        self.assets = settings.ASSETS
        self.processed_data_path = settings.PROCESSED_DATA_PATH
        self.merged_data_path = settings.MERGED_DATA_PATH

    # ==========================================================
    # CARGAR DATOS LIMPIOS DE UN ACTIVO
    # ==========================================================

    def load_clean_data(self, symbol):
        """
        Carga datos limpios desde data/processed/{symbol}_clean.csv
        """
        file_path = self.processed_data_path / f"{symbol}_clean.csv"

        if not file_path.exists():
            print(f"[ERROR] Archivo no encontrado: {file_path}")
            return []

        with open(file_path, mode="r") as file:
            reader = csv.DictReader(file)
            return list(reader)

    # ==========================================================
    # CONSTRUIR MATRIZ DE PRECIOS DE CIERRE ALINEADA POR FECHA
    # ==========================================================

    def build_price_matrix(self):
        """
        Construye una matriz de precios de cierre alineada por fecha.
        """
        price_matrix = defaultdict(dict)

        for symbol in self.assets:
            data = self.load_clean_data(symbol)
            for row in data:
                date = row["Date"]
                close_price = float(row["Close"])
                price_matrix[date][symbol] = close_price

        return price_matrix

    # ==========================================================
    # CONSTRUIR MATRIZ DE VOLUMEN ALINEADA POR FECHA
    # ==========================================================

    def build_volume_matrix(self):
        """
        Construye una matriz de volumenes alineada por fecha.
        Misma logica que build_price_matrix() pero leyendo 'Volume'.
        """
        volume_matrix = defaultdict(dict)

        for symbol in self.assets:
            data = self.load_clean_data(symbol)
            for row in data:
                date = row["Date"]
                try:
                    volume = float(row["Volume"])
                except (ValueError, KeyError):
                    volume = None

                if volume is not None:
                    volume_matrix[date][symbol] = volume

        return volume_matrix

    # ==========================================================
    # FILTRAR SOLO FECHAS COMPLETAS
    # ==========================================================

    def filter_complete_dates(self, price_matrix):
        """
        Conserva solo fechas donde TODOS los activos tengan datos.
        """
        filtered_matrix = {}

        for date, values in price_matrix.items():
            if len(values) == len(self.assets):
                filtered_matrix[date] = values

        return filtered_matrix

    # ==========================================================
    # GUARDAR MATRIZ DE PRECIOS CONSOLIDADA
    # ==========================================================

    def save_merged_data(self, filtered_matrix):
        """
        Guarda la matriz de precios de cierre en data/merged/merged_prices.csv
        """
        if not filtered_matrix:
            print("[WARNING] No hay datos de precios para guardar en merged.")
            return

        file_path = self.merged_data_path / "merged_prices.csv"
        fieldnames = ["Date"] + self.assets

        with open(file_path, mode="w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for date in sorted(filtered_matrix.keys()):
                row = {"Date": date}
                row.update(filtered_matrix[date])
                writer.writerow(row)

        print(f"[OK] Precios consolidados guardados en: {file_path}")

    # ==========================================================
    # GUARDAR MATRIZ DE VOLUMENES CONSOLIDADA
    # ==========================================================

    def save_merged_data_volumen(self, filtered_matrix):
        """
        Guarda la matriz de volumenes en data/merged/merged_volumenes.csv
        """
        if not filtered_matrix:
            print("[WARNING] No hay datos de volumenes para guardar en merged.")
            return

        file_path = self.merged_data_path / "merged_volumenes.csv"
        fieldnames = ["Date"] + self.assets

        with open(file_path, mode="w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for date in sorted(filtered_matrix.keys()):
                row = {"Date": date}
                row.update(filtered_matrix[date])
                writer.writerow(row)

        print(f"[OK] Volumenes consolidados guardados en: {file_path}")

    # ==========================================================
    # FLUJO COMPLETO: MERGE DE PRECIOS
    # ==========================================================

    def merge_all_assets(self):
        """
        Construye y guarda la matriz de precios de cierre.
        Genera: data/merged/merged_prices.csv
        """
        print("\nConstruyendo matriz de precios de cierre...")
        matrix = self.build_price_matrix()
        filtered_matrix = self.filter_complete_dates(matrix)
        self.save_merged_data(filtered_matrix)
        print("Matriz de precios lista.\n")

    # ==========================================================
    # FLUJO COMPLETO: MERGE DE VOLUMENES
    # ==========================================================

    def merge_volume(self):
        """
        Construye y guarda la matriz de volumenes.
        Genera: data/merged/merged_volumenes.csv
        """
        print("\nConstruyendo matriz de volumenes...")
        matrix = self.build_volume_matrix()
        filtered_matrix = self.filter_complete_dates(matrix)
        self.save_merged_data_volumen(filtered_matrix)   # <- corregido: era save_merged_data
        print("Matriz de volumenes lista.\n")

    # ==========================================================
    # OBTENER LISTA PLANA DE REGISTROS DE VOLUMEN
    # ==========================================================

    def get_volume_records(self):
        """
        Retorna una lista plana de tuplas (volume, date, symbol)
        con todos los registros de volumen disponibles en los
        archivos limpios individuales ({symbol}_clean.csv).

        No requiere que todos los activos tengan datos en la misma
        fecha — incluye todos los registros validos.

        Uso directo en el Requerimiento 2 para el top-15 de volumen.
        """
        records = []
        volume_matrix = self.build_volume_matrix()

        for date, assets in volume_matrix.items():
            for symbol, volume in assets.items():
                if volume and volume > 0:
                    records.append((volume, date, symbol))

        return records


    # ==========================================================
    # CONSTRUIR MATRIZ DE PRECIOS SIN ORDENAR POR FECHA
    # ==========================================================
    """
   def build_price_matrix_unordered(self):
      
        Construye una matriz de precios de cierre igual que
        build_price_matrix(), pero preserva el orden de insercion
        de las fechas tal como aparecen en los archivos _clean.csv,
        sin aplicar ningun criterio de ordenamiento.

        Proposito: generar un dataset desordenado para evaluar el
        comportamiento real de los algoritmos de ordenamiento con
        datos de entrada sin estructura previa.
        
        price_matrix = defaultdict(dict)

        for symbol in self.assets:
            data = self.load_clean_data(symbol)
            for row in data:
                date = row["Date"]
                close_price = float(row["Close"])
                price_matrix[date][symbol] = close_price

        return price_matrix
        """

#------------------------------

    def build_price_matrix_unordered(self):
        """
        Construye una matriz de precios SIN ningun orden temporal real.

        Diferencia clave:
        - Se mezclan (shuffle) las filas de cada activo antes
          de insertarlas en la matriz.
        - El orden final depende del azar, no del archivo fuente.
        """

        price_matrix = defaultdict(dict)

        for symbol in self.assets:
            data = self.load_clean_data(symbol)

            # 🔥 CLAVE: romper el orden cronologico del archivo
            random.shuffle(data)

            for row in data:
                date = row["Date"]
                close_price = float(row["Close"])
                price_matrix[date][symbol] = close_price

        return price_matrix

    # ==========================================================
    # GUARDAR MATRIZ DE PRECIOS DESORDENADA
    # ==========================================================

    def save_merged_data_unordered(self, price_matrix):
        """
        Guarda la matriz de precios SIN ordenar por fecha en
        data/merged/merged_desorganized.csv

        A diferencia de save_merged_data(), este metodo:
        - NO aplica sorted() sobre las fechas.
        - Conserva el orden en que las fechas fueron insertadas
          en el diccionario (orden de procesamiento de activos).
        - Solo incluye fechas donde TODOS los activos tienen datos
          (misma logica de completitud que filter_complete_dates).
        """
        # Filtrar fechas completas sin reordenar
        filtered = {
            date: values
            for date, values in price_matrix.items()
            if len(values) == len(self.assets)
        }

        if not filtered:
            print("[WARNING] No hay datos para guardar en merged_desorganized.")
            return

        file_path = self.merged_data_path / "merged_desorganized.csv"
        fieldnames = ["Date"] + self.assets

        with open(file_path, mode="w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            # Iteracion directa — sin sorted(), preserva el orden de insercion
            for date, values in filtered.items():
                row = {"Date": date}
                row.update(values)
                writer.writerow(row)

        print(f"[OK] Datos desordenados guardados en: {file_path}")

    # ==========================================================
    # FLUJO COMPLETO: MERGE DE PRECIOS DESORDENADO
    # ==========================================================

    def merge_unordered_assets(self):
        """
        Construye y guarda la matriz de precios sin ordenar por fecha.
        Genera: data/merged/merged_desorganized.csv

        El orden de las fechas en el archivo resultante depende del
        orden en que Python procesa los activos y las fechas dentro
        de cada archivo _clean.csv, produciendo un dataset mezclado
        adecuado para evaluar algoritmos de ordenamiento con
        datos de entrada sin estructura previa.
        """
        print("\nConstruyendo matriz de precios desordenada...")
        matrix = self.build_price_matrix_unordered()
        self.save_merged_data_unordered(matrix)
        print("Matriz desordenada lista.\n")



    # ==========================================================
    # CONSTRUIR DATASET COMPLETO EN FORMATO LONG (OHLCV + Ticker)
    # ==========================================================

    def build_long_format_matrix(self):
        """
        Construye una lista de registros en formato long con todas
        las columnas OHLCV para cada activo y fecha.

        Estructura de cada registro (dict):
          {
            "Date":   "2019-01-02",
            "Ticker": "VOO",
            "Open":   229.99,
            "High":   231.54,
            "Low":    228.12,
            "Close":  229.99,
            "Volume": 3500000.0
          }

        A diferencia del formato wide (merged_prices.csv donde cada
        activo es una columna), aqui cada fila es un par (fecha, activo),
        lo que permite analizar todos los atributos de cada activo
        de forma individual.

        Retorna una lista de dicts ordenada por (Date, Ticker).
        """
        records = []

        for symbol in self.assets:
            data = self.load_clean_data(symbol)
            for row in data:
                try:
                    record = {
                        "Date":   row.get("Date", "").strip(),
                        "Ticker": symbol,
                        "Open":   float(row["Open"])   if row.get("Open")   else None,
                        "High":   float(row["High"])   if row.get("High")   else None,
                        "Low":    float(row["Low"])    if row.get("Low")    else None,
                        "Close":  float(row["Close"])  if row.get("Close")  else None,
                        "Volume": float(row["Volume"]) if row.get("Volume") else None,
                    }
                    # Solo incluir registros con al menos Date, Ticker y Close validos
                    if record["Date"] and record["Close"] is not None:
                        records.append(record)
                except (ValueError, KeyError):
                    continue

        # Ordenar por fecha ascendente, luego por ticker alfabetico
        records.sort(key=lambda r: (r["Date"], r["Ticker"]))
        return records

    # ==========================================================
    # GUARDAR DATASET COMPLETO EN FORMATO LONG
    # ==========================================================

    def save_long_format_data(self, records):
        """
        Guarda el dataset completo en formato long en:
          data/merged/merged_long_format.csv

        Columnas: Date, Ticker, Open, High, Low, Close, Volume

        Este archivo tiene la misma estructura que produce
        requerimiento_2.py en dataset_ordenado_fecha_close.csv
        pero incluye TODAS las columnas OHLCV, no solo Close.
        Es el formato ideal para analisis por activo individual
        y para los algoritmos de ordenamiento del Requerimiento 2.
        """
        if not records:
            print("[WARNING] No hay datos para guardar en merged_long_format.")
            return

        file_path = self.merged_data_path / "merged_long_format.csv"
        fieldnames = ["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]

        with open(file_path, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for record in records:
                # Escribir None como cadena vacia para consistencia
                row = {
                    "Date":   record["Date"],
                    "Ticker": record["Ticker"],
                    "Open":   round(record["Open"],   6) if record["Open"]   is not None else "",
                    "High":   round(record["High"],   6) if record["High"]   is not None else "",
                    "Low":    round(record["Low"],    6) if record["Low"]    is not None else "",
                    "Close":  round(record["Close"],  6) if record["Close"]  is not None else "",
                    "Volume": int(record["Volume"])      if record["Volume"] is not None else "",
                }
                writer.writerow(row)

        total_dates   = len(set(r["Date"]   for r in records))
        total_tickers = len(set(r["Ticker"] for r in records))
        print(f"[OK] Dataset long format guardado en: {file_path}")
        print(f"     Registros: {len(records):,}  |  Fechas: {total_dates:,}  |  Activos: {total_tickers}")

    # ==========================================================
    # FLUJO COMPLETO: MERGE EN FORMATO LONG
    # ==========================================================

    def merge_long_format(self):
        """
        Construye y guarda el dataset completo en formato long.
        Genera: data/merged/merged_long_format.csv

        Diferencias respecto a merge_all_assets():
          - Formato long en lugar de wide: una fila por (fecha, activo)
            en lugar de una fila por fecha con un activo por columna.
          - Incluye todas las columnas OHLCV (Open, High, Low, Close, Volume)
            en lugar de solo Close.
          - NO filtra fechas incompletas: incluye todos los registros
            validos de cada activo aunque otros activos no tengan datos
            ese dia. Esto preserva el historial completo de cada activo.
          - Columna Ticker identifica el activo en cada fila.
        """
        print("\nConstruyendo dataset completo en formato long (OHLCV)...")
        records = self.build_long_format_matrix()
        self.save_long_format_data(records)
        print("Dataset long format listo.\n")