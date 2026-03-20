import csv
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