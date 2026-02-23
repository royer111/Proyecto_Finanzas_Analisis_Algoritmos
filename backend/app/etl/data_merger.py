import csv
from collections import defaultdict
from backend.app.config import settings


class DataMerger:
    """
    Encargado de:
    - Leer datos limpios desde data/processed/
    - Alinear activos por fecha
    - Construir matriz de precios combinada
    - Guardar dataset consolidado
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
    # CONSTRUIR MATRIZ ALINEADA POR FECHA
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
    # GUARDAR MATRIZ CONSOLIDADA
    # ==========================================================

    def save_merged_data(self, filtered_matrix):
        """
        Guarda la matriz consolidada en data/merged/merged_prices.csv
        """

        if not filtered_matrix:
            print("[WARNING] No hay datos para guardar en merged.")
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

        print(f"[OK] Datos consolidados guardados en: {file_path}")

    # ==========================================================
    # FLUJO COMPLETO DE MERGE
    # ==========================================================

    def merge_all_assets(self):
        """
        Ejecuta el proceso completo de unificación.
        """

        print("\nConstruyendo matriz de precios...")

        matrix = self.build_price_matrix()
        filtered_matrix = self.filter_complete_dates(matrix)

        self.save_merged_data(filtered_matrix)

        print("Matriz consolidada lista.\n")