import csv
from datetime import datetime
from backend.app.config import settings


class DataCleaner:
    """
    Encargado de:
    - Leer datos crudos
    - Validar estructura
    - Validar integridad temporal
    - Eliminar duplicados
    - Detectar valores faltantes
    - Detectar anomalías (outliers)
    - Convertir tipos
    - Guardar datos procesados
    """

    def __init__(self):
        self.assets = settings.ASSETS
        self.raw_data_path = settings.RAW_DATA_PATH
        self.processed_data_path = settings.PROCESSED_DATA_PATH

        self.required_columns = settings.REQUIRED_COLUMNS
        self.drop_nulls = settings.DROP_ROWS_WITH_NULLS
        self.anomaly_threshold = settings.ANOMALY_THRESHOLD

    # ==========================================================
    # CARGA DE DATOS CRUDOS
    # ==========================================================

    def load_raw_data(self, symbol):
        file_path = self.raw_data_path / f"{symbol}.csv"

        if not file_path.exists():
            print(f"[ERROR] Archivo no encontrado: {file_path}")
            return []

        with open(file_path, mode="r") as file:
            reader = csv.DictReader(file)
            return list(reader)

    # ==========================================================
    # VALIDACIÓN DE COLUMNAS
    # ==========================================================

    def validate_columns(self, data):
        if not data:
            return False

        columns = data[0].keys()

        for col in self.required_columns:
            if col not in columns:
                print(f"[ERROR] Columna faltante: {col}")
                return False

        return True

    # ==========================================================
    # ELIMINAR DUPLICADOS
    # ==========================================================

    def remove_duplicates(self, data):
        seen_dates = set()
        unique_data = []

        for row in data:
            if row["Date"] not in seen_dates:
                unique_data.append(row)
                seen_dates.add(row["Date"])

        return unique_data

    # ==========================================================
    # VALIDAR INTEGRIDAD TEMPORAL
    # ==========================================================

    def validate_time_series_integrity(self, data):
        """
        Verifica que las fechas estén ordenadas cronológicamente.
        """

        try:
            dates = [datetime.strptime(row["Date"], "%Y-%m-%d") for row in data]
            if dates != sorted(dates):
                print("[WARNING] Fechas desordenadas detectadas.")
                data.sort(key=lambda x: x["Date"])
        except Exception:
            print("[WARNING] Error validando fechas.")

        return data

    # ==========================================================
    # MANEJO DE VALORES FALTANTES
    # ==========================================================

    def handle_missing_values(self, data):
        if not self.drop_nulls:
            return data

        cleaned = []

        for row in data:
            if all(row[col] not in (None, "", "null") for col in self.required_columns):
                cleaned.append(row)

        return cleaned

    # ==========================================================
    # CONVERSIÓN DE TIPOS
    # ==========================================================

    def convert_data_types(self, data):
        converted = []

        for row in data:
            try:
                converted.append({
                    "Date": datetime.strptime(row["Date"], "%Y-%m-%d").strftime("%Y-%m-%d"),
                    "Open": float(row["Open"]),
                    "High": float(row["High"]),
                    "Low": float(row["Low"]),
                    "Close": float(row["Close"]),
                    "Volume": int(float(row["Volume"]))
                })
            except Exception:
                continue

        return converted

    # ==========================================================
    # DETECCIÓN DE ANOMALÍAS
    # ==========================================================

    def detect_and_handle_anomalies(self, data):
        """
        Detecta outliers basados en retornos diarios.
        Si el retorno absoluto supera el umbral, se elimina la fila.
        """

        if not data or len(data) < 2:
            return data

        cleaned = [data[0]]

        for i in range(1, len(data)):
            prev_close = cleaned[-1]["Close"]
            current_close = data[i]["Close"]

            if prev_close == 0:
                continue

            daily_return = abs((current_close - prev_close) / prev_close)

            if daily_return <= self.anomaly_threshold:
                cleaned.append(data[i])
            else:
                print(f"[ANOMALY] Eliminado retorno extremo: {daily_return:.2%}")

        return cleaned

    # ==========================================================
    # GUARDAR DATOS LIMPIOS
    # ==========================================================

    def save_clean_data(self, symbol, cleaned_data):

        if not cleaned_data:
            print(f"[WARNING] No hay datos limpios para {symbol}")
            return

        file_path = self.processed_data_path / f"{symbol}_clean.csv"

        fieldnames = [
            "Date", "Open", "High", "Low", "Close", "Volume"
        ]

        with open(file_path, mode="w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(cleaned_data)

        print(f"[OK] Datos limpios guardados: {file_path}")

    # ==========================================================
    # FLUJO COMPLETO POR ACTIVO
    # ==========================================================

    def clean_single_asset(self, symbol):

        print(f"\nLimpiando {symbol}...")

        data = self.load_raw_data(symbol)

        if not data:
            print(f"[WARNING] No hay datos para {symbol}")
            return

        if not self.validate_columns(data):
            print(f"[ERROR] Estructura inválida para {symbol}")
            return

        data = self.remove_duplicates(data)
        data = self.validate_time_series_integrity(data)
        data = self.handle_missing_values(data)
        data = self.convert_data_types(data)
        data = self.detect_and_handle_anomalies(data)

        self.save_clean_data(symbol, data)

    # ==========================================================
    # LIMPIEZA GLOBAL
    # ==========================================================

    def clean_all_assets(self):

        print("\n=== INICIANDO LIMPIEZA AVANZADA ===\n")

        for symbol in self.assets:
            try:
                self.clean_single_asset(symbol)
            except Exception as e:
                print(f"[ERROR] Fallo procesando {symbol}: {e}")

        print("\n=== LIMPIEZA FINALIZADA ===\n")