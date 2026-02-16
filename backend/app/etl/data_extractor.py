import csv
import time
import requests
from datetime import datetime
from backend.app.config import settings


class DataExtractor:
    """
    Encargado exclusivamente de:
    - Construir la URL de consulta a Yahoo Finance
    - Descargar datos históricos
    - Parsear respuesta JSON
    - Guardar datos crudos en data/raw/
    """

    def __init__(self):
        """
        Carga configuración desde settings.py
        """

        self.assets = settings.ASSETS
        self.start_date = settings.START_DATE
        self.end_date = settings.END_DATE
        self.base_url = settings.YAHOO_BASE_URL
        self.raw_data_path = settings.RAW_DATA_PATH
        self.sleep_seconds = settings.REQUEST_SLEEP_SECONDS

        # Convertir fechas a timestamp UNIX
        self.period1 = int(datetime.strptime(self.start_date, "%Y-%m-%d").timestamp())
        self.period2 = int(datetime.strptime(self.end_date, "%Y-%m-%d").timestamp())

    # ==========================================================
    # CONSTRUCCIÓN DE URL
    # ==========================================================

    def build_query_url(self, symbol):
        """
        Construye la URL completa para descargar datos históricos.
        """

        return (
            f"{self.base_url}/{symbol}"
            f"?period1={self.period1}"
            f"&period2={self.period2}"
            f"&interval=1d"
        )

    # ==========================================================
    # DESCARGA DE DATOS
    # ==========================================================

    def fetch_asset_data(self, symbol):
        """
        Descarga datos históricos usando endpoint JSON estable de Yahoo.
        """

        url = self.build_query_url(symbol)

        headers = {
            "User-Agent": "Mozilla/5.0"
        }

        try:
            response = requests.get(url, headers=headers, timeout=15)

            if response.status_code != 200:
                print(f"[ERROR] Status {response.status_code} para {symbol}")
                return None

            return response.json()

        except Exception as e:
            print(f"[ERROR] Fallo de red para {symbol}: {e}")
            return None

    # ==========================================================
    # PARSEO DE RESPUESTA
    # ==========================================================

    def parse_response(self, json_data):
        """
        Convierte el JSON en lista estructurada.
        """

        parsed_data = []

        if not json_data:
            return parsed_data

        try:
            result = json_data["chart"]["result"][0]

            timestamps = result["timestamp"]
            indicators = result["indicators"]["quote"][0]

            opens = indicators["open"]
            highs = indicators["high"]
            lows = indicators["low"]
            closes = indicators["close"]
            volumes = indicators["volume"]

            for i in range(len(timestamps)):

                # Si algún valor viene None, lo dejamos pasar.
                # La limpieza formal se hará en data_cleaner.py
                date = datetime.fromtimestamp(timestamps[i]).strftime("%Y-%m-%d")

                parsed_data.append({
                    "Date": date,
                    "Open": opens[i],
                    "High": highs[i],
                    "Low": lows[i],
                    "Close": closes[i],
                    "Volume": volumes[i]
                })

        except Exception as e:
            print(f"[ERROR] Parseando datos: {e}")

        return parsed_data

    # ==========================================================
    # GUARDADO DE DATOS CRUDOS
    # ==========================================================

    def save_raw_data(self, symbol, parsed_data):
        """
        Guarda los datos crudos en data/raw/{symbol}.csv
        """

        if not parsed_data:
            print(f"[WARNING] No se guardaron datos para {symbol}")
            return

        file_path = self.raw_data_path / f"{symbol}.csv"

        fieldnames = [
            "Date",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume"
        ]

        with open(file_path, mode="w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(parsed_data)

        print(f"[OK] Datos guardados: {file_path}")

    # ==========================================================
    # FLUJO POR ACTIVO
    # ==========================================================

    def download_single_asset(self, symbol):
        """
        Ejecuta flujo completo para un activo.
        """

        print(f"\nDescargando {symbol}...")

        raw_data = self.fetch_asset_data(symbol)

        if not raw_data:
            print(f"[WARNING] No se pudo descargar {symbol}")
            return

        parsed_data = self.parse_response(raw_data)
        self.save_raw_data(symbol, parsed_data)

        time.sleep(self.sleep_seconds)

    # ==========================================================
    # FLUJO COMPLETO
    # ==========================================================

    def download_all_assets(self):
        """
        Descarga todos los activos definidos en settings.
        """

        print("\n=== INICIANDO DESCARGA DE ACTIVOS ===\n")

        for symbol in self.assets:
            try:
                self.download_single_asset(symbol)
            except Exception as e:
                print(f"[ERROR] Fallo procesando {symbol}: {e}")

        print("\n=== DESCARGA FINALIZADA ===\n")