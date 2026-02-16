import os
import csv
import time
import requests
from datetime import datetime
from pathlib import Path


class DataExtractor:
    """
    Encargado exclusivamente de:
    - Construir la URL de consulta a Yahoo Finance
    - Descargar datos históricos
    - Parsear respuesta CSV
    - Guardar datos crudos en data/raw/
    """

    def __init__(self, assets, start_date, end_date):
        """
        :param assets: lista de símbolos financieros
        :param start_date: fecha inicio (YYYY-MM-DD)
        :param end_date: fecha fin (YYYY-MM-DD)
        """

        self.assets = assets
        self.start_date = start_date
        self.end_date = end_date

        # Convertir fechas a formato timestamp UNIX (requerido por Yahoo)
        self.period1 = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
        self.period2 = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())

        # URL base oficial de descarga histórica
        self.base_url = "https://finance.yahoo.com"

        # Ruta absoluta del proyecto (compatible Mac/Linux)
        self.project_root = Path(__file__).resolve().parents[3]
        self.raw_data_path = self.project_root / "data" / "raw"

        # Crear carpeta si no existe
        os.makedirs(self.raw_data_path, exist_ok=True)

    def build_query_url(self, symbol):
        """
        Construye la URL completa para descargar datos históricos.
        """

        url = (
            f"{self.base_url}/{symbol}"
            f"?period1={self.period1}"
            f"&period2={self.period2}"
            f"&interval=1d"
            f"&events=history"
            f"&includeAdjustedClose=true"
        )

        return url

    def fetch_asset_data(self, symbol):
        """
        Descarga datos históricos usando endpoint JSON estable de Yahoo.
        """

        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            f"?period1={self.period1}"
            f"&period2={self.period2}"
            f"&interval=1d"
        )

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
            print(f"[ERROR] {e}")
            return None

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

    def save_raw_data(self, symbol, parsed_data):
        """
        Guarda los datos crudos en data/raw/{symbol}.csv
        """

        if not parsed_data:
            print(f"[WARNING] No se guardaron datos para {symbol}")
            return

        file_path = self.raw_data_path / f"{symbol}.csv"

        fieldnames = [
            "Date", "Open", "High",
            "Low", "Close", "Adj Close", "Volume"
        ]

        with open(file_path, mode="w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(parsed_data)

        print(f"[OK] Datos guardados: {file_path}")

    def download_single_asset(self, symbol):
        """
        Ejecuta flujo completo para un activo.
        """

        print(f"\nDescargando {symbol}...")

        raw_data = self.fetch_asset_data(symbol)
        parsed_data = self.parse_response(raw_data)
        self.save_raw_data(symbol, parsed_data)

        # Pausa pequeña para evitar bloqueos por rate limit
        time.sleep(1)

    def download_all_assets(self):
        """
        Descarga todos los activos definidos.
        """

        for symbol in self.assets:
            try:
                self.download_single_asset(symbol)
            except Exception as e:
                print(f"[ERROR] Fallo procesando {symbol}: {e}")