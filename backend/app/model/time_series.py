import csv
from pathlib import Path
from typing import List, Tuple


class TimeSeries:
    """
    Modelo encargado de:

    - Leer datos consolidados desde data/merged/merged_prices.csv
    - Extraer series alineadas de dos activos
    - Retornar precios o retornos
    """

    def __init__(self):
        # Ruta al archivo consolidado generado por el ETL
        self.base_path = Path(__file__).resolve().parents[3]
        self.merged_file = self.base_path / "data" / "merged" / "merged_prices.csv"

        if not self.merged_file.exists():
            raise FileNotFoundError(
                f"No se encontró el archivo merged: {self.merged_file}"
            )

    # ==========================================================
    # CARGA COMPLETA DEL DATASET CONSOLIDADO
    # ==========================================================

    def _load_merged_data(self) -> List[dict]:
        """
        Carga el CSV consolidado y retorna lista de diccionarios.
        """
        with open(self.merged_file, mode="r") as file:
            reader = csv.DictReader(file)
            return list(reader)

    # ==========================================================
    # OBTENER SERIES DE DOS ACTIVOS (PRECIOS)
    # ==========================================================

    def get_price_series(
            self, asset_1: str, asset_2: str
    ) -> Tuple[List[str], List[float], List[float]]:
        """
        Retorna:
        - Fechas
        - Serie de precios de asset_1
        - Serie de precios de asset_2
        """

        data = self._load_merged_data()

        if not data:
            raise ValueError("El archivo merged no contiene datos.")

        if asset_1 not in data[0] or asset_2 not in data[0]:
            raise ValueError("Uno o ambos activos no existen en el dataset merged.")

        dates = []
        series_1 = []
        series_2 = []

        for row in data:
            try:
                price_1 = float(row[asset_1])
                price_2 = float(row[asset_2])

                dates.append(row["Date"])
                series_1.append(price_1)
                series_2.append(price_2)

            except (ValueError, TypeError):
                continue

        if len(series_1) == 0 or len(series_2) == 0:
            raise ValueError("No se pudieron construir las series de precios.")

        return dates, series_1, series_2

    # ==========================================================
    # CALCULAR RETORNOS DIARIOS
    # ==========================================================

    @staticmethod
    def calculate_returns(prices: List[float]) -> List[float]:
        """
        Calcula retornos simples diarios:
        r_t = (P_t - P_{t-1}) / P_{t-1}
        """

        if len(prices) < 2:
            raise ValueError("Se requieren al menos 2 precios para calcular retornos.")

        returns = []

        for i in range(1, len(prices)):
            previous = prices[i - 1]

            if previous == 0:
                returns.append(0.0)
                continue

            r = (prices[i] - previous) / previous
            returns.append(r)

        return returns

    # ==========================================================
    # OBTENER SERIES SEGÚN TIPO
    # ==========================================================

    def get_series(
            self,
            asset_1: str,
            asset_2: str,
            series_type: str = "prices"
    ) -> Tuple[List[str], List[float], List[float]]:
        """
        series_type:
            - "prices"
            - "returns"

        Retorna:
            fechas, serie_1, serie_2
        """

        dates, series_1, series_2 = self.get_price_series(asset_1, asset_2)

        if series_type == "prices":
            return dates, series_1, series_2

        elif series_type == "returns":
            returns_1 = self.calculate_returns(series_1)
            returns_2 = self.calculate_returns(series_2)

            # Ajustamos fechas porque retornos tiene longitud -1
            adjusted_dates = dates[1:]

            return adjusted_dates, returns_1, returns_2

        else:
            raise ValueError("series_type debe ser 'prices' o 'returns'")