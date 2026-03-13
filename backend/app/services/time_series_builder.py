from pathlib import Path
import pandas as pd
from backend.app.model.time_series import TimeSeries


class TimeSeriesBuilder:

    def __init__(self):

        # Obtener raíz del proyecto
        ROOT = Path(__file__).resolve().parents[3]

        # Construir ruta absoluta
        self.merged_path = ROOT / "data" / "merged" / "merged_prices.csv"

    def build(self, asset_symbol: str):

        df = pd.read_csv(self.merged_path)

        if asset_symbol not in df.columns:
            raise ValueError(f"Activo '{asset_symbol}' no encontrado en dataset merged.")

        dates = df["Date"].tolist()
        prices = df[asset_symbol].tolist()

        return TimeSeries(
            symbol=asset_symbol,
            dates=dates,
            prices=prices
        )