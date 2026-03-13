from pathlib import Path
import sys

# Asegurar que la raíz del proyecto esté en sys.path cuando se ejecute como script
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.services.similarity_service import SimilarityService
from backend.app.etl.etl_pipeline import ETLPipeline
from backend.app.config import settings


def main():

    # 1) Ejecutar pipeline ETL (clean + merge). Si ya se generó merged_prices.csv,
    # ETLPipeline seguirá y sobrescribirá si es necesario.
    pipeline = ETLPipeline()
    pipeline.run()

    service = SimilarityService()

    # Seleccionar dos activos del portfolio (primeros dos por defecto)
    assets = settings.ASSETS
    if len(assets) < 2:
        print("Se requieren al menos 2 activos en settings.ASSETS")
        return

    asset_a = assets[0]
    asset_b = assets[1]

    print("============== COMPARACIÓN PRECIOS ==============")
    try:
        result_prices = service.compare_assets(asset_a, asset_b, series_type="prices")
        print(f"Activos: {asset_a} vs {asset_b}")
        print("Métricas:", result_prices["metrics"])
    except Exception as e:
        print(f"Error comparando precios: {e}")

    print("\n============== COMPARACIÓN RETORNOS ==============")
    try:
        result_returns = service.compare_assets(asset_a, asset_b, series_type="returns")
        print(f"Activos: {asset_a} vs {asset_b}")
        print("Métricas:", result_returns["metrics"])
    except Exception as e:
        print(f"Error comparando retornos: {e}")


if __name__ == "__main__":
    main()


