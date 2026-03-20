from pathlib import Path
import sys

# Asegurar que la raíz del proyecto esté en sys.path cuando se ejecute como script
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.services.service_orchestrator import ServiceOrchestrator


def main():
    orchestrator = ServiceOrchestrator()

    # 1) Ejecutar pipeline ETL (clean + merge)
    try:
        orchestrator.run_etl()
    except Exception as e:
        print(f"Error ejecutando ETL: {e}")

    # Seleccionar dos activos del portfolio (primeros dos por defecto)
    assets = orchestrator.list_assets()
    if len(assets) < 2:
        print("Se requieren al menos 2 activos en settings.ASSETS")
        return

    asset_a = assets[0]
    asset_b = assets[1]

    print("============== COMPARACIÓN PRECIOS ==============")
    try:
        result_prices = orchestrator.compare_assets(asset_a, asset_b, series_type="prices")
        print(f"Activos: {asset_a} vs {asset_b}")
        print("Métricas:", result_prices.get("metrics"))
    except Exception as e:
        print(f"Error comparando precios: {e}")

    print("\n============== COMPARACIÓN RETORNOS ==============")
    try:
        result_returns = orchestrator.compare_assets(asset_a, asset_b, series_type="returns")
        print(f"Activos: {asset_a} vs {asset_b}")
        print("Métricas:", result_returns.get("metrics"))
    except Exception as e:
        print(f"Error comparando retornos: {e}")


if __name__ == "__main__":
    main()

