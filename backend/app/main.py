from datetime import datetime
from backend.app.etl.data_extractor import DataExtractor


def main():
    # Activos de prueba (empieza con pocos)
    assets = ["AAPL", "MSFT", "VOO"]

    # Rango de fechas (5 años hacia atrás)
    start_date = "2019-01-01"
    end_date = "2024-01-01"

    print("\n=== INICIANDO DESCARGA DE ACTIVOS ===\n")

    extractor = DataExtractor(
        assets=assets,
        start_date=start_date,
        end_date=end_date
    )

    extractor.download_all_assets()

    print("\n=== DESCARGA FINALIZADA ===\n")


if __name__ == "__main__":
    main()