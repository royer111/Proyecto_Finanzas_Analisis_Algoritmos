from pathlib import Path
import sys

# Asegurar que la raíz del proyecto esté en sys.path cuando se ejecute como script
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.services.similarity_service import SimilarityService
from backend.app.etl.etl_pipeline import ETLPipeline
from backend.app.config import settings


class ServiceOrchestrator:
    """Orquestador para ETL y servicios de similitud.

    Provee una interfaz simple para:
      - ejecutar el pipeline ETL (clean + merge)
      - consultar lista de activos configurada
      - comparar dos activos por distintos tipos de series (prices/returns)

    Esta clase está diseñada para ser importada y usada por un runner/test.
    """

    def __init__(self, pipeline: ETLPipeline | None = None, similarity_service: SimilarityService | None = None, assets: list | None = None):
        # inyectar dependencias permite tests más sencillos
        self.pipeline = pipeline or ETLPipeline()
        self.similarity = similarity_service or SimilarityService()
        self.assets = assets or settings.ASSETS

    def run_etl(self) -> None:
        """Ejecuta el pipeline ETL (limpieza + unificación)."""
        self.pipeline.run()

    def list_assets(self) -> list:
        """Devuelve la lista de activos configurada."""
        return list(self.assets)

    def compare_assets(self, asset_a: str, asset_b: str, series_type: str = "prices") -> dict:
        """Delegar la comparación al SimilarityService.

        Retorna el diccionario devuelto por SimilarityService.compare_assets.
        """
        return self.similarity.compare_assets(asset_a, asset_b, series_type=series_type)

    def compare_first_two(self, series_type: str = "prices") -> dict:
        """Conveniencia: compara los dos primeros activos en la configuración."""
        if len(self.assets) < 2:
            raise ValueError("Se requieren al menos 2 activos en settings.ASSETS")
        return self.compare_assets(self.assets[0], self.assets[1], series_type=series_type)
