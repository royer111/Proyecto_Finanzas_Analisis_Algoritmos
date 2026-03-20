from backend.app.etl.data_extractor import DataExtractor
from backend.app.etl.data_cleaner import DataCleaner
from backend.app.etl.data_merger import DataMerger


class ETLPipeline:
    """
    Orquestador del proceso ETL completo.

    Flujo:
    1. Extract  -> Descarga datos desde Yahoo
    2. Clean    -> Limpia y valida los datos
    3. Merge    -> Unifica activos en una matriz de precios de cierre
    4. Merge    -> Unifica activos en una matriz de volumenes
    """

    def __init__(self):
        # self.extractor = DataExtractor()
        self.cleaner = DataCleaner()
        self.merger = DataMerger()

    def run(self):
        """
        Ejecuta el flujo completo ETL.
        """

        print("\n==============================")
        print("   INICIANDO PROCESO ETL")
        print("==============================\n")

        # FASE 1: EXTRACCION
        print(">>> FASE 1: EXTRACCION")
        # self.extractor.download_all_assets()

        # FASE 2: LIMPIEZA
        print("\n>>> FASE 2: LIMPIEZA")
        self.cleaner.clean_all_assets()

        # FASE 3: MERGE DE PRECIOS DE CIERRE -> merged_prices.csv
        print("\n>>> FASE 3: UNIFICACION DE PRECIOS")
        self.merger.merge_all_assets()

        # FASE 4: MERGE DE VOLUMENES -> merged_volumenes.csv
        print("\n>>> FASE 4: UNIFICACION DE VOLUMENES")
        self.merger.merge_volume()

        print("\n==============================")
        print("   PROCESO ETL FINALIZADO")
        print("==============================\n")