from backend.app.etl.data_extractor import DataExtractor
from backend.app.etl.data_cleaner import DataCleaner
from backend.app.etl.data_merger import DataMerger


class ETLPipeline:
    """
    Orquestador del proceso ETL completo.

    Flujo:
    1. Extract  -> Descarga datos desde Yahoo
    2. Clean    -> Limpia y valida los datos
    3. Merge    -> Unifica activos en estructuras conjuntas
    """

    def __init__(self):
        #self.extractor = DataExtractor()
        self.cleaner = DataCleaner()
        self.merger = DataMerger()

    # ==========================================================
    # EJECUCIÓN COMPLETA DEL PIPELINE
    # ==========================================================

    def run(self):
        """
        Ejecuta el flujo completo ETL.
        """

        print("\n==============================")
        print("   INICIANDO PROCESO ETL")
        print("==============================\n")

        # 1️⃣ EXTRACT
        print(">>> FASE 1: EXTRACCIÓN")
        #self.extractor.download_all_assets()

        # 2️⃣ CLEAN
        print("\n>>> FASE 2: LIMPIEZA")
        self.cleaner.clean_all_assets()

        # 3️⃣ MERGE
        print("\n>>> FASE 3: UNIFICACIÓN")
        self.merger.merge_all_assets()

        print("\n==============================")
        print("   PROCESO ETL FINALIZADO")
        print("==============================\n")