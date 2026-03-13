# backend/app/services/similarity_service.py

from backend.app.model.time_series import TimeSeries
from backend.app.algorithms.similarity.euclidean_distance import EuclideanDistance
from backend.app.algorithms.similarity.pearson_correlation import PearsonCorrelation
from backend.app.algorithms.similarity.cosine_similarity import CosineSimilarity
from backend.app.algorithms.similarity.dynamic_time_warping import DynamicTimeWarping


class SimilarityService:
    """
    SimilarityService

    Esta clase actúa como capa de orquestación entre la API y los algoritmos
    de similitud implementados en el sistema.

    Responsabilidades:
    - Validar datos de entrada
    - Instanciar objetos TimeSeries
    - Seleccionar dinámicamente el algoritmo
    - Ejecutar la comparación
    - Estandarizar el resultado
    """

    def compare(self, series_a, series_b, metric: str):
        """
        Método principal para comparar dos series temporales.

        :param series_a: Lista de valores numéricos
        :param series_b: Lista de valores numéricos
        :param metric: Nombre de la métrica ("euclidean", "pearson", "cosine", "dtw")
        :return: Diccionario con el resultado estructurado
        """

        self._validate_input(series_a, series_b)

        # No es necesario envolver las series en TimeSeries aquí; el servicio
        # recibe listas ya preparadas (precios o retornos). Simplemente
        # seleccionamos el algoritmo y llamamos a su método `calculate`.
        algorithm = self._get_algorithm(metric)

        value = algorithm.calculate(series_a, series_b)

        result_type = self._get_result_type(metric)

        return {
            "metric": metric,
            "value": value,
            "type": result_type
        }

    def compare_all(self, series_a, series_b):
        """
        Ejecuta todas las métricas disponibles y retorna los resultados.
        """

        metrics = ["euclidean", "pearson", "cosine", "dtw"]

        results = {}

        for metric in metrics:
            results[metric] = self.compare(series_a, series_b, metric)["value"]

        return results

    def compare_assets(self, asset_a: str, asset_b: str, series_type: str = "prices"):
        """
        Obtiene series alineadas desde TimeSeries y calcula todas las métricas.

        Retorna un diccionario con:
            - dates: lista de fechas alineadas
            - series_a: valores de asset_a (prices o returns)
            - series_b: valores de asset_b (prices o returns)
            - metrics: diccionario con resultados de cada métrica

        :param asset_a: símbolo del primer activo
        :param asset_b: símbolo del segundo activo
        :param series_type: 'prices' o 'returns'
        """

        ts = TimeSeries()

        dates, series_a, series_b = ts.get_series(asset_a, asset_b, series_type=series_type)

        # Validar longitudes ya se maneja en los algoritmos; aquí sólo comprobamos que no estén vacías
        self._validate_input(series_a, series_b)

        metrics = self.compare_all(series_a, series_b)

        return {
            "dates": dates,
            "series_a": series_a,
            "series_b": series_b,
            "metrics": metrics
        }

    def _get_algorithm(self, metric: str):
        """
        Retorna la instancia del algoritmo correspondiente.
        """

        metric = metric.lower()

        if metric == "euclidean":
            return EuclideanDistance()

        elif metric == "pearson":
            return PearsonCorrelation()

        elif metric == "cosine":
            return CosineSimilarity()

        elif metric == "dtw":
            return DynamicTimeWarping()

        else:
            raise ValueError(f"Métrica '{metric}' no soportada.")

    def _get_result_type(self, metric: str):
        """
        Indica si la métrica retorna distancia o similitud.
        """

        if metric in ["euclidean", "dtw"]:
            return "distance"
        else:
            return "similarity"

    def _validate_input(self, series_a, series_b):
        """
        Valida que las series no estén vacías y sean listas.
        """

        if not isinstance(series_a, list) or not isinstance(series_b, list):
            raise TypeError("Las series deben ser listas.")

        if len(series_a) == 0 or len(series_b) == 0:
            raise ValueError("Las series no pueden estar vacías.")

