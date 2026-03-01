from typing import List
import math


class DynamicTimeWarping:
    """
    DynamicTimeWarping

    Implementa el algoritmo clásico de Dynamic Time Warping (DTW)
    para medir la distancia entre dos series temporales permitiendo
    deformaciones no lineales en el eje temporal.

    Definición conceptual:
        DTW encuentra el camino de costo mínimo en una matriz
        de alineación acumulada entre dos series.

    Propiedades:
        - Retorna distancia >= 0
        - 0 indica series idénticas
        - Permite series de distinta longitud
        - Complejidad temporal: O(n * m)
        - Complejidad espacial: O(n * m)
    """

    def calculate(self, series_1: List[float], series_2: List[float]) -> float:
        """
        Calcula la distancia DTW entre dos series.

        Parámetros:
        ----------
        series_1 : List[float]
        series_2 : List[float]

        Retorna:
        -------
        float
            Distancia acumulativa mínima entre ambas series.
        """

        self._validate_input(series_1, series_2)

        n = len(series_1)
        m = len(series_2)

        # Crear matriz (n+1) x (m+1) inicializada en infinito
        dtw_matrix = [[math.inf] * (m + 1) for _ in range(n + 1)]

        dtw_matrix[0][0] = 0

        for i in range(1, n + 1):
            for j in range(1, m + 1):

                cost = self._local_distance(series_1[i - 1], series_2[j - 1])

                dtw_matrix[i][j] = cost + min(
                    dtw_matrix[i - 1][j],      # inserción
                    dtw_matrix[i][j - 1],      # eliminación
                    dtw_matrix[i - 1][j - 1]   # match
                )

        return dtw_matrix[n][m]

    def _local_distance(self, x: float, y: float) -> float:
        """
        Distancia local entre dos puntos.
        Usamos diferencia cuadrática para coherencia con Euclidiana.
        """
        return (x - y) ** 2

    def _validate_input(self, series_1: List[float], series_2: List[float]) -> None:
        """
        Valida que:
        - No sean None
        - No estén vacías
        """

        if series_1 is None or series_2 is None:
            raise ValueError("Las series no pueden ser None.")

        if len(series_1) == 0 or len(series_2) == 0:
            raise ValueError("Las series no pueden estar vacías.")