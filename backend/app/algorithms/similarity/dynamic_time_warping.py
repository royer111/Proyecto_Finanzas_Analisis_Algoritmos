import math
from typing import List

class DynamicTimeWarping:
    """
    Implementación explícita de DTW para el análisis de series
    temporales financieras (Requerimiento 2).
    """

    def calculate(self, series_1: List[float], series_2: List[float]) -> float:
        # 1. Validación de entrada conforme al proceso de limpieza del ETL [cite: 29]
        self._validate_input(series_1, series_2)

        n = len(series_1)
        m = len(series_2)

        # 2. Creación de la matriz de costo acumulado [cite: 38, 63]
        # Se inicializa con infinito para representar el costo de caminos no explorados.
        dtw_matrix = [[math.inf] * (m + 1) for _ in range(n + 1)]
        dtw_matrix[0][0] = 0

        # 3. Construcción algorítmica paso a paso
        for i in range(1, n + 1):
            for j in range(1, m + 1):
                # Distancia local (Diferencia absoluta o cuadrática)
                cost = self._local_distance(series_1[i - 1], series_2[j - 1])

                # Regla de transición: mínimo de (match, inserción, eliminación)
                dtw_matrix[i][j] = cost + min(
                    dtw_matrix[i - 1][j],      # Inserción
                    dtw_matrix[i][j - 1],      # Eliminación
                    dtw_matrix[i - 1][j - 1]   # Match (Diagonal)
                )

        # 4. Retorno de la distancia DTW final
        # Para que sea comparable con la distancia Euclidiana, se suele usar la raíz
        # si se usó costo cuadrático, o el valor directo si fue absoluto.
        return math.sqrt(dtw_matrix[n][m])

    def _local_distance(self, x: float, y: float) -> float:
        """Calcula el costo local entre dos puntos financieros."""
        return (x - y) ** 2

    def _validate_input(self, series_1: List[float], series_2: List[float]) -> None:
        if not series_1 or not series_2:
            raise ValueError("Las series resultantes del ETL no pueden estar vacías[cite: 29].")