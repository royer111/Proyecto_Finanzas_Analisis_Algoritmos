from typing import List
import math


class EuclideanDistance:
    """
    Implementación del algoritmo de Distancia Euclidiana
    aplicado a series de tiempo financieras.

    La distancia euclidiana entre dos vectores X y Y de tamaño n
    se define como:

        d(X, Y) = sqrt( Σ (x_i - y_i)^2 )

    Donde:
        - x_i pertenece a la serie 1
        - y_i pertenece a la serie 2
        - n es la longitud de las series

    Esta métrica mide la diferencia absoluta en magnitud
    entre dos series alineadas.

    Complejidad temporal:
        O(n)

    Complejidad espacial:
        O(1)
    """

    # ==========================================================
    # METODO PRINCIPAL
    # ==========================================================

    def calculate(self, series_1: List[float], series_2: List[float]) -> float:
        """
        Calcula la distancia euclidiana entre dos series.

        Parámetros:
            series_1 (List[float]): Primera serie numérica.
            series_2 (List[float]): Segunda serie numérica.

        Retorna:
            float: Distancia euclidiana.

        Lanza:
            ValueError: Si las series son inválidas.
        """

        self._validate_input(series_1, series_2)

        squared_sum = 0.0

        for x, y in zip(series_1, series_2):
            squared_sum += (x - y) ** 2

        distance = math.sqrt(squared_sum)

        return distance

    # ==========================================================
    # VALIDACIÓN DE ENTRADA
    # ==========================================================

    def _validate_input(self, series_1: List[float], series_2: List[float]) -> None:
        """
        Valida que las series:

        - No sean None
        - No estén vacías
        - Tengan la misma longitud

        Lanza:
            ValueError si alguna condición no se cumple.
        """

        if series_1 is None or series_2 is None:
            raise ValueError("Las series no pueden ser None.")

        if len(series_1) == 0 or len(series_2) == 0:
            raise ValueError("Las series no pueden estar vacías.")

        if len(series_1) != len(series_2):
            raise ValueError("Las series deben tener la misma longitud.")