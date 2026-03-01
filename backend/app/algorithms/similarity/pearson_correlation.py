from typing import List
import math


class PearsonCorrelation:
    """
    PearsonCorrelation

    Implementa el cálculo del coeficiente de correlación de Pearson
    entre dos series numéricas.

    Definición matemática:

        r = Σ[(x_i - mean_x)(y_i - mean_y)] /
            ( sqrt(Σ(x_i - mean_x)^2) * sqrt(Σ(y_i - mean_y)^2) )

    Propiedades:
        - r ∈ [-1, 1]
        - r = 1  → correlación positiva perfecta
        - r = 0  → sin correlación lineal
        - r = -1 → correlación negativa perfecta

    Complejidad:
        - Temporal: O(n)
        - Espacial: O(1)
    """

    def calculate(self, series_1: List[float], series_2: List[float]) -> float:
        """
        Calcula el coeficiente de correlación de Pearson entre dos series.

        Parámetros:
        ----------
        series_1 : List[float]
        series_2 : List[float]

        Retorna:
        -------
        float
            Valor entre -1 y 1.

        Lanza:
        ------
        ValueError si las series son inválidas.
        """

        self._validate_input(series_1, series_2)

        mean_1 = self._mean(series_1)
        mean_2 = self._mean(series_2)

        numerator = 0.0
        sum_sq_1 = 0.0
        sum_sq_2 = 0.0

        for x, y in zip(series_1, series_2):
            diff_1 = x - mean_1
            diff_2 = y - mean_2

            numerator += diff_1 * diff_2
            sum_sq_1 += diff_1 ** 2
            sum_sq_2 += diff_2 ** 2

        denominator = math.sqrt(sum_sq_1) * math.sqrt(sum_sq_2)

        # Manejo de varianza cero
        if denominator == 0:
            return 0.0

        return numerator / denominator

    def _mean(self, series: List[float]) -> float:
        """
        Calcula la media aritmética de una serie.
        """
        return sum(series) / len(series)

    def _validate_input(self, series_1: List[float], series_2: List[float]) -> None:
        """
        Valida que:
        - No sean None
        - No estén vacías
        - Tengan la misma longitud
        - Tengan al menos 2 elementos
        """

        if series_1 is None or series_2 is None:
            raise ValueError("Las series no pueden ser None.")

        if len(series_1) == 0 or len(series_2) == 0:
            raise ValueError("Las series no pueden estar vacías.")

        if len(series_1) != len(series_2):
            raise ValueError("Las series deben tener la misma longitud.")

        if len(series_1) < 2:
            raise ValueError("Se requieren al menos 2 puntos para calcular correlación.")