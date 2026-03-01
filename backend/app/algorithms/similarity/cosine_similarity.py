from typing import List
import math


class CosineSimilarity:
    """
    CosineSimilarity

    Implementa el cálculo de la similitud del coseno entre dos vectores numéricos.

    Definición matemática:

        cosine_similarity = (X · Y) / (||X|| * ||Y||)

    Donde:
        - X · Y es el producto punto
        - ||X|| es la norma euclidiana del vector X
        - ||Y|| es la norma euclidiana del vector Y

    Propiedades:
        - Rango: [-1, 1]
        - 1  → vectores perfectamente alineados
        - 0  → vectores ortogonales
        - -1 → vectores en dirección opuesta

    Complejidad:
        - Temporal: O(n)
        - Espacial: O(1)
    """

    def calculate(self, series_1: List[float], series_2: List[float]) -> float:
        """
        Calcula la similitud del coseno entre dos series.

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

        dot_product = self._dot_product(series_1, series_2)
        norm_1 = self._norm(series_1)
        norm_2 = self._norm(series_2)

        denominator = norm_1 * norm_2

        # Manejo de norma cero
        if denominator == 0:
            return 0.0

        return dot_product / denominator

    def _dot_product(self, series_1: List[float], series_2: List[float]) -> float:
        """
        Calcula el producto punto entre dos vectores.
        """
        return sum(x * y for x, y in zip(series_1, series_2))

    def _norm(self, series: List[float]) -> float:
        """
        Calcula la norma euclidiana de un vector.
        """
        return math.sqrt(sum(x ** 2 for x in series))

    def _validate_input(self, series_1: List[float], series_2: List[float]) -> None:
        """
        Valida que:
        - No sean None
        - No estén vacías
        - Tengan la misma longitud
        """

        if series_1 is None or series_2 is None:
            raise ValueError("Las series no pueden ser None.")

        if len(series_1) == 0 or len(series_2) == 0:
            raise ValueError("Las series no pueden estar vacías.")

        if len(series_1) != len(series_2):
            raise ValueError("Las series deben tener la misma longitud.")