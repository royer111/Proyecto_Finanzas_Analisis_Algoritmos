from backend.app.algorithms.similarity.euclidean_distance import EuclideanDistance
from backend.app.algorithms.similarity.pearson_correlation import PearsonCorrelation
from backend.app.algorithms.similarity.cosine_similarity import CosineSimilarity
from backend.app.algorithms.similarity.dynamic_time_warping import DynamicTimeWarping


def main():
    euclidean = EuclideanDistance()
    pearson = PearsonCorrelation()
    cosine = CosineSimilarity()
    dtw = DynamicTimeWarping()

    print("====================================")
    print("CASO 1 - Proporcionalidad perfecta")
    print("====================================")

    series_a = [1, 2, 3]
    series_b = [10, 20, 30]

    print("Euclidean:", euclidean.calculate(series_a, series_b))
    print("Pearson:", pearson.calculate(series_a, series_b))
    print("Cosine:", cosine.calculate(series_a, series_b))
    print("DTW:", dtw.calculate(series_a, series_b))
    print()

    print("====================================")
    print("CASO 2 - Desplazamiento temporal")
    print("====================================")

    series_c = [1, 2, 3, 4]
    series_d = [0, 1, 2, 3, 4]

    print("Euclidean:", euclidean.calculate(series_c, series_d[:4]))  # igualamos tamaño
    print("Pearson:", pearson.calculate(series_c, series_d[:4]))
    print("Cosine:", cosine.calculate(series_c, series_d[:4]))
    print("DTW:", dtw.calculate(series_c, series_d))
    print()

    print("====================================")
    print("CASO 3 - Dirección opuesta")
    print("====================================")

    series_e = [1, 2, 3]
    series_f = [-1, -2, -3]

    print("Euclidean:", euclidean.calculate(series_e, series_f))
    print("Pearson:", pearson.calculate(series_e, series_f))
    print("Cosine:", cosine.calculate(series_e, series_f))
    print("DTW:", dtw.calculate(series_e, series_f))
    print()


if __name__ == "__main__":
    main()