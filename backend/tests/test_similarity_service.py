import pytest
from backend.app.services.similarity_service import SimilarityService


def test_compare_all_basic():
    s = SimilarityService()
    a = [1, 2, 3]
    b = [2, 4, 6]

    results = s.compare_all(a, b)

    assert "euclidean" in results
    assert "pearson" in results
    assert "cosine" in results
    assert "dtw" in results


def test_compare_invalid_length():
    s = SimilarityService()
    a = [1, 2, 3]
    b = [1, 2]

    with pytest.raises(ValueError):
        s.compare(a, b, "euclidean")


def test_compare_assets_returns():
    # This test assumes that data/merged/merged_prices.csv exists and contains
    # the assets from settings.ASSETS. It will run compare_assets for the first
    # two assets using returns. If merged file isn't present, the test will be skipped.
    import os
    from backend.app.model.time_series import TimeSeries
    ts = TimeSeries()

    s = SimilarityService()
    assets = ts._load_merged_data()[0].keys()
    asset_list = [k for k in assets if k != "Date"]

    if len(asset_list) < 2:
        pytest.skip("Not enough assets in merged file for test")

    a, b = asset_list[0], asset_list[1]

    result = s.compare_assets(a, b, series_type="returns")

    assert "metrics" in result
    assert isinstance(result["metrics"], dict)

