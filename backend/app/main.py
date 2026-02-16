from backend.app.etl.data_extractor import DataExtractor


if __name__ == "__main__":
    extractor = DataExtractor()
    extractor.download_all_assets()