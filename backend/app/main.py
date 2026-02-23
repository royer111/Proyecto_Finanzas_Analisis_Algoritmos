from backend.app.etl.etl_pipeline import ETLPipeline

if __name__ == "__main__":
    pipeline = ETLPipeline()
    pipeline.run()