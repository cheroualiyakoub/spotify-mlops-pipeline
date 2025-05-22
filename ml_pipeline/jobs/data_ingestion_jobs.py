from dagster import job
from ml_pipeline.assets.data_ingestion import download_spotify_data
from ml_pipeline.resources.kaggel import kaggle_api

@job(resource_defs={"kaggle": kaggle_api})
def ingest_spotify_job():
    download_spotify_data()
