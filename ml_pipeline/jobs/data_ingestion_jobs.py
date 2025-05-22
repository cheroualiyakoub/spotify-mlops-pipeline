from dagster import job
from ml_pipeline.assets.data_ingestion import download_and_store_spotify_data
from ml_pipeline.resources.kaggel import kaggle_api
from ml_pipeline.resources.lakefs import lakefs_resource

@job(resource_defs={"kaggle": kaggle_api, "lakefs": lakefs_resource})
def ingest_spotify_job():
    download_and_store_spotify_data()
