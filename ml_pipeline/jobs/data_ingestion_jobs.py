from dagster import job
from ml_pipeline.assets.data_ingestion import download_and_store_spotify_data
from ml_pipeline.resources.kaggel import kaggle_api
from ml_pipeline.resources.lakefs_client_resource import lakefs_client_resource

@job(resource_defs={"kaggle": kaggle_api, "lakefs_client": lakefs_client_resource})
def ingest_spotify_job():
    download_and_store_spotify_data()
