from dagster import Definitions
from dagster import repository, job, asset, AssetIn
from ml_pipeline.assets.data_ingestion import download_and_store_spotify_data
from ml_pipeline.jobs.data_ingestion_jobs import ingest_spotify_job
from ml_pipeline.resources.kaggel import kaggle_api
from ml_pipeline.resources.lakefs import  lakefs_resource

defs = Definitions(
    assets=[download_and_store_spotify_data],
    resources={
        "kaggle": kaggle_api,
        "lakefs": lakefs_resource,
    }
)
