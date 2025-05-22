from dagster import Definitions
from dagster import repository, job, asset, AssetIn
from ml_pipeline.assets.data_ingestion import download_and_store_spotify_data
from ml_pipeline.jobs.data_ingestion_jobs import ingest_spotify_job
from ml_pipeline.resources.kaggel import kaggle_api
from ml_pipeline.resources.lakefs_client_resource import  lakefs_client_resource
from ml_pipeline.resources.lakefs_spec_resource import  lakefs_fs_resource
from ml_pipeline.io_manager.lakefs_io import  lakefs_io_manager

defs = Definitions(
    assets=[download_and_store_spotify_data],
    resources={
        "kaggle": kaggle_api,
        "lakefs_client": lakefs_client_resource,
        "lakefs_fs": lakefs_fs_resource,
         "lakefs_io": lakefs_io_manager
    }
)
