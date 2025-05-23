from dagster import job
from ml_pipeline.assets.data_ingestion import versioned_spotify_data
from ml_pipeline.assets.data_ingestion import processed_spotify_data
from ml_pipeline.resources.kaggel import kaggle_api
from ml_pipeline.io_manager.lakefs_io import lakefs_io_manager
from ml_pipeline.resources.lakefs_client_resource import lakefs_client_resource
from ml_pipeline.resources.lakefs_spec_resource import lakefs_fs_resource

@job(
    resource_defs={
        "kaggle": kaggle_api,
        "lakefs_io": lakefs_io_manager,
        "lakefs_client": lakefs_client_resource,
        "lakefs_fs": lakefs_fs_resource
    }
)
def ingest_spotify_job():
    versioned_spotify_data()


@job(
    resource_defs={
        "lakefs_io": lakefs_io_manager,
        "lakefs_client": lakefs_client_resource,
        "lakefs_fs": lakefs_fs_resource
    }
)
def process_data_job():
    processed_spotify_data()


