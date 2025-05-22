from dagster import Definitions
from dagster import repository, job, asset, AssetIn
from ml_pipeline.assets.data_ingestion import download_spotify_data
from ml_pipeline.jobs.data_ingestion_jobs import ingest_spotify_job
from ml_pipeline.resources.kaggel import kaggle_api

defs = Definitions(
    assets=[download_spotify_data],
    jobs=[ingest_spotify_job],
    resources={"kaggle": kaggle_api}
)

