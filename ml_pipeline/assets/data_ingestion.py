
from dagster import asset, OpExecutionContext
from ml_pipeline.resources.kaggel import kaggle_api
import pandas as pd


@asset(required_resource_keys={"kaggle"}) 
def download_spotify_data(context: OpExecutionContext):
    api = context.resources.kaggle
    
    # Use a real, public Spotify dataset
    dataset_path = "amitanshjoshi/spotify-1million-tracks"
    
    api.dataset_download_files(
        dataset_path,
        path="/opt/dagster/app/data/raw",
        unzip=True
    )
