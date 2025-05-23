from dagster import asset, OpExecutionContext, get_dagster_logger
from ml_pipeline.resources.kaggel import kaggle_api
from ml_pipeline.io_manager.lakefs_io import lakefs_io_manager
import os
import pandas as pd
from pathlib import Path

@asset(
    io_manager_key="lakefs_io",
    required_resource_keys={"kaggle"}
)
def versioned_spotify_data(context: OpExecutionContext):
    """Downloads from Kaggle and auto-uploads to LakeFS via I/O manager"""
    logger = get_dagster_logger()
    kaggle = context.resources.kaggle
    # 1. Download from Kaggle
    dataset_path = "amitanshjoshi/spotify-1million-tracks"
    local_path = "/tmp/spotify_data"
    os.makedirs(local_path, exist_ok=True)
    
    logger.info("Downloading dataset from Kaggle...")
    kaggle.dataset_download_files(
        dataset_path,
        path=local_path,
        unzip=True
    )
    
    # 2. Load data (will be auto-uploaded by I/O manager)
    data_files = [f for f in os.listdir(local_path) if f.endswith('.csv')]
    if not data_files:
        raise ValueError("No CSV files found in downloaded dataset")
    
    main_file = os.path.join(local_path, data_files[0])
    logger.info(f"Loading main data file: {main_file}")
    
    # Return value will be handled by LakeFSIOManager
    return pd.read_csv(main_file)


@asset(
    io_manager_key="lakefs_io",
    deps=["versioned_spotify_data"]  # This ensures it runs after the initial data load
)
def processed_spotify_data(context: OpExecutionContext):
    """Processes the Spotify data stored in LakeFS"""
    logger = get_dagster_logger()
    
    # 1. Load data from LakeFS (handled automatically by your IOManager)
    df = context.resources.lakefs_io.load_input( )
    
    # 2. Add processing metadata
    context.add_output_metadata({
        "original_rows": len(df),
        "columns": list(df.columns),
        "sample_data": str(df.head(1).to_dict())
    })
    
    # 3. Example processing - you'll replace this with your actual logic
    logger.info(f"Processing {len(df)} tracks...")
    
    # Example: Calculate average popularity per artist
    if 'artist_name' in df.columns and 'popularity' in df.columns:
        artist_stats = df.groupby('artist_name')['popularity'].agg(['mean', 'count'])
        top_artists = artist_stats.sort_values('mean', ascending=False).head(10)
        
        context.add_output_metadata({
            "top_artists": top_artists.to_dict(),
            "missing_values": df.isnull().sum().to_dict()
        })
    
    # Return processed DataFrame (will be stored back to LakeFS)
    return df