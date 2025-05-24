# Fixed Assets Code
from dagster import asset, OpExecutionContext, get_dagster_logger
from ml_pipeline.resources.kaggel import kaggle_api
import os
import pandas as pd

@asset(
    io_manager_key="dynamic_lakefs_io",
    required_resource_keys={"kaggle"},
    metadata={
        "lakefs_config": {
            "repo": "spotify-repo",
            "branch": "raw-data",
            "path": "raw_data.csv",
            "commit_message": "raw data - ziped from Kaggle",
            "auto_commit": True
        }
    }
)
def versioned_spotify_data_dev(context: OpExecutionContext):
    """Asset that downloads raw data from Kaggle"""
    
    logger = get_dagster_logger()
    logger.info("Downloading dataset from Kaggle...")
    
    kaggle = context.resources.kaggle
    dataset_path = "amitanshjoshi/spotify-1million-tracks"
    local_path = "/tmp/spotify_data"
    
    kaggle.dataset_download_files(
        dataset_path,
        path=local_path,
        unzip=True
    )
    
    # Load data
    data_files = [f for f in os.listdir(local_path) if f.endswith('.csv')]
    if not data_files:
        raise ValueError("No CSV files found in downloaded dataset")
    
    main_file = os.path.join(local_path, data_files[0])
    logger.info(f"Loading main data file: {main_file}")
    df = pd.read_csv(main_file)
    logger.info(f"Data loaded successfully. Shape: {df.shape}, Columns: {list(df.columns)}")
    return pd.read_csv(main_file)


@asset(
    io_manager_key="dynamic_lakefs_io",
    metadata={
        "lakefs_config": {
            "repo": "spotify-repo", 
            "branch": "raw-data",
            "path": "raw_data.csv",
            "commit_message": "Analysis results",
            "auto_commit": True
        }
    }
)
def spotify_data_analysis(context: OpExecutionContext, versioned_spotify_data_dev: pd.DataFrame):
    """Asset that automatically loads data via I/O manager and returns it with metadata"""
    
    logger = get_dagster_logger()
    
    # The I/O manager automatically calls load_input() and passes the DataFrame here
    logger.info(f"✅ Received data via I/O manager: {versioned_spotify_data_dev.shape}")
    logger.info(f"Columns: {list(versioned_spotify_data_dev.columns)[:5]}...")
    
    # Generate metadata about the loaded dataset
    data_metadata = {
        "dataset_info": {
            "total_rows": len(versioned_spotify_data_dev),
            "total_columns": len(versioned_spotify_data_dev.columns),
            "column_names": list(versioned_spotify_data_dev.columns),
            "memory_usage_mb": round(versioned_spotify_data_dev.memory_usage(deep=True).sum() / 1024 / 1024, 2)
        },
        "data_quality": {
            "missing_values_per_column": versioned_spotify_data_dev.isnull().sum().to_dict(),
            "duplicate_rows": int(versioned_spotify_data_dev.duplicated().sum()),
            "completeness_percentage": round((1 - versioned_spotify_data_dev.isnull().sum().sum() / versioned_spotify_data_dev.size) * 100, 2)
        },
        "analysis_metadata": {
            "analysis_timestamp": pd.Timestamp.now().isoformat(),
            "loaded_via": "lakefs_io_manager",
            "source_asset": "versioned_spotify_data_dev"
        }
    }
    
    # Log key insights
    logger.info(f"📊 Dataset Analysis:")
    logger.info(f"  - Rows: {data_metadata['dataset_info']['total_rows']:,}")
    logger.info(f"  - Columns: {data_metadata['dataset_info']['total_columns']}")
    logger.info(f"  - Memory: {data_metadata['dataset_info']['memory_usage_mb']} MB")
    logger.info(f"  - Completeness: {data_metadata['data_quality']['completeness_percentage']}%")
    
    # Add metadata to Dagster context
    context.add_output_metadata({
        "dataset_shape": list(versioned_spotify_data_dev.shape),
        "columns": list(versioned_spotify_data_dev.columns),
        "data_quality_score": float(data_metadata['data_quality']['completeness_percentage']),
        "memory_usage_mb": float(data_metadata['dataset_info']['memory_usage_mb']),
        "loaded_via": "lakefs_io_manager",
        "analysis_completed": True
    })
    
    # Return the dataset (I/O manager will store it in development branch)
    logger.info("Returning dataset via I/O manager...")
    return versioned_spotify_data_dev
