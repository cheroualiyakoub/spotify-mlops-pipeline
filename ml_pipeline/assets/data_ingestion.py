# Fixed Assets Code
from dagster import asset, OpExecutionContext, get_dagster_logger, Out
from ml_pipeline.resources.kaggel import kaggle_api
import os
import pandas as pd
from dagster import StaticPartitionsDefinition

@asset(
    group_name="data_ingestion",
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
def raw_kaggle_data(context: OpExecutionContext):
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
    group_name="data_ingestion",
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
def spotify_data_analysis(context: OpExecutionContext, raw_kaggle_data: pd.DataFrame):
    """Asset that automatically loads data via I/O manager and returns it with metadata"""
    
    logger = get_dagster_logger()
    
    # The I/O manager automatically calls load_input() and passes the DataFrame here
    logger.info(f"✅ Received data via I/O manager: {raw_kaggle_data.shape}")
    logger.info(f"Columns: {list(raw_kaggle_data.columns)[:5]}...")
    
    # Generate metadata about the loaded dataset
    data_metadata = {
        "dataset_info": {
            "total_rows": len(raw_kaggle_data),
            "total_columns": len(raw_kaggle_data.columns),
            "column_names": list(raw_kaggle_data.columns),
            "memory_usage_mb": round(raw_kaggle_data.memory_usage(deep=True).sum() / 1024 / 1024, 2)
        },
        "data_quality": {
            "missing_values_per_column": raw_kaggle_data.isnull().sum().to_dict(),
            "duplicate_rows": int(raw_kaggle_data.duplicated().sum()),
            "completeness_percentage": round((1 - raw_kaggle_data.isnull().sum().sum() / raw_kaggle_data.size) * 100, 2)
        },
        "analysis_metadata": {
            "analysis_timestamp": pd.Timestamp.now().isoformat(),
            "loaded_via": "lakefs_io_manager",
            "source_asset": "raw_kaggle_data"
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
        "dataset_shape": list(raw_kaggle_data.shape),
        "columns": list(raw_kaggle_data.columns),
        "data_quality_score": float(data_metadata['data_quality']['completeness_percentage']),
        "memory_usage_mb": float(data_metadata['dataset_info']['memory_usage_mb']),
        "loaded_via": "lakefs_io_manager",
        "analysis_completed": True
    })
    
    # Return the dataset (I/O manager will store it in development branch)
    logger.info("Returning dataset via I/O manager...")
    return raw_kaggle_data

year_partitions = StaticPartitionsDefinition(
    [str(year) for year in range(2000, 2024)]  # 2015-2023
)

@asset(
    group_name="data_ingestion",
    partitions_def=year_partitions,
    io_manager_key="dynamic_lakefs_io", 
    deps=["spotify_data_analysis"],
    metadata={
        "lakefs_config": {
            "repo": "spotify-repo",
            "branch": "splited-data",
            "path": "year={year}/data.csv",  # Template with {year}
            "commit_message": "Yearly data for {year}",  # Template
            "auto_commit": True
        }
    }
)
def yearly_data(context, spotify_data_analysis: pd.DataFrame) -> pd.DataFrame:
    year = context.partition_key
    partitioned_df = spotify_data_analysis[spotify_data_analysis["year"] == int(year)]
    return partitioned_df 