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
            "branch": "development",
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



def spotify_data_cleaned(context: OpExecutionContext, versioned_spotify_data_dev: pd.DataFrame):
    """Asset that cleans the raw data for safe processing"""
    
    logger = get_dagster_logger()
    raw_df = versioned_spotify_data_dev.copy()
    
    logger.info(f"Starting data cleaning. Raw shape: {raw_df.shape}")
    logger.info(f"Raw columns: {list(raw_df.columns)}")
    
    # Create cleaning report
    cleaning_report = {
        "original_shape": raw_df.shape,
        "original_columns": list(raw_df.columns),
        "original_dtypes": raw_df.dtypes.astype(str).to_dict(),
        "cleaning_steps": []
    }
    
    # Step 1: Handle encoding issues in string columns ONLY
    for col in raw_df.select_dtypes(include=['object']).columns:
        if raw_df[col].dtype == 'object':
            original_nulls = raw_df[col].isnull().sum()
            
            # Only fix encoding, don't change the data
            raw_df[col] = raw_df[col].astype(str)
            raw_df[col] = raw_df[col].apply(
                lambda x: x.encode('utf-8', errors='replace').decode('utf-8') 
                if isinstance(x, str) and x != 'nan' else x
            )
            
            # Convert 'nan' strings back to actual NaN
            raw_df[col] = raw_df[col].replace('nan', pd.NA)
            
            final_nulls = raw_df[col].isnull().sum()
            
            cleaning_report["cleaning_steps"].append({
                "step": "encoding_fix",
                "column": col,
                "original_nulls": int(original_nulls),
                "final_nulls": int(final_nulls)
            })
    
    # Step 2: Handle problematic characters that break CSV (newlines, etc.)
    for col in raw_df.select_dtypes(include=['object']).columns:
        if raw_df[col].dtype == 'object':
            # Only remove characters that break CSV format
            raw_df[col] = raw_df[col].astype(str)
            raw_df[col] = raw_df[col].str.replace('\r\n', ' ', regex=False)
            raw_df[col] = raw_df[col].str.replace('\n', ' ', regex=False)
            raw_df[col] = raw_df[col].str.replace('\r', ' ', regex=False)
            
            # Convert 'nan' back to NaN
            raw_df[col] = raw_df[col].replace('nan', pd.NA)
            
            cleaning_report["cleaning_steps"].append({
                "step": "newline_removal",
                "column": col,
                "description": "Removed newline characters that break CSV"
            })
    
    # Step 3: Validate data integrity
    cleaning_report.update({
        "final_shape": raw_df.shape,
        "final_columns": list(raw_df.columns),
        "final_dtypes": raw_df.dtypes.astype(str).to_dict(),
        "columns_changed": list(raw_df.columns) != cleaning_report["original_columns"],
        "shape_changed": raw_df.shape != cleaning_report["original_shape"]
    })
    
    # Log cleaning summary
    logger.info(f"Cleaning completed. Final shape: {raw_df.shape}")
    logger.info(f"Columns preserved: {not cleaning_report['columns_changed']}")
    logger.info(f"Shape preserved: {not cleaning_report['shape_changed']}")
    
    # Add cleaning report to metadata
    context.add_output_metadata({
        "cleaning_report": cleaning_report,
        "data_quality": {
            "total_rows": len(raw_df),
            "total_columns": len(raw_df.columns),
            "missing_values_per_column": raw_df.isnull().sum().to_dict(),
            "duplicate_rows": int(raw_df.duplicated().sum())
        }
    })
    
    return raw_df


# @asset(
#     io_manager_key="dynamic_lakefs_io",
#     deps=["spotify_data_cleaned"],  # Ensures cleaning runs first, but no data passing
#     required_resource_keys={"lakefs_fs"},  # Direct LakeFS access
#     metadata={
#         "lakefs_config": {
#             "repo": "spotify-repo",
#             "branch": "development",
#             "commit_message": "Spotify data analysis results",
#             "auto_commit": True
#         }
#     }
# )
# def spotify_data_analysis(context: OpExecutionContext):  # No input parameter - downloads directly
#     """Asset that downloads cleaned data directly from LakeFS and returns it with metadata"""
    
#     logger = get_dagster_logger()
    
#     # Download data directly from LakeFS using the I/O manager's logic
#     logger.info("Downloading cleaned data directly from LakeFS...")
    
#     # Use the same path logic as the I/O manager
#     fs = context.resources.lakefs_fs
    
#     # Try to load using the I/O manager's path convention
#     repo = "spotify-repo"
#     branch = "development"
#     asset_name = "spotify_data_cleaned"
    
#     # Try different file formats
#     spotify_data = None
#     loaded_format = None
    
#     # Strategy 1: Try CSV first (most reliable based on your setup)
#     csv_path = f"lakefs://{repo}/{branch}/{asset_name}.csv"
#     try:
#         logger.info(f"Attempting to download CSV: {csv_path}")
#         with fs.open(csv_path, "r", encoding='utf-8') as f:
#             spotify_data = pd.read_csv(f, header=0)
#         loaded_format = "csv"
#         logger.info(f"✅ Successfully downloaded CSV: {spotify_data.shape}")
        
#     except UnicodeDecodeError:
#         logger.warning("UTF-8 failed, trying latin1 encoding...")
#         try:
#             with fs.open(csv_path, "r", encoding='latin1') as f:
#                 spotify_data = pd.read_csv(f, header=0)
#             loaded_format = "csv_latin1"
#             logger.info(f"✅ Successfully downloaded CSV with latin1: {spotify_data.shape}")
#         except Exception as e:
#             logger.warning(f"CSV latin1 failed: {e}")
            
#     except FileNotFoundError:
#         logger.info("CSV file not found, trying Parquet...")
        
#     except Exception as e:
#         logger.warning(f"CSV download failed: {e}")
    
#     # Strategy 2: Try Parquet if CSV failed
#     if spotify_data is None:
#         parquet_path = f"lakefs://{repo}/{branch}/{asset_name}.parquet"
#         try:
#             logger.info(f"Attempting to download Parquet: {parquet_path}")
#             with fs.open(parquet_path, "rb") as f:
#                 spotify_data = pd.read_parquet(f, engine='pyarrow')
#             loaded_format = "parquet"
#             logger.info(f"✅ Successfully downloaded Parquet: {spotify_data.shape}")
            
#         except Exception as e:
#             logger.warning(f"Parquet download failed: {e}")
    
#     # Strategy 3: List available files for debugging
#     if spotify_data is None:
#         try:
#             logger.info("Listing available files in LakeFS for debugging...")
#             files = fs.ls(f"lakefs://{repo}/{branch}/")
#             logger.info(f"Available files: {files}")
            
#             # Try to find any file with the asset name
#             matching_files = [f for f in files if asset_name in f]
#             if matching_files:
#                 logger.info(f"Found matching files: {matching_files}")
#                 # Try the first matching file
#                 first_match = matching_files[0]
#                 logger.info(f"Attempting to load: {first_match}")
                
#                 if first_match.endswith('.csv'):
#                     with fs.open(first_match, "r", encoding='utf-8') as f:
#                         spotify_data = pd.read_csv(f)
#                     loaded_format = "csv_discovered"
#                 elif first_match.endswith('.parquet'):
#                     with fs.open(first_match, "rb") as f:
#                         spotify_data = pd.read_parquet(f)
#                     loaded_format = "parquet_discovered"
                    
#         except Exception as e:
#             logger.error(f"File discovery failed: {e}")
    
#     # Fail if no data could be loaded
#     if spotify_data is None:
#         raise ValueError(f"Could not download data from LakeFS. Tried paths: {csv_path}, {parquet_path}")
    
#     logger.info(f"🎉 Successfully downloaded data from LakeFS!")
#     logger.info(f"  - Format: {loaded_format}")
#     logger.info(f"  - Shape: {spotify_data.shape}")
#     logger.info(f"  - Columns: {list(spotify_data.columns)[:5]}...")  # Show first 5 columns
    
#     # Generate comprehensive metadata about the downloaded dataset
#     download_metadata = {
#         "download_info": {
#             "source_repo": repo,
#             "source_branch": branch,
#             "source_asset": asset_name,
#             "file_format": loaded_format,
#             "download_timestamp": pd.Timestamp.now().isoformat(),
#             "download_method": "direct_lakefs_access"
#         },
#         "dataset_info": {
#             "total_rows": len(spotify_data),
#             "total_columns": len(spotify_data.columns),
#             "column_names": list(spotify_data.columns),
#             "data_types": spotify_data.dtypes.astype(str).to_dict(),
#             "memory_usage_mb": round(spotify_data.memory_usage(deep=True).sum() / 1024 / 1024, 2)
#         },
#         "data_quality": {
#             "missing_values_per_column": spotify_data.isnull().sum().to_dict(),
#             "duplicate_rows": int(spotify_data.duplicated().sum()),
#             "total_missing_values": int(spotify_data.isnull().sum().sum()),
#             "completeness_percentage": round((1 - spotify_data.isnull().sum().sum() / spotify_data.size) * 100, 2)
#         }
#     }
    
#     # Log key insights
#     logger.info(f"📊 Dataset Analysis:")
#     logger.info(f"  - Rows: {download_metadata['dataset_info']['total_rows']:,}")
#     logger.info(f"  - Columns: {download_metadata['dataset_info']['total_columns']}")
#     logger.info(f"  - Memory: {download_metadata['dataset_info']['memory_usage_mb']} MB")
#     logger.info(f"  - Completeness: {download_metadata['data_quality']['completeness_percentage']}%")
#     logger.info(f"  - Duplicates: {download_metadata['data_quality']['duplicate_rows']}")
    
#     # Add comprehensive metadata to Dagster
#     context.add_output_metadata({
#         "download_source": f"lakefs://{repo}/{branch}/{asset_name}",
#         "file_format": loaded_format,
#         "dataset_shape": spotify_data.shape,
#         "columns": list(spotify_data.columns),
#         "data_quality_score": download_metadata['data_quality']['completeness_percentage'],
#         "memory_usage_mb": download_metadata['dataset_info']['memory_usage_mb'],
#         "download_timestamp": download_metadata['download_info']['download_timestamp'],
#         "download_successful": True
#     })
    
#     # Return the downloaded dataset (will be stored by I/O manager)
#     logger.info("Returning downloaded dataset for storage...")
    
#     return spotify_data


