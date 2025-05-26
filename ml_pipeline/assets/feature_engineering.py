from dagster import asset, get_dagster_logger, MetadataValue, io_manager
import pandas as pd
from ml_pipeline.io_manager.lakefs_io import set_dynamic_lakefs_config
from dagster import asset, get_dagster_logger, MetadataValue, DynamicPartitionsDefinition

# Create a dynamic partitions definition
spotify_year_partitions = DynamicPartitionsDefinition(name="spotify_years")

@asset(
    required_resource_keys={"lakefs_client"}
)
def available_years(context) -> list:
    """Discover available years in LakeFS"""
    logger = get_dagster_logger()
    
    try:
        # Use the LakeFS client to list objects
        response = context.resources.lakefs_client.objects.list_objects(
            repository="spotify-repo",
            ref="development",
            prefix="year="
        )
        
        # Extract years from paths
        years = set()
        for obj in response.results:
            # Expected format: data/year=YYYY/data.csv
            path = obj.path
            if 'year=' in path:
                year = path.split('year=')[1].split('/')[0]
                years.add(year)
        
        years_list = sorted(list(years))
        logger.info(f"Found years: {years_list}")
        
    
        # add_request = spotify_year_partitions.build_add_request(years_list)
        context.instance.add_dynamic_partitions(partition_keys=years_list,
                                                partitions_def_name=spotify_year_partitions.name)

        return years_list
    
    except Exception as e:
        logger.error(f"Failed to get available years: {e}")
        raise
@asset(
    partitions_def=spotify_year_partitions,
)
def historical_training_data(context) -> str:
    """Return the partition key (year) without downloading data"""
    logger = get_dagster_logger()
    
    year = context.partition_key
    logger.info(f"Registering selected year: {year}")
    
    # Just return the year string - no data downloading
    return year

@asset(
    io_manager_key="dynamic_lakefs_io",
    deps=[historical_training_data],
)
def combined_historical_data(context, historical_training_data: dict) -> pd.DataFrame:
    """Download and combine data for ONLY the selected years"""
    logger = get_dagster_logger()
    

    # Get ONLY the selected years from the current run
    # This gets the partition keys that are being materialized in this specific run
    if hasattr(context, 'selected_asset_keys_and_partition_keys'):
        # Get partition keys for the current run
        selected_partitions = []
        for asset_key, partition_keys in context.selected_asset_keys_and_partition_keys:
            if asset_key.path[-1] == "historical_training_data":
                selected_partitions = partition_keys
                break
        
        selected_years = selected_partitions
        logger.info(f"Selected years from current run: {selected_years}")
    else:
        # Fallback: if we can't get the selected partitions, use the upstream data
        # But only use the keys that correspond to the current execution
        logger.warning("Could not determine selected partitions, using upstream data")
        selected_years = list(historical_training_data.keys())
        logger.info(f"Using upstream partition keys: {selected_years}")
    
    logger.info(f"Final selected years to combine: {selected_years}")
    


    # Create years string in format "00_01_02" (last 2 digits of each year)
    years_string = "_".join([str(year)[-2:] for year in sorted(selected_years)])
    logger.info(f"Years string for filename: {years_string}")
    
    # Create dynamic filename with years suffix
    dynamic_filename = f"data/combined/combined_data_years_{years_string}.csv"
    logger.info(f"Dynamic filename: {dynamic_filename}")

    asset_key_str = str(context.asset_key)
    dynamic_config = {
        "repo": "spotify-repo",
        "branch": "development",
        "path": dynamic_filename,
        "commit_message": f"Combined data for years: {', '.join(sorted(selected_years))}",
        "auto_commit": True
    }
    set_dynamic_lakefs_config(asset_key_str, dynamic_config)

    # DEBUG: Verify the config was set correctly
    logger.info(f"🔍 Setting dynamic config...")
    logger.info(f"🔍 Dynamic filename: {dynamic_filename}")
    logger.info(f"🔍 Selected years: {selected_years}")
    logger.info(f"🔍 Has _dynamic_lakefs_config: {hasattr(context, '_dynamic_lakefs_config')}")
    logger.info(f"🔍 Dynamic config value: {getattr(context, '_dynamic_lakefs_config', 'NOT FOUND')}")

    # Verify it's accessible
    if hasattr(context, '_dynamic_lakefs_config'):
        logger.info(f"✅ Dynamic config successfully set: {context._dynamic_lakefs_config}")
    else:
        logger.error("❌ Failed to set dynamic config!")

    dfs = []
    for year in selected_years:
        try:
            lakefs_config = {
                "repo": "spotify-repo",
                "branch": "development",
                "path": f"year={year}/data.csv"
            }
            
            df = context.resources.dynamic_lakefs_io.load_input(
                context,
                lakefs_config=lakefs_config
            )
            
            df['year'] = year
            dfs.append(df)
            logger.info(f"Loaded {len(df)} rows for year {year}")
            
        except Exception as e:
            logger.error(f"Failed to load year {year}: {str(e)}")
            continue
    
    combined_df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Successfully combined {len(selected_years)} years of data")
    
    # Add metadata for tracking
    context.add_output_metadata({
        "total_years": MetadataValue.int(len(selected_years)),
        "total_rows": MetadataValue.int(len(combined_df)),
        "years_included": MetadataValue.text(", ".join(sorted(selected_years))),
        "years_string": MetadataValue.text(years_string),
        "output_filename": MetadataValue.text(dynamic_filename),
        "lakefs_config": {
            "repo": "spotify-repo",
            "branch": "development",
            "path": dynamic_filename,
            "commit_message": f"Combined data for years: {', '.join(sorted(selected_years))}",
            "auto_commit": True
    }
    })
    
    return combined_df

@asset(
    required_resource_keys={"lakefs_client", "dynamic_lakefs_io"},
)
def latest_combined_data_by_push_date(context) -> pd.DataFrame:
    """Download the most recently pushed combined dataset from LakeFS"""
    logger = get_dagster_logger()
    
    try:
        # List objects in combined directory with metadata
        response = context.resources.lakefs_client.objects.list_objects(
            repository="spotify-repo",
            ref="development",
            prefix="data/combined/",
            amount=100
        )
        
        # Debug: Check what attributes are available
        if response.results:
            sample_obj = response.results[0]
            logger.info(f"🔍 Available attributes: {dir(sample_obj)}")
            logger.info(f"🔍 Sample object: {sample_obj}")
        
        # Filter for combined data files and find the most recent
        combined_files = []
        for obj in response.results:
            if 'combined_data_years_' in obj.path and obj.path.endswith('.csv'):
                # Check different possible time attributes
                time_attr = None
                if hasattr(obj, 'mtime'):
                    time_attr = obj.mtime
                elif hasattr(obj, 'last_modified'):
                    time_attr = obj.last_modified
                elif hasattr(obj, 'modified_time'):
                    time_attr = obj.modified_time
                elif hasattr(obj, 'creation_time'):
                    time_attr = obj.creation_time
                elif hasattr(obj, 'timestamp'):
                    time_attr = obj.timestamp
                else:
                    # Fallback: use the filename to determine recency
                    logger.warning(f"No time attribute found for {obj.path}, using filename for ordering")
                    time_attr = obj.path  # Will sort alphabetically
                
                combined_files.append({
                    'path': obj.path,
                    'time_reference': time_attr,
                    'size': getattr(obj, 'size_bytes', getattr(obj, 'size', 0))
                })
        
        if not combined_files:
            raise ValueError("No combined data files found in data/combined/")
        
        # Sort by time reference (or filename if no time available)
        try:
            # Try to sort by actual time
            latest_file = max(combined_files, key=lambda x: x['time_reference'])
        except TypeError:
            # If time comparison fails, sort by filename (most recent pattern)
            logger.warning("Could not sort by time, using filename pattern")
            latest_file = max(combined_files, key=lambda x: x['path'])
        
        logger.info(f"Found {len(combined_files)} combined files")
        logger.info(f"Latest file: {latest_file['path']}")
        logger.info(f"Time reference: {latest_file['time_reference']}")
        
        # Log all files for debugging
        for file_info in sorted(combined_files, key=lambda x: str(x['time_reference']), reverse=True):
            logger.info(f"  📄 {file_info['path']} - {file_info['time_reference']}")
        
        # Download the latest file
        lakefs_config = {
            "repo": "spotify-repo",
            "branch": "development",
            "path": latest_file['path']
        }
        
        df = context.resources.dynamic_lakefs_io.load_input(
            context,
            lakefs_config=lakefs_config
        )
        
        logger.info(f"Successfully loaded latest combined data: {df.shape}")
        logger.info("✅ Data will be kept in memory, not uploaded back to LakeFS")
        
        # Extract years from filename for metadata
        import re
        years_match = re.search(r'combined_data_years_(.+)\.csv', latest_file['path'])
        years_string = years_match.group(1) if years_match else "unknown"
        
        context.add_output_metadata({
            "source_file": MetadataValue.text(latest_file['path']),
            "time_reference": MetadataValue.text(str(latest_file['time_reference'])),
            "total_rows": MetadataValue.int(len(df)),
            "total_columns": MetadataValue.int(len(df.columns)),
            "years_string": MetadataValue.text(years_string),
            "storage_method": MetadataValue.text("memory_only"),
            "total_files_available": MetadataValue.int(len(combined_files))
        })
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to load latest combined data: {str(e)}")
        raise