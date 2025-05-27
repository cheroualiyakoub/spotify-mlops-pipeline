from dagster import asset, get_dagster_logger, MetadataValue
import pandas as pd
import re

@asset(
    group_name="preprocessing",
    required_resource_keys={"lakefs_client", "year_selector"}
)
def selected_years(context) -> list:
    """Get the selected years to process based on configuration"""
    logger = get_dagster_logger()
    
    # Use the year selector resource to get and validate selected years
    valid_years = context.resources.year_selector.validate_selection(context)
    
    logger.info(f"Selected and validated years: {valid_years}")
    
    # Add metadata
    context.add_output_metadata({
        "total_years": MetadataValue.int(len(valid_years)),
        "years": MetadataValue.text(", ".join(sorted(valid_years)))
    })
    
    return valid_years

@asset(
    group_name="preprocessing",
    deps=[selected_years],
    required_resource_keys={"dynamic_lakefs_io"}
)
def combined_data(context, selected_years: list) -> pd.DataFrame:
    """Download and combine data for the selected years - returned in memory"""
    logger = get_dagster_logger()
    
    logger.info(f"Processing {len(selected_years)} years: {', '.join(selected_years)}")
    
    # Create years string in format "00_01_02" (last 2 digits of each year)
    years_string = "_".join([str(year)[-2:] for year in sorted(selected_years)])
    logger.info(f"Years string: {years_string}")
    
    # Download and combine data
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
            
            df['year'] = year  # Add year column for reference
            dfs.append(df)
            logger.info(f"Loaded {len(df)} rows for year {year}")
            
        except Exception as e:
            logger.error(f"Failed to load year {year}: {str(e)}")
            continue
    
    if not dfs:
        raise ValueError(f"Failed to load any data for years: {selected_years}")
    
    combined_df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Successfully combined {len(selected_years)} years of data: {combined_df.shape}")
    
    # Add metadata for tracking (but don't save to LakeFS)
    context.add_output_metadata({
        "total_years": MetadataValue.int(len(selected_years)),
        "total_rows": MetadataValue.int(len(combined_df)),
        "years_included": MetadataValue.text(", ".join(sorted(selected_years))),
        "years_string": MetadataValue.text(years_string),
        "columns": MetadataValue.json({
            "count": len(combined_df.columns),
            "names": list(combined_df.columns)
        }),
        "storage": MetadataValue.text("in_memory_only")
    })
    
    return combined_df


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