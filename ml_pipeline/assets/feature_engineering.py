from dagster import asset, get_dagster_logger, MetadataValue
import pandas as pd

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
    metadata={
        "lakefs_config": {
            "repo": "spotify-repo",
            "branch": "development", 
            "path": "data/combined/all_years.csv"
        }
    }
)

def combined_historical_data(context, historical_training_data: dict) -> pd.DataFrame:
    """Download and combine data for ONLY the selected years"""
    logger = get_dagster_logger()
    
    # Get ONLY the selected years from the upstream partitioned asset
    selected_years = list(historical_training_data.values())
    logger.info(f"Selected years to combine: {selected_years}")
    
    dfs = []
    for year in selected_years:
        try:
            lakefs_config = {
                "repo": "spotify-repo",
                "branch": "development",
                "path": f"year={year}/data.csv"  # Load from source data
            }
            
            df = context.resources.dynamic_lakefs_io.load_input(
                context,
                lakefs_config=lakefs_config
            )
            
            # Add year column
            df['year'] = year
            dfs.append(df)
            logger.info(f"Loaded {len(df)} rows for year {year}")
            
        except Exception as e:
            logger.error(f"Failed to load year {year}: {str(e)}")
            continue
    
    combined_df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Successfully combined {len(selected_years)} years of data")
    
    context.add_output_metadata({
        "total_years": MetadataValue.int(len(selected_years)),
        "total_rows": MetadataValue.int(len(combined_df)),
        "years_included": MetadataValue.text(", ".join(sorted(selected_years)))
    })
    
    return combined_df