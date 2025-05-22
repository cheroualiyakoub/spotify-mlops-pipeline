
from dagster import asset, OpExecutionContext
from ml_pipeline.resources.kaggel import kaggle_api
from ml_pipeline.resources.lakefs import lakefs_resource
import pandas as pd



# @asset(required_resource_keys={"kaggle"}) 
# def download_spotify_data(context: OpExecutionContext):
#     api = context.resources.kaggle
    
#     # Use a real, public Spotify dataset
#     dataset_path = "amitanshjoshi/spotify-1million-tracks"
    
#     api.dataset_download_files(
#         dataset_path,
#         path="/opt/dagster/app/data/raw",
#         unzip=True
#     )



from dagster import asset, OpExecutionContext, get_dagster_logger
import os
import pandas as pd
from pathlib import Path



@asset(required_resource_keys={"kaggle", "lakefs"}) 
def download_and_store_spotify_data(context: OpExecutionContext):
    """Download Spotify data from Kaggle and store in LakeFS"""
    
    logger = get_dagster_logger()
    kaggle_api = context.resources.kaggle
    lakefs_client = context.resources.lakefs
    
    # Step 1: Download from Kaggle
    logger.info("Downloading Spotify data from Kaggle...")
    dataset_path = "amitanshjoshi/spotify-1million-tracks"
    local_path = "/opt/dagster/app/data/raw"
    
    # Create directory if it doesn't exist
    os.makedirs(local_path, exist_ok=True)
    
    kaggle_api.dataset_download_files(
        dataset_path,
        path=local_path,
        unzip=True
    )
    
    # Step 2: Find downloaded files
    downloaded_files = []
    for file in os.listdir(local_path):
        if file.endswith(('.csv', '.json', '.parquet')):
            downloaded_files.append(os.path.join(local_path, file))
    
    logger.debug(f"Downloaded files: {[os.path.basename(f) for f in downloaded_files]}")
    
    # Step 3: Upload to LakeFS
    repository_name = "spotify-data-test1"
    branch_name = "main"
    
    try:
        # Create repository if it doesn't exist
        try:
            repo = lakefs_client.repositories.get_repository(repository_name)
            logger.debug(f"Repository '{repository_name}' already exists")
        except:
            logger.debug(f"Creating repository '{repository_name}'")
            repo = lakefs_client.repositories.create_repository(
                repository_creation={
                        "name": repository_name,
                        "storage_namespace": "local://spotify-data-test",
                        "default_branch": "main"
                    }
            )
        
        # Upload each file to LakeFS
        uploaded_files = []
        for file_path in downloaded_files:
            file_name = os.path.basename(file_path)
            lakefs_path = f"raw/{file_name}"
            
            logger.debug(f"Uploading {file_name} to LakeFS...")
            
            # Upload file
            with open(file_path, 'rb') as f:
                lakefs_client.objects.upload_object(
                    repository=repository_name,
                    branch=branch_name,
                    path=lakefs_path,
                    content=f
                )
            
            uploaded_files.append(lakefs_path)
            logger.info(f"Successfully uploaded {file_name} to {lakefs_path}")
        
        # Commit the changes
        commit_message = f"Add Spotify dataset from Kaggle - {len(uploaded_files)} files"
        commit = lakefs_client.commits.commit(
            repository=repository_name,
            branch=branch_name,
            commit_creation={
                "message": commit_message,
                "metadata": {
                    "source": "kaggle",
                    "dataset": dataset_path,
                    "files_count": str(len(uploaded_files))
                }
            }
        )
        
        logger.info(f"Committed changes: {commit.id}")
        
        return {
            "repository": repository_name,
            "branch": branch_name,
            # "commit_id": commit.id,
            # "uploaded_files": uploaded_files,
            "local_files": downloaded_files
        }
        
    except Exception as e:
        logger.error(f"Failed to upload to LakeFS: {e}")
        raise