#!/usr/bin/env python
"""
Script to initialize LakeFS repository and branch structure for Spotify ML Pipeline.

This script:
1. Creates a LakeFS repository for the Spotify tracks dataset
2. Sets up branch structure (main, staging, development)
3. Configures initial metadata and policies
4. Creates directory structure for year-based data partitioning

Requirements:
- Python 3.8+
- lakefs-client package
- python-dotenv package
- .env file with LakeFS credentials or environment variables

Usage:
    python setup_lakefs.py
"""

import os
import sys
import logging
import argparse
from pathlib import Path
from typing import Dict, List, Optional
from dotenv import load_dotenv

import lakefs_client
from lakefs_client import models
from lakefs_client.client import LakeFSClient
from lakefs_client.configuration import Configuration

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Constants
REPO_NAME = "spotify_tracks"  # Changed from spotify-tracks to spotify_tracks
REPO_STORAGE_NAMESPACE = "spotify_data"  # Adjust based on your storage setup
BRANCHES = ["main", "staging", "development"]
YEARS = list(range(2015, 2023))  # Adjust year range as needed
DATA_DIRS = ["raw", "processed", "features"]


def get_lakefs_client(
    endpoint: Optional[str] = None,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
) -> LakeFSClient:
    """
    Create a LakeFS client using credentials from args, environment variables, or .env file.
    
    Args:
        endpoint: LakeFS endpoint URL
        access_key: LakeFS access key
        secret_key: LakeFS secret key
        
    Returns:
        Configured LakeFSClient
    """
    # Try to load .env file from project root
    # First check if .env exists in current directory
    if Path('.env').exists():
        load_dotenv()
    # Then look for .env in the parent directory (project root)
    elif Path('..', '.env').exists():
        load_dotenv(Path('..', '.env'))
    # Also try two directories up (in case run from scripts/some_subdir/)
    elif Path('..', '..', '.env').exists():
        load_dotenv(Path('..', '..', '.env'))
    
    # Use provided values or fall back to environment variables
    endpoint = endpoint or os.environ.get("LAKEFS_ENDPOINT", "http://localhost:8000")
    access_key = access_key or os.environ.get("LAKEFS_ADMIN_ACCESS_KEY")
    secret_key = secret_key or os.environ.get("LAKEFS_ADMIN_SECRET_KEY")
    
    if not access_key or not secret_key:
        raise ValueError(
            "LakeFS credentials not provided. Set LAKEFS_ADMIN_ACCESS_KEY and "
            "LAKEFS_ADMIN_SECRET_KEY in your .env file or environment variables, "
            "or pass them as arguments."
        )
    
    # Configure LakeFS client
    configuration = Configuration(
        host=endpoint,
        username=access_key,
        password=secret_key,
    )
    
    return LakeFSClient(configuration)


def check_if_setup_needed(client: LakeFSClient) -> bool:
    """
    Check if LakeFS needs to be set up by checking if repository
    and all required branches already exist.
    
    Args:
        client: LakeFS client
        
    Returns:
        True if setup is needed, False if everything is already configured
    """
    try:
        # Check if repository exists
        try:
            repo = client.repositories.get_repository(REPO_NAME)
        except lakefs_client.exceptions.NotFoundException:
            logger.info(f"Repository '{REPO_NAME}' doesn't exist. Setup needed.")
            return True
            
        # Check if all required branches exist
        existing_branches = [
            branch.id for branch in client.branches.list_branches(REPO_NAME).results
        ]
        
        missing_branches = [branch for branch in BRANCHES if branch not in existing_branches]
        if missing_branches:
            logger.info(f"Missing branches: {', '.join(missing_branches)}. Setup needed.")
            return True
            
        # Check if directory structure exists in main branch
        try:
            # Try to get a single placeholder file to check if structure exists
            client.objects.get_object(
                repository=REPO_NAME,
                branch="main",
                path=f"data/{YEARS[0]}/{DATA_DIRS[0]}/.gitkeep"
            )
        except lakefs_client.exceptions.NotFoundException:
            logger.info("Directory structure not found. Setup needed.")
            return True
            
        # Everything seems to be set up already
        logger.info("LakeFS repository, branches, and structure already exist. No setup needed.")
        return False
        
    except Exception as e:
        logger.error(f"Error checking setup status: {str(e)}")
        # If we can't determine the status, assume setup is needed
        return True


def create_repository(client: LakeFSClient) -> bool:
    """
    Create a new repository in LakeFS if it doesn't exist.
    
    Args:
        client: LakeFS client
        
    Returns:
        True if repository was created or already exists, False otherwise
    """
    try:
        # Check if repository already exists
        try:
            client.repositories.get_repository(REPO_NAME)
            logger.info(f"Repository '{REPO_NAME}' already exists")
            return True
        except lakefs_client.exceptions.NotFoundException:
            # Repository doesn't exist, create it
            client.repositories.create_repository(
                models.RepositoryCreation(
                    name=REPO_NAME,
                    storage_namespace=REPO_STORAGE_NAMESPACE,
                    default_branch="main",
                )
            )
            logger.info(f"Repository '{REPO_NAME}' created successfully")
            return True
    except Exception as e:
        logger.error(f"Failed to create repository: {str(e)}")
        return False


def setup_branches(client: LakeFSClient) -> bool:
    """
    Create branch structure in the repository.
    
    Args:
        client: LakeFS client
        
    Returns:
        True if branches were created successfully, False otherwise
    """
    try:
        # 'main' branch is created by default when repository is created
        existing_branches = [
            branch.id for branch in client.branches.list_branches(REPO_NAME).results
        ]
        
        for branch in BRANCHES:
            if branch != "main" and branch not in existing_branches:
                client.branches.create_branch(
                    repository=REPO_NAME,
                    branch_creation=models.BranchCreation(
                        name=branch,
                        source="main",
                    ),
                )
                logger.info(f"Created branch '{branch}'")
            else:
                logger.info(f"Branch '{branch}' already exists")
        return True
    except Exception as e:
        logger.error(f"Failed to create branches: {str(e)}")
        return False


def create_directory_structure(client: LakeFSClient, branch: str) -> bool:
    """
    Create directory structure for data organization.
    
    Args:
        client: LakeFS client
        branch: Branch name to create directories in
        
    Returns:
        True if directory structure was created successfully, False otherwise
    """
    try:
        # Create empty placeholder file to represent directories
        for year in YEARS:
            for data_dir in DATA_DIRS:
                path = f"data/{year}/{data_dir}/.gitkeep"
                
                # Create an empty object to represent the directory
                client.objects.upload_object(
                    repository=REPO_NAME,
                    branch=branch,
                    path=path,
                    content="",  # Empty content for placeholder
                )
                logger.info(f"Created directory placeholder: {path}")
        
        # Commit the directory structure
        client.commits.commit(
            repository=REPO_NAME,
            branch=branch,
            commit_creation=models.CommitCreation(
                message=f"Initialize directory structure for data organization",
                metadata={
                    "operation": "init",
                    "created_by": "setup_script",
                },
            ),
        )
        logger.info(f"Committed directory structure to branch '{branch}'")
        return True
    except Exception as e:
        logger.error(f"Failed to create directory structure: {str(e)}")
        return False


def main(args):
    """
    Main function to set up the LakeFS repository and structure.
    """
    logger.info("Initializing LakeFS setup for Spotify ML Pipeline")
    
    # Get LakeFS client
    try:
        client = get_lakefs_client(
            endpoint=args.endpoint,
            access_key=args.access_key,
            secret_key=args.secret_key,
        )
        logger.info("Successfully connected to LakeFS")
    except ValueError as e:
        logger.error(f"Connection error: {str(e)}")
        logger.error("Make sure your .env file contains LAKEFS_ADMIN_ACCESS_KEY and LAKEFS_ADMIN_SECRET_KEY")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error connecting to LakeFS: {str(e)}")
        return 1
        
    # Check if setup is needed
    if not check_if_setup_needed(client):
        logger.info("LakeFS is already set up properly. Exiting.")
        return 0
    
    # Create repository
    if not create_repository(client):
        logger.error("Failed to create or access repository. Exiting.")
        return 1
    
    # Setup branches
    if not setup_branches(client):
        logger.error("Failed to create branch structure. Exiting.")
        return 1
    
    # Create directory structure in development branch first
    logger.info("Creating directory structure in 'development' branch")
    if not create_directory_structure(client, "development"):
        logger.error("Failed to create directory structure. Exiting.")
        return 1
    
    # Success message
    logger.info(f"""
    LakeFS Setup Complete:
    - Repository: {REPO_NAME}
    - Branches: {', '.join(BRANCHES)}
    - Years: {YEARS[0]} - {YEARS[-1]}
    - Data Directories: {', '.join(DATA_DIRS)}
    
    Next steps:
    1. Upload your Spotify data by year to the development branch
    2. Use the Dagster pipeline to process the data
    3. Merge changes to staging and main branches as needed
    """)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize LakeFS structure for Spotify ML Pipeline")
    parser.add_argument("--endpoint", help="LakeFS endpoint URL")
    parser.add_argument("--access-key", help="LakeFS access key")
    parser.add_argument("--secret-key", help="LakeFS secret key")
    
    args = parser.parse_args()
    sys.exit(main(args))
