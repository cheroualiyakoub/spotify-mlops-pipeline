import os
import requests 
import sys
import logging
from pathlib import Path
from lakefs_client.client import LakeFSClient
from dotenv import load_dotenv
from lakefs_client import Configuration

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

print("loading environment variables")


load_dotenv()


# Check connection via health check
def check_lakefs_connection(client):
    """
    Check if LakeFS is accessible and responding.
    
    Args:
        client: LakeFSClient instance
        
    Returns:
        bool: True if connection is successful, False otherwise
    """
    try:
        # Try to list repositories as a connection test
        client.repositories.list_repositories(after='', prefix='')
        print("✅ Connected to LakeFS successfully")
        return True
    except Exception as e:
        print(f"❌ Failed to connect to LakeFS: {str(e)}")
        return False



LAKEFS_ADMIN_ACCESS_KEY = os.getenv("LAKEFS_ADMIN_ACCESS_KEY")
LAKEFS_ADMIN_SECRET_KEY = os.getenv("LAKEFS_ADMIN_SECRET_KEY")
LAKEFS_ENDPOINT = os.getenv("LAKEFS_ENDPOINT")

if not LAKEFS_ADMIN_ACCESS_KEY or not LAKEFS_ADMIN_SECRET_KEY:
    print("LAKEFS_ADMIN_ACCESS_KEY and LAKEFS_ADMIN_SECRET_KEY must be set in .env file")
    sys.exit(1)

configuration = Configuration(
    host=LAKEFS_ENDPOINT,
    username=LAKEFS_ADMIN_ACCESS_KEY,
    password=LAKEFS_ADMIN_SECRET_KEY
)

clt = LakeFSClient(configuration=configuration)

if not check_lakefs_connection(clt):
    sys.exit(1)

print("connected to lakeFS")

try:
    # Check if the repository already exists
    repo_name = "spotify-data"
    repo = clt.repositories.get_repository(repo_name)
    logging.info(f"Repository '{repo_name}' already exists.")
except Exception as e:
    if "not found" in str(e).lower():
        try:
            clt.repositories.create_repository(
                repository_creation={
                    "name": repo_name,
                    "storage_namespace": "local://spotify-data",
                    "default_branch": "main"
                }
            )
            logging.info(f"Repository '{repo_name}' created successfully.")
        except Exception as e:
            logging.error(f"Failed to create repository '{repo_name}': {str(e)}")
            sys.exit(1)
    else:
        logging.error(f"Error checking repository: {str(e)}")
        sys.exit(1)


