from dagster import resource
from lakefs_client.client import LakeFSClient
from lakefs_client import Configuration
import os
from dagster import get_dagster_logger

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


@resource
def lakefs_client_resource():
    """For repository/branch management"""
    config = Configuration(
        host=os.getenv('LAKEFS_ENDPOINT', 'http://lakefs:8000'),
        username=os.getenv('LAKEFS_ACCESS_KEY_ID'),
        password=os.getenv('LAKEFS_SECRET_ACCESS_KEY'),
    )
    client = LakeFSClient(configuration=config)
    if not check_lakefs_connection(client):
        raise Exception("Failed to connect to LakeFS")
    get_dagster_logger().info("LakeFS client initialized")
    return client