from dagster import resource
from lakefs_spec import LakeFSFileSystem
from lakefs_client import Configuration
import os

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
def lakefs_fs_resource(context):
    """For versioned data operations (I/O manager compatible)"""
    fs = LakeFSFileSystem(
        host=os.getenv('LAKEFS_ENDPOINT', 'http://lakefs:8000'),
        username=os.getenv('LAKEFS_ACCESS_KEY_ID'),
        password=os.getenv('LAKEFS_SECRET_ACCESS_KEY'),
    )    
    return fs