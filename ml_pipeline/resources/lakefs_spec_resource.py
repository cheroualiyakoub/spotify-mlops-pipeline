from dagster import resource
from lakefs_spec import LakeFSFileSystem
from lakefs_client import Configuration
import os

@resource
def lakefs_fs_resource(context):
    """For versioned data operations (I/O manager compatible)"""
    fs = LakeFSFileSystem(
        host=os.getenv('LAKEFS_ENDPOINT', 'http://lakefs:8000'),
        username=os.getenv('LAKEFS_ACCESS_KEY_ID'),
        password=os.getenv('LAKEFS_SECRET_ACCESS_KEY'),
    )
    
    # Verify connection
    try:
        fs.ls("lakefs://")  # Simple operation to test connection
        context.log.info("LakeFS filesystem connected")
    except Exception as e:
        context.log.error(f"LakeFS connection failed: {str(e)}")
        raise
    
    return fs