# ml_pipeline/io_manager/file_io_manager.py
from dagster import IOManager, io_manager
import pickle
import os
import pandas as pd

class FileIOManager(IOManager):
    """File-based IO manager that uses pickle to persist data between processes"""
    
    def __init__(self, base_dir="/opt/dagster/dagster_home/storage"):
        self._base_dir = base_dir
        # Ensure storage directory exists
        os.makedirs(self._base_dir, exist_ok=True)
    
    def _get_path(self, context):
        """Generate a unique file path for an asset"""
        if hasattr(context, 'asset_key'):
            # For output context with asset key
            key = "_".join(context.asset_key.path)
        elif hasattr(context.upstream_output, 'asset_key'):
            # For input context with upstream asset key
            key = "_".join(context.upstream_output.asset_key.path)
        else:
            # Fallback using string representation
            key = str(context).replace("/", "_").replace(":", "_")
            
        return os.path.join(self._base_dir, f"{key}.pkl")
    
    def handle_output(self, context, obj):
        """Store object as a pickle file"""
        filepath = self._get_path(context)
        with open(filepath, "wb") as f:
            pickle.dump(obj, f)
        context.log.info(f"Stored object at: {filepath}")
    
    def load_input(self, context):
        """Load object from a pickle file"""
        filepath = self._get_path(context)
        if not os.path.exists(filepath):
            available_files = os.listdir(self._base_dir) if os.path.exists(self._base_dir) else []
            context.log.error(f"No file found at: {filepath}")
            context.log.error(f"Available files: {available_files}")
            raise FileNotFoundError(f"No file found at: {filepath}")
        
        with open(filepath, "rb") as f:
            obj = pickle.load(f)
        context.log.info(f"Loaded object from: {filepath}")
        return obj

@io_manager
def file_io_manager():
    """Returns a file-based IO manager that persists data between processes"""
    return FileIOManager()

# For backward compatibility with existing code
@io_manager
def custom_memory_io():
    """Maps the existing 'custom_memory_io' to use file-based storage"""
    return FileIOManager()