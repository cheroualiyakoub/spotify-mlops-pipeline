from dagster import IOManager, io_manager
import pickle
import multiprocessing

# Create a Manager object that will be shared across processes
_PROCESS_MANAGER = multiprocessing.Manager()
# Create a shared dictionary that will persist across processes
_SHARED_OBJECT_STORE = _PROCESS_MANAGER.dict()

class MemoryIOManager(IOManager):
    """Memory-based IO manager that works across processes"""
    
    def __init__(self):
        # Use the shared dictionary instead of a local one
        self._object_store = _SHARED_OBJECT_STORE
    
    def _get_path(self, context):
        """Generate a unique key for storing objects"""
        if hasattr(context, 'asset_key'):
            # For output context with asset key
            key = context.asset_key.path
        elif hasattr(context, 'get_output_identifier'):
            # For output context with output identifier
            key = context.get_output_identifier()
        elif hasattr(context.upstream_output, 'asset_key'):
            # For input context with upstream asset key
            key = context.upstream_output.asset_key.path
        elif hasattr(context.upstream_output, 'get_output_identifier'):
            # For input context with upstream output identifier
            key = context.upstream_output.get_output_identifier()
        else:
            # Fallback using string representation
            key = str(context)
            
        return tuple(key) if isinstance(key, list) else (str(key),)  # Convert to tuple for dict key
    
    def handle_output(self, context, obj):
        """Store object in memory dictionary"""
        key = self._get_path(context)
        self._object_store[key] = obj  # Store in shared dict
        context.log.info(f"Stored object in memory at key: {key}, keys: {list(self._object_store.keys())}")
        context.log.debug(f"Stored object in memory at key: {key}")
    
    def load_input(self, context):
        """Load object from memory dictionary"""
        key = self._get_path(context.upstream_output)
        context.log.info(f"Loading from memory at key: {key}, available keys: {list(self._object_store.keys())}")
        obj = self._object_store.get(key)
        
        if obj is None:
            context.log.error(f"No object found in memory at key: {key}")
            raise KeyError(f"No object found in memory at key: {key}")
        
        context.log.debug(f"Loaded object from memory at key: {key}")
        return obj

# Also expose it with the custom_memory_io name to match your asset definitions
@io_manager
def custom_memory_io():
    """Returns the same memory IO manager for the custom_memory_io key"""
    return MemoryIOManager()