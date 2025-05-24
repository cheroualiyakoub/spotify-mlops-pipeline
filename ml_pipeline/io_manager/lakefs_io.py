# Fixed IO Manager Code
from dagster import IOManager, io_manager, get_dagster_logger
from lakefs_spec import LakeFSFileSystem
import pandas as pd
from dagster import IOManager, io_manager, ConfigurableResource
from typing import Optional, Dict, Any
from dataclasses import dataclass
import zipfile
import json
import pickle
import io

@dataclass
class LakeFSConfig:
    """Configuration for LakeFS operations"""
    repo: str
    branch: str
    path :str
    commit_message: Optional[str] = None
    auto_commit: bool = False
    metadata: Optional[Dict[str, Any]] = None

class DynamicLakeFSIOManager(IOManager):
    def __init__(self, fs, client, default_repo: str = "spotify-repo", default_branch: str = "main"):
        self.fs = fs
        self.client = client
        self.default_repo = default_repo
        self.default_branch = default_branch

    def _get_config_from_context(self, context) -> LakeFSConfig:
        """Get LakeFS config from static metadata + optional runtime overrides"""
        
        # Get static config from definition_metadata
        static_config = {}
        if hasattr(context, 'definition_metadata') and context.definition_metadata:
            static_config = context.definition_metadata.get('lakefs_config', {})
        
        # Get runtime config from context.metadata (for add_output_metadata overrides)
        runtime_config = {}
        if hasattr(context, 'metadata') and context.metadata:
            runtime_config = context.metadata.get('lakefs_config', {})
            # If they're identical, no runtime override was setcontent
            if runtime_config == static_config:
                runtime_config = {}
        
        # Runtime overrides static
        final_config = {**static_config, **runtime_config}
        
        context.log.debug(f"Static config: {static_config}")
        context.log.debug(f"Runtime config: {runtime_config}")
        context.log.debug(f"Final config: {final_config}")
        
        return LakeFSConfig(
            repo=final_config.get('repo', self.default_repo),
            branch=final_config.get('branch', self.default_branch),
            path=final_config.get('path'),
            commit_message=final_config.get('commit_message'),
            auto_commit=final_config.get('auto_commit', False),
            metadata=final_config.get('metadata', {})
        )

    def handle_output(self, context, obj):
        """Simple CSV upload using custom path from lakefs_config"""
        logger = get_dagster_logger()

        logger.info(f"Handling output for {context.asset_key} with object type {type(obj)}")

        if isinstance(obj, pd.DataFrame):
            logger.info(f"📊 DataFrame shape: {obj.shape}")
            config = self._get_config_from_context(context)
            logger.info(f"Using LakeFS config: {config}")
            
            # Use custom path if provided, otherwise use asset name
            if hasattr(config, 'path') and config.path:
                file_path = config.path
            else:
                file_path = f"{'/'.join(context.asset_key.path)}.csv"
            
            context.log.info(f"📤 Uploading CSV: {obj.shape} to {config.repo}/{config.branch}/{file_path}")
            
            # Rest of your upload code stays the same...
            csv_content = obj.to_csv(index=False)
            csv_bytes = csv_content.encode('utf-8')
            csv_file = io.BytesIO(csv_bytes)
            
            self.client.objects.upload_object(
                repository=config.repo,
                branch=config.branch,
                path=file_path,  # Use the custom path
                content=csv_file
            )
            
            context.log.info(f"✅ Upload successful!")
            
            context.add_output_metadata({
                "repo": config.repo,
                "branch": config.branch,
                "path": file_path,  # Store the actual path used
                "file_format": "csv",
                "size_bytes": len(csv_bytes),
                "shape": list(obj.shape)
            })

    def load_input(self, context):
        """Simple CSV download using upstream metadata only"""
        
        logger = get_dagster_logger()

        logger.info(f"Loading input for {context.asset_key} from upstream output")
        
        upstream_metadata = context.upstream_output.metadata
        logger.info(f"Upstream metadata: {upstream_metadata}")

        # Get file info from upstream metadata (stored by handle_output)
        lakef_config = upstream_metadata.get("lakefs_config")
        repo = lakef_config.get("repo", self.default_repo)
        branch = lakef_config.get("branch", self.default_branch)
        file_path = lakef_config.get("path")
        
        if not file_path:
            # Fallback: construct from asset name
            upstream_asset_key = context.upstream_output.asset_key
            file_path = f"{'/'.join(upstream_asset_key.path)}.csv"
        
        context.log.info(f"📥 Loading from upstream metadata: {repo}/{branch}/{file_path}")
        
        try:
            response = self.client.objects.get_object(
                repository=repo,
                ref=branch,
                path=file_path
            )
            
            csv_content = response.read().decode('utf-8')
            context.log.info(f"✅ Downloaded {len(csv_content)} characters")
            
            from io import StringIO
            df = pd.read_csv(StringIO(csv_content))
            context.log.info(f"✅ Loaded DataFrame: {df.shape}")
            
            return df
            
        except Exception as e:
            context.log.error(f"❌ Download failed from {repo}/{branch}/{file_path}: {e}")
            
            # Debug: show what metadata we have
            context.log.info(f"🔍 Available upstream metadata: {list(upstream_metadata.keys())}")
            context.log.info(f"🔍 Upstream metadata values: {upstream_metadata}")
            
            raise

@io_manager(
    required_resource_keys={"lakefs_fs", "lakefs_client"},
    config_schema={
        "default_repo": str,
        "default_branch": str,
    }
)
def dynamic_lakefs_io_manager(context):
    return DynamicLakeFSIOManager(
        fs=context.resources.lakefs_fs,
        client=context.resources.lakefs_client,
        default_repo=context.resource_config.get("default_repo", "spotify-repo"),
        default_branch=context.resource_config.get("default_branch", "main")
    )