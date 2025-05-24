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
            commit_message=final_config.get('commit_message'),
            auto_commit=final_config.get('auto_commit', False),
            metadata=final_config.get('metadata', {})
        )

    def handle_output(self, context, obj):
        """Simple CSV upload using lakefs_client with IOBase"""
        
        if isinstance(obj, pd.DataFrame):
            config = self._get_config_from_context(context)
            file_path = f"{'/'.join(context.asset_key.path)}.csv" 
            
            context.log.info(f"📤 Uploading CSV: {obj.shape} to {config.repo}/{config.branch}/{file_path}")
            
            try:
                # Step 1: Convert DataFrame to CSV and create file-like object
                csv_content = obj.to_csv(index=False)
                csv_bytes = csv_content.encode('utf-8')
                
                # Step 2: Create IOBase object (file-like)
                csv_file = io.BytesIO(csv_bytes)
                
                context.log.info(f"📊 CSV size: {len(csv_bytes)} bytes")
                
                # Step 3: Upload using lakefs_client with IOBase
                self.client.objects.upload_object(
                    repository=config.repo,
                    branch=config.branch,
                    path=file_path,
                    content=csv_file  # Pass IOBase object, not bytes
                )
                
                context.log.info(f"✅ Successfully uploaded to LakeFS!")
                
                # Step 4: Simple metadata
                context.add_output_metadata({
                    "repo": config.repo,
                    "branch": config.branch,
                    "path": file_path,
                    "file_format": "csv",
                    "size_bytes": len(csv_bytes),
                    "shape": str(obj.shape)
                })
                
            except Exception as e:
                context.log.error(f"❌ Upload failed: {e}")
                import traceback
                context.log.error(f"Traceback: {traceback.format_exc()}")
                raise


    def load_input(self, context):
        """Simple hardcoded test to check if lakefs_client works"""
        
        # Hardcode the values for testing
        repo = "spotify-repo"
        branch = "raw-data"  # Try raw-data first
        file_path = "versioned_spotify_data_dev.csv"
        
        context.log.info(f"🔍 Testing download: {repo}/{branch}/{file_path}")
        
        try:
            # First, list what files actually exist
            context.log.info("📋 Listing all files in repository...")
            objects = self.client.objects.list_objects(
                repository=repo,
                ref=branch
            )
            
            available_files = [obj.path for obj in objects.results]
            context.log.info(f"📁 Available files: {available_files}")
            
            # Try to find any CSV file
            csv_files = [f for f in available_files if f.endswith('.csv')]
            context.log.info(f"📄 CSV files found: {csv_files}")
            
            if csv_files:
                # Use the first CSV file found
                actual_file = csv_files[0]
                context.log.info(f"📥 Trying to download: {actual_file}")
                
                response = self.client.objects.get_object(
                    repository=repo,
                    ref=branch,
                    path=actual_file
                )
                
                csv_content = response.read().decode('utf-8')
                context.log.info(f"✅ Downloaded {len(csv_content)} characters")
                
                # Convert to DataFrame
                from io import StringIO
                df = pd.read_csv(StringIO(csv_content))
                context.log.info(f"✅ Loaded DataFrame: {df.shape}")
                
                return df
            else:
                context.log.error("❌ No CSV files found!")
                raise FileNotFoundError("No CSV files in repository")
                
        except Exception as e:
            context.log.error(f"❌ Error: {e}")
            
            # Try different branches
            for test_branch in ["main", "development", "raw-data"]:
                try:
                    context.log.info(f"🔍 Trying branch: {test_branch}")
                    objects = self.client.objects.list_objects(
                        repository=repo,
                        ref=test_branch
                    )
                    files = [obj.path for obj in objects.results]
                    context.log.info(f"Files in {test_branch}: {files[:5]}...")
                except:
                    context.log.info(f"Branch {test_branch} not accessible")
            
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