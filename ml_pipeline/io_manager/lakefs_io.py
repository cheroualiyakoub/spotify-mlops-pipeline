from dagster import IOManager, io_manager
from lakefs_spec import LakeFSFileSystem

import ml_pipeline.resources.lakefs_client_resource as lakefs_client_resource
import ml_pipeline.resources.lakefs_spec_resource as lakefs_spec_resource
import os
import pandas as pd
import numpy as np

class LakeFSIOManager(IOManager):
    def __init__(self, fs, client, repo: str, branch: str = "main"):
        self.fs = fs  # Use the provided filesystem
        self.client = client  # Use the provided client
        self.repo = repo
        self.branch = branch

    def _get_path(self, context):
        return f"{self.repo}/{self.branch}/{'/'.join(context.asset_key.path)}.parquet"

    def handle_output(self, context, obj):
        lakefs_path = self._get_path(context)
        
        # Enhanced metadata collection
        metadata = {
            "lakefs_path": f"lakefs://{lakefs_path}",
            "file_format": "parquet",
        }
        
        if isinstance(obj, pd.DataFrame):
            # Add DataFrame-specific metadata
            metadata.update({
                "row_count": int(len(obj)), 
                "column_count": int(len(obj.columns)), 
                "size_mb": float(obj.memory_usage(deep=True).sum() / (1024 * 1024))
            })
            
            # Write with better Parquet settings
            with self.fs.open(f"lakefs://{lakefs_path}", "wb") as f:
                obj.to_parquet(
                    f,
                    engine='pyarrow',
                    compression='snappy',
                    index=False
                )
        else:
            with self.fs.open(f"lakefs://{lakefs_path}", "wb") as f:
                f.write(obj)

        # Add commit information
        try:
            branch = self.client.branches.get_branch(
                repository=self.repo,
                branch=self.branch
            )
            metadata["commit_id"] = branch.commit_id
            metadata["commit_url"] = f"{self.client.config.host_url}/repositories/{self.repo}/commits/{branch.commit_id}"
        except Exception as e:
            context.log.warning(f"Couldn't get commit info: {str(e)}")
        
        context.add_output_metadata(metadata)

    def load_input(self, context):
        upstream_path = context.upstream_output.metadata["lakefs_path"]
        
        # Enhanced read with error handling
        try:
            with self.fs.open(upstream_path, "rb") as f:
                if upstream_path.endswith(".parquet"):
                    return pd.read_parquet(f)
                elif upstream_path.endswith(".csv"):
                    return pd.read_csv(f)
                return f.read()
        except Exception as e:
            context.log.error(f"Failed to read {upstream_path}: {str(e)}")
            raise

@io_manager(required_resource_keys={"lakefs_fs", "lakefs_client"})
def lakefs_io_manager(context):
    return LakeFSIOManager(
        fs=context.resources.lakefs_fs,
        client=context.resources.lakefs_client,
        repo="spotify-repo",
        branch="main"  # or your default branch
    )